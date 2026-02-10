"""
Telegram notification system for Polymarket data events.

Polls the DB for:
1. New possible turnarounds (price hit floor then recovered)
2. New confirmed turnarounds (confirmed_winner = true)
3. Data staleness (no new price_snapshots for 15+ minutes)

Runs as a background asyncio task inside the FastAPI lifespan.
"""

import asyncio
import logging
import time

import httpx
import asyncpg

logger = logging.getLogger(__name__)

TELEGRAM_API = "https://api.telegram.org"


class TelegramNotifier:
    """Sends messages via Telegram Bot API using httpx."""

    def __init__(self, bot_token: str, chat_id: str):
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._client = httpx.AsyncClient(timeout=10)

    async def send(self, message: str) -> bool:
        url = f"{TELEGRAM_API}/bot{self.bot_token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": message,
            "parse_mode": "HTML",
        }
        for attempt in range(3):
            try:
                resp = await self._client.post(url, json=payload)
                if resp.status_code == 200:
                    return True
                # Rate limited — back off and retry
                if resp.status_code == 429:
                    retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
                    logger.warning(f"Telegram rate limited, retrying in {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                logger.error(f"Telegram API error {resp.status_code}: {resp.text}")
                return False
            except Exception as e:
                logger.error(f"Telegram send error (attempt {attempt + 1}): {e}")
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
        return False

    async def close(self):
        await self._client.aclose()


class NotificationPoller:
    """Background polling loop for DB events."""

    POLL_INTERVAL = 30  # seconds between checks
    STALENESS_THRESHOLD = 900  # 15 minutes in seconds
    STALENESS_COOLDOWN = 900  # re-alert every 15 min max

    def __init__(self, db_pool: asyncpg.Pool, notifier: TelegramNotifier):
        self.db_pool = db_pool
        self.notifier = notifier
        self.seen_turnaround_ids: set[int] = set()
        self.seen_confirmed_ids: set[int] = set()
        self.last_staleness_alert: float = 0
        self._stale = False  # tracks if we are currently in a stale state

    async def _bootstrap(self):
        """Pre-populate seen sets with current DB state to avoid alert flood on startup."""
        async with self.db_pool.acquire() as conn:
            # Current posible turnarounds
            rows = await conn.fetch(
                "SELECT market_id FROM polymarket.posible_turnarounds"
            )
            self.seen_turnaround_ids = {r["market_id"] for r in rows}
            logger.info(f"Bootstrapped {len(self.seen_turnaround_ids)} posible turnarounds")

            # Current confirmed turnarounds
            rows = await conn.fetch(
                "SELECT id FROM polymarket.confirmed_turnarounds ORDER BY id DESC LIMIT 50"
            )
            self.seen_confirmed_ids = {r["id"] for r in rows}
            logger.info(f"Bootstrapped {len(self.seen_confirmed_ids)} confirmed turnarounds")

    async def check_posible_turnarounds(self):
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT market_id, question, min_up, min_down,
                       last_up_price, last_down_price, ultimo_registro
                FROM polymarket.posible_turnarounds
            """)

        current_ids = {r["market_id"] for r in rows}
        new_ids = current_ids - self.seen_turnaround_ids

        for row in rows:
            if row["market_id"] in new_ids:
                msg = (
                    f"\U0001f504 <b>Posible Turnaround Detectado</b>\n\n"
                    f"{row['question']}\n"
                    f"UP: {row['min_up']} \u2192 {row['last_up_price']} | "
                    f"DOWN: {row['min_down']} \u2192 {row['last_down_price']}"
                )
                await self.notifier.send(msg)

        # Update seen set to current state (handles removals too)
        self.seen_turnaround_ids = current_ids

    async def check_confirmed_turnarounds(self):
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, question, start_time, end_time
                FROM polymarket.confirmed_turnarounds
                ORDER BY id DESC LIMIT 20
            """)

        for row in rows:
            if row["id"] not in self.seen_confirmed_ids:
                self.seen_confirmed_ids.add(row["id"])
                msg = (
                    f"\u2705 <b>Turnaround Confirmado!</b>\n\n"
                    f"{row['question']}\n"
                    f"Market ID: {row['id']}"
                )
                await self.notifier.send(msg)

    async def check_data_staleness(self):
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT now() - max(timestamp) AS age,
                       max(timestamp) AS last_ts
                FROM polymarket.price_snapshots
            """)

        if not row or row["age"] is None:
            return

        age_seconds = row["age"].total_seconds()
        now = time.monotonic()

        if age_seconds > self.STALENESS_THRESHOLD:
            if not self._stale or (now - self.last_staleness_alert > self.STALENESS_COOLDOWN):
                msg = (
                    f"\u26a0\ufe0f <b>Sin datos hace {int(age_seconds // 60)}+ minutos</b>\n\n"
                    f"\u00daltimo snapshot: {row['last_ts'].strftime('%Y-%m-%d %H:%M:%S UTC')}"
                )
                await self.notifier.send(msg)
                self.last_staleness_alert = now
                self._stale = True
        elif self._stale:
            # Data resumed after a stale period
            stale_duration = int((now - self.last_staleness_alert) // 60)
            msg = (
                f"\u2705 <b>Datos restaurados</b>\n\n"
                f"Interrupci\u00f3n: ~{max(stale_duration, 1)} minutos"
            )
            await self.notifier.send(msg)
            self._stale = False

    async def run(self):
        """Main polling loop. Runs until cancelled."""
        logger.info("Notification poller starting...")
        await self._bootstrap()
        logger.info("Notification poller running")

        cycle = 0
        while True:
            try:
                await self.check_posible_turnarounds()
            except Exception:
                logger.exception("Error checking posible turnarounds")

            try:
                await self.check_confirmed_turnarounds()
            except Exception:
                logger.exception("Error checking confirmed turnarounds")

            # Staleness check every other cycle (~60s)
            if cycle % 2 == 0:
                try:
                    await self.check_data_staleness()
                except Exception:
                    logger.exception("Error checking data staleness")

            cycle += 1
            await asyncio.sleep(self.POLL_INTERVAL)
