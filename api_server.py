"""
Local API server to receive market data from the bot and store in PostgreSQL.

Run: uvicorn api_server:app --host 0.0.0.0 --port 8000
"""

import os
import logging
from datetime import datetime
from typing import List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pathlib import Path
import asyncpg


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    
    # Look for .env in current directory and parent directories
    env_path = Path('.env')
    if env_path.exists():
        load_dotenv(env_path)
        logger.debug(f"Loaded environment variables from {env_path.absolute()}")
    else:
        # Try to find .env in parent directory
        parent_env = Path(__file__).parent / '.env'
        if parent_env.exists():
            load_dotenv(parent_env)
            logger.debug(f"Loaded environment variables from {parent_env.absolute()}")
except ImportError:
    logger.debug("python-dotenv not installed. Using system environment variables only.")


logging.info(f"DB_HOST: {os.getenv('DB_HOST')}")
logging.info(f"DB_PORT: {os.getenv('DB_PORT')}")
logging.info(f"DB_NAME: {os.getenv('DB_NAME')}")
logging.info(f"DB_USER: {os.getenv('DB_USER')}")
logging.info(f"DB_PASSWORD: {os.getenv('DB_PASSWORD')}")

# Database configuration
DB_SCHEMA = os.getenv("DB_SCHEMA", "public")

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "polymarket"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
    "server_settings": {"search_path": DB_SCHEMA},
}

# Global connection pool
db_pool: Optional[asyncpg.Pool] = None


# Pydantic models
class PriceSnapshot(BaseModel):
    timestamp: datetime
    up_price: float
    down_price: float


class MarketData(BaseModel):
    condition_id: str
    question: str
    start_time: datetime
    end_time: datetime
    up_token_id: str
    down_token_id: str
    winner: Optional[str] = None
    snapshots: List[PriceSnapshot]


class MarketResponse(BaseModel):
    id: int
    condition_id: str
    snapshots_saved: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage database connection pool lifecycle"""
    global db_pool
    
    logger.info("Connecting to PostgreSQL...")
    try:
        db_pool = await asyncpg.create_pool(**DB_CONFIG, min_size=2, max_size=10)
        logger.info("Database connected")
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        logger.info("Server will start but database operations will fail")
    
    yield
    
    if db_pool:
        await db_pool.close()
        logger.info("Database connection closed")


app = FastAPI(
    title="Polymarket Data API",
    description="API to store BTC 15-min market data",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    db_status = "connected" if db_pool else "disconnected"
    return {"status": "ok", "database": db_status}


@app.post("/market", response_model=MarketResponse)
async def save_market(data: MarketData):
    """
    Save market data with price snapshots.
    
    Called by the bot when a market ends.
    """
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Insert or update market
            market_id = await conn.fetchval("""
                INSERT INTO markets (condition_id, question, start_time, end_time, up_token_id, down_token_id, winner)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (condition_id) 
                DO UPDATE SET winner = EXCLUDED.winner
                RETURNING id
            """, 
                data.condition_id,
                data.question,
                data.start_time,
                data.end_time,
                data.up_token_id,
                data.down_token_id,
                data.winner
            )
            
            # Insert price snapshots
            snapshots_saved = 0
            if data.snapshots:
                # Prepare batch insert
                values = [
                    (market_id, s.timestamp, s.up_price, s.down_price)
                    for s in data.snapshots
                ]
                
                await conn.executemany("""
                    INSERT INTO price_snapshots (market_id, timestamp, up_price, down_price)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (market_id, timestamp) DO NOTHING
                """, values)
                
                snapshots_saved = len(data.snapshots)
            
            logger.info(f"Saved market {data.condition_id[:10]}... with {snapshots_saved} snapshots")
            
            return MarketResponse(
                id=market_id,
                condition_id=data.condition_id,
                snapshots_saved=snapshots_saved
            )


@app.get("/markets")
async def list_markets(limit: int = 20):
    """List recent markets"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT m.id, m.condition_id, m.question, m.start_time, m.end_time, m.winner,
                   COUNT(p.id) as snapshot_count
            FROM markets m
            LEFT JOIN price_snapshots p ON m.id = p.market_id
            GROUP BY m.id
            ORDER BY m.end_time DESC
            LIMIT $1
        """, limit)
        
        return [dict(row) for row in rows]


@app.get("/market/{market_id}/prices")
async def get_market_prices(market_id: int):
    """Get all price snapshots for a market"""
    if not db_pool:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    async with db_pool.acquire() as conn:
        # Get market info
        market = await conn.fetchrow("""
            SELECT * FROM markets WHERE id = $1
        """, market_id)
        
        if not market:
            raise HTTPException(status_code=404, detail="Market not found")
        
        # Get snapshots
        rows = await conn.fetch("""
            SELECT timestamp, up_price, down_price
            FROM price_snapshots
            WHERE market_id = $1
            ORDER BY timestamp ASC
        """, market_id)
        
        return {
            "market": dict(market),
            "snapshots": [dict(row) for row in rows]
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

