"""PostgreSQL database client with connection pooling."""
import asyncpg
from typing import List, Dict, Any, Optional
import logging

from .config import config

logger = logging.getLogger("campaign-executor")

class DatabaseClient:
    """PostgreSQL connection pool manager."""
    
    def __init__(self):
        self.dsn = config.POSTGRES_DSN
        self.pool: Optional[asyncpg.Pool] = None
    
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            self.dsn,
            min_size=config.DB_POOL_MIN_SIZE,
            max_size=config.DB_POOL_MAX_SIZE
        )
        logger.info(f"Database pool created (min={config.DB_POOL_MIN_SIZE}, max={config.DB_POOL_MAX_SIZE})")
    
    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    @property
    def is_connected(self) -> bool:
        """Check if pool is connected."""
        return self.pool is not None and not self.pool._closed
    
    async def fetch_recipients(self, campaign_id: int, last_id: int, 
                               limit: int) -> List[Dict[str, Any]]:
        """
        Fetch next batch of recipients using keyset pagination.
        
        Args:
            campaign_id: Campaign to fetch recipients for
            last_id: Last processed recipient ID (0 for start)
            limit: Batch size
            
        Returns:
            List of recipient dictionaries
        """
        if not self.pool:
            raise Exception("Database not connected")
        
        async with self.pool.acquire() as conn:
            query = """
                SELECT 
                    id,
                    msisdn,
                    language,
                    variables
                FROM scheduler_manager_audience
                WHERE campaign_id = $1 
                  AND id > $2
                  AND status = 'PENDING'
                ORDER BY id
                LIMIT $3
            """
            rows = await conn.fetch(query, campaign_id, last_id, limit)
            return [dict(row) for row in rows]
    
    async def update_checkpoint(self, campaign_id: int, last_id: int, 
                                batch_size: int):
        """
        Update campaign checkpoint in database.
        
        Args:
            campaign_id: Campaign being processed
            last_id: Last processed recipient ID
            batch_size: Number of recipients in this batch
        """
        if not self.pool:
            raise Exception("Database not connected")
        
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO campaign_checkpoints (campaign_id, last_processed_id, total_processed, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (campaign_id) 
                DO UPDATE SET 
                    last_processed_id = $2,
                    total_processed = campaign_checkpoints.total_processed + $3,
                    updated_at = NOW()
            """, campaign_id, last_id, batch_size)