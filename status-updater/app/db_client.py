"""PostgreSQL client for direct database updates."""
import logging
from typing import List, Optional
from datetime import datetime

import asyncpg

from .config import config
from .models import StatusUpdate

logger = logging.getLogger("status-updater")

class DatabaseClient:
    """
    PostgreSQL connection pool manager for high-throughput status updates.
    
    Features:
    - Connection pooling with configurable size
    - Batch update support
    - Transaction management
    - Error handling and retries
    """
    
    def __init__(self):
        self.pool: Optional[asyncpg.Pool] = None
        self.stats = {
            "updates": 0,
            "batches": 0,
            "errors": 0
        }
        
    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(
            host=config.POSTGRES_HOST,
            port=config.POSTGRES_PORT,
            database=config.POSTGRES_DB,
            user=config.POSTGRES_USER,
            password=config.POSTGRES_PASSWORD,
            min_size=5,
            max_size=config.MAX_DB_CONNECTIONS,
            command_timeout=5.0,
            max_queries=50000,
            max_inactive_connection_lifetime=300
        )
        logger.info(f"PostgreSQL pool created with {config.MAX_DB_CONNECTIONS} connections")
        
        # Test connection
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT 1")
            logger.info("Database connection test passed")
    
    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")
    
    @property
    def is_connected(self) -> bool:
        """Check if pool is connected."""
        return self.pool is not None and not self.pool._closed
    
    async def batch_update_statuses(self, updates: List[StatusUpdate]) -> int:
        """
        Batch update message statuses in database.
        
        Args:
            updates: List of status updates to apply
            
        Returns:
            Number of successful updates
        """
        if not updates:
            return 0
            
        if not self.pool:
            logger.error("Database not connected")
            return 0
        
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # This query assumes Django's message model table name
                    # Adjust table and column names based on your Django models
                    query = """
                        UPDATE campaigns_messagestatus
                        SET 
                            status = COALESCE($2, status),
                            provider_message_id = COALESCE($3, provider_message_id),
                            provider_response = COALESCE($4, provider_response),
                            attempts = COALESCE($5, attempts),
                            delivered_at = COALESCE($6, delivered_at),
                            updated_at = NOW()
                        WHERE message_id = $1
                    """
                    
                    success_count = 0
                    for update in updates:
                        try:
                            result = await conn.execute(
                                query,
                                update.message_id,
                                update.status,
                                update.provider_message_id,
                                update.provider_response or update.error,
                                update.attempts,
                                update.delivered_at
                            )
                            
                            # Check if row was updated (result format: "UPDATE X")
                            if result and result.startswith("UPDATE") and int(result.split()[1]) > 0:
                                success_count += 1
                                self.stats["updates"] += 1
                            else:
                                logger.debug(f"No row found for message {update.message_id}")
                                
                        except Exception as e:
                            logger.error(f"Failed to update {update.message_id}: {e}")
                            self.stats["errors"] += 1
                    
                    self.stats["batches"] += 1
                    logger.debug(f"Batch update: {success_count}/{len(updates)} successful")
                    return success_count
                    
                except Exception as e:
                    logger.error(f"Batch update failed: {e}")
                    self.stats["errors"] += len(updates)
                    return 0
    
    async def get_message_status(self, message_id: str) -> Optional[dict]:
        """Get current status of a message (for debugging)."""
        if not self.pool:
            return None
            
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM campaigns_messagestatus WHERE message_id = $1",
                message_id
            )
            return dict(row) if row else None