"""Redis client for queue and retry operations."""
import json
import logging
from typing import List, Optional

import redis.asyncio as redis

from .config import config
from .models import SMSMessage

logger = logging.getLogger("sms-sender")

class RedisClient:
    """
    Handles all Redis operations for queue management.
    
    Features:
    - Batch pop from main queue
    - Delayed retry queue with TTL
    - Pipeline operations for efficiency
    """
    
    def __init__(self):
        self.url = config.REDIS_URL
        self.queue_key = config.REDIS_QUEUE_KEY
        self.retry_key_prefix = config.REDIS_RETRY_KEY
        self.client: Optional[redis.Redis] = None
        
    async def connect(self):
        """Connect to Redis."""
        self.client = await redis.from_url(
            self.url, 
            decode_responses=True,
            health_check_interval=30
        )
        # Test connection
        await self.client.ping()
        logger.info(f"Connected to Redis: {self.url}")
    
    async def close(self):
        """Close Redis connection."""
        if self.client:
            await self.client.close()
            logger.info("Redis connection closed")
    
    @property
    def is_connected(self) -> bool:
        """Check if Redis is connected."""
        return self.client is not None
    
    async def pop_batch(self, batch_size: int) -> List[SMSMessage]:
        """
        Pop a batch of messages from the main Redis queue.
        
        Args:
            batch_size: Maximum number of messages to pop
            
        Returns:
            List of SMSMessage objects
        """
        if not self.client:
            logger.error("Redis not connected")
            return []
        
        try:
            # Use pipeline for efficiency
            pipeline = self.client.pipeline()
            for _ in range(batch_size):
                pipeline.lpop(self.queue_key)
            results = await pipeline.execute()
            
            # Parse results
            messages = []
            for result in results:
                if result:
                    try:
                        data = json.loads(result)
                        messages.append(SMSMessage(**data))
                    except json.JSONDecodeError as e:
                        logger.error(f"Invalid JSON in Redis: {e}")
                    except Exception as e:
                        logger.error(f"Error parsing message: {e}")
            
            if messages:
                logger.debug(f"Popped {len(messages)} messages from Redis")
            
            return messages
            
        except Exception as e:
            logger.error(f"Redis pop error: {e}", exc_info=True)
            return []
    
    async def push_for_retry(self, message: SMSMessage, delay_seconds: int):
        """
        Push message to retry queue with delay.
        
        Uses Redis SETEX to store message with TTL equal to delay.
        A separate process will move these back to main queue when expired.
        
        Args:
            message: Message to retry
            delay_seconds: Seconds to wait before retry
        """
        if not self.client:
            logger.error("Redis not connected")
            return
        
        try:
            # Store in retry queue with TTL
            key = f"{self.retry_key_prefix}:{message.message_id}"
            await self.client.setex(
                key,
                delay_seconds,
                json.dumps(message.model_dump())
            )
            
            logger.debug(f"Message {message.message_id} scheduled for retry in {delay_seconds}s")
            
        except Exception as e:
            logger.error(f"Redis retry push error: {e}", exc_info=True)
    
    async def get_queue_size(self) -> int:
        """Get current size of main queue."""
        if not self.client:
            return 0
        return await self.client.llen(self.queue_key)
    
    async def get_retry_count(self) -> int:
        """Get number of messages currently in retry."""
        if not self.client:
            return 0
        # Count keys matching retry pattern
        cursor = 0
        count = 0
        pattern = f"{self.retry_key_prefix}:*"
        while True:
            cursor, keys = await self.client.scan(cursor, match=pattern, count=100)
            count += len(keys)
            if cursor == 0:
                break
        return count