"""Redis client for queue management."""
import json
import logging
from typing import List, Dict, Any, Optional

import redis.asyncio as redis

from .config import config

logger = logging.getLogger("campaign-executor")

class RedisQueueClient:
    """Redis client for managing dispatch queue."""
    
    def __init__(self):
        self.url = config.REDIS_URL
        self.queue_key = config.REDIS_QUEUE_KEY
        self.max_queue_size = config.MAX_QUEUE_SIZE
        self.client: Optional[redis.Redis] = None
    
    async def connect(self):
        """Connect to Redis."""
        self.client = await redis.from_url(self.url, decode_responses=True)
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
    
    async def get_queue_size(self) -> int:
        """Get current queue size."""
        if not self.client:
            return 0
        return await self.client.llen(self.queue_key)
    
    async def push_messages(self, messages: List[Dict[str, Any]]):
        """
        Push multiple messages to Redis queue using pipeline.
        
        Args:
            messages: List of message dictionaries to push
        """
        if not self.client:
            raise Exception("Redis not connected")
        
        async with self.client.pipeline() as pipe:
            for msg in messages:
                await pipe.rpush(self.queue_key, json.dumps(msg))
            await pipe.execute()
        
        logger.info(f"Pushed {len(messages)} messages to Redis")
    
    async def check_backpressure(self) -> bool:
        """
        Check if queue is under backpressure.
        
        Returns:
            True if queue size exceeds max_queue_size
        """
        size = await self.get_queue_size()
        under_pressure = size > self.max_queue_size
        if under_pressure:
            logger.warning(f"Backpressure: queue size {size} > {self.max_queue_size}")
        return under_pressure