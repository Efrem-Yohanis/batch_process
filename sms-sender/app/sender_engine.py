"""Main SMS sending orchestrator with Kafka-first architecture."""
import asyncio
import time
import logging
from typing import Optional

from .config import config
from .models import SMSMessage, APIResponse
from .rate_limiter import TokenBucketRateLimiter
from .api_client import ThirdPartyAPIClient
from .redis_client import RedisClient
from .kafka_client import KafkaClient
from .workers import WorkerPool

logger = logging.getLogger("sms-sender")

class SMSSender:
    """
    Main SMS sending orchestrator.
    
    Kafka-first architecture:
    - No direct Django updates
    - All statuses published to Kafka
    - Decoupled from campaign management
    """
    
    def __init__(self):
        self.redis_client = RedisClient()
        self.api_client = ThirdPartyAPIClient()
        self.kafka_client = KafkaClient()
        self.rate_limiter = TokenBucketRateLimiter(config.MAX_TPS)
        self.worker_pool: Optional[WorkerPool] = None
        
        self.running = False
        self.stats = {
            "sent": 0,
            "failed": 0,
            "retried": 0,
            "total_processed": 0,
            "start_time": time.time()
        }
        
    async def start(self):
        """Start all clients and begin processing."""
        logger.info("=" * 60)
        logger.info("Starting Layer 3 SMS Sender (Kafka-first architecture)")
        logger.info("=" * 60)
        logger.info("Configuration:")
        for key, value in config.dict().items():
            logger.info(f"  {key}: {value}")
        logger.info("=" * 60)
        logger.info("NOTE: No direct Django updates - using Kafka events only")
        logger.info("=" * 60)
        
        # Connect to Redis
        await self.redis_client.connect()
        
        # Start Kafka producer
        await self.kafka_client.start()
        
        # Create and start worker pool
        self.worker_pool = WorkerPool(
            worker_count=config.WORKER_COUNT,
            redis_client=self.redis_client,
            api_client=self.api_client,
            kafka_client=self.kafka_client,
            rate_limiter=self.rate_limiter,
            stats=self.stats
        )
        
        await self.worker_pool.start()
        self.running = True
        
        # Start stats reporter
        asyncio.create_task(self._stats_reporter())
        
        logger.info("SMSSender started successfully")
        
    async def stop(self):
        """Graceful shutdown."""
        logger.info("Shutting down SMSSender...")
        self.running = False
        
        if self.worker_pool:
            await self.worker_pool.stop()
        
        await self.api_client.close()
        await self.kafka_client.stop()
        await self.redis_client.close()
        
        logger.info("SMSSender stopped")
    
    async def _stats_reporter(self):
        """Report statistics periodically."""
        while self.running:
            await asyncio.sleep(60)  # Report every minute
            
            elapsed = time.time() - self.stats["start_time"]
            tps = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
            
            # Get queue sizes
            queue_size = await self.redis_client.get_queue_size()
            retry_count = await self.redis_client.get_retry_count()
            
            logger.info("=" * 60)
            logger.info("STATISTICS")
            logger.info("=" * 60)
            logger.info(f"  Total processed: {self.stats['total_processed']}")
            logger.info(f"  Sent: {self.stats['sent']}")
            logger.info(f"  Failed: {self.stats['failed']}")
            logger.info(f"  Retried: {self.stats['retried']}")
            logger.info(f"  Queue size: {queue_size}")
            logger.info(f"  Retry queue: {retry_count}")
            logger.info(f"  Average TPS: {tps:.2f}")
            logger.info("=" * 60)
    
    async def get_stats(self) -> dict:
        """Get current statistics."""
        elapsed = time.time() - self.stats["start_time"]
        tps = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
        
        queue_size = await self.redis_client.get_queue_size() if self.redis_client else 0
        retry_count = await self.redis_client.get_retry_count() if self.redis_client else 0
        
        return {
            "total_processed": self.stats["total_processed"],
            "sent": self.stats["sent"],
            "failed": self.stats["failed"],
            "retried": self.stats["retried"],
            "average_tps": round(tps, 2),
            "uptime_seconds": round(elapsed),
            "queue_size": queue_size,
            "retry_queue_size": retry_count,
            "running": self.running
        }