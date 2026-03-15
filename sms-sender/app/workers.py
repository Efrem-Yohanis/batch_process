"""Worker pool implementation for parallel message processing."""
import asyncio
import logging
from typing import List

from .config import config
from .models import SMSMessage, APIResponse
from .redis_client import RedisClient
from .api_client import ThirdPartyAPIClient
from .kafka_client import KafkaClient
from .rate_limiter import TokenBucketRateLimiter

logger = logging.getLogger("sms-sender")

class Worker:
    """Individual worker that processes messages from Redis."""
    
    def __init__(self, worker_id: int, redis_client: RedisClient, 
                 api_client: ThirdPartyAPIClient, kafka_client: KafkaClient,
                 rate_limiter: TokenBucketRateLimiter, stats: dict):
        self.worker_id = worker_id
        self.redis = redis_client
        self.api = api_client
        self.kafka = kafka_client
        self.rate_limiter = rate_limiter
        self.stats = stats
        self.running = False
        self.task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start the worker."""
        self.running = True
        self.task = asyncio.create_task(self._run())
        logger.info(f"Worker {self.worker_id} started")
        
    async def stop(self):
        """Stop the worker."""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info(f"Worker {self.worker_id} stopped")
        
    async def _run(self):
        """Main worker loop."""
        log = logging.LoggerAdapter(logger, {'correlation_id': f"worker-{self.worker_id}"})
        
        while self.running:
            try:
                # Wait for rate limit tokens
                await self.rate_limiter.wait_and_acquire(config.BATCH_SIZE)
                
                # Pop batch from Redis
                messages = await self.redis.pop_batch(config.BATCH_SIZE)
                
                if not messages:
                    # No messages, short sleep
                    await asyncio.sleep(0.1)
                    continue
                
                log.info(f"Processing batch of {len(messages)} messages")
                
                # Send to 3rd party API
                responses = await self.api.send_batch(messages)
                
                # Process results
                await self._process_responses(responses, log)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.error(f"Worker error: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _process_responses(self, responses: List[APIResponse], log):
        """Process API responses and update Kafka/retry queues."""
        for response in responses:
            message = response.message
            
            if response.success:
                # Success - publish SENT status
                self.stats["sent"] += 1
                self.stats["total_processed"] += 1
                
                await self.kafka.publish_send_status(
                    message=message,
                    status="SENT",
                    provider_msg_id=response.provider_message_id
                )
                
                log.debug(f"Message {message.message_id} sent successfully")
                
            elif response.retryable and message.retry_count < config.MAX_RETRIES:
                # Retryable failure - schedule retry
                self.stats["retried"] += 1
                new_retry_count = message.retry_count + 1
                delay = config.RETRY_DELAYS[new_retry_count - 1]
                
                message.retry_count = new_retry_count
                await self.redis.push_for_retry(message, delay)
                
                log.info(f"Message {message.message_id} scheduled for retry {new_retry_count}/{config.MAX_RETRIES} in {delay}s")
                
            else:
                # Permanent failure - publish FAILED status
                self.stats["failed"] += 1
                self.stats["total_processed"] += 1
                
                await self.kafka.publish_send_status(
                    message=message,
                    status="FAILED",
                    error=response.error
                )
                
                log.error(f"Message {message.message_id} permanently failed: {response.error}")

class WorkerPool:
    """Manages a pool of worker tasks."""
    
    def __init__(self, worker_count: int, redis_client: RedisClient,
                 api_client: ThirdPartyAPIClient, kafka_client: KafkaClient,
                 rate_limiter: TokenBucketRateLimiter, stats: dict):
        self.worker_count = worker_count
        self.workers: List[Worker] = []
        
        # Create workers
        for i in range(worker_count):
            worker = Worker(
                worker_id=i,
                redis_client=redis_client,
                api_client=api_client,
                kafka_client=kafka_client,
                rate_limiter=rate_limiter,
                stats=stats
            )
            self.workers.append(worker)
    
    async def start(self):
        """Start all workers."""
        for worker in self.workers:
            await worker.start()
        logger.info(f"Worker pool started with {self.worker_count} workers")
    
    async def stop(self):
        """Stop all workers."""
        for worker in self.workers:
            await worker.stop()
        logger.info("Worker pool stopped")