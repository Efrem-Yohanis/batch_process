"""
Layer 3: High-Throughput SMS Sender
Updated with Kafka-first architecture - no direct Django calls
"""

import os
import json
import asyncio
import logging
import uuid
import time
from datetime import datetime
from typing import List, Dict, Optional
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from dataclasses import dataclass, field
from contextlib import asynccontextmanager

import aiokafka
import httpx
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# =============== DOCKER-SPECIFIC DEFAULTS ===============
DOCKER_DEFAULTS = {
    "REDIS_URL": "redis://redis:6379/0",
    "REDIS_QUEUE_KEY": "sms:dispatch:queue",
    "REDIS_RETRY_KEY": "sms:retry:queue",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_SEND_STATUS_TOPIC": "sms-send-status",  # New topic for send status
    "KAFKA_DELIVERY_TOPIC": "sms-delivery-status",
    "API_URL": "https://api.your-sms-provider.com/v1/send",  # REPLACE THIS
    "API_KEY": "your-actual-api-key-here",  # REPLACE THIS
    "API_TIMEOUT": "5",
    "LOG_LEVEL": "INFO",
    "LOG_DIR": "/app/logs/layer3",
    "LOG_RETENTION_DAYS": "7",
    "BATCH_SIZE": "1000",
    "MAX_TPS": "4000",
    "WORKER_COUNT": "4",
    "MAX_RETRIES": "3"
}

# Set defaults for any missing environment variables
for key, value in DOCKER_DEFAULTS.items():
    if key not in os.environ:
        os.environ[key] = value

# =============== LOGGING CONFIGURATION ===============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs/layer3")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
LOG_WHEN = os.getenv("LOG_WHEN", "midnight")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(100 * 1024 * 1024)))

os.makedirs(LOG_DIR, exist_ok=True)
print(f"Layer 3 logs will be written to: {LOG_DIR}")

class DailyRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, log_dir, when='midnight', interval=1, backupCount=7, encoding=None):
        self.log_dir = log_dir
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.filename = os.path.join(log_dir, f"{self.current_date}.log")
        super().__init__(filename=self.filename, when=when, interval=interval, 
                         backupCount=backupCount, encoding=encoding, utc=False)
    
    def doRollover(self):
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.baseFilename = os.path.join(self.log_dir, f"{self.current_date}.log")
        super().doRollover()

class CorrelationIDFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = '-'
        return True

# Configure root logger
root_logger = logging.getLogger()
root_logger.handlers.clear()
root_logger.setLevel(getattr(logging, LOG_LEVEL))

console_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
file_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(filename)s:%(lineno)d - %(message)s'
console_formatter = logging.Formatter(console_format)
file_formatter = logging.Formatter(file_format)

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
console_handler.addFilter(CorrelationIDFilter())
root_logger.addHandler(console_handler)

# File handler
file_handler = DailyRotatingFileHandler(
    log_dir=LOG_DIR, when=LOG_WHEN, interval=1,
    backupCount=LOG_RETENTION_DAYS, encoding='utf-8'
)
file_handler.setFormatter(file_formatter)
file_handler.addFilter(CorrelationIDFilter())
root_logger.addHandler(file_handler)

logger = logging.getLogger("layer3")

# =============== CONFIGURATION ===============
@dataclass
class Config:
    # Redis
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis:6379/0")
    REDIS_QUEUE_KEY: str = os.getenv("REDIS_QUEUE_KEY", "sms:dispatch:queue")
    REDIS_RETRY_KEY: str = os.getenv("REDIS_RETRY_KEY", "sms:retry:queue")
    
    # Processing
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    MAX_TPS: int = int(os.getenv("MAX_TPS", "4000"))
    WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", "4"))
    
    # Retry settings
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "3"))
    RETRY_DELAYS: List[int] = field(default_factory=lambda: [60, 300, 900])
    
    # 3rd Party API
    API_URL: str = os.getenv("API_URL", "https://api.your-sms-provider.com/v1/send")
    API_KEY: str = os.getenv("API_KEY", "your-actual-api-key-here")
    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "5"))
    
    # Kafka - Updated with new topics
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_SEND_STATUS_TOPIC: str = os.getenv("KAFKA_SEND_STATUS_TOPIC", "sms-send-status")
    KAFKA_DELIVERY_TOPIC: str = os.getenv("KAFKA_DELIVERY_TOPIC", "sms-delivery-status")
    
    # Django API - No longer needed for Layer 3, but kept for backward compatibility
    # Will be removed in next version
    DJANGO_URL: str = os.getenv("DJANGO_URL", "http://django-app:8000")
    DJANGO_USERNAME: str = os.getenv("DJANGO_USERNAME", "test")
    DJANGO_PASSWORD: str = os.getenv("DJANGO_PASSWORD", "test")

config = Config()

# =============== DATA MODELS ===============
class SMSMessage(BaseModel):
    """Message from Redis queue"""
    message_id: str
    campaign_id: int
    msisdn: str
    text: str
    language: Optional[str] = "en"
    retry_count: int = 0
    timestamp: str
    
    class Config:
        arbitrary_types_allowed = True

class APIResponse:
    """Response from 3rd party API"""
    def __init__(self, message: SMSMessage, success: bool, provider_message_id: str = None,
                 retryable: bool = False, error: str = None, status_code: int = None):
        self.message = message
        self.success = success
        self.provider_message_id = provider_message_id
        self.retryable = retryable
        self.error = error
        self.status_code = status_code

# =============== RATE LIMITER ===============
class TokenBucketRateLimiter:
    """Token bucket algorithm for rate limiting at 4000 TPS"""
    
    def __init__(self, rate: int, capacity: int = None):
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
        
    async def acquire(self, tokens: int = 1) -> float:
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now
            
            if self.tokens >= tokens:
                self.tokens -= tokens
                return 0
            else:
                wait_time = (tokens - self.tokens) / self.rate
                return wait_time

# =============== 3RD PARTY API CLIENT ===============
class ThirdPartyAPIClient:
    """Client for 3rd party SMS API (handles 4000 TPS)"""
    
    def __init__(self):
        self.api_url = config.API_URL
        self.api_key = config.API_KEY
        self.timeout = config.API_TIMEOUT
        self.client = httpx.AsyncClient(timeout=self.timeout, limits=httpx.Limits(max_keepalive_connections=100, max_connections=200))
        
    async def send_batch(self, messages: List[SMSMessage]) -> List[APIResponse]:
        """Send batch of messages in parallel"""
        tasks = [self._send_single(msg) for msg in messages]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        responses = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                responses.append(APIResponse(
                    message=messages[i],
                    success=False,
                    retryable=True,
                    error=str(result),
                    status_code=500
                ))
            else:
                responses.append(result)
        return responses
    
    async def _send_single(self, message: SMSMessage) -> APIResponse:
        """Send single message to API"""
        try:
            payload = {
                "to": message.msisdn,
                "text": message.text,
                "message_id": message.message_id,
                "campaign_id": message.campaign_id
            }
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "X-Message-ID": message.message_id
            }
            
            response = await self.client.post(self.api_url, json=payload, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                return APIResponse(
                    message=message,
                    success=True,
                    provider_message_id=data.get("message_id", data.get("id", str(uuid.uuid4()))),
                    status_code=200
                )
            else:
                # 429 = too many requests, 5xx = server errors - retryable
                retryable = response.status_code in [429, 500, 502, 503, 504]
                return APIResponse(
                    message=message,
                    success=False,
                    retryable=retryable,
                    error=f"HTTP {response.status_code}: {response.text[:100]}",
                    status_code=response.status_code
                )
                
        except httpx.TimeoutException:
            return APIResponse(message=message, success=False, retryable=True, error="timeout", status_code=408)
        except Exception as e:
            return APIResponse(message=message, success=False, retryable=True, error=str(e), status_code=500)
    
    async def close(self):
        await self.client.aclose()

# =============== DJANGO CLIENT - DEPRECATED ===============
# Kept for backward compatibility but will be removed in next version
# Layer 3 no longer directly updates Django
class DjangoClient:
    """DEPRECATED: Use Kafka events instead"""
    
    def __init__(self, token_manager=None):
        self.token_manager = token_manager
        self.client = httpx.AsyncClient(timeout=5.0)
        logger.warning("[DEPRECATED] DjangoClient is deprecated. Use Kafka events instead.")
        
    async def update_status(self, message_id: str, status: str, provider_msg_id: str = None, 
                           provider_response: str = None, attempts: int = 0):
        """DEPRECATED: This method should not be called"""
        logger.error(f"[ERROR] Direct Django update attempted for {message_id}. This should be handled by Kafka consumer.")
        # Do nothing - we want to fail loudly to catch any remaining calls
    
    async def close(self):
        await self.client.aclose()

# =============== KAFKA CLIENT - UPDATED ===============
class KafkaClient:
    """Publishes send status and delivery events to Kafka"""
    
    def __init__(self):
        self.producer = None
        
    async def start(self):
        """Start Kafka producer with optimized batching for high throughput"""
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode(),
            linger_ms=5,  # Small delay to batch messages
            batch_size=32768,  # 32KB batches for better throughput
            compression_type='gzip',  # Compress for better network utilization
            max_batch_size=65536
        )
        await self.producer.start()
        logger.info("[OK] Kafka producer started with optimized settings")
        
    async def publish_send_status(self, message: SMSMessage, status: str, provider_msg_id: str = None, error: str = None):
        """
        Publish send status event to Kafka (SENT or FAILED)
        This replaces direct Django updates
        """
        try:
            event = {
                "event_type": "SEND_STATUS",
                "message_id": message.message_id,
                "campaign_id": message.campaign_id,
                "msisdn": message.msisdn,
                "status": status,  # "SENT" or "FAILED"
                "provider_message_id": provider_msg_id,
                "error": error,
                "retry_count": message.retry_count,
                "timestamp": datetime.utcnow().isoformat()
            }
            await self.producer.send(config.KAFKA_SEND_STATUS_TOPIC, event)
            logger.debug(f"[KAFKA] Published send status {status} for {message.message_id}")
        except Exception as e:
            logger.error(f"[ERROR] Kafka publish error: {e}")
    
    async def publish_delivery_status(self, message_id: str, provider_message_id: str, status: str, error: str = None):
        """
        Publish delivery status event to Kafka (from webhook)
        """
        try:
            event = {
                "event_type": "DELIVERY_STATUS",
                "message_id": message_id,
                "provider_message_id": provider_message_id,
                "status": status,  # "DELIVERED", "FAILED", etc.
                "error": error,
                "timestamp": datetime.utcnow().isoformat()
            }
            await self.producer.send(config.KAFKA_DELIVERY_TOPIC, event)
            logger.debug(f"[KAFKA] Published delivery status {status} for {message_id}")
        except Exception as e:
            logger.error(f"[ERROR] Kafka publish error: {e}")
    
    async def stop(self):
        if self.producer:
            await self.producer.stop()

# =============== REDIS CLIENT ===============
class RedisClient:
    """Handles Redis operations for queue and retries"""
    
    def __init__(self):
        self.client = None
        
    async def connect(self):
        self.client = await redis.from_url(config.REDIS_URL, decode_responses=True)
        
    async def pop_batch(self, batch_size: int) -> List[SMSMessage]:
        """Pop a batch of messages from Redis queue"""
        try:
            pipeline = self.client.pipeline()
            for _ in range(batch_size):
                pipeline.lpop(config.REDIS_QUEUE_KEY)
            results = await pipeline.execute()
            
            messages = []
            for result in results:
                if result:
                    try:
                        data = json.loads(result)
                        messages.append(SMSMessage(**data))
                    except Exception as e:
                        logger.error(f"[ERROR] Invalid message format: {e}")
            return messages
        except Exception as e:
            logger.error(f"[ERROR] Redis pop error: {e}")
            return []
    
    async def push_for_retry(self, message: SMSMessage, delay_seconds: int):
        """Push message back to Redis with delay for retry"""
        try:
            key = f"{config.REDIS_RETRY_KEY}:{message.message_id}"
            await self.client.setex(
                key,
                delay_seconds,
                json.dumps(message.model_dump())
            )
            logger.debug(f"[INFO] Message {message.message_id} scheduled for retry in {delay_seconds}s")
        except Exception as e:
            logger.error(f"[ERROR] Redis retry push error: {e}")
    
    async def close(self):
        if self.client:
            await self.client.close()

# =============== MAIN PROCESSOR - UPDATED ===============
class SMSSender:
    """Main SMS sending orchestrator - Now Kafka-first, no direct Django updates"""
    
    def __init__(self):
        # Django client kept for backward compatibility but not used
        self.django_client = None  # Will be initialized only if needed
        self.kafka_client = KafkaClient()
        self.redis_client = RedisClient()
        self.api_client = ThirdPartyAPIClient()
        self.rate_limiter = TokenBucketRateLimiter(config.MAX_TPS)
        self.running = True
        self.stats = {
            "sent": 0,
            "failed": 0,
            "retried": 0,
            "total_processed": 0,
            "start_time": time.time()
        }
        
    async def start(self):
        """Start all clients and begin processing"""
        logger.info("[START] Starting Layer 3 SMS Sender (Kafka-first architecture)")
        logger.info(f"[CONFIG] Configuration:")
        logger.info(f"   - Redis: {config.REDIS_URL}")
        logger.info(f"   - Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"   - Kafka Send Topic: {config.KAFKA_SEND_STATUS_TOPIC}")
        logger.info(f"   - Kafka Delivery Topic: {config.KAFKA_DELIVERY_TOPIC}")
        logger.info(f"   - API URL: {config.API_URL}")
        logger.info(f"   - Max TPS: {config.MAX_TPS}")
        logger.info(f"   - Batch Size: {config.BATCH_SIZE}")
        logger.info(f"   - Workers: {config.WORKER_COUNT}")
        logger.info("[NOTE] Direct Django updates are disabled - using Kafka events only")
        
        await self.redis_client.connect()
        await self.kafka_client.start()
        
        # Start worker tasks
        workers = []
        for i in range(config.WORKER_COUNT):
            worker = asyncio.create_task(self._worker_loop(i))
            workers.append(worker)
            logger.info(f"[WORKER] Worker {i} started")
        
        # Monitor stats periodically
        asyncio.create_task(self._stats_reporter())
        
        # Wait for workers
        await asyncio.gather(*workers)
        
    async def _worker_loop(self, worker_id: int):
        """Main worker loop - processes batches from Redis"""
        log = logging.LoggerAdapter(logger, {'correlation_id': f"worker-{worker_id}"})
        
        while self.running:
            try:
                # Rate limiting
                wait_time = await self.rate_limiter.acquire(config.BATCH_SIZE)
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                
                # Pop batch from Redis
                messages = await self.redis_client.pop_batch(config.BATCH_SIZE)
                if not messages:
                    await asyncio.sleep(0.1)  # No messages, short sleep
                    continue
                
                log.info(f"[BATCH] Processing batch of {len(messages)} messages")
                
                # Send to 3rd party API
                responses = await self.api_client.send_batch(messages)
                
                # Process results
                for response in responses:
                    await self._process_response(response)
                    
            except Exception as e:
                log.error(f"[ERROR] Worker error: {e}")
                await asyncio.sleep(1)
    
    async def _process_response(self, response: APIResponse):
        """
        Process API response - ONLY publishes to Kafka, no Django updates
        """
        message = response.message
        
        if response.success:
            # Success - publish SEND_STATUS to Kafka
            self.stats["sent"] += 1
            self.stats["total_processed"] += 1
            
            await self.kafka_client.publish_send_status(
                message=message,
                status="SENT",
                provider_msg_id=response.provider_message_id
            )
            
            logger.debug(f"[OK] Message {message.message_id} sent successfully, published to Kafka")
            
        elif response.retryable and message.retry_count < config.MAX_RETRIES:
            # Retryable failure - schedule retry (no Kafka event yet)
            self.stats["retried"] += 1
            new_retry_count = message.retry_count + 1
            delay = config.RETRY_DELAYS[new_retry_count - 1]
            
            message.retry_count = new_retry_count
            await self.redis_client.push_for_retry(message, delay)
            
            log = logging.LoggerAdapter(logger, {'correlation_id': f"retry-{message.message_id}"})
            log.info(f"[RETRY] Message {message.message_id} scheduled for retry {new_retry_count}/{config.MAX_RETRIES} in {delay}s")
            
        else:
            # Permanent failure - publish FAILED status to Kafka
            self.stats["failed"] += 1
            self.stats["total_processed"] += 1
            
            await self.kafka_client.publish_send_status(
                message=message,
                status="FAILED",
                error=response.error
            )
            
            logger.error(f"[ERROR] Message {message.message_id} permanently failed: {response.error}")
    
    async def _stats_reporter(self):
        """Report statistics periodically"""
        while self.running:
            await asyncio.sleep(60)  # Report every minute
            elapsed = time.time() - self.stats["start_time"]
            tps = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
            
            logger.info("=" * 60)
            logger.info(f"[STATS] STATISTICS")
            logger.info(f"   Total processed: {self.stats['total_processed']}")
            logger.info(f"   Sent: {self.stats['sent']}")
            logger.info(f"   Failed: {self.stats['failed']}")
            logger.info(f"   Retried: {self.stats['retried']}")
            logger.info(f"   Average TPS: {tps:.2f}")
            logger.info("=" * 60)
    
    async def stop(self):
        """Graceful shutdown"""
        logger.info("[SHUTDOWN] Shutting down Layer 3...")
        self.running = False
        await self.api_client.close()
        if self.django_client:
            await self.django_client.close()
        await self.kafka_client.stop()
        await self.redis_client.close()

# =============== FASTAPI WEBHOOK ENDPOINTS - UPDATED ===============
# Global sender instance
sender = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global sender
    sender = SMSSender()
    asyncio.create_task(sender.start())
    yield
    await sender.stop()

app = FastAPI(title="Layer 3 - SMS Sender", lifespan=lifespan)

class DeliveryReport(BaseModel):
    """Delivery report from 3rd party API"""
    message_id: str
    status: str  # delivered, failed, etc.
    provider_message_id: str
    timestamp: str
    error: Optional[str] = None

@app.post("/webhook/delivery", status_code=200)
async def delivery_report(report: DeliveryReport):
    """
    Webhook endpoint for async delivery reports from 3rd party API
    Now publishes to Kafka instead of updating Django directly
    """
    logger.info(f"[WEBHOOK] Received delivery report for {report.message_id}: {report.status}")
    
    if sender and sender.kafka_client and sender.kafka_client.producer:
        # Map provider status to our status
        status_map = {
            "delivered": "DELIVERED",
            "failed": "FAILED",
            "pending": "PENDING",
            "sent": "SENT",
            "read": "READ"
        }
        
        our_status = status_map.get(report.status.lower(), report.status.upper())
        
        # Publish to Kafka only - no Django direct update
        await sender.kafka_client.publish_delivery_status(
            message_id=report.message_id,
            provider_message_id=report.provider_message_id,
            status=our_status,
            error=report.error
        )
        
        logger.info(f"[WEBHOOK] Published delivery status to Kafka for {report.message_id}")
    else:
        logger.error(f"[WEBHOOK] Kafka client not available")
    
    # Always return 200 to acknowledge receipt
    return {"status": "received", "message": "Delivery report acknowledged"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    if not sender:
        return {"status": "starting"}
    
    return {
        "status": "healthy",
        "service": "Layer 3 SMS Sender (Kafka-first)",
        "tps_limit": config.MAX_TPS,
        "workers": config.WORKER_COUNT,
        "stats": {
            "sent": sender.stats["sent"],
            "failed": sender.stats["failed"],
            "retried": sender.stats["retried"],
            "total_processed": sender.stats["total_processed"]
        },
        "timestamp": datetime.utcnow().isoformat()
    }

@app.get("/stats")
async def stats():
    """Get current statistics"""
    if not sender:
        return {"error": "Sender not initialized"}
    
    elapsed = time.time() - sender.stats["start_time"]
    tps = sender.stats["total_processed"] / elapsed if elapsed > 0 else 0
    
    return {
        "total_processed": sender.stats["total_processed"],
        "sent": sender.stats["sent"],
        "failed": sender.stats["failed"],
        "retried": sender.stats["retried"],
        "average_tps": round(tps, 2),
        "uptime_seconds": round(elapsed),
        "running": sender.running
    }

# =============== MAIN ===============
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "layer3_sender:app",
        host="0.0.0.0",
        port=8002,
        reload=True,
        log_level="info"
    )