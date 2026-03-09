"""
Layer 3: High-Throughput SMS Sender
Single file implementation for 4000 TPS with batch processing, rate limiting, and retry logic
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
# These will be used if environment variables are not set
DOCKER_DEFAULTS = {
    "REDIS_URL": "redis://redis:6379/0",
    "REDIS_QUEUE_KEY": "sms:dispatch:queue",
    "REDIS_RETRY_KEY": "sms:retry:queue",
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_DELIVERY_TOPIC": "sms-delivery-status",
    "DJANGO_URL": "http://django-app:8000",
    "DJANGO_USERNAME": "test",
    "DJANGO_PASSWORD": "test",
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
    
    # Django API
    DJANGO_URL: str = os.getenv("DJANGO_URL", "http://django-app:8000")
    DJANGO_USERNAME: str = os.getenv("DJANGO_USERNAME", "test")
    DJANGO_PASSWORD: str = os.getenv("DJANGO_PASSWORD", "test")
    
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_DELIVERY_TOPIC: str = os.getenv("KAFKA_DELIVERY_TOPIC", "sms-delivery-status")

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

# =============== TOKEN MANAGER ===============
class TokenManager:
    """Manages JWT tokens for Django API authentication"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        
    async def get_valid_token(self) -> str:
        async with self.lock:
            current_time = time.time()
            if not self.access_token or current_time >= self.token_expiry - 60:
                await self._refresh_token()
            return self.access_token
    
    async def _refresh_token(self):
        try:
            async with httpx.AsyncClient() as client:
                # Try to get new token
                response = await client.post(
                    f"{config.DJANGO_URL}/api/token/",
                    json={"username": config.DJANGO_USERNAME, "password": config.DJANGO_PASSWORD},
                    timeout=10.0
                )
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['access']
                    self.refresh_token = data.get('refresh')
                    self.token_expiry = time.time() + 280  # 5 min - 20s
                    logger.info("[OK] Django token acquired")
                else:
                    logger.error(f"[ERROR] Token acquisition failed: {response.status_code}")
        except Exception as e:
            logger.error(f"[ERROR] Token refresh error: {e}")

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

# =============== DJANGO CLIENT ===============
class DjangoClient:
    """Client for updating message status in Django"""
    
    def __init__(self, token_manager: TokenManager):
        self.token_manager = token_manager
        self.client = httpx.AsyncClient(timeout=5.0)
        
    async def update_status(self, message_id: str, status: str, provider_msg_id: str = None, 
                           provider_response: str = None, attempts: int = 0):
        """Update message status in Django"""
        try:
            token = await self.token_manager.get_valid_token()
            
            payload = {
                "status": status,
                "attempts": attempts
            }
            if provider_msg_id:
                payload["provider_message_id"] = provider_msg_id
            if provider_response:
                payload["provider_response"] = provider_response
                
            response = await self.client.patch(
                f"{config.DJANGO_URL}/api/message-statuses/{message_id}/",
                headers={"Authorization": f"Bearer {token}"},
                json=payload
            )
            
            if response.status_code == 200:
                logger.debug(f"[OK] Updated {message_id} to {status}")
            else:
                logger.error(f"[ERROR] Failed to update {message_id}: {response.status_code}")
                
        except Exception as e:
            logger.error(f"[ERROR] Django update error: {e}")
    
    async def close(self):
        await self.client.aclose()

# =============== KAFKA CLIENT ===============
class KafkaClient:
    """Publishes delivery events to Kafka"""
    
    def __init__(self):
        self.producer = None
        
    async def start(self):
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode()
        )
        await self.producer.start()
        logger.info("[OK] Kafka producer started")
        
    async def publish_delivery(self, message: SMSMessage, status: str, provider_msg_id: str = None):
        """Publish delivery event to Kafka"""
        try:
            event = {
                "event_type": f"MESSAGE_{status}",
                "message_id": message.message_id,
                "campaign_id": message.campaign_id,
                "msisdn": message.msisdn,
                "status": status,
                "provider_message_id": provider_msg_id,
                "timestamp": datetime.utcnow().isoformat()
            }
            await self.producer.send(config.KAFKA_DELIVERY_TOPIC, event)
            logger.debug(f"[INFO] Published {status} event to Kafka")
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

# =============== MAIN PROCESSOR ===============
class SMSSender:
    """Main SMS sending orchestrator"""
    
    def __init__(self):
        self.token_manager = TokenManager()
        self.api_client = ThirdPartyAPIClient()
        self.django_client = DjangoClient(self.token_manager)
        self.kafka_client = KafkaClient()
        self.redis_client = RedisClient()
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
        logger.info("[START] Starting Layer 3 SMS Sender")
        logger.info(f"[CONFIG] Configuration:")
        logger.info(f"   - Redis: {config.REDIS_URL}")
        logger.info(f"   - Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"   - Django: {config.DJANGO_URL}")
        logger.info(f"   - API URL: {config.API_URL}")
        logger.info(f"   - Max TPS: {config.MAX_TPS}")
        logger.info(f"   - Batch Size: {config.BATCH_SIZE}")
        logger.info(f"   - Workers: {config.WORKER_COUNT}")
        
        await self.redis_client.connect()
        await self.kafka_client.start()
        await self.token_manager.get_valid_token()
        
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
        """Process API response (success, failure, retry)"""
        message = response.message
        
        if response.success:
            # Success - update to SENT
            self.stats["sent"] += 1
            self.stats["total_processed"] += 1
            
            await self.django_client.update_status(
                message_id=message.message_id,
                status="SENT",
                provider_msg_id=response.provider_message_id,
                provider_response="Message accepted by provider",
                attempts=message.retry_count + 1
            )
            
            await self.kafka_client.publish_delivery(
                message, "SENT", response.provider_message_id
            )
            
        elif response.retryable and message.retry_count < config.MAX_RETRIES:
            # Retryable failure - schedule retry
            self.stats["retried"] += 1
            new_retry_count = message.retry_count + 1
            delay = config.RETRY_DELAYS[new_retry_count - 1]
            
            message.retry_count = new_retry_count
            await self.redis_client.push_for_retry(message, delay)
            
            log = logging.LoggerAdapter(logger, {'correlation_id': f"retry-{message.message_id}"})
            log.info(f"[RETRY] Message {message.message_id} scheduled for retry {new_retry_count}/{config.MAX_RETRIES} in {delay}s")
            
        else:
            # Permanent failure
            self.stats["failed"] += 1
            self.stats["total_processed"] += 1
            
            await self.django_client.update_status(
                message_id=message.message_id,
                status="FAILED",
                provider_response=response.error,
                attempts=message.retry_count + 1
            )
            
            await self.kafka_client.publish_delivery(message, "FAILED")
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
        await self.django_client.close()
        await self.kafka_client.stop()
        await self.redis_client.close()

# =============== FASTAPI WEBHOOK ENDPOINTS ===============
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
    Updates message status to DELIVERED
    """
    logger.info(f"[WEBHOOK] Received delivery report for {report.message_id}: {report.status}")
    
    if sender and report.status.lower() == "delivered":
        await sender.django_client.update_status(
            message_id=report.message_id,
            status="DELIVERED",
            provider_msg_id=report.provider_message_id
        )
        
        # Also publish to Kafka
        if sender.kafka_client and sender.kafka_client.producer:
            await sender.kafka_client.producer.send(
                config.KAFKA_DELIVERY_TOPIC,
                {
                    "event_type": "MESSAGE_DELIVERED",
                    "message_id": report.message_id,
                    "provider_message_id": report.provider_message_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
    
    return {"status": "received"}

@app.get("/health")
async def health():
    """Health check endpoint"""
    if not sender:
        return {"status": "starting"}
    
    return {
        "status": "healthy",
        "service": "Layer 3 SMS Sender",
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