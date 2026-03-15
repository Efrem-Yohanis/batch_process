"""
Layer 4: Status Updater Service
Consumes Kafka events from Layer 3 and updates Django/PostgreSQL
Handles high-throughput status updates with batching and connection pooling
"""

import os
import json
import asyncio
import logging
import signal
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from logging.handlers import TimedRotatingFileHandler
from collections import defaultdict

import aiokafka
import asyncpg
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# =============== DOCKER-SPECIFIC DEFAULTS ===============
DOCKER_DEFAULTS = {
    # Kafka
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_SEND_STATUS_TOPIC": "sms-send-status",
    "KAFKA_DELIVERY_TOPIC": "sms-delivery-status",
    "KAFKA_CONSUMER_GROUP": "layer4-status-updater",
    
    # PostgreSQL (Django DB)
    "POSTGRES_HOST": "postgres",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "django_db",
    "POSTGRES_USER": "django_user",
    "POSTGRES_PASSWORD": "django_password",
    
    # Django API (fallback for complex updates)
    "DJANGO_URL": "http://django-app:8000",
    "DJANGO_USERNAME": "test",
    "DJANGO_PASSWORD": "test",
    
    # Processing
    "BATCH_SIZE": "1000",           # Messages to batch before DB update
    "BATCH_TIMEOUT_MS": "100",      # Max wait to form a batch
    "WORKER_COUNT": "4",             # Parallel consumers
    "MAX_DB_CONNECTIONS": "20",      # Connection pool size
    
    # Logging
    "LOG_LEVEL": "INFO",
    "LOG_DIR": "/app/logs/layer4",
    "LOG_RETENTION_DAYS": "7"
}

for key, value in DOCKER_DEFAULTS.items():
    if key not in os.environ:
        os.environ[key] = value

# =============== LOGGING CONFIGURATION ===============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs/layer4")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))

os.makedirs(LOG_DIR, exist_ok=True)

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

# Configure logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("layer4")

# Add file handler
file_handler = DailyRotatingFileHandler(
    log_dir=LOG_DIR,
    backupCount=LOG_RETENTION_DAYS
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
logger.addHandler(file_handler)

# =============== CONFIGURATION ===============
@dataclass
class Config:
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    KAFKA_SEND_STATUS_TOPIC: str = os.getenv("KAFKA_SEND_STATUS_TOPIC", "sms-send-status")
    KAFKA_DELIVERY_TOPIC: str = os.getenv("KAFKA_DELIVERY_TOPIC", "sms-delivery-status")
    KAFKA_CONSUMER_GROUP: str = os.getenv("KAFKA_CONSUMER_GROUP", "layer4-status-updater")
    
    # PostgreSQL
    POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "postgres")
    POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
    POSTGRES_DB: str = os.getenv("POSTGRES_DB", "django_db")
    POSTGRES_USER: str = os.getenv("POSTGRES_USER", "django_user")
    POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "django_password")
    
    # Django API (fallback)
    DJANGO_URL: str = os.getenv("DJANGO_URL", "http://django-app:8000")
    DJANGO_USERNAME: str = os.getenv("DJANGO_USERNAME", "test")
    DJANGO_PASSWORD: str = os.getenv("DJANGO_PASSWORD", "test")
    
    # Processing
    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "1000"))
    BATCH_TIMEOUT_MS: int = int(os.getenv("BATCH_TIMEOUT_MS", "100"))
    WORKER_COUNT: int = int(os.getenv("WORKER_COUNT", "4"))
    MAX_DB_CONNECTIONS: int = int(os.getenv("MAX_DB_CONNECTIONS", "20"))

config = Config()

# =============== DATA MODELS ===============
class SendStatusEvent(BaseModel):
    """Send status event from Layer 3"""
    event_type: str
    message_id: str
    campaign_id: int
    msisdn: str
    status: str  # SENT or FAILED
    provider_message_id: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    timestamp: str

class DeliveryStatusEvent(BaseModel):
    """Delivery status event from webhook"""
    event_type: str
    message_id: str
    provider_message_id: str
    status: str  # DELIVERED, FAILED, etc.
    error: Optional[str] = None
    timestamp: str

class StatusUpdate(BaseModel):
    """Unified status update for database"""
    message_id: str
    status: str
    provider_message_id: Optional[str] = None
    provider_response: Optional[str] = None
    attempts: Optional[int] = None
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None

# =============== DATABASE CONNECTION POOL ===============
class DatabasePool:
    """PostgreSQL connection pool manager"""
    
    def __init__(self):
        self.pool = None
        self.stats = {
            "updates": 0,
            "batches": 0,
            "errors": 0
        }
        
    async def connect(self):
        """Create connection pool"""
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
        logger.info(f"[OK] PostgreSQL pool created with {config.MAX_DB_CONNECTIONS} connections")
        
        # Test connection
        async with self.pool.acquire() as conn:
            await conn.execute("SELECT 1")
            logger.info("[OK] Database connection test passed")
    
    async def batch_update_statuses(self, updates: List[StatusUpdate]) -> int:
        """
        Batch update message statuses in database
        Returns number of successful updates
        """
        if not updates:
            return 0
            
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    # Prepare batch update
                    # This assumes Django's message model table name - adjust as needed
                    query = """
                        UPDATE campaigns_messagestatus
                        SET 
                            status = data.status,
                            provider_message_id = COALESCE(data.provider_message_id, provider_message_id),
                            provider_response = COALESCE(data.provider_response, provider_response),
                            attempts = CASE 
                                WHEN data.attempts IS NOT NULL THEN data.attempts
                                ELSE attempts
                            END,
                            delivered_at = COALESCE(data.delivered_at, delivered_at),
                            updated_at = NOW()
                        FROM (VALUES ($1, $2, $3, $4, $5, $6)) AS data(
                            message_id, status, provider_message_id, 
                            provider_response, attempts, delivered_at
                        )
                        WHERE campaigns_messagestatus.message_id = data.message_id
                    """
                    
                    # Execute for each update
                    success_count = 0
                    for update in updates:
                        try:
                            result = await conn.execute(
                                query,
                                update.message_id,
                                update.status,
                                update.provider_message_id,
                                update.provider_response,
                                update.attempts,
                                update.delivered_at
                            )
                            # Check if row was updated
                            if result != "UPDATE 0":
                                success_count += 1
                                self.stats["updates"] += 1
                        except Exception as e:
                            logger.error(f"[ERROR] Failed to update {update.message_id}: {e}")
                            self.stats["errors"] += 1
                    
                    self.stats["batches"] += 1
                    return success_count
                    
                except Exception as e:
                    logger.error(f"[ERROR] Batch update failed: {e}")
                    self.stats["errors"] += len(updates)
                    return 0
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("[OK] Database pool closed")

# =============== TOKEN MANAGER (for Django API fallback) ===============
class TokenManager:
    """Manages JWT tokens for Django API authentication (fallback only)"""
    
    def __init__(self):
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        self.client = httpx.AsyncClient(timeout=10.0)
        
    async def get_valid_token(self) -> str:
        async with self.lock:
            current_time = time.time()
            if not self.access_token or current_time >= self.token_expiry - 60:
                await self._refresh_token()
            return self.access_token
    
    async def _refresh_token(self):
        try:
            response = await self.client.post(
                f"{config.DJANGO_URL}/api/token/",
                json={"username": config.DJANGO_USERNAME, "password": config.DJANGO_PASSWORD}
            )
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['access']
                self.refresh_token = data.get('refresh')
                self.token_expiry = time.time() + 280
                logger.info("[OK] Django token acquired (fallback)")
            else:
                logger.error(f"[ERROR] Token acquisition failed: {response.status_code}")
        except Exception as e:
            logger.error(f"[ERROR] Token refresh error: {e}")
    
    async def close(self):
        await self.client.aclose()

# =============== DJANGO API CLIENT (FALLBACK) ===============
class DjangoApiClient:
    """Fallback client for Django API when direct DB updates fail"""
    
    def __init__(self, token_manager: TokenManager):
        self.token_manager = token_manager
        self.client = httpx.AsyncClient(timeout=5.0)
        self.stats = {"fallback_calls": 0, "fallback_errors": 0}
        
    async def update_status(self, update: StatusUpdate) -> bool:
        """Update status via Django API (fallback method)"""
        try:
            token = await self.token_manager.get_valid_token()
            
            payload = {
                "status": update.status,
                "provider_message_id": update.provider_message_id,
                "provider_response": update.provider_response or update.error,
                "attempts": update.attempts
            }
            
            response = await self.client.patch(
                f"{config.DJANGO_URL}/api/message-statuses/{update.message_id}/",
                headers={"Authorization": f"Bearer {token}"},
                json=payload
            )
            
            if response.status_code == 200:
                self.stats["fallback_calls"] += 1
                return True
            else:
                logger.error(f"[ERROR] Fallback update failed: {response.status_code}")
                self.stats["fallback_errors"] += 1
                return False
                
        except Exception as e:
            logger.error(f"[ERROR] Fallback update error: {e}")
            self.stats["fallback_errors"] += 1
            return False
    
    async def close(self):
        await self.client.aclose()

# =============== KAFKA CONSUMER ===============
class KafkaConsumer:
    """Consumes status events from Kafka and batches them for DB updates"""
    
    def __init__(self, db_pool: DatabasePool, django_client: DjangoApiClient):
        self.db_pool = db_pool
        self.django_client = django_client
        self.consumer = None
        self.running = True
        self.stats = {
            "messages_received": 0,
            "send_events": 0,
            "delivery_events": 0,
            "batches_processed": 0,
            "batch_errors": 0,
            "kafka_lag": 0
        }
        
        # Batching queues
        self.update_queue = asyncio.Queue()
        self.batch_task = None
        
    async def start(self):
        """Start Kafka consumer and batch processor"""
        # Create consumer
        self.consumer = aiokafka.AIOKafkaConsumer(
            config.KAFKA_SEND_STATUS_TOPIC,
            config.KAFKA_DELIVERY_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode()),
            enable_auto_commit=False,  # Manual commit for exactly-once processing
            auto_offset_reset='earliest',
            max_poll_records=500,  # Don't pull too many at once
            max_poll_interval_ms=300000,  # 5 minutes
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
        
        await self.consumer.start()
        logger.info(f"[OK] Kafka consumer started for topics: {config.KAFKA_SEND_STATUS_TOPIC}, {config.KAFKA_DELIVERY_TOPIC}")
        
        # Get initial partition info
        for topic in [config.KAFKA_SEND_STATUS_TOPIC, config.KAFKA_DELIVERY_TOPIC]:
            partitions = await self.consumer.partitions_for_topic(topic)
            if partitions:
                logger.info(f"[INFO] Topic {topic} has {len(partitions)} partitions")
        
        # Start batch processor
        self.batch_task = asyncio.create_task(self._batch_processor())
        
        # Start consumer loop
        asyncio.create_task(self._consume_loop())
        
    async def _consume_loop(self):
        """Main consume loop"""
        logger.info("[START] Kafka consumer loop started")
        
        while self.running:
            try:
                # Get batch of messages
                batch = await self.consumer.getmany(timeout_ms=1000, max_records=500)
                
                if not batch:
                    await asyncio.sleep(0.1)
                    continue
                
                # Process each partition's messages
                for tp, messages in batch.items():
                    for msg in messages:
                        await self._process_message(msg)
                    
                    # Commit offset for this partition after successful processing
                    await self.consumer.commit({tp: messages[-1].offset + 1})
                    
                    # Update lag stats
                    if hasattr(self.consumer, 'end_offsets'):
                        end_offsets = await self.consumer.end_offsets([tp])
                        if tp in end_offsets:
                            lag = end_offsets[tp] - (messages[-1].offset + 1)
                            self.stats["kafka_lag"] = max(self.stats["kafka_lag"], lag)
                    
            except Exception as e:
                logger.error(f"[ERROR] Consumer loop error: {e}")
                await asyncio.sleep(1)
    
    async def _process_message(self, msg):
        """Process individual Kafka message"""
        try:
            self.stats["messages_received"] += 1
            value = msg.value
            
            # Parse based on event type
            event_type = value.get("event_type")
            
            if event_type == "SEND_STATUS":
                self.stats["send_events"] += 1
                event = SendStatusEvent(**value)
                await self._handle_send_status(event)
                
            elif event_type == "DELIVERY_STATUS":
                self.stats["delivery_events"] += 1
                event = DeliveryStatusEvent(**value)
                await self._handle_delivery_status(event)
                
            else:
                logger.warning(f"[WARN] Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"[ERROR] Failed to process message: {e}")
            self.stats["batch_errors"] += 1
    
    async def _handle_send_status(self, event: SendStatusEvent):
        """Convert send status to DB update and queue it"""
        update = StatusUpdate(
            message_id=event.message_id,
            status=event.status,
            provider_message_id=event.provider_message_id,
            provider_response=event.error,
            attempts=event.retry_count + 1  # +1 for this attempt
        )
        
        # Add to batch queue
        await self.update_queue.put(update)
    
    async def _handle_delivery_status(self, event: DeliveryStatusEvent):
        """Convert delivery status to DB update and queue it"""
        update = StatusUpdate(
            message_id=event.message_id,
            status=event.status,
            provider_message_id=event.provider_message_id,
            provider_response=event.error,
            delivered_at=datetime.utcnow() if event.status == "DELIVERED" else None
        )
        
        # Add to batch queue
        await self.update_queue.put(update)
    
    async def _batch_processor(self):
        """
        Process updates in batches for better DB performance
        Batches based on size or timeout
        """
        logger.info("[START] Batch processor started")
        
        while self.running:
            batch = []
            batch_start = time.time()
            
            # Collect batch
            while len(batch) < config.BATCH_SIZE:
                try:
                    # Wait for next update with timeout
                    update = await asyncio.wait_for(
                        self.update_queue.get(), 
                        timeout=config.BATCH_TIMEOUT_MS / 1000
                    )
                    batch.append(update)
                    
                except asyncio.TimeoutError:
                    # Timeout reached, process whatever we have
                    break
                except Exception as e:
                    logger.error(f"[ERROR] Batch collection error: {e}")
                    break
            
            if batch:
                await self._process_batch(batch)
                self.stats["batches_processed"] += 1
    
    async def _process_batch(self, batch: List[StatusUpdate]):
        """Process a batch of updates"""
        try:
            # Try direct DB update first
            success_count = await self.db_pool.batch_update_statuses(batch)
            
            # If some failed, try fallback API for those
            if success_count < len(batch):
                logger.warning(f"[WARN] DB batch only updated {success_count}/{len(batch)}. Trying fallback...")
                
                for update in batch[success_count:]:
                    # Try fallback API
                    success = await self.django_client.update_status(update)
                    if not success:
                        logger.error(f"[ERROR] Fallback also failed for {update.message_id}")
                        
        except Exception as e:
            logger.error(f"[ERROR] Batch processing failed: {e}")
            self.stats["batch_errors"] += 1
    
    async def get_lag(self) -> Dict[str, int]:
        """Get current lag for all partitions"""
        lag_info = {}
        if self.consumer:
            try:
                partitions = self.consumer.assignment()
                if partitions:
                    end_offsets = await self.consumer.end_offsets(partitions)
                    for tp in partitions:
                        position = await self.consumer.position(tp)
                        lag = end_offsets.get(tp, 0) - position
                        lag_info[f"{tp.topic}-{tp.partition}"] = lag
            except Exception as e:
                logger.error(f"[ERROR] Failed to get lag: {e}")
        return lag_info
    
    async def stop(self):
        """Stop consumer"""
        self.running = False
        if self.batch_task:
            self.batch_task.cancel()
        if self.consumer:
            await self.consumer.stop()
            logger.info("[OK] Kafka consumer stopped")

# =============== MAIN SERVICE ===============
class StatusUpdater:
    """Main Layer 4 service orchestrator"""
    
    def __init__(self):
        self.db_pool = DatabasePool()
        self.token_manager = TokenManager()
        self.django_client = DjangoApiClient(self.token_manager)
        self.consumer = KafkaConsumer(self.db_pool, self.django_client)
        self.running = True
        self.start_time = time.time()
        
    async def start(self):
        """Start all components"""
        logger.info("=" * 60)
        logger.info("[START] Layer 4 Status Updater Service")
        logger.info(f"[CONFIG] Configuration:")
        logger.info(f"   - Kafka: {config.KAFKA_BOOTSTRAP_SERVERS}")
        logger.info(f"   - Send Topic: {config.KAFKA_SEND_STATUS_TOPIC}")
        logger.info(f"   - Delivery Topic: {config.KAFKA_DELIVERY_TOPIC}")
        logger.info(f"   - Consumer Group: {config.KAFKA_CONSUMER_GROUP}")
        logger.info(f"   - PostgreSQL: {config.POSTGRES_HOST}:{config.POSTGRES_PORT}/{config.POSTGRES_DB}")
        logger.info(f"   - Batch Size: {config.BATCH_SIZE}")
        logger.info(f"   - Batch Timeout: {config.BATCH_TIMEOUT_MS}ms")
        logger.info(f"   - Workers: {config.WORKER_COUNT}")
        logger.info("=" * 60)
        
        # Connect to database
        await self.db_pool.connect()
        
        # Start Kafka consumer
        await self.consumer.start()
        
        # Start stats reporter
        asyncio.create_task(self._stats_reporter())
        
        logger.info("[OK] Layer 4 service started")
        
    async def _stats_reporter(self):
        """Report statistics periodically"""
        while self.running:
            await asyncio.sleep(60)  # Every minute
            
            elapsed = time.time() - self.start_time
            rate = self.db_pool.stats["updates"] / elapsed if elapsed > 0 else 0
            
            # Get Kafka lag
            lag = await self.consumer.get_lag()
            total_lag = sum(lag.values()) if lag else 0
            
            logger.info("=" * 60)
            logger.info("[STATS] Layer 4 Status Updater")
            logger.info(f"   Uptime: {timedelta(seconds=int(elapsed))}")
            logger.info(f"   DB Updates: {self.db_pool.stats['updates']} ({rate:.2f}/sec)")
            logger.info(f"   DB Batches: {self.db_pool.stats['batches']}")
            logger.info(f"   DB Errors: {self.db_pool.stats['errors']}")
            logger.info(f"   Kafka Messages: {self.consumer.stats['messages_received']}")
            logger.info(f"     - Send Events: {self.consumer.stats['send_events']}")
            logger.info(f"     - Delivery Events: {self.consumer.stats['delivery_events']}")
            logger.info(f"   Kafka Lag: {total_lag}")
            logger.info(f"   Batches Processed: {self.consumer.stats['batches_processed']}")
            logger.info(f"   Batch Errors: {self.consumer.stats['batch_errors']}")
            logger.info(f"   Fallback Calls: {self.django_client.stats['fallback_calls']}")
            logger.info(f"   Fallback Errors: {self.django_client.stats['fallback_errors']}")
            logger.info("=" * 60)
    
    async def stop(self):
        """Graceful shutdown"""
        logger.info("[SHUTDOWN] Stopping Layer 4...")
        self.running = False
        
        await self.consumer.stop()
        await self.db_pool.close()
        await self.django_client.close()
        await self.token_manager.close()
        
        logger.info("[OK] Layer 4 stopped")

# =============== FASTAPI HEALTH ENDPOINTS ===============
service = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown events"""
    global service
    service = StatusUpdater()
    asyncio.create_task(service.start())
    yield
    await service.stop()

app = FastAPI(title="Layer 4 - Status Updater", lifespan=lifespan)

@app.get("/health")
async def health():
    """Health check endpoint"""
    if not service:
        return {"status": "starting"}
    
    lag = await service.consumer.get_lag()
    total_lag = sum(lag.values()) if lag else 0
    
    return {
        "status": "healthy",
        "service": "Layer 4 Status Updater",
        "uptime": str(timedelta(seconds=int(time.time() - service.start_time))),
        "kafka_lag": total_lag,
        "db_updates": service.db_pool.stats["updates"],
        "db_errors": service.db_pool.stats["errors"],
        "kafka_messages": service.consumer.stats["messages_received"]
    }

@app.get("/stats")
async def stats():
    """Detailed statistics"""
    if not service:
        return {"error": "Service not initialized"}
    
    lag = await service.consumer.get_lag()
    elapsed = time.time() - service.start_time
    rate = service.db_pool.stats["updates"] / elapsed if elapsed > 0 else 0
    
    return {
        "uptime_seconds": round(elapsed),
        "uptime_human": str(timedelta(seconds=int(elapsed))),
        "database": {
            "updates": service.db_pool.stats["updates"],
            "batches": service.db_pool.stats["batches"],
            "errors": service.db_pool.stats["errors"],
            "update_rate": round(rate, 2)
        },
        "kafka": {
            "messages_received": service.consumer.stats["messages_received"],
            "send_events": service.consumer.stats["send_events"],
            "delivery_events": service.consumer.stats["delivery_events"],
            "batches_processed": service.consumer.stats["batches_processed"],
            "batch_errors": service.consumer.stats["batch_errors"],
            "lag_by_partition": lag,
            "total_lag": sum(lag.values())
        },
        "fallback": {
            "calls": service.django_client.stats["fallback_calls"],
            "errors": service.django_client.stats["fallback_errors"]
        }
    }

@app.get("/lag")
async def lag():
    """Get current Kafka lag"""
    if not service:
        return {"error": "Service not initialized"}
    
    lag = await service.consumer.get_lag()
    return {
        "lag_by_partition": lag,
        "total_lag": sum(lag.values())
    }

@app.post("/pause", status_code=200)
async def pause():
    """Pause consumption (maintenance)"""
    if not service:
        return {"error": "Service not initialized"}
    
    service.consumer.running = False
    return {"status": "paused"}

@app.post("/resume", status_code=200)
async def resume():
    """Resume consumption"""
    if not service:
        return {"error": "Service not initialized"}
    
    service.consumer.running = True
    return {"status": "resumed"}

# =============== MAIN ===============
if __name__ == "__main__":
    import uvicorn
    import argparse
    
    parser = argparse.ArgumentParser(description="Layer 4 Status Updater")
    parser.add_argument("--standalone", action="store_true", help="Run as standalone consumer without API")
    args = parser.parse_args()
    
    if args.standalone:
        # Run as standalone consumer (no web server)
        async def run_standalone():
            service = StatusUpdater()
            await service.start()
            
            # Handle shutdown signals
            stop = asyncio.Future()
            for sig in [signal.SIGINT, signal.SIGTERM]:
                asyncio.get_event_loop().add_signal_handler(sig, stop.set_result, None)
            
            await stop
            await service.stop()
        
        asyncio.run(run_standalone())
    else:
        # Run with web server
        uvicorn.run(
            "layer4_updater:app",
            host="0.0.0.0",
            port=8003,
            reload=True,
            log_level="info"
        )