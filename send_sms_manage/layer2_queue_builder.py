"""
Layer 2: Campaign Execution Engine
Consumes commands from Kafka, fetches campaign metadata from Django API,
streams recipients directly from PostgreSQL, builds messages, and pushes to Redis.

Step-by-step flow:
1. Listen to Kafka for commands (START, STOP, PAUSE, RESUME)
2. Fetch campaign metadata from Django API (templates, counts, checkpoint)
3. Stream recipients in batches using keyset pagination (direct DB)
4. Build messages with language matching & placeholder replacement
5. Push batches to Redis using pipeline
6. Update checkpoint after each batch
7. Publish progress to Kafka
8. Handle backpressure, stop signals, and errors
"""

import os
import json
import uuid
import asyncio
import logging
import threading
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import httpx
import asyncpg
import redis.asyncio as redis
from datetime import datetime
from typing import Dict, Optional, List
from contextlib import asynccontextmanager

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
import uvicorn

# =============== DOCKER-SPECIFIC DEFAULTS ===============
DOCKER_DEFAULTS = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "CAMPAIGN_MANAGER_URL": "http://django-app:8000",
    "DJANGO_USERNAME": "test",
    "DJANGO_PASSWORD": "test",
    "POSTGRES_DSN": "postgresql://campaign_user:campaign_pass@campaign-db:5432/campaign_db",
    "REDIS_URL": "redis://redis:6379/0",
    "REDIS_QUEUE_KEY": "sms:dispatch:queue",
    "LOG_LEVEL": "INFO",
    "LOG_DIR": "/app/logs/layer2",
    "BATCH_SIZE": "50000",
    "MAX_QUEUE_SIZE": "500000",
    "HEALTH_PORT": "8003"
}

# Set defaults for any missing environment variables
for key, value in DOCKER_DEFAULTS.items():
    if key not in os.environ:
        os.environ[key] = value

# =============== PRODUCTION LOGGING CONFIGURATION ===============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "/app/logs/layer2")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
LOG_WHEN = os.getenv("LOG_WHEN", "midnight")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(100 * 1024 * 1024)))  # 100MB fallback
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8003"))

# Create logs directory
os.makedirs(LOG_DIR, exist_ok=True)
print(f"Layer 2 logs will be written to: {LOG_DIR}")

class DailyRotatingFileHandler(TimedRotatingFileHandler):
    """Custom handler that creates daily log files with YYYY-MM-DD.log format"""
    
    def __init__(self, log_dir, when='midnight', interval=1, backupCount=7, encoding=None):
        self.log_dir = log_dir
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.filename = os.path.join(log_dir, f"{self.current_date}.log")
        
        super().__init__(
            filename=self.filename,
            when=when,
            interval=interval,
            backupCount=backupCount,
            encoding=encoding,
            utc=False
        )
    
    def doRollover(self):
        """Override to handle daily rollover with new filename"""
        self.current_date = datetime.now().strftime("%Y-%m-%d")
        self.baseFilename = os.path.join(self.log_dir, f"{self.current_date}.log")
        super().doRollover()

# Correlation ID filter
class CorrelationIDFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = '-'
        return True

# Create formatters
console_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
file_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(filename)s:%(lineno)d - %(message)s'
console_formatter = logging.Formatter(console_format)
file_formatter = logging.Formatter(file_format)

# Configure root logger - REMOVE any existing handlers
root_logger = logging.getLogger()
root_logger.handlers.clear()  # Clear any existing handlers
root_logger.setLevel(getattr(logging, LOG_LEVEL))

# Console handler
console_handler = logging.StreamHandler()
console_handler.setFormatter(console_formatter)
console_handler.addFilter(CorrelationIDFilter())
root_logger.addHandler(console_handler)

# Daily rotating file handler
file_handler = DailyRotatingFileHandler(
    log_dir=LOG_DIR,
    when=LOG_WHEN,
    interval=1,
    backupCount=LOG_RETENTION_DAYS,
    encoding='utf-8'
)
file_handler.setFormatter(file_formatter)
file_handler.addFilter(CorrelationIDFilter())
root_logger.addHandler(file_handler)

# Optional: Size-based rotation as fallback
size_handler = RotatingFileHandler(
    filename=os.path.join(LOG_DIR, "emergency.log"),
    maxBytes=LOG_MAX_BYTES,
    backupCount=3
)
size_handler.setLevel(logging.WARNING)
size_handler.setFormatter(file_formatter)
size_handler.addFilter(CorrelationIDFilter())
root_logger.addHandler(size_handler)

# Get logger for this module
logger = logging.getLogger("layer2")

# Log startup
logger.info("[START] Layer 2 Logger initialized")
logger.info("[CONFIG] Log configuration:")
logger.info(f"   - Level: {LOG_LEVEL}")
logger.info(f"   - Directory: {LOG_DIR}")
logger.info(f"   - Format: YYYY-MM-DD.log")
logger.info(f"   - Retention: {LOG_RETENTION_DAYS} days")
logger.info(f"   - Health Port: {HEALTH_PORT}")

# =============== HEALTH CHECK SERVER ===============
health_app = FastAPI()

@health_app.get("/health")
async def health():
    """Health check endpoint for Docker"""
    return {
        "status": "healthy",
        "service": "layer2",
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_connected": consumer is not None if 'consumer' in globals() else False
    }

@health_app.get("/ready")
async def ready():
    """Readiness check endpoint"""
    return {"status": "ready"}

def run_health_server():
    """Run health check server in a separate thread"""
    uvicorn.run(health_app, host="0.0.0.0", port=HEALTH_PORT, log_level="error")

# =============== CONFIGURATION ===============
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC", "sms-commands")
PROGRESS_TOPIC = os.getenv("KAFKA_PROGRESS_TOPIC", "sms-batch-started")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "layer2-engine")

# Django API
DJANGO_API_URL = os.getenv("CAMPAIGN_MANAGER_URL", "http://django-app:8000")
DJANGO_USERNAME = os.getenv("DJANGO_USERNAME", "test")
DJANGO_PASSWORD = os.getenv("DJANGO_PASSWORD", "test")

# PostgreSQL (direct connection for audience streaming)
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://campaign_user:campaign_pass@campaign-db:5432/campaign_db")

# Redis
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
REDIS_QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "sms:dispatch:queue")
MAX_QUEUE_SIZE = int(os.getenv("MAX_QUEUE_SIZE", "500000"))  # Backpressure threshold
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "50000"))  # Recipients per batch

# =============== TOKEN MANAGER ===============
class TokenManager:
    """Manages JWT tokens for Django API authentication"""
    
    def __init__(self, api_url: str, username: str, password: str):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        
    async def get_valid_token(self) -> str:
        """Get a valid token (refresh if expired)"""
        async with self.lock:
            current_time = datetime.now().timestamp()
            
            if not self.access_token or current_time >= self.token_expiry - 60:
                logger.info("[INFO] Token expiring soon, refreshing...")
                await self._refresh_token()
                
            return self.access_token
    
    async def _refresh_token(self):
        """Refresh the access token"""
        try:
            async with httpx.AsyncClient() as client:
                if self.refresh_token:
                    # Try refresh first
                    response = await client.post(
                        f"{self.api_url}/api/token/refresh/",
                        json={"refresh": self.refresh_token},
                        timeout=10.0
                    )
                    if response.status_code == 200:
                        data = response.json()
                        self.access_token = data['access']
                        self.token_expiry = datetime.now().timestamp() + 280  # 5 min - 20s
                        logger.info("[OK] Token refreshed successfully")
                        return
                
                # Get new token
                logger.info("[INFO] Getting new token from Django")
                response = await client.post(
                    f"{self.api_url}/api/token/",
                    json={"username": self.username, "password": self.password},
                    timeout=10.0
                )
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['access']
                    self.refresh_token = data.get('refresh')
                    self.token_expiry = datetime.now().timestamp() + 280
                    logger.info("[OK] New token acquired")
                else:
                    raise Exception(f"Token acquisition failed: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"[ERROR] Token refresh error: {e}")
            raise

# =============== CAMPAIGN METADATA ===============
class CampaignMetadata:
    """Campaign metadata fetched from Django API"""
    
    def __init__(self, data: dict):
        self.id = data['id']
        self.name = data['name']
        self.status = data['status']
        
        # Message templates
        msg_content = data.get('message_content', {})
        self.templates = msg_content.get('content', {})
        self.default_language = msg_content.get('default_language', 'en')
        
        # Audience summary
        audience = data.get('audience', {})
        self.audience_summary = audience.get('summary', {})
        self.total_recipients = self.audience_summary.get('total', 0)
        self.valid_recipients = self.audience_summary.get('valid', 0)
        
        # Database info for direct access
        self.db_info = audience.get('database_info', {})
        
        # Checkpoint info
        checkpoint = data.get('checkpoint_info', {})
        self.last_processed = checkpoint.get('last_processed', 0)
        self.has_checkpoint = checkpoint.get('has_checkpoint', False)
        
        # Progress
        progress = data.get('progress', {})
        self.progress_status = progress.get('status', 'PENDING')

# =============== COMMAND HANDLER ===============
class CommandHandler:
    """Handles different command types"""
    
    def __init__(self):
        self.running_campaigns: Dict[int, asyncio.Task] = {}
        self.campaign_status: Dict[int, str] = {}  # RUNNING, PAUSED, STOPPED
        
    async def handle_start(self, campaign_id: int, correlation_id: str, batch_size: int = BATCH_SIZE):
        """Handle START command"""
        # Create log with correlation ID
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"[START] Handling START for campaign {campaign_id}")
        
        # Check if already running
        if campaign_id in self.running_campaigns:
            log.warning(f"[WARN] Campaign {campaign_id} already running")
            return
        
        # Create processing task
        task = asyncio.create_task(
            process_campaign(campaign_id, correlation_id, batch_size)
        )
        self.running_campaigns[campaign_id] = task
        self.campaign_status[campaign_id] = "RUNNING"
        
        log.info(f"[OK] Started processing campaign {campaign_id}")
    
    async def handle_stop(self, campaign_id: int, correlation_id: str):
        """Handle STOP command"""
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"[STOP] Handling STOP for campaign {campaign_id}")
        
        self.campaign_status[campaign_id] = "STOPPED"
        
        if campaign_id in self.running_campaigns:
            log.info(f"[STOP] Stop signal sent to campaign {campaign_id}")
    
    async def handle_pause(self, campaign_id: int, correlation_id: str):
        """Handle PAUSE command"""
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"[PAUSE] Handling PAUSE for campaign {campaign_id}")
        
        self.campaign_status[campaign_id] = "PAUSED"
        log.info(f"[OK] Paused campaign {campaign_id}")
    
    async def handle_resume(self, campaign_id: int, correlation_id: str):
        """Handle RESUME command"""
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"[RESUME] Handling RESUME for campaign {campaign_id}")
        
        if campaign_id in self.campaign_status:
            self.campaign_status[campaign_id] = "RUNNING"
            log.info(f"[OK] Resumed campaign {campaign_id}")
        else:
            log.warning(f"[WARN] Campaign {campaign_id} not found for resume")

# =============== KAFKA CONSUMER ===============
class KafkaCommandConsumer:
    """Consumes commands from Kafka"""
    
    def __init__(self, command_handler: CommandHandler):
        self.command_handler = command_handler
        self.consumer = None
        self.running = True
        self.connected = False
        
    async def start(self):
        """Start consuming commands"""
        self.consumer = AIOKafkaConsumer(
            COMMAND_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await self.consumer.start()
        self.connected = True
        logger.info(f"[OK] Kafka consumer started on topic {COMMAND_TOPIC}")
        
        asyncio.create_task(self._consume_loop())
    
    async def _consume_loop(self):
        """Main consume loop"""
        try:
            async for msg in self.consumer:
                command = msg.value
                correlation_id = command.get('correlation_id', str(uuid.uuid4()))
                
                # Create log with correlation ID
                log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
                
                log.info(f"[RECV] Received command: {command.get('command_type')} for campaign {command.get('campaign_id')}")
                log.debug(f"Command details: {command}")
                
                # Handle command
                cmd_type = command.get('command_type')
                campaign_id = command.get('campaign_id')
                
                if cmd_type == 'START':
                    await self.command_handler.handle_start(
                        campaign_id, 
                        correlation_id,
                        command.get('batch_size', BATCH_SIZE)
                    )
                elif cmd_type == 'STOP':
                    await self.command_handler.handle_stop(campaign_id, correlation_id)
                elif cmd_type == 'PAUSE':
                    await self.command_handler.handle_pause(campaign_id, correlation_id)
                elif cmd_type == 'RESUME':
                    await self.command_handler.handle_resume(campaign_id, correlation_id)
                else:
                    log.warning(f"[WARN] Unknown command type: {cmd_type}")
                
                # Commit offset
                await self.consumer.commit()
                log.debug(f"[OK] Committed offset for campaign {campaign_id}")
                
        except Exception as e:
            logger.error(f"[ERROR] Consumer error: {e}", exc_info=True)
        finally:
            await self.consumer.stop()
            self.connected = False

# =============== MAIN PROCESSING FUNCTION ===============
async def process_campaign(campaign_id: int, correlation_id: str, batch_size: int):
    """
    Main campaign processing function
    - Fetches metadata from Django API
    - Streams recipients from PostgreSQL
    - Builds messages
    - Pushes to Redis
    - Updates checkpoints
    """
    # Create log with correlation ID
    log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    log.info(f"[START] Starting campaign {campaign_id} processing")
    
    # Initialize connections
    token_manager = TokenManager(DJANGO_API_URL, DJANGO_USERNAME, DJANGO_PASSWORD)
    pg_pool = await asyncpg.create_pool(POSTGRES_DSN, min_size=2, max_size=10)
    redis_client = await redis.from_url(REDIS_URL, decode_responses=True)
    
    # Kafka producer for progress events
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    await kafka_producer.start()
    
    # Global command_handler reference
    global command_handler
    
    try:
        # STEP 1: Get campaign metadata from Django API
        log.info("[INFO] Fetching campaign metadata from Django API")
        metadata = await fetch_campaign_metadata(campaign_id, token_manager, log)
        
        if not metadata:
            log.error("[ERROR] Failed to fetch campaign metadata")
            return
        
        log.info(f"[DATA] Campaign: {metadata.name}")
        log.info(f"   - Total recipients: {metadata.total_recipients}")
        log.info(f"   - Valid recipients: {metadata.valid_recipients}")
        log.info(f"   - Templates: {list(metadata.templates.keys())}")
        log.info(f"   - Default language: {metadata.default_language}")
        
        # STEP 2: Get starting point from checkpoint
        last_id = metadata.last_processed
        if metadata.has_checkpoint:
            log.info(f"[RESUME] Resuming from checkpoint ID: {last_id}")
        else:
            log.info(f"[START] Starting from beginning (ID: 0)")
        
        batch_num = 0
        total_processed = 0
        start_time = datetime.now()
        
        # STEP 3: Main processing loop
        while True:
            # Check if we should stop/pause
            current_status = command_handler.campaign_status.get(campaign_id, "RUNNING")
            if current_status != "RUNNING":
                log.info(f"[PAUSE] Campaign {campaign_id} status changed to {current_status}")
                break
            
            # STEP 4: Backpressure check
            queue_size = await redis_client.llen(REDIS_QUEUE_KEY)
            if queue_size > MAX_QUEUE_SIZE:
                log.info(f"[BACKPRESSURE] queue size {queue_size} > {MAX_QUEUE_SIZE}, waiting 2s")
                await asyncio.sleep(2)
                continue
            
            # STEP 5: Fetch next batch from PostgreSQL (keyset pagination)
            log.debug(f"Fetching batch {batch_num+1} from DB (last_id={last_id})")
            batch = await fetch_recipient_batch(pg_pool, campaign_id, last_id, batch_size, log)
            
            if not batch:
                log.info(f"[DONE] No more recipients for campaign {campaign_id}")
                break
            
            log.info(f"[BATCH] Fetched batch {batch_num+1}: {len(batch)} recipients (IDs {batch[0]['id']} - {batch[-1]['id']})")
            
            # STEP 6: Build messages
            messages = build_messages(batch, metadata, campaign_id, log)
            log.info(f"[BUILD] Built {len(messages)} messages")
            
            # STEP 7: Push to Redis using pipeline
            await push_to_redis(redis_client, messages, log)
            
            # STEP 8: Update checkpoint
            last_id = batch[-1]['id']
            await update_checkpoint(pg_pool, campaign_id, last_id, len(messages), log)
            
            # STEP 9: Publish progress to Kafka
            await publish_progress(kafka_producer, campaign_id, batch_num, messages, correlation_id, log)
            
            batch_num += 1
            total_processed += len(messages)
            
            # Calculate rate
            elapsed = (datetime.now() - start_time).total_seconds()
            rate = total_processed / elapsed if elapsed > 0 else 0
            
            log.info(f"[BATCH] Batch {batch_num} complete: {len(messages)} messages")
            log.info(f"[PROGRESS] Progress: {total_processed}/{metadata.total_recipients} ({total_processed/metadata.total_recipients*100:.1f}%)")
            log.info(f"[RATE] Rate: {rate:.0f} msgs/sec")
        
        # STEP 10: Campaign complete
        elapsed = (datetime.now() - start_time).total_seconds()
        log.info(f"[COMPLETE] Campaign {campaign_id} completed!")
        log.info(f"[STATS] Final stats: {total_processed} messages in {elapsed:.0f} seconds ({total_processed/elapsed:.0f} msgs/sec)")
        
        await mark_campaign_complete(campaign_id, token_manager, log)
        
    except Exception as e:
        log.error(f"[ERROR] Error processing campaign {campaign_id}: {e}", exc_info=True)
    finally:
        # Cleanup
        await pg_pool.close()
        await redis_client.close()
        await kafka_producer.stop()
        if campaign_id in command_handler.running_campaigns:
            command_handler.running_campaigns.pop(campaign_id, None)
        log.info(f"[SHUTDOWN] Campaign {campaign_id} processing finished")

# =============== HELPER FUNCTIONS ===============
async def fetch_campaign_metadata(campaign_id: int, token_manager: TokenManager, log) -> Optional[CampaignMetadata]:
    """Fetch campaign metadata from Django API"""
    try:
        token = await token_manager.get_valid_token()
        
        async with httpx.AsyncClient() as client:
            url = f"{DJANGO_API_URL}/api/campaigns/{campaign_id}/"
            log.debug(f"GET {url}")
            
            response = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=10.0
            )
            
            if response.status_code == 200:
                data = response.json()
                log.debug(f"Received metadata: {data.get('name')}")
                return CampaignMetadata(data)
            else:
                log.error(f"[ERROR] Django API returned {response.status_code}: {response.text}")
                return None
    except Exception as e:
        log.error(f"[ERROR] Error fetching campaign metadata: {e}")
        return None

async def fetch_recipient_batch(pool, campaign_id: int, last_id: int, limit: int, log) -> List[dict]:
    """Fetch next batch of recipients using keyset pagination"""
    try:
        async with pool.acquire() as conn:
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
    except Exception as e:
        log.error(f"[ERROR] Error fetching recipients: {e}")
        return []

def build_messages(recipients: List[dict], metadata: CampaignMetadata, campaign_id: int, log) -> List[dict]:
    """Build SMS messages for a batch of recipients"""
    messages = []
    
    for recipient in recipients:
        # Get correct language template
        lang = recipient.get('language') or metadata.default_language
        template = metadata.templates.get(lang, metadata.templates.get(metadata.default_language, ""))
        
        # Replace placeholders (if any)
        text = template
        if recipient.get('variables'):
            for key, value in recipient['variables'].items():
                placeholder = f"{{{{{key}}}}}"
                if placeholder in text:
                    text = text.replace(placeholder, str(value))
                    log.debug(f"Replaced {placeholder} with {value}")
        
        # Create message object
        message = {
            "message_id": str(uuid.uuid4()),
            "campaign_id": campaign_id,
            "msisdn": recipient['msisdn'],
            "text": text,
            "language": lang,
            "retry_count": 0,
            "timestamp": datetime.utcnow().isoformat(),
            "recipient_id": recipient['id']  # For tracking
        }
        
        messages.append(message)
    
    return messages

async def push_to_redis(redis_client, messages: List[dict], log):
    """Push messages to Redis using pipeline"""
    try:
        start = datetime.now()
        async with redis_client.pipeline() as pipe:
            for msg in messages:
                await pipe.rpush(REDIS_QUEUE_KEY, json.dumps(msg))
            await pipe.execute()
        
        elapsed = (datetime.now() - start).total_seconds() * 1000
        log.info(f"[REDIS] Pushed {len(messages)} messages to Redis in {elapsed:.0f}ms")
    except Exception as e:
        log.error(f"[ERROR] Redis push error: {e}")
        raise

async def update_checkpoint(pool, campaign_id: int, last_id: int, batch_size: int, log):
    """Update checkpoint in database"""
    try:
        start = datetime.now()
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO campaign_checkpoints (campaign_id, last_processed_id, total_processed, updated_at)
                VALUES ($1, $2, $3, NOW())
                ON CONFLICT (campaign_id) 
                DO UPDATE SET 
                    last_processed_id = $2,
                    total_processed = campaign_checkpoints.total_processed + $3,
                    updated_at = NOW()
            """, campaign_id, last_id, batch_size)
        
        elapsed = (datetime.now() - start).total_seconds() * 1000
        log.debug(f"[CHECKPOINT] Updated: last_id={last_id} ({elapsed:.0f}ms)")
    except Exception as e:
        log.error(f"[ERROR] Checkpoint update error: {e}")

async def publish_progress(producer, campaign_id: int, batch_num: int, messages: List[dict], correlation_id: str, log):
    """Publish batch progress to Kafka"""
    try:
        event = {
            "event_type": "BATCH_COMPLETED",
            "campaign_id": campaign_id,
            "batch_number": batch_num,
            "message_count": len(messages),
            "first_message_id": messages[0]['message_id'],
            "last_message_id": messages[-1]['message_id'],
            "first_recipient_id": messages[0]['recipient_id'],
            "last_recipient_id": messages[-1]['recipient_id'],
            "timestamp": datetime.utcnow().isoformat(),
            "correlation_id": correlation_id
        }
        
        await producer.send(PROGRESS_TOPIC, event)
        log.debug(f"[KAFKA] Published batch {batch_num} progress to Kafka")
    except Exception as e:
        log.error(f"[ERROR] Kafka publish error: {e}")

async def mark_campaign_complete(campaign_id: int, token_manager: TokenManager, log):
    """Mark campaign as complete in Django"""
    try:
        token = await token_manager.get_valid_token()
        
        async with httpx.AsyncClient() as client:
            url = f"{DJANGO_API_URL}/api/campaigns/{campaign_id}/complete/"
            response = await client.post(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json={"reason": "All recipients processed"},
                timeout=5.0
            )
            
            if response.status_code == 202:
                log.info(f"[OK] Campaign {campaign_id} marked as complete")
            else:
                log.warning(f"[WARN] Django returned {response.status_code} for complete")
    except Exception as e:
        log.error(f"[ERROR] Error marking campaign complete: {e}")

# =============== MAIN ===============
# Global command handler instance
command_handler = None
consumer = None

async def main():
    """Main entry point"""
    logger.info("=" * 60)
    logger.info("[START] Starting Layer 2: Campaign Execution Engine")
    logger.info("=" * 60)
    logger.info(f"[CONFIG] Configuration:")
    logger.info(f"   - Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   - Command Topic: {COMMAND_TOPIC}")
    logger.info(f"   - Progress Topic: {PROGRESS_TOPIC}")
    logger.info(f"   - Django API: {DJANGO_API_URL}")
    logger.info(f"   - PostgreSQL: {POSTGRES_DSN}")
    logger.info(f"   - Redis: {REDIS_URL}")
    logger.info(f"   - Batch Size: {BATCH_SIZE}")
    logger.info(f"   - Max Queue Size: {MAX_QUEUE_SIZE}")
    logger.info(f"   - Log Directory: {LOG_DIR}")
    logger.info(f"   - Health Port: {HEALTH_PORT}")
    logger.info("=" * 60)
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    logger.info(f"[OK] Health check server started on port {HEALTH_PORT}")
    
    global command_handler, consumer
    command_handler = CommandHandler()
    consumer = KafkaCommandConsumer(command_handler)
    
    try:
        await consumer.start()
        
        # Keep running
        logger.info("[OK] Layer 2 is running. Press Ctrl+C to stop.")
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("[SHUTDOWN] Shutdown signal received")
    except Exception as e:
        logger.error(f"[ERROR] Fatal error: {e}", exc_info=True)
    finally:
        if consumer and consumer.consumer:
            await consumer.consumer.stop()
        logger.info("[SHUTDOWN] Layer 2 stopped")

if __name__ == "__main__":
    asyncio.run(main())