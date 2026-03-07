"""
Layer 1: SMS Campaign Command Ingestion Service
Production-ready version with daily log rotation, token management, and all endpoints
"""

import os
import json
import uuid
import asyncio
import logging
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
import httpx
from datetime import datetime
from contextlib import asynccontextmanager
from enum import Enum
from typing import Optional
import uvicorn

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel, Field, validator
from aiokafka import AIOKafkaProducer

# =============== PRODUCTION LOGGING CONFIGURATION ===============
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_DIR = os.getenv("LOG_DIR", "logs/layer1")
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "7"))
LOG_WHEN = os.getenv("LOG_WHEN", "midnight")
LOG_MAX_BYTES = int(os.getenv("LOG_MAX_BYTES", str(100 * 1024 * 1024)))  # 100MB fallback

# Create logs directory
os.makedirs(LOG_DIR, exist_ok=True)

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

# Configure root logger
root_logger = logging.getLogger()
root_logger.setLevel(getattr(logging, LOG_LEVEL))

# Console handler (for live viewing)
console_handler = logging.StreamHandler()
console_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(message)s'
console_handler.setFormatter(logging.Formatter(console_format))
root_logger.addHandler(console_handler)

# Daily rotating file handler
file_handler = DailyRotatingFileHandler(
    log_dir=LOG_DIR,
    when=LOG_WHEN,
    interval=1,
    backupCount=LOG_RETENTION_DAYS,
    encoding='utf-8'
)
file_format = '%(asctime)s - %(name)s - %(levelname)s - [%(correlation_id)s] - %(filename)s:%(lineno)d - %(message)s'
file_handler.setFormatter(logging.Formatter(file_format))
root_logger.addHandler(file_handler)

# Optional: Size-based rotation as fallback
size_handler = RotatingFileHandler(
    filename=os.path.join(LOG_DIR, "emergency.log"),
    maxBytes=LOG_MAX_BYTES,
    backupCount=3
)
size_handler.setLevel(logging.WARNING)
size_handler.setFormatter(logging.Formatter(file_format))
root_logger.addHandler(size_handler)

# Correlation ID filter
class CorrelationIDFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, 'correlation_id'):
            record.correlation_id = '-'
        return True

# Get logger for this module
logger = logging.getLogger("sms-layer1")
logger.addFilter(CorrelationIDFilter())

# Log startup
logger.info("🚀 Logger initialized")
logger.info(f"📋 Log configuration:")
logger.info(f"   - Level: {LOG_LEVEL}")
logger.info(f"   - Directory: {os.path.abspath(LOG_DIR)}")
logger.info(f"   - Format: YYYY-MM-DD.log")
logger.info(f"   - Retention: {LOG_RETENTION_DAYS} days")

# =============== CONFIGURATION ===============
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CAMPAIGN_MANAGER_URL = os.getenv("CAMPAIGN_MANAGER_URL", "http://django-app:8000")
KAFKA_COMMAND_TOPIC = os.getenv("KAFKA_COMMAND_TOPIC", "sms-commands")

# Django auth credentials
DJANGO_USERNAME = os.getenv("DJANGO_USERNAME", "test")
DJANGO_PASSWORD = os.getenv("DJANGO_PASSWORD", "test")

# Service name for correlation
SERVICE_NAME = os.getenv("SERVICE_NAME", "layer1")

# Global variables
producer = None
http_client = None
token_manager = None

# =============== ENUMS ===============
class CommandType(str, Enum):
    START = "START"
    STOP = "STOP"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    COMPLETE = "COMPLETE"

# =============== TOKEN MANAGER ===============
class TokenManager:
    """Manages JWT tokens for API authentication"""
    
    def __init__(self, api_url: str, username: str, password: str):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Get initial token on startup"""
        await self._get_new_token()
        logger.info(f"✅ TokenManager initialized")
        
    async def get_valid_token(self) -> str:
        """Get a valid token (refresh if expired)"""
        async with self.lock:
            current_time = datetime.now().timestamp()
            
            # If token expires in less than 60 seconds, refresh
            if current_time >= self.token_expiry - 60:
                logger.info("⏰ Token expiring soon, refreshing...")
                await self._refresh_token()
                
            return self.access_token
    
    async def _get_new_token(self):
        """Get new token with username/password"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/api/token/",
                    json={
                        "username": self.username,
                        "password": self.password
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['access']
                    self.refresh_token = data.get('refresh')
                    
                    # JWT tokens typically expire in 5 minutes (300 seconds)
                    self.token_expiry = datetime.now().timestamp() + 280  # Refresh 20s early
                    
                    logger.info(f"✅ New token obtained")
                else:
                    logger.error(f"❌ Failed to get token: {response.status_code}")
                    raise Exception(f"Token acquisition failed: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"❌ Token acquisition error: {e}")
            raise
    
    async def _refresh_token(self):
        """Refresh the access token"""
        try:
            async with httpx.AsyncClient() as client:
                if self.refresh_token:
                    response = await client.post(
                        f"{self.api_url}/api/token/refresh/",
                        json={"refresh": self.refresh_token},
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        self.access_token = data['access']
                        self.token_expiry = datetime.now().timestamp() + 280
                        logger.info(f"✅ Token refreshed successfully")
                    else:
                        logger.warning(f"⚠️ Refresh failed, getting new token")
                        await self._get_new_token()
                else:
                    await self._get_new_token()
                    
        except Exception as e:
            logger.error(f"❌ Token refresh error: {e}")
            await self._get_new_token()
    
    def get_auth_header(self) -> dict:
        """Get authorization header for requests"""
        return {"Authorization": f"Bearer {self.access_token}"}

# =============== REQUEST MODELS ===============
class CommandRequest(BaseModel):
    campaign_id: int = Field(..., gt=0, description="ID of the campaign")
    user_id: Optional[int] = Field(None, description="ID of user performing action")
    reason: Optional[str] = Field(None, description="Reason for the action")
    
    @validator('campaign_id')
    def validate_campaign_id(cls, v):
        if v <= 0:
            raise ValueError('campaign_id must be positive')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "campaign_id": 123,
                "user_id": 456,
                "reason": "Scheduled start from Airflow"
            }
        }

class CommandResponse(BaseModel):
    status: str = "accepted"
    message: str
    campaign_id: int
    command_id: str
    timestamp: str

# =============== LIFESPAN MANAGEMENT ===============
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, http_client, token_manager
    
    # Generate correlation ID for startup
    correlation_id = str(uuid.uuid4())
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info("🚀 Starting SMS Layer 1 Ingestion Service...")
    logger.info(f"📋 Configuration:")
    logger.info(f"   - Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"   - Campaign Manager: {CAMPAIGN_MANAGER_URL}")
    logger.info(f"   - Django Auth: {DJANGO_USERNAME}")
    
    # Initialize HTTP client
    http_client = httpx.AsyncClient(timeout=10.0)
    
    # Initialize Token Manager
    token_manager = TokenManager(
        api_url=CAMPAIGN_MANAGER_URL,
        username=DJANGO_USERNAME,
        password=DJANGO_PASSWORD
    )
    
    # Get initial token
    try:
        await token_manager.initialize()
        logger.info(f"✅ Token manager initialized successfully")
    except Exception as e:
        logger.error(f"❌ Failed to initialize token manager: {e}")
        logger.error(f"   Make sure Django is running and credentials are correct")
    
    # Connect to Kafka with retries
    connected = False
    for attempt in range(1, 11):
        try:
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                acks="all",
                compression_type="gzip",
                max_request_size=1048576  # 1MB
            )
            await producer.start()
            logger.info(f"✅ Connected to Kafka on attempt {attempt}")
            connected = True
            break
        except Exception as e:
            logger.warning(f"⚠️ Kafka not ready (Attempt {attempt}/10): {e}")
            if attempt < 10:
                await asyncio.sleep(5)
    
    if not connected:
        logger.error("❌ Failed to connect to Kafka after 10 attempts.")
    
    yield
    
    # Cleanup
    if producer:
        await producer.stop()
        logger.info("✅ Kafka producer stopped")
    if http_client:
        await http_client.aclose()
        logger.info("✅ HTTP client closed")
    logger.info("👋 SMS Layer 1 stopped")

app = FastAPI(
    title="SMS Layer 1 - Command Ingestion",
    description="Receives campaign commands from Airflow, validates with Django, and publishes to Kafka",
    version="1.0.0",
    lifespan=lifespan
)

# =============== MIDDLEWARE ===============
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Add correlation ID to each request"""
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    
    # Add to logger
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

# =============== VALIDATION FUNCTIONS ===============
async def validate_campaign_exists(campaign_id: int, correlation_id: str) -> tuple[bool, Optional[str]]:
    """Check if campaign exists in Django with token auth"""
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    try:
        # Get valid token
        token = await token_manager.get_valid_token()
        
        url = f"{CAMPAIGN_MANAGER_URL}/api/campaigns/{campaign_id}/"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "X-Correlation-ID": correlation_id
        }
        
        logger.info(f"🔍 Checking campaign at: {url}")
        
        response = await http_client.get(url, headers=headers)
        
        if response.status_code == 200:
            return True, None
        elif response.status_code == 401:
            logger.error(f"❌ Authentication failed - token invalid")
            # Force token refresh on next attempt
            await token_manager._refresh_token()
            return False, f"Authentication failed"
        elif response.status_code == 404:
            return False, f"Campaign {campaign_id} does not exist"
        else:
            return False, f"Campaign Manager returned status {response.status_code}"
            
    except httpx.ConnectError as e:
        logger.error(f"❌ Cannot connect to Campaign Manager at {CAMPAIGN_MANAGER_URL}")
        return False, f"Cannot connect to Campaign Manager"
    except Exception as e:
        logger.error(f"Error checking campaign existence: {e}")
        return False, f"Validation error: {str(e)}"

async def validate_campaign_has_content(campaign_id: int, correlation_id: str) -> tuple[bool, Optional[str]]:
    """Check if campaign has message content"""
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    try:
        token = await token_manager.get_valid_token()
        url = f"{CAMPAIGN_MANAGER_URL}/api/campaigns/{campaign_id}/message-content/"
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": correlation_id
        }
        
        response = await http_client.get(url, headers=headers)
        
        if response.status_code != 200:
            return False, f"Cannot fetch message content"
        
        data = response.json()
        content = data.get('content', {})
        
        if not any(content.values()):
            return False, f"Campaign {campaign_id} has empty message content"
        
        return True, None
    except Exception as e:
        logger.error(f"Error checking campaign content: {e}")
        return False, f"Content validation failed"

async def validate_campaign_has_audience(campaign_id: int, correlation_id: str) -> tuple[bool, Optional[str]]:
    """Check if campaign has recipients"""
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    try:
        token = await token_manager.get_valid_token()
        url = f"{CAMPAIGN_MANAGER_URL}/api/campaigns/{campaign_id}/audience/"
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": correlation_id
        }
        
        response = await http_client.get(url, headers=headers)
        
        if response.status_code != 200:
            return False, f"Cannot fetch audience"
        
        data = response.json()
        valid_count = data.get('valid_count', 0)
        
        if valid_count == 0:
            return False, f"Campaign {campaign_id} has no valid recipients"
        
        return True, None
    except Exception as e:
        logger.error(f"Error checking campaign audience: {e}")
        return False, f"Audience validation failed"

async def validate_campaign_for_action(campaign_id: int, action: str, correlation_id: str) -> tuple[bool, Optional[str]]:
    """
    Validate campaign based on action type
    Returns: (is_valid, error_message)
    """
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    logger.info(f"🔍 Validating campaign {campaign_id} for action {action}")
    
    # Check if campaign exists (always required)
    exists, error = await validate_campaign_exists(campaign_id, correlation_id)
    if not exists:
        return False, error
    
    # For START commands, need additional validation
    if action == "START":
        has_content, content_error = await validate_campaign_has_content(campaign_id, correlation_id)
        if not has_content:
            return False, content_error
        
        has_audience, audience_error = await validate_campaign_has_audience(campaign_id, correlation_id)
        if not has_audience:
            return False, audience_error
    
    return True, None

# =============== KAFKA PUBLISH FUNCTION ===============
async def publish_to_kafka(cmd_type: str, campaign_id: int, correlation_id: str, user_id: int = None, reason: str = None):
    """Publish command to Kafka"""
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    if not producer:
        logger.error("❌ Kafka producer not connected")
        raise HTTPException(status_code=503, detail="Kafka not connected")
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    # Create command message
    message = {
        "command_id": command_id,
        "command_type": cmd_type,
        "campaign_id": campaign_id,
        "timestamp": datetime.utcnow().isoformat(),
        "source": "airflow",
        "correlation_id": correlation_id
    }
    
    if user_id:
        message["user_id"] = user_id
    if reason:
        message["reason"] = reason
    
    try:
        await producer.send_and_wait(
            KAFKA_COMMAND_TOPIC,
            key=str(campaign_id).encode(),
            value=json.dumps(message).encode()
        )
        logger.info(f"✅ Published {cmd_type} for campaign {campaign_id} to Kafka (Command ID: {command_id})")
        return message
    except Exception as e:
        logger.error(f"❌ Kafka error for campaign {campaign_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to publish to Kafka")

# =============== BACKGROUND TASK ===============
async def publish_and_log(cmd_type: str, campaign_id: int, correlation_id: str, user_id: int = None, reason: str = None):
    """Background task to publish to Kafka and log"""
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    try:
        message = await publish_to_kafka(cmd_type, campaign_id, correlation_id, user_id, reason)
        logger.info(f"✅ Background publish complete: {cmd_type} for campaign {campaign_id}")
    except Exception as e:
        logger.error(f"❌ Background publish failed for campaign {campaign_id}: {e}")
        # In production, you might want to store failed commands in a dead-letter queue

# =============== API ENDPOINTS ===============
@app.post("/campaign/start", 
          status_code=202,
          response_model=CommandResponse,
          summary="Start a campaign",
          description="Publish START command to Kafka after validation")
async def start_campaign(command: CommandRequest, request: Request, background_tasks: BackgroundTasks):
    """Start a campaign execution."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info(f"📥 Received START request for campaign {command.campaign_id}")
    
    # Validate campaign
    is_valid, error = await validate_campaign_for_action(command.campaign_id, "START", correlation_id)
    if not is_valid:
        logger.warning(f"❌ Validation failed: {error}")
        raise HTTPException(status_code=400, detail=error)
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    # Publish to Kafka in background
    background_tasks.add_task(
        publish_and_log, 
        "START", 
        command.campaign_id,
        correlation_id,
        command.user_id,
        command.reason
    )
    
    # Return 202 Accepted immediately
    return CommandResponse(
        status="accepted",
        message="START command accepted",
        campaign_id=command.campaign_id,
        command_id=command_id,
        timestamp=datetime.utcnow().isoformat()
    )

@app.post("/campaign/stop", 
          status_code=202,
          response_model=CommandResponse,
          summary="Stop a campaign")
async def stop_campaign(command: CommandRequest, request: Request, background_tasks: BackgroundTasks):
    """Stop a running campaign."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info(f"📥 Received STOP request for campaign {command.campaign_id}")
    
    # Validate campaign exists
    is_valid, error = await validate_campaign_for_action(command.campaign_id, "STOP", correlation_id)
    if not is_valid:
        logger.warning(f"❌ Validation failed: {error}")
        raise HTTPException(status_code=400, detail=error)
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    background_tasks.add_task(
        publish_and_log, 
        "STOP", 
        command.campaign_id,
        correlation_id,
        command.user_id,
        command.reason
    )
    
    return CommandResponse(
        status="accepted",
        message="STOP command accepted",
        campaign_id=command.campaign_id,
        command_id=command_id,
        timestamp=datetime.utcnow().isoformat()
    )

@app.post("/campaign/pause", 
          status_code=202,
          response_model=CommandResponse,
          summary="Pause a campaign")
async def pause_campaign(command: CommandRequest, request: Request, background_tasks: BackgroundTasks):
    """Pause a campaign."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info(f"📥 Received PAUSE request for campaign {command.campaign_id}")
    
    is_valid, error = await validate_campaign_for_action(command.campaign_id, "PAUSE", correlation_id)
    if not is_valid:
        logger.warning(f"❌ Validation failed: {error}")
        raise HTTPException(status_code=400, detail=error)
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    background_tasks.add_task(
        publish_and_log, 
        "PAUSE", 
        command.campaign_id,
        correlation_id,
        command.user_id,
        command.reason
    )
    
    return CommandResponse(
        status="accepted",
        message="PAUSE command accepted",
        campaign_id=command.campaign_id,
        command_id=command_id,
        timestamp=datetime.utcnow().isoformat()
    )

@app.post("/campaign/resume", 
          status_code=202,
          response_model=CommandResponse,
          summary="Resume a paused campaign")
async def resume_campaign(command: CommandRequest, request: Request, background_tasks: BackgroundTasks):
    """Resume a paused campaign."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info(f"📥 Received RESUME request for campaign {command.campaign_id}")
    
    is_valid, error = await validate_campaign_for_action(command.campaign_id, "RESUME", correlation_id)
    if not is_valid:
        logger.warning(f"❌ Validation failed: {error}")
        raise HTTPException(status_code=400, detail=error)
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    background_tasks.add_task(
        publish_and_log, 
        "RESUME", 
        command.campaign_id,
        correlation_id,
        command.user_id,
        command.reason
    )
    
    return CommandResponse(
        status="accepted",
        message="RESUME command accepted",
        campaign_id=command.campaign_id,
        command_id=command_id,
        timestamp=datetime.utcnow().isoformat()
    )

@app.post("/campaign/complete", 
          status_code=202,
          response_model=CommandResponse,
          summary="Complete a campaign")
async def complete_campaign(command: CommandRequest, request: Request, background_tasks: BackgroundTasks):
    """Mark a campaign as completed."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    logger.info(f"📥 Received COMPLETE request for campaign {command.campaign_id}")
    
    is_valid, error = await validate_campaign_for_action(command.campaign_id, "COMPLETE", correlation_id)
    if not is_valid:
        logger.warning(f"❌ Validation failed: {error}")
        raise HTTPException(status_code=400, detail=error)
    
    command_id = f"cmd_{uuid.uuid4().hex[:8]}"
    
    background_tasks.add_task(
        publish_and_log, 
        "COMPLETE", 
        command.campaign_id,
        correlation_id,
        command.user_id,
        command.reason
    )
    
    return CommandResponse(
        status="accepted",
        message="COMPLETE command accepted",
        campaign_id=command.campaign_id,
        command_id=command_id,
        timestamp=datetime.utcnow().isoformat()
    )

# =============== UTILITY ENDPOINTS ===============
@app.get("/health")
async def health(request: Request):
    """Health check endpoint."""
    correlation_id = request.state.correlation_id
    logger = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    kafka_status = "connected" if producer else "disconnected"
    
    # Test Django connection with token
    django_status = "unknown"
    token_status = "unknown"
    
    try:
        if token_manager and token_manager.access_token:
            token_status = "valid"
            
            # Quick test
            headers = token_manager.get_auth_header()
            headers["X-Correlation-ID"] = correlation_id
            response = await http_client.get(
                f"{CAMPAIGN_MANAGER_URL}/api/campaigns/",
                headers=headers,
                timeout=2.0
            )
            django_status = "connected" if response.status_code == 200 else "error"
        else:
            django_status = "no_token"
    except Exception as e:
        logger.error(f"Health check error: {e}")
        django_status = "disconnected"
    
    return {
        "status": "healthy" if producer and django_status == "connected" else "degraded",
        "service": "SMS Layer 1 - Command Ingestion",
        "version": "1.0.0",
        "kafka": kafka_status,
        "django": django_status,
        "token": token_status,
        "timestamp": datetime.utcnow().isoformat(),
        "log_file": os.path.join(LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    }

@app.get("/info")
async def info():
    """Service information endpoint."""
    return {
        "name": "SMS Layer 1 - Command Ingestion",
        "version": "1.0.0",
        "description": "Receives campaign commands from Airflow, validates with Django, and publishes to Kafka",
        "commands": [cmd.value for cmd in CommandType],
        "kafka_config": {
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "topic": KAFKA_COMMAND_TOPIC
        },
        "django_api": CAMPAIGN_MANAGER_URL,
        "logging": {
            "directory": os.path.abspath(LOG_DIR),
            "retention_days": LOG_RETENTION_DAYS,
            "format": "YYYY-MM-DD.log"
        },
        "endpoints": [
            {"path": "/campaign/start", "method": "POST", "description": "Start campaign"},
            {"path": "/campaign/stop", "method": "POST", "description": "Stop campaign"},
            {"path": "/campaign/pause", "method": "POST", "description": "Pause campaign"},
            {"path": "/campaign/resume", "method": "POST", "description": "Resume campaign"},
            {"path": "/campaign/complete", "method": "POST", "description": "Complete campaign"},
            {"path": "/health", "method": "GET", "description": "Health check"},
            {"path": "/info", "method": "GET", "description": "Service info"}
        ]
    }

# =============== RUN ===============
if __name__ == "__main__":
    uvicorn.run(
        "layer1_ingestion:app",
        host="0.0.0.0",
        port=8001,
        reload=True,
        log_level="info"
    )