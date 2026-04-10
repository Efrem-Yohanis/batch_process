"""Main entry point for Layer 2 Campaign Execution Engine."""
import asyncio
import threading
import uuid
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
import uvicorn

from .config import config
from .logger import logger
from . import __version__, __description__
from .api_client import CampaignAPIClient
from .db_client import DatabaseClient  # FIXED: changed from .database to .db_client
from .redis_client import RedisQueueClient
from .kafka_client import KafkaConsumerClient, KafkaProducerClient
from .campaign_engine import CampaignEngine
from .command_handler import CommandHandler

# =============== HEALTH CHECK SERVER ===============
health_app = FastAPI()

@health_app.get("/health")
async def health():
    """Health check endpoint."""
    # Import here to avoid circular imports
    from .main import app as main_app
    
    api_ok, api_status = await main_app.state.api_client.check_api_health()
    
    return {
        "status": "healthy",
        "service": "layer2",
        "version": __version__,
        "kafka_connected": (
            main_app.state.kafka_consumer.is_connected 
            if hasattr(main_app.state, 'kafka_consumer') else False
        ),
        "postgres_connected": (
            main_app.state.db_client.is_connected
            if hasattr(main_app.state, 'db_client') else False
        ),
        "redis_connected": (
            main_app.state.redis_client.is_connected
            if hasattr(main_app.state, 'redis_client') else False
        ),
        "api_connected": api_ok,
        "api_status": api_status,
        "active_campaigns": len(main_app.state.command_handler.running_campaigns) if hasattr(main_app.state, 'command_handler') else 0,
        "timestamp": datetime.utcnow().isoformat(),
        "log_file": f"{config.LOG_DIR}/{datetime.now().strftime('%Y-%m-%d')}.log"
    }

@health_app.get("/ready")
async def ready():
    """Readiness check endpoint."""
    return {"status": "ready"}

def run_health_server():
    """Run health check server in a separate thread."""
    uvicorn.run(health_app, host="0.0.0.0", port=config.HEALTH_PORT, log_level="error")

# =============== MAIN APPLICATION ===============
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    correlation_id = str(uuid.uuid4())
    log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    log.info("=" * 60)
    log.info(f"Starting {__description__} v{__version__}")
    log.info("=" * 60)
    log.info("Configuration:")
    for key, value in config.dict().items():
        log.info(f"  {key}: {value}")
    log.info("=" * 60)
    
    # Start health check server in background thread
    health_thread = threading.Thread(target=run_health_server, daemon=True)
    health_thread.start()
    log.info(f"Health check server started on port {config.HEALTH_PORT}")
    
    # Initialize components
    app.state.api_client = CampaignAPIClient()
    app.state.db_client = DatabaseClient()
    app.state.redis_client = RedisQueueClient()
    app.state.kafka_producer = KafkaProducerClient()
    
    # Initialize token by checking health
    api_ok, api_status = await app.state.api_client.check_api_health()
    if not api_ok:
        log.error(f"API client health check failed: {api_status}")
    else:
        log.info("API client initialized successfully")
    
    # Connect to other services
    await app.state.db_client.connect()
    await app.state.redis_client.connect()
    await app.state.kafka_producer.start()
    
    # Create campaign engine and command handler
    app.state.campaign_engine = CampaignEngine(
        api_client=app.state.api_client,
        db_client=app.state.db_client,
        redis_client=app.state.redis_client,
        kafka_producer=app.state.kafka_producer
    )
    
    app.state.command_handler = CommandHandler(app.state.campaign_engine)
    app.state.campaign_engine.command_handler = app.state.command_handler  # Set circular reference
    
    # Start Kafka consumer
    app.state.kafka_consumer = KafkaConsumerClient(app.state.command_handler.handle_command)
    await app.state.kafka_consumer.start()
    
    log.info("=" * 60)
    log.info("Service startup complete")
    log.info("=" * 60)
    
    yield
    
    # Cleanup
    log.info("Shutting down...")
    
    await app.state.kafka_consumer.stop()
    await app.state.kafka_producer.stop()
    await app.state.db_client.close()
    await app.state.redis_client.close()
    await app.state.api_client.close()
    
    log.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Layer 2 - Campaign Execution Engine",
    description=__description__,
    version=__version__,
    lifespan=lifespan
)

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": __description__,
        "version": __version__,
        "status": "running",
        "endpoints": ["/health", "/ready", "/stats"]
    }

@app.get("/stats")
async def stats():
    """Get current campaign statistics."""
    return {
        "active_campaigns": len(app.state.command_handler.running_campaigns),
        "campaigns": list(app.state.command_handler.running_campaigns.keys()),
        "statuses": {
            str(cid): status.value 
            for cid, status in app.state.command_handler.campaign_status.items()
        }
    }

# =============== MAIN ===============
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8002,  # Different from health port
        reload=False,
        log_level=config.LOG_LEVEL.lower()
    )