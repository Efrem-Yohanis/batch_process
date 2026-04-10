"""Main FastAPI application entry point."""
import logging
import uuid
import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
import uvicorn

from .config import config
from .logger import logger
from .api_client import APIClient
from .kafka_client import KafkaClient
from .services import CommandService
from .urls import router
from . import __version__, __description__

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager - handles startup/shutdown events."""
    correlation_id = str(uuid.uuid4())
    log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    log.info("=" * 60)
    log.info(f"Starting {config.SERVICE_NAME} v{__version__}")
    log.info("=" * 60)
    
    # Initialize API Client
    log.info("Initializing API Client...")
    app.state.api_client = APIClient(
        base_url=config.CAMPAIGN_MANAGER_URL,
        username=config.DJANGO_USERNAME,
        password=config.DJANGO_PASSWORD
    )
    
    api_ok = await app.state.api_client.initialize()
    if not api_ok:
        log.error("Failed to initialize API Client")
    
    # Initialize Kafka Client
    log.info("Connecting to Kafka...")
    app.state.kafka_client = KafkaClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        topic=config.KAFKA_COMMAND_TOPIC
    )
    
    # Connect with retries
    kafka_ok = False
    for attempt in range(1, 11):
        kafka_ok = await app.state.kafka_client.connect()
        if kafka_ok:
            log.info(f"Connected to Kafka on attempt {attempt}")
            break
        
        log.warning(f"Kafka not ready (Attempt {attempt}/10)")
        if attempt < 10:
            await asyncio.sleep(5)
    
    if not kafka_ok:
        log.error("Failed to connect to Kafka after 10 attempts")
    
    # Initialize Command Service
    app.state.command_service = CommandService(
        api_client=app.state.api_client,
        kafka_client=app.state.kafka_client
    )
    
    log.info("=" * 60)
    log.info("Service startup complete")
    log.info("=" * 60)
    
    yield
    
    # Cleanup
    log.info("Shutting down...")
    
    if app.state.kafka_client:
        await app.state.kafka_client.disconnect()
    
    if app.state.api_client:
        await app.state.api_client.close()
    
    log.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="SMS Layer 1 - Command Ingestion",
    description=__description__,
    version=__version__,
    lifespan=lifespan
)

# Add correlation ID middleware
@app.middleware("http")
async def add_correlation_id(request: Request, call_next):
    """Add correlation ID to each request."""
    correlation_id = request.headers.get("X-Correlation-ID", str(uuid.uuid4()))
    request.state.correlation_id = correlation_id
    
    response = await call_next(request)
    response.headers["X-Correlation-ID"] = correlation_id
    return response

# Include routes
app.include_router(router)

# Root endpoint
@app.get("/")
async def root():
    """Root endpoint with service information."""
    return {
        "service": "SMS Layer 1 - Command Ingestion",
        "version": __version__,
        "status": "running",
        "endpoints": [
            "/campaign/start",
            "/campaign/stop", 
            "/campaign/pause",
            "/campaign/resume",
            "/campaign/complete",
            "/health",
            "/info"
        ],
        "docs": "/docs"
    }

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=config.HOST,
        port=config.PORT,
        reload=config.RELOAD,
        log_level=config.LOG_LEVEL.lower()
    )