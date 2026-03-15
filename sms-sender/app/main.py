"""Main FastAPI application for Layer 3 SMS Sender."""
import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException
import uvicorn

from .config import config
from .logger import logger
from . import __version__, __description__
from .models import DeliveryReport, HealthStatus, StatsResponse
from .sender_engine import SMSSender

# Global sender instance
sender: SMSSender = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global sender
    
    log = logging.LoggerAdapter(logger, {'correlation_id': 'startup'})
    
    log.info("=" * 60)
    log.info(f"Starting {__description__} v{__version__}")
    log.info("=" * 60)
    
    # Initialize and start sender
    sender = SMSSender()
    asyncio.create_task(sender.start())
    
    yield
    
    # Shutdown
    log.info("Shutting down...")
    if sender:
        await sender.stop()
    log.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Layer 3 - High-Throughput SMS Sender",
    description=__description__,
    version=__version__,
    lifespan=lifespan
)

# =============== WEBHOOK ENDPOINTS ===============

@app.post("/webhook/delivery", status_code=200)
async def delivery_report(report: DeliveryReport):
    """
    Webhook endpoint for async delivery reports from 3rd party API.
    
    Receives delivery receipts and publishes them to Kafka.
    Always returns 200 to acknowledge receipt.
    """
    log = logging.LoggerAdapter(logger, {'correlation_id': f"webhook-{report.message_id}"})
    log.info(f"Received delivery report for {report.message_id}: {report.status}")
    
    if not sender or not sender.kafka_client:
        log.error("Kafka client not available")
        # Still return 200 to acknowledge receipt
        return {"status": "received", "warning": "Kafka unavailable"}
    
    # Map provider status to standard status
    status_map = {
        "delivered": "DELIVERED",
        "failed": "FAILED",
        "pending": "PENDING",
        "sent": "SENT",
        "read": "READ",
        "expired": "EXPIRED",
        "rejected": "REJECTED"
    }
    
    our_status = status_map.get(report.status.lower(), report.status.upper())
    
    # Publish to Kafka
    await sender.kafka_client.publish_delivery_status(
        message_id=report.message_id,
        provider_message_id=report.provider_message_id,
        status=our_status,
        error=report.error
    )
    
    log.info(f"Published delivery status to Kafka")
    
    # Always return 200 to acknowledge receipt
    return {"status": "received", "message": "Delivery report acknowledged"}

# =============== MANAGEMENT ENDPOINTS ===============

@app.get("/health", response_model=HealthStatus)
async def health():
    """Health check endpoint."""
    global sender
    
    if not sender or not sender.running:
        raise HTTPException(status_code=503, detail="Service starting or unavailable")
    
    return HealthStatus(
        status="healthy",
        service="Layer 3 SMS Sender",
        version=__version__,
        tps_limit=config.MAX_TPS,
        workers=config.WORKER_COUNT,
        stats={
            "sent": sender.stats["sent"],
            "failed": sender.stats["failed"],
            "retried": sender.stats["retried"],
            "total_processed": sender.stats["total_processed"]
        },
        timestamp=datetime.utcnow().isoformat()
    )

@app.get("/stats", response_model=StatsResponse)
async def stats():
    """Get current statistics."""
    global sender
    
    if not sender:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    stats_data = await sender.get_stats()
    return StatsResponse(**stats_data)

@app.get("/ready")
async def ready():
    """Readiness probe for Kubernetes."""
    global sender
    
    if not sender or not sender.running:
        raise HTTPException(status_code=503, detail="Not ready")
    
    # Check if Kafka is connected
    if not sender.kafka_client.is_connected:
        raise HTTPException(status_code=503, detail="Kafka not connected")
    
    # Check if Redis is connected
    if not sender.redis_client.is_connected:
        raise HTTPException(status_code=503, detail="Redis not connected")
    
    return {"status": "ready"}

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": __description__,
        "version": __version__,
        "status": "running",
        "endpoints": [
            "/webhook/delivery (POST)",
            "/health (GET)",
            "/stats (GET)",
            "/ready (GET)"
        ],
        "docs": "/docs"
    }

# =============== MAIN ===============
if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=config.HTTP_PORT,
        reload=True,
        log_level=config.LOG_LEVEL.lower()
    )