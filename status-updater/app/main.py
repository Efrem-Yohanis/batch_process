"""Main FastAPI application for Layer 4 Status Updater."""
import asyncio
import time
from datetime import timedelta
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
import uvicorn

from .config import config
from .logger import logger
from . import __version__, __description__
from .updater_engine import StatusUpdater, run_standalone
from .models import HealthResponse, StatsResponse, LagResponse, ControlResponse

# Global updater instance
updater: StatusUpdater = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    global updater
    
    log = logging.LoggerAdapter(logger, {'correlation_id': 'startup'})
    
    log.info("=" * 60)
    log.info(f"Starting {__description__} v{__version__}")
    log.info("=" * 60)
    
    # Initialize and start updater
    updater = StatusUpdater()
    asyncio.create_task(updater.start())
    
    yield
    
    # Shutdown
    log.info("Shutting down...")
    if updater:
        await updater.stop()
    log.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="Layer 4 - Status Updater",
    description=__description__,
    version=__version__,
    lifespan=lifespan
)

# =============== API ENDPOINTS ===============

@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    global updater
    
    if not updater or not updater.running:
        raise HTTPException(status_code=503, detail="Service starting or unavailable")
    
    lag = await updater.consumer.get_lag()
    total_lag = sum(lag.values()) if lag else 0
    
    return HealthResponse(
        status="healthy",
        service="Layer 4 Status Updater",
        uptime=str(timedelta(seconds=int(time.time() - updater.start_time))),
        kafka_lag=total_lag,
        db_updates=updater.db_client.stats["updates"],
        db_errors=updater.db_client.stats["errors"],
        kafka_messages=updater.consumer.stats["messages_received"]
    )

@app.get("/stats", response_model=StatsResponse)
async def stats():
    """Detailed statistics."""
    global updater
    
    if not updater:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    stats_data = await updater.get_stats()
    return StatsResponse(**stats_data)

@app.get("/lag", response_model=LagResponse)
async def lag():
    """Get current Kafka lag."""
    global updater
    
    if not updater:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    lag = await updater.consumer.get_lag()
    return LagResponse(
        lag_by_partition=lag,
        total_lag=sum(lag.values()) if lag else 0
    )

@app.post("/pause", response_model=ControlResponse)
async def pause():
    """Pause consumption (for maintenance)."""
    global updater
    
    if not updater:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    await updater.consumer.pause()
    return ControlResponse(status="paused")

@app.post("/resume", response_model=ControlResponse)
async def resume():
    """Resume consumption."""
    global updater
    
    if not updater:
        raise HTTPException(status_code=503, detail="Service not initialized")
    
    await updater.consumer.resume()
    return ControlResponse(status="resumed")

@app.get("/ready")
async def ready():
    """Readiness probe for Kubernetes."""
    global updater
    
    if not updater or not updater.running:
        raise HTTPException(status_code=503, detail="Not ready")
    
    # Check if Kafka is connected
    if not updater.consumer.is_connected:
        raise HTTPException(status_code=503, detail="Kafka not connected")
    
    # Check if database is connected
    if not updater.db_client.is_connected:
        raise HTTPException(status_code=503, detail="Database not connected")
    
    return {"status": "ready"}

@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "service": __description__,
        "version": __version__,
        "status": "running",
        "endpoints": [
            "/health (GET)",
            "/stats (GET)",
            "/lag (GET)",
            "/pause (POST)",
            "/resume (POST)",
            "/ready (GET)"
        ],
        "docs": "/docs"
    }

# =============== MAIN ===============
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Layer 4 Status Updater")
    parser.add_argument("--standalone", action="store_true", 
                       help="Run as standalone consumer without web server")
    args = parser.parse_args()
    
    if args.standalone:
        # Run as standalone consumer (no web server)
        asyncio.run(run_standalone())
    else:
        # Run with web server
        uvicorn.run(
            "app.main:app",
            host="0.0.0.0",
            port=config.HTTP_PORT,
            reload=True,
            log_level=config.LOG_LEVEL.lower()
        )