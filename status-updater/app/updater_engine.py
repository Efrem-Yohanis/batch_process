"""Main business logic orchestrator for Layer 4 Status Updater."""
import asyncio
import logging
import signal
from datetime import datetime, timedelta
from typing import Optional

from .config import config
from .logger import logger
from . import __version__
from .db_client import DatabaseClient
from .api_client import APIClient
from .kafka_client import KafkaConsumer

class StatusUpdater:
    """
    MAIN BUSINESS LOGIC: Orchestrates all components.
    
    Responsibilities:
    - Coordinates database, API, and Kafka clients
    - Manages service lifecycle (start/stop)
    - Collects and reports statistics
    - Handles graceful shutdown
    - Routes data between components
    """
    
    def __init__(self):
        # Initialize all clients
        self.db_client = DatabaseClient()      # Direct PostgreSQL access
        self.api_client = APIClient()          # Django API + token management
        self.consumer = KafkaConsumer(         # Kafka event consumer
            db_client=self.db_client,
            api_client=self.api_client
        )
        
        # Service state
        self.running = False
        self.start_time: Optional[float] = None
        
        logger.info("StatusUpdater engine initialized")
        
    async def start(self):
        """Start all components and begin processing."""
        logger.info("=" * 60)
        logger.info(f"Starting Layer 4 Status Updater v{__version__}")
        logger.info("=" * 60)
        logger.info("Configuration:")
        for key, value in config.dict().items():
            logger.info(f"  {key}: {value}")
        logger.info("=" * 60)
        
        self.running = True
        self.start_time = datetime.now().timestamp()
        
        # Step 1: Connect to database
        logger.info("Connecting to PostgreSQL...")
        await self.db_client.connect()
        
        # Step 2: Initialize API client (gets token)
        logger.info("Initializing API client...")
        try:
            await self.api_client.get_valid_token()
            logger.info("API Client initialized successfully")
        except Exception as e:
            logger.warning(f"API Client initialization failed: {e}")
        
        # Step 3: Start Kafka consumer
        logger.info("Starting Kafka consumer...")
        await self.consumer.start()
        
        # Step 4: Start statistics reporter
        asyncio.create_task(self._stats_reporter())
        
        logger.info("=" * 60)
        logger.info("Status Updater started successfully")
        logger.info("=" * 60)
        
    async def stop(self):
        """Graceful shutdown of all components."""
        logger.info("Shutting down Status Updater...")
        self.running = False
        
        # Stop in reverse order
        logger.info("Stopping Kafka consumer...")
        await self.consumer.stop()
        
        logger.info("Closing database connections...")
        await self.db_client.close()
        
        logger.info("Closing API client...")
        await self.api_client.close()
        
        logger.info("Status Updater stopped")
    
    async def _stats_reporter(self):
        """Report statistics periodically."""
        while self.running:
            await asyncio.sleep(60)  # Every minute
            
            if not self.start_time:
                continue
                
            elapsed = time.time() - self.start_time
            rate = self.db_client.stats["updates"] / elapsed if elapsed > 0 else 0
            
            # Get Kafka lag
            lag = await self.consumer.get_lag()
            total_lag = sum(lag.values()) if lag else 0
            
            logger.info("=" * 60)
            logger.info("STATISTICS")
            logger.info("=" * 60)
            logger.info(f"  Uptime: {timedelta(seconds=int(elapsed))}")
            logger.info(f"  Database:")
            logger.info(f"    Updates: {self.db_client.stats['updates']} ({rate:.2f}/sec)")
            logger.info(f"    Batches: {self.db_client.stats['batches']}")
            logger.info(f"    Errors: {self.db_client.stats['errors']}")
            logger.info(f"  Kafka:")
            logger.info(f"    Messages: {self.consumer.stats['messages_received']}")
            logger.info(f"    Send Events: {self.consumer.stats['send_events']}")
            logger.info(f"    Delivery Events: {self.consumer.stats['delivery_events']}")
            logger.info(f"    Batches Processed: {self.consumer.stats['batches_processed']}")
            logger.info(f"    Batch Errors: {self.consumer.stats['batch_errors']}")
            logger.info(f"    Total Lag: {total_lag}")
            logger.info(f"  API Client:")
            logger.info(f"    Token Refreshes: {self.api_client.stats['token_refreshes']}")
            logger.info(f"    API Calls: {self.api_client.stats['api_calls']}")
            logger.info(f"    API Errors: {self.api_client.stats['api_errors']}")
            logger.info(f"    Fallback Updates: {self.api_client.stats['fallback_updates']}")
            logger.info(f"    Fallback Errors: {self.api_client.stats['fallback_errors']}")
            logger.info("=" * 60)
    
    async def get_stats(self) -> dict:
        """Get current statistics for API endpoint."""
        if not self.start_time:
            return {}
            
        elapsed = time.time() - self.start_time
        rate = self.db_client.stats["updates"] / elapsed if elapsed > 0 else 0
        
        lag = await self.consumer.get_lag()
        total_lag = sum(lag.values()) if lag else 0
        
        return {
            "uptime_seconds": round(elapsed),
            "uptime_human": str(timedelta(seconds=int(elapsed))),
            "database": {
                "updates": self.db_client.stats["updates"],
                "batches": self.db_client.stats["batches"],
                "errors": self.db_client.stats["errors"],
                "update_rate": round(rate, 2)
            },
            "kafka": {
                "messages_received": self.consumer.stats["messages_received"],
                "send_events": self.consumer.stats["send_events"],
                "delivery_events": self.consumer.stats["delivery_events"],
                "batches_processed": self.consumer.stats["batches_processed"],
                "batch_errors": self.consumer.stats["batch_errors"],
                "lag_by_partition": lag,
                "total_lag": total_lag
            },
            "api_client": {
                "token_refreshes": self.api_client.stats["token_refreshes"],
                "api_calls": self.api_client.stats["api_calls"],
                "api_errors": self.api_client.stats["api_errors"],
                "fallback_updates": self.api_client.stats["fallback_updates"],
                "fallback_errors": self.api_client.stats["fallback_errors"]
            }
        }

# Standalone mode entry point
async def run_standalone():
    """Run as standalone consumer without web server."""
    updater = StatusUpdater()
    
    # Handle shutdown signals
    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    
    for sig in [signal.SIGINT, signal.SIGTERM]:
        loop.add_signal_handler(sig, stop.set_result, None)
    
    await updater.start()
    await stop
    await updater.stop()