"""Kafka client for consuming status events with batching."""
import asyncio
import json
import logging
from typing import Dict, List, Optional
from datetime import datetime

import aiokafka

from .config import config
from .models import SendStatusEvent, DeliveryStatusEvent, StatusUpdate
from .db_client import DatabaseClient
from .api_client import APIClient

logger = logging.getLogger("status-updater")

class KafkaClient:
    """
    Kafka consumer client for high-throughput status processing.
    
    Features:
    - Consumes from both send-status and delivery-status topics
    - Batches updates for database efficiency
    - Manual offset commits for exactly-once processing
    - Fallback to API Client if DB update fails
    """
    
    def __init__(self, db_client: DatabaseClient, api_client: APIClient):
        self.db_client = db_client
        self.api_client = api_client
        self.consumer: Optional[aiokafka.AIOKafkaConsumer] = None
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
        self.update_queue: asyncio.Queue = asyncio.Queue(maxsize=10000)
        self.batch_task: Optional[asyncio.Task] = None
        
    async def start(self):
        """Start Kafka consumer and batch processor."""
        # Create consumer
        self.consumer = aiokafka.AIOKafkaConsumer(
            config.KAFKA_SEND_STATUS_TOPIC,
            config.KAFKA_DELIVERY_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id=config.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode()),
            enable_auto_commit=False,  # Manual commit for exactly-once
            auto_offset_reset='earliest',
            max_poll_records=500,
            max_poll_interval_ms=300000,
            session_timeout_ms=30000,
            heartbeat_interval_ms=3000
        )
        
        await self.consumer.start()
        logger.info(f"Kafka consumer started for topics: {config.KAFKA_SEND_STATUS_TOPIC}, {config.KAFKA_DELIVERY_TOPIC}")
        
        # Start batch processor
        self.batch_task = asyncio.create_task(self._batch_processor())
        
        # Start consumer loop
        asyncio.create_task(self._consume_loop())
    
    async def stop(self):
        """Stop consumer gracefully."""
        self.running = False
        if self.batch_task:
            self.batch_task.cancel()
            try:
                await self.batch_task
            except asyncio.CancelledError:
                pass
        if self.consumer:
            await self.consumer.stop()
            logger.info("Kafka consumer stopped")
    
    @property
    def is_connected(self) -> bool:
        """Check if consumer is connected."""
        return self.consumer is not None and not self.consumer._closed
    
    async def _consume_loop(self):
        """Main consume loop."""
        logger.info("Kafka consumer loop started")
        
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
                    try:
                        end_offsets = await self.consumer.end_offsets([tp])
                        if tp in end_offsets:
                            lag = end_offsets[tp] - (messages[-1].offset + 1)
                            self.stats["kafka_lag"] = max(self.stats["kafka_lag"], lag)
                    except:
                        pass
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer loop error: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _process_message(self, msg):
        """Process individual Kafka message."""
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
                logger.warning(f"Unknown event type: {event_type}")
                
        except Exception as e:
            logger.error(f"Failed to process message: {e}")
            self.stats["batch_errors"] += 1
    
    async def _handle_send_status(self, event: SendStatusEvent):
        """Convert send status to DB update and queue it."""
        update = StatusUpdate(
            message_id=event.message_id,
            status=event.status,
            provider_message_id=event.provider_message_id,
            provider_response=event.error,
            attempts=event.retry_count + 1  # +1 for this attempt
        )
        
        # Add to batch queue
        try:
            await asyncio.wait_for(self.update_queue.put(update), timeout=1.0)
        except asyncio.TimeoutError:
            logger.error(f"Update queue full, dropping message {event.message_id}")
            self.stats["batch_errors"] += 1
    
    async def _handle_delivery_status(self, event: DeliveryStatusEvent):
        """Convert delivery status to DB update and queue it."""
        update = StatusUpdate(
            message_id=event.message_id,
            status=event.status,
            provider_message_id=event.provider_message_id,
            provider_response=event.error,
            delivered_at=datetime.utcnow() if event.status == "DELIVERED" else None
        )
        
        # Add to batch queue
        try:
            await asyncio.wait_for(self.update_queue.put(update), timeout=1.0)
        except asyncio.TimeoutError:
            logger.error(f"Update queue full, dropping message {event.message_id}")
            self.stats["batch_errors"] += 1
    
    async def _batch_processor(self):
        """
        Process updates in batches for better DB performance.
        
        Batches based on:
        - Size: When BATCH_SIZE reached
        - Timeout: When BATCH_TIMEOUT_MS elapsed
        """
        logger.info(f"Batch processor started (size={config.BATCH_SIZE}, timeout={config.BATCH_TIMEOUT_MS}ms)")
        
        while self.running:
            batch = []
            batch_start = asyncio.get_event_loop().time()
            
            # Collect batch
            while len(batch) < config.BATCH_SIZE:
                try:
                    # Calculate remaining timeout
                    elapsed = (asyncio.get_event_loop().time() - batch_start) * 1000
                    timeout = max(0, (config.BATCH_TIMEOUT_MS - elapsed) / 1000)
                    
                    # Wait for next update with timeout
                    update = await asyncio.wait_for(
                        self.update_queue.get(), 
                        timeout=timeout
                    )
                    batch.append(update)
                    
                except asyncio.TimeoutError:
                    # Timeout reached, process whatever we have
                    break
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error(f"Batch collection error: {e}")
                    break
            
            if batch:
                await self._process_batch(batch)
                self.stats["batches_processed"] += 1
    
    async def _process_batch(self, batch: List[StatusUpdate]):
        """Process a batch of updates with fallback to API if needed."""
        try:
            # Try direct DB update first
            success_count = await self.db_client.batch_update_statuses(batch)
            
            # If some failed, try fallback API for those
            if success_count < len(batch):
                failed_updates = batch[success_count:]
                logger.warning(f"DB batch only updated {success_count}/{len(batch)}. Trying API fallback for {len(failed_updates)}...")
                
                # Try bulk API update first
                api_success = await self.api_client.bulk_update_statuses(failed_updates)
                
                if api_success < len(failed_updates):
                    logger.error(f"API fallback only updated {api_success}/{len(failed_updates)}")
                        
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            self.stats["batch_errors"] += 1
    
    async def get_lag(self) -> Dict[str, int]:
        """Get current lag for all partitions."""
        lag_info = {}
        if not self.consumer:
            return lag_info
            
        try:
            partitions = self.consumer.assignment()
            if partitions:
                end_offsets = await self.consumer.end_offsets(partitions)
                for tp in partitions:
                    position = await self.consumer.position(tp)
                    lag = end_offsets.get(tp, 0) - position
                    lag_info[f"{tp.topic}-{tp.partition}"] = lag
        except Exception as e:
            logger.error(f"Failed to get lag: {e}")
        
        return lag_info
    
    async def pause(self):
        """Pause consumption (for maintenance)."""
        self.running = False
        logger.info("Consumer paused")
    
    async def resume(self):
        """Resume consumption."""
        self.running = True
        logger.info("Consumer resumed")