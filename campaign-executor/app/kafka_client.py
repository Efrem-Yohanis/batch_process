"""Kafka consumer and producer client."""
import json
import asyncio
import logging
from typing import Callable, Awaitable, Optional

# Try to import aiokafka, but handle import errors gracefully
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
    AIOKAFKA_AVAILABLE = True
except ImportError as e:
    AIOKAFKA_AVAILABLE = False
    logger = logging.getLogger("campaign-executor")
    logger.error(f"Failed to import aiokafka: {e}. Kafka will be disabled.")
    # Create dummy classes
    class AIOKafkaConsumer:
        def __init__(self, *args, **kwargs): 
            pass
        async def start(self): 
            pass
        async def stop(self): 
            pass
        def __aiter__(self):
            return self
        async def __anext__(self):
            await asyncio.sleep(3600)  # Wait forever
            raise StopAsyncIteration
    
    class AIOKafkaProducer:
        def __init__(self, *args, **kwargs): 
            pass
        async def start(self): 
            pass
        async def stop(self): 
            pass
        async def send(self, *args, **kwargs): 
            pass

from .config import config
from .models import KafkaCommand, BatchProgress

logger = logging.getLogger("campaign-executor")

class KafkaConsumerClient:
    """Consumes commands from Kafka."""
    
    def __init__(self, message_handler: Callable[[KafkaCommand], Awaitable[None]]):
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = config.KAFKA_COMMAND_TOPIC
        self.group_id = config.KAFKA_CONSUMER_GROUP
        self.message_handler = message_handler
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.running = False
        self._task: Optional[asyncio.Task] = None
        self._available = AIOKAFKA_AVAILABLE
    
    async def start(self):
        """Start consuming messages."""
        if not self._available:
            logger.error("Kafka is not available - aiokafka not installed")
            return
            
        try:
            # Log connection info
            logger.info(f"Connecting to Kafka at {self.bootstrap_servers}")
            logger.info(f"Topic: {self.topic}, Group: {self.group_id}")
            
            # Create consumer with proper timeouts
            self.consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                # Timeout settings to prevent hanging
                request_timeout_ms=40000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000,
                max_poll_records=500,
                # Connection settings
                max_poll_interval_ms=300000,
                auto_commit_interval_ms=5000
            )
            
            # Start the consumer
            await self.consumer.start()
            self.running = True
            logger.info(f"✅ Kafka consumer started successfully on topic {self.topic}")
            
            # Get partition assignment info
            partitions = self.consumer.assignment()
            logger.info(f"Assigned partitions: {partitions}")
            
            # Start consume loop
            self._task = asyncio.create_task(self._consume_loop())
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka consumer: {e}", exc_info=True)
            raise
    
    async def stop(self):
        """Stop consuming."""
        logger.info("Stopping Kafka consumer...")
        self.running = False
        
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        if self.consumer:
            await self.consumer.stop()
            logger.info("✅ Kafka consumer stopped")
    
    @property
    def is_connected(self) -> bool:
        """Check if consumer is connected."""
        return self._available and self.consumer is not None and not self.consumer._closed
    
    async def _consume_loop(self):
        """Main consume loop."""
        logger.info("Starting consumer loop...")
        
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                
                try:
                    # Log received message
                    logger.debug(f"Received message from topic {msg.topic}, "
                               f"partition {msg.partition}, offset {msg.offset}")
                    
                    # Parse message
                    command = KafkaCommand(**msg.value)
                    
                    # Handle message
                    await self.message_handler(command)
                    
                    # Commit offset after successful processing
                    await self.consumer.commit()
                    logger.debug(f"Committed offset {msg.offset} for command {command.command_id}")
                    
                except json.JSONDecodeError as e:
                    logger.error(f"❌ Failed to decode message: {e}, raw: {msg.value[:200]}")
                    # Still commit to avoid blocking
                    await self.consumer.commit()
                    
                except Exception as e:
                    logger.error(f"❌ Error processing message: {e}", exc_info=True)
                    # Don't commit on error - will reprocess
                    
        except asyncio.CancelledError:
            logger.info("Consumer loop cancelled")
        except Exception as e:
            logger.error(f"❌ Consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop ended")


class KafkaProducerClient:
    """Publishes progress events to Kafka."""
    
    def __init__(self):
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = config.KAFKA_PROGRESS_TOPIC
        self.producer: Optional[AIOKafkaProducer] = None
        self._available = AIOKAFKA_AVAILABLE
    
    async def start(self):
        """Start producer."""
        if not self._available:
            logger.error("Kafka is not available - aiokafka not installed")
            return
            
        try:
            logger.info(f"Starting Kafka producer, connecting to {self.bootstrap_servers}")
            logger.info(f"Target topic: {self.topic}")
            
            # Create producer with optimized settings
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                # Optimize for high throughput
                linger_ms=5,           # Wait up to 5ms to batch messages
                batch_size=16384,      # 16KB batch size
                compression_type='gzip', # Compress messages
                max_request_size=10485760,  # 10MB max request size
                # Connection settings
                request_timeout_ms=40000,
                retry_backoff_ms=100
            )
            
            await self.producer.start()
            logger.info(f"✅ Kafka producer started successfully on topic {self.topic}")
            
        except Exception as e:
            logger.error(f"❌ Failed to start Kafka producer: {e}", exc_info=True)
            raise
    
    async def stop(self):
        """Stop producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("✅ Kafka producer stopped")
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self._available and self.producer is not None
    
    async def publish_progress(self, progress: BatchProgress):
        """Publish batch progress."""
        if not self._available:
            logger.error("❌ Kafka not available")
            return
            
        if not self.producer:
            logger.error("❌ Producer not connected")
            return
        
        try:
            # Send message
            await self.producer.send(self.topic, progress.dict())
            logger.debug(f"✅ Published batch {progress.batch_number} progress to Kafka")
            
        except Exception as e:
            logger.error(f"❌ Failed to publish progress: {e}", exc_info=True)