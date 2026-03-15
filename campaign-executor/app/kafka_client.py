"""Kafka consumer and producer client."""
import json
import asyncio
import logging
from typing import Callable, Awaitable, Optional

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

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
    
    async def start(self):
        """Start consuming messages."""
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        
        await self.consumer.start()
        self.running = True
        logger.info(f"Kafka consumer started on topic {self.topic}")
        
        self._task = asyncio.create_task(self._consume_loop())
    
    async def stop(self):
        """Stop consuming."""
        self.running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
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
        try:
            async for msg in self.consumer:
                if not self.running:
                    break
                
                try:
                    command = KafkaCommand(**msg.value)
                    await self.message_handler(command)
                    await self.consumer.commit()
                except Exception as e:
                    logger.error(f"Error processing message: {e}", exc_info=True)
                    
        except Exception as e:
            logger.error(f"Consumer error: {e}", exc_info=True)
        finally:
            logger.info("Consumer loop ended")

class KafkaProducerClient:
    """Publishes progress events to Kafka."""
    
    def __init__(self):
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.topic = config.KAFKA_PROGRESS_TOPIC
        self.producer: Optional[AIOKafkaProducer] = None
    
    async def start(self):
        """Start producer."""
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        await self.producer.start()
        logger.info("Kafka producer started")
    
    async def stop(self):
        """Stop producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self.producer is not None
    
    async def publish_progress(self, progress: BatchProgress):
        """Publish batch progress."""
        if not self.producer:
            logger.error("Producer not connected")
            return
        
        try:
            await self.producer.send(self.topic, progress.dict())
            logger.debug(f"Published batch {progress.batch_number} progress")
        except Exception as e:
            logger.error(f"Failed to publish progress: {e}")