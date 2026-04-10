"""Kafka producer client for publishing send and delivery status events."""
import json
import logging
from datetime import datetime
from typing import Optional

import aiokafka

from .config import config
from .models import SMSMessage, SendStatusEvent, DeliveryStatusEvent

logger = logging.getLogger("sms-sender")

class KafkaClient:
    """
    Publishes send status and delivery events to Kafka.
    
    This is the ONLY way Layer 3 communicates with the rest of the system.
    No direct database or API calls to Django.
    """
    
    def __init__(self):
        self.bootstrap_servers = config.KAFKA_BOOTSTRAP_SERVERS
        self.send_topic = config.KAFKA_SEND_STATUS_TOPIC
        self.delivery_topic = config.KAFKA_DELIVERY_TOPIC
        self.producer: Optional[aiokafka.AIOKafkaProducer] = None
        
    async def start(self):
        """
        Start Kafka producer with optimized settings for high throughput.
        
        Settings:
        - linger_ms: Small delay to batch messages
        - batch_size: Larger batches for better throughput
        - compression: Gzip to reduce network usage
        """
        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode(),
            linger_ms=5,           # Wait up to 5ms to batch messages
            batch_size=32768,       # 32KB batches
            compression_type='gzip', # Compress for better network utilization
            max_batch_size=65536,    # 64KB max batch
            acks=1                   # Wait for leader acknowledgment only
        )
        
        await self.producer.start()
        logger.info(f"Kafka producer started")
        logger.info(f"  - Send topic: {self.send_topic}")
        logger.info(f"  - Delivery topic: {self.delivery_topic}")
        logger.info(f"  - Batching: 5ms / 32KB")
        logger.info(f"  - Compression: gzip")
        
    async def stop(self):
        """Stop Kafka producer."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self.producer is not None
    
    async def publish_send_status(self, message: SMSMessage, status: str, 
                                   provider_msg_id: str = None, error: str = None):
        """
        Publish send status event to Kafka.
        
        Called immediately after API response:
        - status="SENT" for successful sends
        - status="FAILED" for permanent failures
        
        Args:
            message: Original SMS message
            status: "SENT" or "FAILED"
            provider_msg_id: Provider's message ID (if successful)
            error: Error message (if failed)
        """
        if not self.producer:
            logger.error("Kafka producer not connected")
            return
        
        try:
            event = SendStatusEvent(
                event_type="SEND_STATUS",
                message_id=message.message_id,
                campaign_id=message.campaign_id,
                msisdn=message.msisdn,
                status=status,
                provider_message_id=provider_msg_id,
                error=error,
                retry_count=message.retry_count,
                timestamp=datetime.utcnow().isoformat()
            )
            
            await self.producer.send(
                self.send_topic,
                event.dict()
            )
            
            logger.debug(f"Published send status {status} for {message.message_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish send status: {e}", exc_info=True)
    
    async def publish_delivery_status(self, message_id: str, provider_message_id: str, 
                                       status: str, error: str = None):
        """
        Publish delivery status event to Kafka.
        
        Called from webhook endpoint when provider sends delivery receipt.
        
        Args:
            message_id: Our internal message ID
            provider_message_id: Provider's message ID
            status: "DELIVERED", "FAILED", etc.
            error: Error message (if failed)
        """
        if not self.producer:
            logger.error("Kafka producer not connected")
            return
        
        try:
            event = DeliveryStatusEvent(
                event_type="DELIVERY_STATUS",
                message_id=message_id,
                provider_message_id=provider_message_id,
                status=status,
                error=error,
                timestamp=datetime.utcnow().isoformat()
            )
            
            await self.producer.send(
                self.delivery_topic,
                event.dict()
            )
            
            logger.debug(f"Published delivery status {status} for {message_id}")
            
        except Exception as e:
            logger.error(f"Failed to publish delivery status: {e}", exc_info=True)