"""Kafka producer client."""
import json
import uuid
from datetime import datetime
from typing import Optional
import logging

from aiokafka import AIOKafkaProducer

from .config import config

logger = logging.getLogger(config.SERVICE_NAME)

class KafkaClient:
    """Handles Kafka producer connection and message publishing."""
    
    def __init__(self, bootstrap_servers: str, topic: str):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.producer = None
    
    async def connect(self) -> bool:
        """Connect to Kafka with retries."""
        try:
            self.producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                acks="all",
                compression_type="gzip",
                max_request_size=10485760,  # 10MB - only once!
                request_timeout_ms=40000,
                metadata_max_age_ms=300000
            )
            
            await self.producer.start()
            logger.info(f"Connected to Kafka at {self.bootstrap_servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            self.producer = None
            return False
    
    async def disconnect(self) -> None:
        """Disconnect from Kafka."""
        if self.producer:
            await self.producer.stop()
            logger.info("Disconnected from Kafka")
    
    @property
    def is_connected(self) -> bool:
        """Check if producer is connected."""
        return self.producer is not None
    
    async def publish_command(self, cmd_type: str, campaign_id: int, 
                             correlation_id: str, user_id: Optional[int] = None, 
                             reason: Optional[str] = None) -> Optional[str]:
        """
        Publish command to Kafka.
        
        Returns:
            command_id if successful, None otherwise
        """
        if not self.producer:
            logger.error("Kafka producer not connected")
            return None
        
        command_id = f"cmd_{uuid.uuid4().hex[:8]}"
        
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
            # Use send instead of send_and_wait for better performance
            await self.producer.send(
                self.topic,
                key=str(campaign_id).encode(),
                value=json.dumps(message).encode()
            )
            
            logger.info(f"Published {cmd_type} for campaign {campaign_id} (ID: {command_id})")
            return command_id
            
        except Exception as e:
            logger.error(f"Failed to publish to Kafka: {e}")
            return None
