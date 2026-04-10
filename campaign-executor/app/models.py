"""Pydantic models for Layer 2."""
from enum import Enum
from typing import Optional, Dict, List, Any
from pydantic import BaseModel, Field
from datetime import datetime

class CommandType(str, Enum):
    """Campaign command types."""
    START = "START"
    STOP = "STOP"
    PAUSE = "PAUSE"
    RESUME = "RESUME"

class KafkaCommand(BaseModel):
    """Command received from Kafka."""
    command_id: str
    command_type: CommandType
    campaign_id: int
    timestamp: str
    source: str
    correlation_id: str
    user_id: Optional[int] = None
    reason: Optional[str] = None
    batch_size: Optional[int] = None

class CampaignStatus(str, Enum):
    """Campaign execution status."""
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    COMPLETED = "COMPLETED"
    PENDING = "PENDING"

class CampaignMetadata(BaseModel):
    """Campaign metadata from Django API."""
    id: int
    name: str
    status: str
    
    # Message templates
    templates: Dict[str, str] = Field(default_factory=dict)
    default_language: str = "en"
    
    # Audience summary
    total_recipients: int = 0
    valid_recipients: int = 0
    
    # Database info
    db_info: Dict[str, Any] = Field(default_factory=dict)
    
    # Checkpoint info
    last_processed: int = 0
    has_checkpoint: bool = False
    
    # Progress
    progress_status: str = "PENDING"
    
    class Config:
        from_attributes = True

class Recipient(BaseModel):
    """Recipient record from database."""
    id: int
    msisdn: str
    language: Optional[str] = None
    variables: Dict[str, Any] = Field(default_factory=dict)

class SMSMessage(BaseModel):
    """SMS message ready for dispatch."""
    message_id: str
    campaign_id: int
    msisdn: str
    text: str
    language: str
    retry_count: int = 0
    timestamp: str
    recipient_id: int

class BatchProgress(BaseModel):
    """Batch processing progress event."""
    event_type: str = "BATCH_COMPLETED"
    campaign_id: int
    batch_number: int
    message_count: int
    first_message_id: str
    last_message_id: str
    first_recipient_id: int
    last_recipient_id: int
    timestamp: str
    correlation_id: str

class HealthStatus(BaseModel):
    """Health check response."""
    status: str
    service: str
    version: str
    kafka_connected: bool
    postgres_connected: bool
    redis_connected: bool
    api_connected: bool
    active_campaigns: int
    timestamp: str
    log_file: str