"""Pydantic models for Layer 3 SMS Sender."""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime

class SMSMessage(BaseModel):
    """Message from Redis queue."""
    message_id: str
    campaign_id: int
    msisdn: str
    text: str
    language: Optional[str] = "en"
    retry_count: int = 0
    timestamp: str
    
    class Config:
        arbitrary_types_allowed = True

class APIResponse:
    """Response from 3rd party API."""
    def __init__(self, message: SMSMessage, success: bool, provider_message_id: str = None,
                 retryable: bool = False, error: str = None, status_code: int = None):
        self.message = message
        self.success = success
        self.provider_message_id = provider_message_id
        self.retryable = retryable
        self.error = error
        self.status_code = status_code

class DeliveryReport(BaseModel):
    """Delivery report from 3rd party API webhook."""
    message_id: str
    status: str  # delivered, failed, etc.
    provider_message_id: str
    timestamp: str
    error: Optional[str] = None

class SendStatusEvent(BaseModel):
    """Send status event published to Kafka."""
    event_type: str = "SEND_STATUS"
    message_id: str
    campaign_id: int
    msisdn: str
    status: str  # "SENT" or "FAILED"
    provider_message_id: Optional[str] = None
    error: Optional[str] = None
    retry_count: int
    timestamp: str

class DeliveryStatusEvent(BaseModel):
    """Delivery status event published to Kafka."""
    event_type: str = "DELIVERY_STATUS"
    message_id: str
    provider_message_id: str
    status: str  # "DELIVERED", "FAILED", etc.
    error: Optional[str] = None
    timestamp: str

class HealthStatus(BaseModel):
    """Health check response."""
    status: str
    service: str
    version: str
    tps_limit: int
    workers: int
    stats: Dict[str, int]
    timestamp: str

class StatsResponse(BaseModel):
    """Statistics response."""
    total_processed: int
    sent: int
    failed: int
    retried: int
    average_tps: float
    uptime_seconds: float
    running: bool