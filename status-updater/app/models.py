"""Pydantic models for Layer 4 Status Updater."""
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime

# =============== KAFKA EVENT MODELS ===============

class SendStatusEvent(BaseModel):
    """Send status event from Layer 3."""
    event_type: str = "SEND_STATUS"
    message_id: str
    campaign_id: int
    msisdn: str
    status: str  # SENT or FAILED
    provider_message_id: Optional[str] = None
    error: Optional[str] = None
    retry_count: int = 0
    timestamp: str

class DeliveryStatusEvent(BaseModel):
    """Delivery status event from webhook (via Layer 3)."""
    event_type: str = "DELIVERY_STATUS"
    message_id: str
    provider_message_id: str
    status: str  # DELIVERED, FAILED, EXPIRED, etc.
    error: Optional[str] = None
    timestamp: str

# =============== DATABASE UPDATE MODELS ===============

class StatusUpdate(BaseModel):
    """Unified status update for database."""
    message_id: str
    status: str
    provider_message_id: Optional[str] = None
    provider_response: Optional[str] = None
    attempts: Optional[int] = None
    delivered_at: Optional[datetime] = None
    error: Optional[str] = None

# =============== API RESPONSE MODELS ===============

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    service: str
    uptime: str
    kafka_lag: int
    db_updates: int
    db_errors: int
    kafka_messages: int

class StatsResponse(BaseModel):
    """Detailed statistics response."""
    uptime_seconds: float
    uptime_human: str
    database: Dict[str, Any]
    kafka: Dict[str, Any]
    fallback: Dict[str, Any]

class LagResponse(BaseModel):
    """Kafka lag information."""
    lag_by_partition: Dict[str, int]
    total_lag: int

class ControlResponse(BaseModel):
    """Response to control commands (pause/resume)."""
    status: str