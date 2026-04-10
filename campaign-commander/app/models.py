"""Pydantic models for request/response validation."""
from enum import Enum
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field, validator

class CommandType(str, Enum):
    """Campaign command types."""
    START = "START"
    STOP = "STOP"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    COMPLETE = "COMPLETE"

class CampaignCommand(BaseModel):
    """Request model for campaign commands."""
    campaign_id: int = Field(..., gt=0, description="ID of the campaign")
    user_id: Optional[int] = Field(None, description="ID of user performing action")
    reason: Optional[str] = Field(None, description="Reason for the action")
    
    @validator('campaign_id')
    def validate_campaign_id(cls, v):
        """Ensure campaign_id is positive."""
        if v <= 0:
            raise ValueError('campaign_id must be positive')
        return v
    
    class Config:
        json_schema_extra = {
            "example": {
                "campaign_id": 123,
                "user_id": 456,
                "reason": "Scheduled start from Airflow"
            }
        }

class CommandResponse(BaseModel):
    """Response model for accepted commands."""
    status: str = "accepted"
    message: str
    campaign_id: int
    command_id: str
    timestamp: str

class ValidationResult(BaseModel):
    """Result of campaign validation."""
    is_valid: bool
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None

class HealthStatus(BaseModel):
    """Health check response model."""
    status: str
    service: str
    version: str
    kafka: str
    django: str
    token: str
    timestamp: str
    log_file: str