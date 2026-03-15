"""Business logic for command validation and processing."""
import logging
from typing import Tuple, Optional
from datetime import datetime
import uuid

from .config import config
from .models import CampaignCommand, CommandResponse, ValidationResult
from .api_client import APIClient
from .kafka_client import KafkaClient

logger = logging.getLogger(config.SERVICE_NAME)

class CommandService:
    """Orchestrates command validation and publishing."""
    
    def __init__(self, api_client: APIClient, kafka_client: KafkaClient):
        self.api_client = api_client
        self.kafka_client = kafka_client
    
    async def validate_campaign(self, campaign_id: int, action: str, 
                               correlation_id: str) -> ValidationResult:
        """
        Validate campaign based on action type.
        
        For START commands, validates existence, content, and audience.
        For other commands, validates existence only.
        """
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"Validating campaign {campaign_id} for action {action}")
        
        # Check if campaign exists
        exists, error = await self._validate_exists(campaign_id, correlation_id)
        if not exists:
            return ValidationResult(is_valid=False, error=error)
        
        # For START commands, validate content and audience
        if action == "START":
            has_content, content_error = await self._validate_content(campaign_id, correlation_id)
            if not has_content:
                return ValidationResult(is_valid=False, error=content_error)
            
            has_audience, audience_error = await self._validate_audience(campaign_id, correlation_id)
            if not has_audience:
                return ValidationResult(is_valid=False, error=audience_error)
        
        return ValidationResult(is_valid=True)
    
    async def _validate_exists(self, campaign_id: int, 
                              correlation_id: str) -> Tuple[bool, Optional[str]]:
        """Check if campaign exists."""
        data, error = await self.api_client.get(
            f"/api/campaigns/{campaign_id}/", 
            correlation_id
        )
        
        if error:
            return False, error
        return True, None
    
    async def _validate_content(self, campaign_id: int,
                               correlation_id: str) -> Tuple[bool, Optional[str]]:
        """Check if campaign has message content."""
        data, error = await self.api_client.get(
            f"/api/campaigns/{campaign_id}/message-content/",
            correlation_id
        )
        
        if error:
            return False, error
        
        content = data.get('content', {}) if data else {}
        if not any(content.values()):
            return False, f"Campaign {campaign_id} has empty message content"
        
        return True, None
    
    async def _validate_audience(self, campaign_id: int,
                                correlation_id: str) -> Tuple[bool, Optional[str]]:
        """Check if campaign has recipients."""
        data, error = await self.api_client.get(
            f"/api/campaigns/{campaign_id}/audience/",
            correlation_id
        )
        
        if error:
            return False, error
        
        valid_count = data.get('valid_count', 0) if data else 0
        if valid_count == 0:
            return False, f"Campaign {campaign_id} has no valid recipients"
        
        return True, None
    
    async def process_command(self, command: CampaignCommand, action: str,
                             correlation_id: str) -> Tuple[Optional[CommandResponse], Optional[str]]:
        """
        Process a campaign command.
        
        Returns:
            Tuple of (response, error_message)
        """
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        
        # Validate campaign
        validation = await self.validate_campaign(command.campaign_id, action, correlation_id)
        if not validation.is_valid:
            log.warning(f"Validation failed: {validation.error}")
            return None, validation.error
        
        # Publish to Kafka
        command_id = await self.kafka_client.publish_command(
            action,
            command.campaign_id,
            correlation_id,
            command.user_id,
            command.reason
        )
        
        if not command_id:
            return None, "Failed to publish to Kafka"
        
        # Create response
        response = CommandResponse(
            status="accepted",
            message=f"{action} command accepted",
            campaign_id=command.campaign_id,
            command_id=command_id,
            timestamp=datetime.utcnow().isoformat()
        )
        
        log.info(f"Command processed successfully: {action} for campaign {command.campaign_id}")
        return response, None