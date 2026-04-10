"""Command handler for campaign control."""
import asyncio
import logging
from typing import Dict, Optional

from .models import KafkaCommand, CampaignStatus
from .campaign_engine import CampaignEngine

logger = logging.getLogger("campaign-executor")

class CommandHandler:
    """Handles different command types for campaign control."""
    
    def __init__(self, campaign_engine: CampaignEngine):
        self.campaign_engine = campaign_engine
        self.running_campaigns: Dict[int, asyncio.Task] = {}
        self.campaign_status: Dict[int, CampaignStatus] = {}
    
    async def handle_command(self, command: KafkaCommand):
        """Route command to appropriate handler."""
        log = logging.LoggerAdapter(logger, {'correlation_id': command.correlation_id})
        log.info(f"Handling {command.command_type} for campaign {command.campaign_id}")
        
        handlers = {
            "START": self._handle_start,
            "STOP": self._handle_stop,
            "PAUSE": self._handle_pause,
            "RESUME": self._handle_resume
        }
        
        handler = handlers.get(command.command_type.value)
        if handler:
            await handler(command)
        else:
            log.warning(f"Unknown command type: {command.command_type}")
    
    async def _handle_start(self, command: KafkaCommand):
        """Handle START command."""
        log = logging.LoggerAdapter(logger, {'correlation_id': command.correlation_id})
        
        if command.campaign_id in self.running_campaigns:
            log.warning(f"Campaign {command.campaign_id} already running")
            return
        
        # Create processing task
        task = asyncio.create_task(
            self.campaign_engine.process_campaign(
                campaign_id=command.campaign_id,
                correlation_id=command.correlation_id,
                batch_size=command.batch_size
            )
        )
        self.running_campaigns[command.campaign_id] = task
        self.campaign_status[command.campaign_id] = CampaignStatus.RUNNING
        
        log.info(f"Started processing campaign {command.campaign_id}")
    
    async def _handle_stop(self, command: KafkaCommand):
        """Handle STOP command."""
        log = logging.LoggerAdapter(logger, {'correlation_id': command.correlation_id})
        
        self.campaign_status[command.campaign_id] = CampaignStatus.STOPPED
        log.info(f"Stop signal sent to campaign {command.campaign_id}")
    
    async def _handle_pause(self, command: KafkaCommand):
        """Handle PAUSE command."""
        log = logging.LoggerAdapter(logger, {'correlation_id': command.correlation_id})
        
        self.campaign_status[command.campaign_id] = CampaignStatus.PAUSED
        log.info(f"Paused campaign {command.campaign_id}")
    
    async def _handle_resume(self, command: KafkaCommand):
        """Handle RESUME command."""
        log = logging.LoggerAdapter(logger, {'correlation_id': command.correlation_id})
        
        if command.campaign_id in self.campaign_status:
            self.campaign_status[command.campaign_id] = CampaignStatus.RUNNING
            log.info(f"Resumed campaign {command.campaign_id}")
        else:
            log.warning(f"Campaign {command.campaign_id} not found for resume")
    
    def get_status(self, campaign_id: int) -> CampaignStatus:
        """Get current status of a campaign."""
        return self.campaign_status.get(campaign_id, CampaignStatus.PENDING)
    
    async def cleanup_campaign(self, campaign_id: int):
        """Remove campaign from tracking after completion."""
        self.running_campaigns.pop(campaign_id, None)
        self.campaign_status.pop(campaign_id, None)
        logger.info(f"Cleaned up campaign {campaign_id}")