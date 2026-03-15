"""API route definitions."""
from fastapi import APIRouter, HTTPException, BackgroundTasks, Request
import logging

from .models import CampaignCommand, CommandResponse
from .services import CommandService
from .config import config

logger = logging.getLogger(config.SERVICE_NAME)
router = APIRouter()

@router.post("/campaign/start", 
             status_code=202,
             response_model=CommandResponse,
             summary="Start a campaign",
             description="Publish START command to Kafka after validation")
async def start_campaign(
    command: CampaignCommand,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Start a campaign execution."""
    correlation_id = request.state.correlation_id
    service: CommandService = request.app.state.command_service
    
    response, error = await service.process_command(command, "START", correlation_id)
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return response

@router.post("/campaign/stop",
             status_code=202,
             response_model=CommandResponse,
             summary="Stop a campaign")
async def stop_campaign(
    command: CampaignCommand,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Stop a running campaign."""
    correlation_id = request.state.correlation_id
    service: CommandService = request.app.state.command_service
    
    response, error = await service.process_command(command, "STOP", correlation_id)
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return response

@router.post("/campaign/pause",
             status_code=202,
             response_model=CommandResponse,
             summary="Pause a campaign")
async def pause_campaign(
    command: CampaignCommand,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Pause a campaign."""
    correlation_id = request.state.correlation_id
    service: CommandService = request.app.state.command_service
    
    response, error = await service.process_command(command, "PAUSE", correlation_id)
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return response

@router.post("/campaign/resume",
             status_code=202,
             response_model=CommandResponse,
             summary="Resume a paused campaign")
async def resume_campaign(
    command: CampaignCommand,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Resume a paused campaign."""
    correlation_id = request.state.correlation_id
    service: CommandService = request.app.state.command_service
    
    response, error = await service.process_command(command, "RESUME", correlation_id)
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return response

@router.post("/campaign/complete",
             status_code=202,
             response_model=CommandResponse,
             summary="Complete a campaign")
async def complete_campaign(
    command: CampaignCommand,
    request: Request,
    background_tasks: BackgroundTasks
):
    """Mark a campaign as completed."""
    correlation_id = request.state.correlation_id
    service: CommandService = request.app.state.command_service
    
    response, error = await service.process_command(command, "COMPLETE", correlation_id)
    if error:
        raise HTTPException(status_code=400, detail=error)
    
    return response

@router.get("/health", summary="Health check endpoint")
async def health(request: Request):
    """Check service health."""
    correlation_id = request.state.correlation_id
    log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
    
    api_client: APIClient = request.app.state.api_client
    kafka_client: KafkaClient = request.app.state.kafka_client
    
    kafka_status = "connected" if kafka_client.is_connected else "disconnected"
    django_ok, django_status = await api_client.check_health()
    token_status = "valid" if api_client.access_token else "none"
    
    from . import __version__
    
    return {
        "status": "healthy" if kafka_status == "connected" and django_ok else "degraded",
        "service": "SMS Layer 1 - Command Ingestion",
        "version": __version__,
        "kafka": kafka_status,
        "django": django_status,
        "token": token_status,
        "timestamp": datetime.utcnow().isoformat(),
        "log_file": os.path.join(config.LOG_DIR, f"{datetime.now().strftime('%Y-%m-%d')}.log")
    }

@router.get("/info", summary="Service information")
async def info():
    """Get service information."""
    from . import __version__, __description__
    from .models import CommandType
    
    return {
        "name": "SMS Layer 1 - Command Ingestion",
        "version": __version__,
        "description": __description__,
        "commands": [cmd.value for cmd in CommandType],
        "kafka_config": {
            "bootstrap_servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "topic": config.KAFKA_COMMAND_TOPIC
        },
        "django_api": config.CAMPAIGN_MANAGER_URL,
        "logging": {
            "directory": config.LOG_DIR,
            "retention_days": config.LOG_RETENTION_DAYS,
            "format": "YYYY-MM-DD.log"
        }
    }