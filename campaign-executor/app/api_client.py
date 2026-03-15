"""Campaign Manager API client with automatic JWT token management."""
import time
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple

import httpx

from .config import config
from .models import CampaignMetadata

logger = logging.getLogger("campaign-executor")

class CampaignAPIClient:
    """
    Handles all HTTP requests to the Campaign Manager API.
    
    Features:
    - Automatic JWT token acquisition and refresh
    - Correlation ID propagation
    - Comprehensive error handling
    - Configurable timeouts
    """
    
    def __init__(self):
        self.base_url = config.CAMPAIGN_MANAGER_URL
        self.username = config.DJANGO_USERNAME
        self.password = config.DJANGO_PASSWORD
        
        # Token management
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry: float = 0
        self.lock = asyncio.Lock()
        
        # HTTP client with connection pooling
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(10.0, connect=5.0),
            limits=httpx.Limits(max_keepalive_connections=5, max_connections=10)
        )
    
    async def close(self):
        """Close HTTP client session."""
        await self.client.aclose()
        logger.debug("API client closed")
    
    # =============== PUBLIC API METHODS ===============
    
    async def get_campaign_metadata(self, campaign_id: int, correlation_id: str) -> Tuple[Optional[CampaignMetadata], Optional[str]]:
        """
        Fetch campaign metadata from Django API.
        
        Returns:
            Tuple of (CampaignMetadata, error_message)
            - If successful: (metadata, None)
            - If failed: (None, error_message)
        """
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"Fetching metadata for campaign {campaign_id}")
        
        try:
            token = await self._get_valid_token()
            url = f"{self.base_url}/api/campaigns/{campaign_id}/"
            
            response = await self.client.get(
                url,
                headers=self._get_headers(token, correlation_id)
            )
            
            if response.status_code == 200:
                data = response.json()
                metadata = self._parse_campaign_metadata(data, log)
                log.info(f"Successfully fetched metadata for campaign {campaign_id}")
                return metadata, None
                
            elif response.status_code == 401:
                # Token expired - force refresh and retry once
                log.warning("Token expired, refreshing and retrying")
                self.token_expiry = 0  # Force refresh
                return await self.get_campaign_metadata(campaign_id, correlation_id)
                
            elif response.status_code == 404:
                error = f"Campaign {campaign_id} not found"
                log.error(error)
                return None, error
                
            else:
                error = f"Campaign Manager API returned {response.status_code}: {response.text[:200]}"
                log.error(error)
                return None, error
                
        except httpx.ConnectError as e:
            error = f"Cannot connect to Campaign Manager at {self.base_url}: {e}"
            log.error(error)
            return None, error
        except httpx.TimeoutException as e:
            error = f"Timeout connecting to Campaign Manager: {e}"
            log.error(error)
            return None, error
        except Exception as e:
            error = f"Unexpected error fetching campaign metadata: {e}"
            log.error(error, exc_info=True)
            return None, error
    
    async def mark_campaign_complete(self, campaign_id: int, correlation_id: str, 
                                     reason: str = "All recipients processed") -> Tuple[bool, Optional[str]]:
        """
        Mark campaign as complete in Django.
        
        Returns:
            Tuple of (success, error_message)
        """
        log = logging.LoggerAdapter(logger, {'correlation_id': correlation_id})
        log.info(f"Marking campaign {campaign_id} as complete")
        
        try:
            token = await self._get_valid_token()
            url = f"{self.base_url}/api/campaigns/{campaign_id}/complete/"
            
            response = await self.client.post(
                url,
                headers=self._get_headers(token, correlation_id),
                json={"reason": reason}
            )
            
            if response.status_code in (200, 202):
                log.info(f"Campaign {campaign_id} marked as complete successfully")
                return True, None
                
            elif response.status_code == 401:
                # Token expired - force refresh and retry once
                log.warning("Token expired, refreshing and retrying")
                self.token_expiry = 0
                return await self.mark_campaign_complete(campaign_id, correlation_id, reason)
                
            else:
                error = f"Failed to mark campaign complete: {response.status_code} - {response.text[:200]}"
                log.error(error)
                return False, error
                
        except Exception as e:
            error = f"Error marking campaign complete: {e}"
            log.error(error, exc_info=True)
            return False, error
    
    async def check_api_health(self) -> Tuple[bool, str]:
        """
        Check if Campaign Manager API is reachable and authenticated.
        
        Returns:
            Tuple of (is_healthy, status_message)
        """
        try:
            token = await self._get_valid_token()
            
            response = await self.client.get(
                f"{self.base_url}/api/campaigns/",
                headers=self._get_headers(token, "health-check"),
                timeout=2.0
            )
            
            if response.status_code == 200:
                return True, "connected"
            elif response.status_code == 401:
                return False, "authentication_failed"
            else:
                return False, f"error_{response.status_code}"
                
        except httpx.ConnectError:
            return False, "disconnected"
        except Exception as e:
            return False, f"error: {e}"
    
    # =============== INTERNAL TOKEN MANAGEMENT ===============
    
    async def _get_valid_token(self) -> str:
        """
        Get a valid token, refreshing if expired.
        This is the main method called by public API methods.
        """
        async with self.lock:
            current_time = time.time()
            
            # Refresh if no token or expiring soon (within buffer)
            if not self.access_token or current_time >= self.token_expiry - config.TOKEN_EXPIRY_BUFFER:
                logger.debug("Token missing or expiring soon, refreshing...")
                await self._refresh_token()
                
            return self.access_token
    
    async def _refresh_token(self):
        """Internal method to refresh or acquire new token."""
        try:
            if self.refresh_token:
                # Try refresh first
                if await self._try_refresh_token():
                    return
            
            # If refresh failed or no refresh token, get new one
            await self._get_new_token()
                
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            raise
    
    async def _try_refresh_token(self) -> bool:
        """Attempt to refresh existing token. Returns True if successful."""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/token/refresh/",
                json={"refresh": self.refresh_token}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['access']
                # Token expires in 5 minutes, refresh 20s early
                self.token_expiry = time.time() + 300 - 20
                logger.debug("Token refreshed successfully")
                return True
            else:
                logger.warning(f"Token refresh failed with status {response.status_code}")
                return False
                
        except Exception as e:
            logger.warning(f"Token refresh error: {e}")
            return False
    
    async def _get_new_token(self):
        """Get brand new token with username/password."""
        logger.info("Acquiring new token from Django")
        
        response = await self.client.post(
            f"{self.base_url}/api/token/",
            json={"username": self.username, "password": self.password}
        )
        
        if response.status_code == 200:
            data = response.json()
            self.access_token = data['access']
            self.refresh_token = data.get('refresh')
            self.token_expiry = time.time() + 300 - 20  # 5 min - 20s
            logger.info("New token acquired successfully")
        else:
            error_msg = f"Token acquisition failed: {response.status_code} - {response.text}"
            logger.error(error_msg)
            raise Exception(error_msg)
    
    def _get_headers(self, token: str, correlation_id: str) -> Dict[str, str]:
        """Get standard headers for API requests."""
        return {
            "Authorization": f"Bearer {token}",
            "X-Correlation-ID": correlation_id,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
    
    # =============== RESPONSE PARSING ===============
    
    def _parse_campaign_metadata(self, data: Dict[str, Any], log) -> CampaignMetadata:
        """
        Parse raw API response into CampaignMetadata model.
        
        Expected API response structure:
        {
            "id": 123,
            "name": "Summer Sale",
            "status": "ACTIVE",
            "message_content": {
                "content": {
                    "en": "Hello {{name}}, special offer!",
                    "es": "Hola {{name}}, oferta especial!"
                },
                "default_language": "en"
            },
            "audience": {
                "summary": {
                    "total": 100000,
                    "valid": 95000
                },
                "database_info": {...}
            },
            "checkpoint_info": {
                "last_processed": 50000,
                "has_checkpoint": true
            },
            "progress": {
                "status": "RUNNING"
            }
        }
        """
        # Extract message content
        msg_content = data.get('message_content', {})
        templates = msg_content.get('content', {})
        default_language = msg_content.get('default_language', 'en')
        
        # Extract audience info
        audience = data.get('audience', {})
        summary = audience.get('summary', {})
        
        # Extract checkpoint info
        checkpoint = data.get('checkpoint_info', {})
        
        # Extract progress
        progress = data.get('progress', {})
        
        metadata = CampaignMetadata(
            id=data['id'],
            name=data['name'],
            status=data['status'],
            templates=templates,
            default_language=default_language,
            total_recipients=summary.get('total', 0),
            valid_recipients=summary.get('valid', 0),
            db_info=audience.get('database_info', {}),
            last_processed=checkpoint.get('last_processed', 0),
            has_checkpoint=checkpoint.get('has_checkpoint', False),
            progress_status=progress.get('status', 'PENDING')
        )
        
        log.debug(f"Parsed metadata: {metadata.id} - {metadata.name}")
        log.debug(f"  Templates: {list(metadata.templates.keys())}")
        log.debug(f"  Total recipients: {metadata.total_recipients}")
        log.debug(f"  Valid recipients: {metadata.valid_recipients}")
        log.debug(f"  Last processed: {metadata.last_processed}")
        
        return metadata