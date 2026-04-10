"""Django API client with automatic JWT token management."""
import time
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple

import httpx

from .config import config
from .models import StatusUpdate

logger = logging.getLogger("status-updater")

class APIClient:
    """
    Handles all communication with Django API.
    
    Features:
    - Automatic JWT token acquisition and refresh
    - Fallback status updates via API
    - Health checks
    - Connection pooling
    """
    
    def __init__(self):
        self.base_url = config.DJANGO_URL
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
            limits=httpx.Limits(max_keepalive_connections=10, max_connections=20)
        )
        
        # Statistics
        self.stats = {
            "token_refreshes": 0,
            "api_calls": 0,
            "api_errors": 0,
            "fallback_updates": 0,
            "fallback_errors": 0
        }
    
    async def close(self):
        """Close HTTP client session."""
        await self.client.aclose()
        logger.debug("API Client closed")
    
    # =============== TOKEN MANAGEMENT ===============
    
    async def get_valid_token(self) -> str:
        """Get a valid token, refreshing if expired."""
        async with self.lock:
            current_time = time.time()
            
            if not self.access_token or current_time >= self.token_expiry - 60:
                logger.debug("Token expiring soon, refreshing...")
                await self._refresh_token()
                self.stats["token_refreshes"] += 1
                
            return self.access_token
    
    async def _refresh_token(self):
        """Refresh or acquire new token."""
        try:
            # Try to get new token with credentials
            response = await self.client.post(
                f"{self.base_url}/api/token/",
                json={"username": self.username, "password": self.password}
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['access']
                self.refresh_token = data.get('refresh')
                self.token_expiry = time.time() + 280  # 5 min - 20s
                logger.info("Django token acquired")
            else:
                logger.error(f"Token acquisition failed: {response.status_code}")
                raise Exception(f"Token acquisition failed: {response.status_code}")
                
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            raise
    
    def _get_auth_header(self) -> dict:
        """Get authorization header."""
        return {"Authorization": f"Bearer {self.access_token}"}
    
    # =============== API METHODS ===============
    
    async def update_message_status(self, update: StatusUpdate) -> bool:
        """
        Update message status via Django API (fallback method).
        
        Returns:
            True if successful, False otherwise
        """
        self.stats["api_calls"] += 1
        
        try:
            token = await self.get_valid_token()
            
            # Build payload based on what's available
            payload = {
                "status": update.status,
                "provider_message_id": update.provider_message_id,
                "provider_response": update.provider_response or update.error,
            }
            
            if update.attempts is not None:
                payload["attempts"] = update.attempts
                
            if update.delivered_at:
                payload["delivered_at"] = update.delivered_at.isoformat()
            
            # Make API call
            response = await self.client.patch(
                f"{self.base_url}/api/message-statuses/{update.message_id}/",
                headers=self._get_auth_header(),
                json=payload
            )
            
            if response.status_code == 200:
                self.stats["fallback_updates"] += 1
                logger.debug(f"API update successful for {update.message_id}")
                return True
            else:
                logger.error(f"API update failed: {response.status_code} - {response.text[:200]}")
                self.stats["api_errors"] += 1
                self.stats["fallback_errors"] += 1
                return False
                
        except Exception as e:
            logger.error(f"API update error: {e}")
            self.stats["api_errors"] += 1
            self.stats["fallback_errors"] += 1
            return False
    
    async def bulk_update_statuses(self, updates: list[StatusUpdate]) -> int:
        """
        Bulk update via API if endpoint exists.
        
        Returns:
            Number of successful updates
        """
        # First try bulk endpoint if available
        try:
            token = await self.get_valid_token()
            
            # Check if bulk endpoint exists (customize based on your Django API)
            payload = {
                "updates": [
                    {
                        "message_id": u.message_id,
                        "status": u.status,
                        "provider_message_id": u.provider_message_id,
                        "provider_response": u.provider_response or u.error,
                        "attempts": u.attempts,
                        "delivered_at": u.delivered_at.isoformat() if u.delivered_at else None
                    }
                    for u in updates
                ]
            }
            
            response = await self.client.post(
                f"{self.base_url}/api/message-statuses/bulk-update/",
                headers=self._get_auth_header(),
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                success_count = data.get("updated", 0)
                self.stats["fallback_updates"] += success_count
                logger.info(f"Bulk API update: {success_count}/{len(updates)} successful")
                return success_count
                
        except Exception as e:
            logger.warning(f"Bulk update failed, falling back to individual: {e}")
        
        # Fall back to individual updates
        success_count = 0
        for update in updates:
            if await self.update_message_status(update):
                success_count += 1
            await asyncio.sleep(0.01)  # Small delay to avoid rate limiting
        
        return success_count
    
    async def check_health(self) -> Tuple[bool, str]:
        """
        Check if Django API is reachable and authenticated.
        
        Returns:
            Tuple of (is_healthy, status_message)
        """
        try:
            token = await self.get_valid_token()
            
            response = await self.client.get(
                f"{self.base_url}/api/health/",
                headers=self._get_auth_header(),
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
    
    async def get_campaign_progress(self, campaign_id: int) -> Optional[Dict]:
        """
        Get campaign progress via API.
        
        Returns:
            Campaign progress data or None if failed
        """
        try:
            token = await self.get_valid_token()
            
            response = await self.client.get(
                f"{self.base_url}/api/campaigns/{campaign_id}/progress/",
                headers=self._get_auth_header()
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Failed to get campaign progress: {response.status_code}")
                return None
                
        except Exception as e:
            logger.error(f"Error getting campaign progress: {e}")
            return None
    
    async def mark_campaign_complete(self, campaign_id: int) -> bool:
        """
        Mark campaign as complete via API.
        
        Returns:
            True if successful
        """
        try:
            token = await self.get_valid_token()
            
            response = await self.client.post(
                f"{self.base_url}/api/campaigns/{campaign_id}/complete/",
                headers=self._get_auth_header(),
                json={"reason": "All messages processed"}
            )
            
            if response.status_code in (200, 202):
                logger.info(f"Campaign {campaign_id} marked as complete")
                return True
            else:
                logger.warning(f"Failed to mark campaign complete: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error marking campaign complete: {e}")
            return False