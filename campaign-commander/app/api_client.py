"""External API client with token management."""
import time
import asyncio
import logging
from typing import Optional, Dict, Any, Tuple

import httpx

from .config import config

logger = logging.getLogger(config.SERVICE_NAME)

class APIClient:
    """Client for external Campaign Manager API with token refresh."""
    
    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        self.client = httpx.AsyncClient(timeout=10.0)
    
    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
    
    async def initialize(self) -> bool:
        """Get initial token on startup."""
        try:
            await self._get_new_token()
            logger.info("API Client initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to initialize API Client: {e}")
            return False
    
    async def get_valid_token(self) -> str:
        """Get a valid token, refreshing if expired."""
        async with self.lock:
            current_time = time.time()
            
            # Refresh if token expires soon
            if not self.access_token or current_time >= self.token_expiry - config.TOKEN_EXPIRY_BUFFER:
                logger.info("Token expiring soon, refreshing...")
                await self._refresh_token()
                
            return self.access_token
    
    async def _get_new_token(self) -> None:
        """Get new token with username/password."""
        try:
            response = await self.client.post(
                f"{self.base_url}/api/token/",
                json={
                    "username": self.username,
                    "password": self.password
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                self.access_token = data['access']
                self.refresh_token = data.get('refresh')
                # JWT tokens typically expire in 5 minutes (300 seconds)
                self.token_expiry = time.time() + 300 - config.TOKEN_REFRESH_EARLY
                logger.debug("New token obtained")
            else:
                logger.error(f"Failed to get token: {response.status_code}")
                raise Exception(f"Token acquisition failed: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"Token acquisition error: {e}")
            raise
    
    async def _refresh_token(self) -> None:
        """Refresh the access token."""
        try:
            if self.refresh_token:
                response = await self.client.post(
                    f"{self.base_url}/api/token/refresh/",
                    json={"refresh": self.refresh_token}
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['access']
                    self.token_expiry = time.time() + 300 - config.TOKEN_REFRESH_EARLY
                    logger.debug("Token refreshed successfully")
                    return
            
            # Fall back to new token
            logger.warning("Refresh failed, getting new token")
            await self._get_new_token()
                    
        except Exception as e:
            logger.error(f"Token refresh error: {e}")
            # Try to get new token as fallback
            try:
                await self._get_new_token()
            except:
                pass
    
    def _get_auth_headers(self, correlation_id: str) -> dict:
        """Get authorization headers for requests."""
        return {
            "Authorization": f"Bearer {self.access_token}",
            "X-Correlation-ID": correlation_id,
            "Content-Type": "application/json"
        }
    
    async def get(self, endpoint: str, correlation_id: str) -> Tuple[Optional[Dict], Optional[str]]:
        """Make authenticated GET request."""
        try:
            token = await self.get_valid_token()
            url = f"{self.base_url}{endpoint}"
            headers = self._get_auth_headers(correlation_id)
            
            response = await self.client.get(url, headers=headers)
            
            if response.status_code == 200:
                return response.json(), None
            elif response.status_code == 401:
                # Force token refresh on next request
                self.token_expiry = 0
                return None, "Authentication failed"
            elif response.status_code == 404:
                return None, "Resource not found"
            else:
                return None, f"API returned status {response.status_code}"
                
        except httpx.ConnectError:
            return None, "Cannot connect to Campaign Manager"
        except Exception as e:
            logger.error(f"GET request error: {e}")
            return None, str(e)
    
    async def check_health(self) -> Tuple[bool, str]:
        """Check API health."""
        try:
            token = await self.get_valid_token()
            headers = self._get_auth_headers("health-check")
            response = await self.client.get(
                f"{self.base_url}/api/campaigns/",
                headers=headers,
                timeout=2.0
            )
            return response.status_code == 200, "connected" if response.status_code == 200 else "error"
        except Exception:
            return False, "disconnected"