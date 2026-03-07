"""
Token Manager for Layer 1
Handles JWT authentication with Django backend
"""

import os
import time
import json
import httpx
import asyncio
import logging
from datetime import datetime
from typing import Optional, Dict

logger = logging.getLogger("sms-token-manager")

class TokenManager:
    """
    Manages JWT tokens for API authentication
    Automatically refreshes tokens when expired
    """
    
    def __init__(self, api_url: str, username: str, password: str):
        self.api_url = api_url
        self.username = username
        self.password = password
        self.access_token = None
        self.refresh_token = None
        self.token_expiry = 0
        self.lock = asyncio.Lock()
        
    async def initialize(self):
        """Get initial token on startup"""
        await self._get_new_token()
        logger.info(f"✅ TokenManager initialized - expires at: {datetime.fromtimestamp(self.token_expiry)}")
        
    async def get_valid_token(self) -> str:
        """Get a valid token (refresh if expired)"""
        async with self.lock:
            current_time = time.time()
            
            # If token expires in less than 60 seconds, refresh
            if current_time >= self.token_expiry - 60:
                logger.info("⏰ Token expiring soon, refreshing...")
                await self._refresh_token()
                
            return self.access_token
    
    async def _get_new_token(self):
        """Get new token with username/password"""
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{self.api_url}/api/token/",
                    json={
                        "username": self.username,
                        "password": self.password
                    },
                    timeout=10.0
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.access_token = data['access']
                    self.refresh_token = data.get('refresh')
                    
                    # Decode token to get expiry (simplified - just estimate)
                    # In production, you might want to actually decode the JWT
                    self.token_expiry = time.time() + 300  # Assume 5 minutes
                    
                    logger.info(f"✅ New token obtained")
                else:
                    logger.error(f"❌ Failed to get token: {response.status_code} - {response.text}")
                    raise Exception(f"Token acquisition failed: {response.status_code}")
                    
        except Exception as e:
            logger.error(f"❌ Token acquisition error: {e}")
            raise
    
    async def _refresh_token(self):
        """Refresh the access token"""
        try:
            async with httpx.AsyncClient() as client:
                if self.refresh_token:
                    # Use refresh token
                    response = await client.post(
                        f"{self.api_url}/api/token/refresh/",
                        json={"refresh": self.refresh_token},
                        timeout=10.0
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        self.access_token = data['access']
                        self.token_expiry = time.time() + 300  # Assume 5 minutes
                        logger.info(f"✅ Token refreshed successfully")
                    else:
                        # Refresh failed, get new token
                        logger.warning(f"⚠️ Refresh failed, getting new token")
                        await self._get_new_token()
                else:
                    # No refresh token, get new one
                    await self._get_new_token()
                    
        except Exception as e:
            logger.error(f"❌ Token refresh error: {e}")
            await self._get_new_token()
    
    def get_auth_header(self) -> Dict[str, str]:
        """Get authorization header for requests"""
        return {"Authorization": f"Bearer {self.access_token}"}

# Global instance
token_manager = None