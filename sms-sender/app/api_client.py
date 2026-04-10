"""3rd party SMS API client for high-throughput sending."""
import uuid
import logging
from typing import List

import httpx

from .config import config
from .models import SMSMessage, APIResponse

logger = logging.getLogger("sms-sender")

class ThirdPartyAPIClient:
    """
    Client for 3rd party SMS API optimized for 4000 TPS.
    
    Features:
    - Connection pooling for high throughput
    - Parallel batch sending
    - Comprehensive error handling
    - Retryable error detection
    """
    
    def __init__(self):
        self.api_url = config.API_URL
        self.api_key = config.API_KEY
        self.timeout = config.API_TIMEOUT
        
        # HTTP/2 support with connection pooling for high throughput
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout, connect=2.0),
            limits=httpx.Limits(
                max_keepalive_connections=100,
                max_connections=200,
                keepalive_expiry=30
            ),
            http2=True  # Enable HTTP/2 for better performance
        )
        
        logger.info(f"API client initialized: {self.api_url}")
        logger.info(f"  - Timeout: {self.timeout}s")
        logger.info(f"  - HTTP/2: Enabled")
        logger.info(f"  - Max connections: 200")
        
    async def send_batch(self, messages: List[SMSMessage]) -> List[APIResponse]:
        """
        Send batch of messages in parallel.
        
        Args:
            messages: List of messages to send
            
        Returns:
            List of APIResponse objects in same order as input
        """
        if not messages:
            return []
        
        logger.debug(f"Sending batch of {len(messages)} messages")
        
        # Create tasks for all messages
        tasks = [self._send_single(msg) for msg in messages]
        
        # Execute all requests in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        responses = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                # Handle unexpected exceptions
                logger.error(f"Unexpected error for message {messages[i].message_id}: {result}")
                responses.append(APIResponse(
                    message=messages[i],
                    success=False,
                    retryable=True,
                    error=str(result),
                    status_code=500
                ))
            else:
                responses.append(result)
        
        # Log summary
        success_count = sum(1 for r in responses if r.success)
        if success_count < len(messages):
            logger.warning(f"Batch partial success: {success_count}/{len(messages)} succeeded")
        
        return responses
    
    async def _send_single(self, message: SMSMessage) -> APIResponse:
        """
        Send single message to 3rd party API.
        
        Expected API response format:
        {
            "message_id": "provider-123",
            "status": "sent",
            ... other fields
        }
        """
        try:
            # Build request payload (customize based on your provider)
            payload = {
                "to": message.msisdn,
                "text": message.text,
                "message_id": message.message_id,
                "campaign_id": message.campaign_id,
                "timestamp": message.timestamp
            }
            
            # Add any provider-specific fields
            if message.language:
                payload["language"] = message.language
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "X-Message-ID": message.message_id,
                "X-Campaign-ID": str(message.campaign_id)
            }
            
            # Make the request
            response = await self.client.post(self.api_url, json=payload, headers=headers)
            
            if response.status_code == 200:
                # Success
                data = response.json()
                provider_message_id = (
                    data.get("message_id") or 
                    data.get("id") or 
                    data.get("msg_id") or 
                    str(uuid.uuid4())
                )
                
                return APIResponse(
                    message=message,
                    success=True,
                    provider_message_id=provider_message_id,
                    status_code=200
                )
                
            else:
                # Determine if error is retryable
                retryable = response.status_code in [429, 500, 502, 503, 504]
                
                return APIResponse(
                    message=message,
                    success=False,
                    retryable=retryable,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                    status_code=response.status_code
                )
                
        except httpx.TimeoutException:
            # Timeout is retryable
            return APIResponse(
                message=message,
                success=False,
                retryable=True,
                error="timeout",
                status_code=408
            )
        except httpx.NetworkError as e:
            # Network errors are retryable
            return APIResponse(
                message=message,
                success=False,
                retryable=True,
                error=f"network_error: {e}",
                status_code=503
            )
        except Exception as e:
            # Unexpected errors - retryable but log
            logger.error(f"Unexpected error sending to {message.msisdn}: {e}", exc_info=True)
            return APIResponse(
                message=message,
                success=False,
                retryable=True,
                error=str(e),
                status_code=500
            )
    
    async def close(self):
        """Close HTTP client session."""
        await self.client.aclose()
        logger.info("API client closed")