"""Token bucket rate limiter for controlling TPS."""
import time
import asyncio
import logging

logger = logging.getLogger("sms-sender")

class TokenBucketRateLimiter:
    """
    Token bucket algorithm for rate limiting.
    
    Features:
    - Thread-safe with asyncio lock
    - Configurable rate (tokens per second)
    - Configurable capacity (burst allowance)
    - Returns wait time if tokens unavailable
    """
    
    def __init__(self, rate: int, capacity: int = None):
        """
        Initialize rate limiter.
        
        Args:
            rate: Tokens per second (e.g., 4000 TPS)
            capacity: Maximum tokens (defaults to rate)
        """
        self.rate = rate
        self.capacity = capacity or rate
        self.tokens = self.capacity
        self.last_refill = time.time()
        self.lock = asyncio.Lock()
        
        logger.info(f"Rate limiter initialized: {rate} TPS, capacity: {self.capacity}")
    
    async def acquire(self, tokens: int = 1) -> float:
        """
        Acquire tokens from the bucket.
        
        Args:
            tokens: Number of tokens needed
            
        Returns:
            Wait time in seconds (0 if tokens available immediately)
        """
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_refill
            
            # Add new tokens based on elapsed time
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            self.last_refill = now
            
            if self.tokens >= tokens:
                # Enough tokens available
                self.tokens -= tokens
                return 0
            else:
                # Need to wait for more tokens
                wait_time = (tokens - self.tokens) / self.rate
                return wait_time
    
    async def wait_and_acquire(self, tokens: int = 1):
        """Wait until tokens are available and acquire them."""
        wait_time = await self.acquire(tokens)
        if wait_time > 0:
            logger.debug(f"Rate limiting: waiting {wait_time:.3f}s for {tokens} tokens")
            await asyncio.sleep(wait_time)