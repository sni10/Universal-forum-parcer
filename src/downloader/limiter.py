"""Per-host rate limiter."""
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator
from urllib.parse import urlparse


class HostLimiter:
    """Manages per-host concurrent connection limits."""
    
    def __init__(self, per_host_limit: int = 3):
        """
        Initialize host limiter.
        
        Args:
            per_host_limit: Maximum concurrent connections per host
        """
        self.per_host_limit = per_host_limit
        self._locks: dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(self.per_host_limit)
        )
    
    @asynccontextmanager
    async def limit(self, url: str) -> AsyncIterator[None]:
        """
        Context manager to limit concurrent requests to a host.
        
        Args:
            url: URL being accessed
            
        Yields:
            None
        """
        hostname = urlparse(url).hostname or "default"
        semaphore = self._locks[hostname]
        
        async with semaphore:
            yield
