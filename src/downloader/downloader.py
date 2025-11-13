"""Async image downloader."""
import asyncio
import logging
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import aiofiles
import aiohttp

from ..domain import ImageLink, LinkStatus
from ..fs import sanitize_filename
from .limiter import HostLimiter


class ImageDownloader:
    """Async downloader for images with retry logic and resume capability."""
    
    def __init__(
        self,
        global_limit: int = 10,
        per_host_limit: int = 3,
        max_retries: int = 5,
        timeout: int = 300,
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize downloader.
        
        Args:
            global_limit: Max concurrent downloads globally
            per_host_limit: Max concurrent downloads per host
            max_retries: Maximum retry attempts
            timeout: Timeout in seconds for downloads
            logger: Logger instance
        """
        self.global_limit = global_limit
        self.max_retries = max_retries
        self.timeout = timeout
        self.logger = logger or logging.getLogger("loader")
        
        self.host_limiter = HostLimiter(per_host_limit)
        self.global_semaphore = asyncio.Semaphore(global_limit)
    
    async def download_image(
        self,
        session: aiohttp.ClientSession,
        link: ImageLink,
        output_path: Path
    ) -> tuple[bool, Optional[str]]:
        """
        Download a single image.
        
        Args:
            session: aiohttp session
            link: ImageLink to download
            output_path: Output file path
            
        Returns:
            Tuple of (success, error_message)
        """
        # Ensure output directory exists
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        part_path = Path(str(output_path) + ".part")
        
        # Check if we can resume
        resume_from = 0
        if part_path.exists():
            resume_from = part_path.stat().st_size
        
        attempt = 0
        backoff = 1.0
        
        while attempt < self.max_retries:
            try:
                # Prepare headers
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
                
                if resume_from > 0:
                    headers["Range"] = f"bytes={resume_from}-"
                    self.logger.info(f"Resuming download from byte {resume_from}: {link.url}")
                
                # Make request
                async with session.get(
                    link.url,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    # Handle rate limiting
                    if response.status in (429, 503):
                        retry_after = int(response.headers.get("Retry-After", 0) or 0)
                        if retry_after > 0:
                            self.logger.warning(
                                f"Rate limited, waiting {retry_after}s: {link.url}"
                            )
                            await asyncio.sleep(retry_after)
                        else:
                            # Use exponential backoff
                            sleep_time = backoff + (os.urandom(1)[0] / 255.0)
                            self.logger.warning(
                                f"Rate limited, waiting {sleep_time:.2f}s: {link.url}"
                            )
                            await asyncio.sleep(sleep_time)
                        
                        attempt += 1
                        backoff = min(backoff * 2, 60)
                        continue
                    
                    # Check status
                    response.raise_for_status()
                    
                    # Validate Content-Type
                    content_type = response.headers.get("Content-Type", "")
                    if not content_type.startswith("image/"):
                        return False, f"Invalid content type: {content_type}"
                    
                    # Get metadata
                    content_length = response.headers.get("Content-Length")
                    etag = response.headers.get("ETag")
                    last_modified = response.headers.get("Last-Modified")
                    
                    # Update link metadata
                    if content_length:
                        link.size = int(content_length)
                    if etag:
                        link.etag = etag
                    if last_modified:
                        link.last_modified = last_modified
                    
                    # Write to file
                    mode = "ab" if resume_from > 0 else "wb"
                    async with aiofiles.open(part_path, mode) as f:
                        async for chunk in response.content.iter_chunked(64 * 1024):
                            await f.write(chunk)
                
                # Validate file size if Content-Length was provided
                if link.size:
                    actual_size = part_path.stat().st_size
                    if actual_size != link.size:
                        self.logger.warning(
                            f"Size mismatch: expected {link.size}, got {actual_size}: {link.url}"
                        )
                        # Don't fail, just log
                
                # Atomic move
                os.replace(part_path, output_path)
                
                self.logger.info(f"Downloaded: {link.url} -> {output_path.name}")
                return True, None
                
            except aiohttp.ClientError as e:
                attempt += 1
                error_msg = f"{type(e).__name__}: {str(e)}"
                
                if attempt >= self.max_retries:
                    self.logger.error(
                        f"Download failed after {attempt} attempts: {link.url} - {error_msg}"
                    )
                    return False, error_msg
                
                # Exponential backoff with jitter
                sleep_time = backoff + (os.urandom(1)[0] / 255.0)
                self.logger.warning(
                    f"Download attempt {attempt} failed, retrying in {sleep_time:.2f}s: {link.url}"
                )
                await asyncio.sleep(sleep_time)
                backoff = min(backoff * 2, 60)
                
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                self.logger.error(f"Unexpected error downloading {link.url}: {error_msg}")
                return False, error_msg
        
        return False, f"Max retries ({self.max_retries}) exceeded"
    
    async def download_link(
        self,
        session: aiohttp.ClientSession,
        link: ImageLink,
        output_dir: Path,
        status_callback: Optional[callable] = None
    ) -> None:
        """
        Download a link with global and per-host limiting.
        
        Args:
            session: aiohttp session
            link: ImageLink to download
            output_dir: Output directory
            status_callback: Optional callback to update link status
        """
        async with self.global_semaphore:
            async with self.host_limiter.limit(link.url):
                # Generate filename from URL
                parsed = urlparse(link.url)
                filename = Path(parsed.path).name
                filename = sanitize_filename(filename)
                
                if not filename:
                    filename = f"image_{abs(hash(link.url))}.jpg"
                
                output_path = output_dir / filename
                link.filename = filename
                
                # Update status to downloading
                if status_callback:
                    status_callback(
                        link,
                        LinkStatus.DOWNLOADING,
                        filename=filename
                    )
                
                # Download
                success, error = await self.download_image(session, link, output_path)
                
                # Update status
                if success:
                    if status_callback:
                        status_callback(
                            link,
                            LinkStatus.DONE,
                            filename=filename,
                            size=link.size,
                            etag=link.etag,
                            last_modified=link.last_modified
                        )
                else:
                    if status_callback:
                        status_callback(
                            link,
                            LinkStatus.FAILED,
                            error=error,
                            increment_retries=True
                        )
    
    async def download_links(
        self,
        links: list[ImageLink],
        output_dir: Path,
        status_callback: Optional[callable] = None
    ) -> dict[str, int]:
        """
        Download multiple links concurrently.
        
        Args:
            links: List of ImageLink objects
            output_dir: Output directory
            status_callback: Optional callback to update link status
            
        Returns:
            Dictionary with download statistics
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create session with custom headers
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=self.global_limit)
        
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        ) as session:
            # Create tasks
            tasks = [
                self.download_link(session, link, output_dir, status_callback)
                for link in links
            ]
            
            # Execute all downloads
            await asyncio.gather(*tasks, return_exceptions=True)
        
        # Calculate statistics
        stats = {
            "total": len(links),
            "done": sum(1 for link in links if link.status == LinkStatus.DONE),
            "failed": sum(1 for link in links if link.status == LinkStatus.FAILED),
            "pending": sum(1 for link in links if link.status in (LinkStatus.NEW, LinkStatus.QUEUED))
        }
        
        return stats
