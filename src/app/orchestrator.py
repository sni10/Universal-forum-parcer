"""Main orchestrator for coordinating all components."""
import asyncio
import logging
from pathlib import Path
from typing import Optional

from ..domain import PostBlock, LinkStatus
from ..downloader import ImageDownloader
from ..fs import ensure_directory
from ..parser import LinkExtractor
from ..storage import LinkRepository


class Orchestrator:
    """Coordinates parsing, storage, and downloading."""
    
    def __init__(
        self,
        db_path: str | Path = "data/loader.db",
        img_dir: str | Path = "img",
        links_dir: str | Path = "links",
        logger: Optional[logging.Logger] = None
    ):
        """
        Initialize orchestrator.
        
        Args:
            db_path: Path to SQLite database
            img_dir: Root directory for images
            links_dir: Root directory for link exports
            logger: Logger instance
        """
        self.db_path = Path(db_path)
        self.img_dir = Path(img_dir)
        self.links_dir = Path(links_dir)
        self.logger = logger or logging.getLogger("loader")
        
        # Initialize components
        self.repository = LinkRepository(db_path)
        self.extractor = LinkExtractor()
        self.downloader = ImageDownloader(logger=self.logger)
        
        # Ensure directories exist
        ensure_directory(self.img_dir)
        ensure_directory(self.links_dir)
        
        # Recover from crashes
        self._recover_from_crash()
    
    def _recover_from_crash(self) -> None:
        """Recover links and pages stuck in processing states."""
        # Recover links stuck in 'downloading' status
        recovered_links = self.repository.recover_downloading_links()
        if recovered_links > 0:
            self.logger.info(f"Recovered {recovered_links} links from interrupted downloads")
        
        # Recover pages stuck in 'processing' status
        recovered_pages = self.repository.recover_processing_pages()
        if recovered_pages > 0:
            self.logger.info(f"Recovered {recovered_pages} pages from interrupted processing")
    
    def _get_next_block_number(self) -> int:
        """
        Get next block number for incremental naming.
        
        Returns:
            Next block number (1, 2, 3, ...)
        """
        # Count existing directories in img/
        existing_dirs = [
            d for d in self.img_dir.iterdir()
            if d.is_dir() and d.name[:4].isdigit()
        ]
        return len(existing_dirs) + 1
    
    def _format_slug_with_number(self, slug: str, number: int) -> str:
        """
        Format slug with incremental number prefix.
        
        Args:
            slug: Base slug
            number: Block number
            
        Returns:
            Formatted slug like "0001_slug"
        """
        return f"{number:04d}_{slug}"
    
    def _create_status_callback(self, block_id: int):
        """
        Create status update callback for downloader.
        
        Args:
            block_id: Block ID
            
        Returns:
            Callback function
        """
        def callback(link, status, **kwargs):
            if link.link_id:
                self.repository.update_link_status(
                    link.link_id,
                    status,
                    filename=kwargs.get("filename"),
                    size=kwargs.get("size"),
                    etag=kwargs.get("etag"),
                    last_modified=kwargs.get("last_modified"),
                    error=kwargs.get("error"),
                    increment_retries=kwargs.get("increment_retries", False)
                )
        return callback
    
    async def process_block(self, block: PostBlock, page_url: Optional[str] = None) -> dict:
        """
        Process a single block: store, download, export.
        
        Args:
            block: PostBlock to process
            page_url: Optional page URL for referer
            
        Returns:
            Statistics dictionary
        """
        # Get next block number
        block_number = self._get_next_block_number()
        numbered_slug = self._format_slug_with_number(block.slug, block_number)
        
        self.logger.info(
            f"Processing block {block_number}: {block.title} "
            f"({len(block.links)} links)"
        )
        
        # Check if block already exists
        existing_block = self.repository.get_block_by_slug(numbered_slug)
        if existing_block:
            self.logger.info(f"Block already exists: {numbered_slug}, adding new links")
            block_id = existing_block.block_id
            
            # Set referer for all links
            for link in block.links:
                if page_url and not link.referer:
                    link.referer = page_url
                link.status = LinkStatus.QUEUED
            
            # Add links to database (UNIQUE constraint prevents duplicates)
            self.repository.add_links(block_id, block.links)
            
            # Reload block to get all links (existing + new)
            block = self.repository.get_block_by_slug(numbered_slug)
        else:
            # Save original links BEFORE creating block in database
            original_links = block.links
            
            # Create block in database
            db_block = self.repository.create_block(block.title, numbered_slug)
            
            # Set referer for all links from original block
            for link in original_links:
                if page_url and not link.referer:
                    link.referer = page_url
                link.status = LinkStatus.QUEUED
            
            # Add links to database
            self.repository.add_links(db_block.block_id, original_links)
            
            # Reload block to get link IDs
            block = self.repository.get_block_by_slug(numbered_slug)
        
        # Create output directories
        img_output = self.img_dir / numbered_slug
        links_output = self.links_dir / numbered_slug
        ensure_directory(img_output)
        ensure_directory(links_output)
        
        # Get queued links
        queued_links = [
            link for link in block.links
            if link.status in (LinkStatus.QUEUED, LinkStatus.NEW)
        ]
        
        if not queued_links:
            self.logger.info(f"No links to download for block {numbered_slug}")
        else:
            self.logger.info(f"Downloading {len(queued_links)} images for block {numbered_slug}")
            
            # Download images
            status_callback = self._create_status_callback(block.block_id)
            stats = await self.downloader.download_links(
                queued_links,
                img_output,
                status_callback
            )
            
            self.logger.info(
                f"Block {numbered_slug} complete: "
                f"{stats['done']} done, {stats['failed']} failed, "
                f"{stats['pending']} pending"
            )
        
        # Reload block with updated statuses
        block = self.repository.get_block_by_slug(numbered_slug)
        
        # Export links to JSONL
        self.repository.export_links_jsonl(block, links_output)
        
        # Return statistics
        return {
            "block_id": block.block_id,
            "slug": numbered_slug,
            "title": block.title,
            "total_links": len(block.links),
            "done": sum(1 for link in block.links if link.status == LinkStatus.DONE),
            "failed": sum(1 for link in block.links if link.status == LinkStatus.FAILED),
            "queued": sum(1 for link in block.links if link.status == LinkStatus.QUEUED)
        }
    
    async def process_html(self, html: str, page_url: Optional[str] = None) -> list[dict]:
        """
        Process HTML page: extract blocks and download images.
        
        Args:
            html: HTML content
            page_url: Optional page URL for referer
            
        Returns:
            List of statistics for each block
        """
        # Extract blocks from HTML
        blocks = self.extractor.extract_blocks(html, page_url)
        
        self.logger.info(f"Extracted {len(blocks)} blocks from HTML")
        
        # Process each block
        results = []
        for block in blocks:
            stats = await self.process_block(block, page_url)
            results.append(stats)
        
        return results
    
    async def process_url(
        self,
        domain: str,
        start_url: str,
        page_param: str = "start",
        posts_per_page: int = 15,
        chrome_profile: Optional[str] = None,
        headless: bool = False,
        max_pages: Optional[int] = None
    ) -> list[dict]:
        """
        Fetch and process forum pages with pagination support.
        
        Args:
            domain: Forum domain (e.g., "https://forum.com")
            start_url: Starting URL path (e.g., "/viewtopic.php?f=9&t=10160660")
            page_param: Pagination parameter name ("start", "page", or "p")
            posts_per_page: Number of posts per page (default: 15)
            chrome_profile: Path to Chrome profile (optional)
            headless: Run Chrome in headless mode (default: False)
            max_pages: Maximum number of pages to process (optional)
            
        Returns:
            List of statistics for all blocks processed
        """
        from ..browser import ChromeFetcher, PaginationManager
        
        self.logger.info("=" * 60)
        self.logger.info("Starting URL-based processing with pagination")
        self.logger.info(f"Domain: {domain}")
        self.logger.info(f"Start URL: {start_url}")
        self.logger.info(f"Page parameter: {page_param}")
        self.logger.info("=" * 60)
        
        # Initialize pagination manager
        pagination_mgr = PaginationManager(
            domain=domain,
            start_url=start_url,
            page_param=page_param,
            posts_per_page=posts_per_page,
            logger=self.logger
        )
        
        # Check for resumption
        last_page = self.repository.get_last_processed_page()
        if last_page:
            start_page_num = last_page["page_number"] + 1
            self.logger.info(
                f"Resuming from page {start_page_num} "
                f"(last processed: {last_page['page_number']})"
            )
        else:
            start_page_num = 0
            self.logger.info("Starting from first page")
        
        # Initialize Chrome fetcher
        fetcher = ChromeFetcher(
            profile_path=chrome_profile,
            headless=headless,
            logger=self.logger
        )
        
        all_results = []
        current_page_num = start_page_num
        pages_processed = 0
        
        try:
            fetcher.initialize()
            
            while True:
                # Check max pages limit
                if max_pages and pages_processed >= max_pages:
                    self.logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                # Generate page URL
                page_url = pagination_mgr.get_page_url(current_page_num)
                
                self.logger.info("=" * 60)
                self.logger.info(f"Processing page {current_page_num}: {page_url}")
                self.logger.info("=" * 60)
                
                # Create or get page record
                self.repository.create_page(page_url, current_page_num, status="new")
                self.repository.update_page_status(page_url, "processing")
                
                try:
                    # Fetch page HTML
                    html = fetcher.get_page_html(page_url)
                    
                    # Extract pagination links
                    pagination_links = fetcher.find_pagination_links(html)
                    
                    # Process HTML (extract blocks and download images)
                    page_results = await self.process_html(html, page_url)
                    all_results.extend(page_results)
                    
                    # Update page status
                    self.repository.update_page_status(
                        page_url,
                        "done",
                        blocks_found=len(page_results)
                    )
                    
                    self.logger.info(
                        f"Page {current_page_num} complete: "
                        f"{len(page_results)} blocks processed"
                    )
                    
                    # Find next page
                    next_url = pagination_mgr.find_next_page(page_url, pagination_links)
                    
                    if not next_url:
                        self.logger.info("No more pages found in pagination")
                        break
                    
                    # Extract next page number
                    next_page_num = pagination_mgr.extract_page_number(next_url)
                    
                    # Sanity check: ensure we're moving forward
                    if next_page_num <= current_page_num:
                        self.logger.warning(
                            f"Next page number {next_page_num} <= current {current_page_num}, "
                            "stopping to avoid infinite loop"
                        )
                        break
                    
                    current_page_num = next_page_num
                    pages_processed += 1
                
                except Exception as e:
                    self.logger.error(f"Error processing page {current_page_num}: {e}")
                    self.repository.update_page_status(
                        page_url,
                        "failed",
                        error=str(e)
                    )
                    # Continue to next page on error
                    current_page_num += 1
                    pages_processed += 1
        
        finally:
            fetcher.close()
        
        self.logger.info("=" * 60)
        self.logger.info(f"URL processing complete: {pages_processed} pages processed")
        self.logger.info(f"Total blocks: {len(all_results)}")
        self.logger.info("=" * 60)
        
        return all_results
    
    def close(self) -> None:
        """Close resources."""
        self.repository.close()
