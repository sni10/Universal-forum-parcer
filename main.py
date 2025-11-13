"""
Image Loader - Forum post image downloader

Usage:
    python main.py                          # Use URL from .env configuration
    python main.py <html_file>              # Process HTML file
    python main.py --html "<raw_html>"      # Process HTML string
    
Examples:
    python main.py                          # Fetch and process from configured forum URL
    python main.py page.html                # Process saved HTML file
    python main.py --html "<div>...</div>"  # Process HTML string directly
"""
import asyncio
import sys
from pathlib import Path

from src.app import Orchestrator
from src.config import Config
from src.log import setup_logger


async def process_html_file(html_file: Path, orchestrator: Orchestrator, logger):
    """Process HTML from a file."""
    logger.info(f"Reading HTML from: {html_file}")
    
    if not html_file.exists():
        logger.error(f"File not found: {html_file}")
        return
    
    html = html_file.read_text(encoding="utf-8")
    
    # Process HTML
    results = await orchestrator.process_html(html, page_url=None)
    
    # Display results
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info("=" * 60)
    
    for result in results:
        logger.info(
            f"Block: {result['slug']}\n"
            f"  Title: {result['title']}\n"
            f"  Total links: {result['total_links']}\n"
            f"  Downloaded: {result['done']}\n"
            f"  Failed: {result['failed']}\n"
            f"  Queued: {result['queued']}"
        )
    
    logger.info("=" * 60)


async def process_html_string(html: str, orchestrator: Orchestrator, logger):
    """Process raw HTML string."""
    logger.info("Processing raw HTML string")
    
    # Process HTML
    results = await orchestrator.process_html(html, page_url=None)
    
    # Display results
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info("=" * 60)
    
    for result in results:
        logger.info(
            f"Block: {result['slug']}\n"
            f"  Title: {result['title']}\n"
            f"  Total links: {result['total_links']}\n"
            f"  Downloaded: {result['done']}\n"
            f"  Failed: {result['failed']}\n"
            f"  Queued: {result['queued']}"
        )
    
    logger.info("=" * 60)


async def process_url_mode(orchestrator: Orchestrator, logger):
    """Process using URL from configuration."""
    logger.info("Using URL mode from configuration")
    
    # Validate required configuration
    if not Config.FORUM_DOMAIN or not Config.START_URL:
        logger.error("URL mode requires FORUM_DOMAIN and START_URL in .env")
        logger.error("Please configure these settings or provide an HTML file instead")
        sys.exit(1)
    
    # Get Chrome settings from environment
    chrome_profile = Config.get_chrome_profile()
    headless = Config.get_chrome_headless()
    
    # Process URL with pagination
    results = await orchestrator.process_url(
        domain=Config.FORUM_DOMAIN,
        start_url=Config.START_URL,
        page_param=Config.PAGE_PARAM,
        posts_per_page=15,  # Standard forum setting
        chrome_profile=chrome_profile,
        headless=headless
    )
    
    # Display results
    logger.info("=" * 60)
    logger.info("Processing complete!")
    logger.info("=" * 60)
    logger.info(f"Total blocks processed: {len(results)}")
    
    for result in results:
        logger.info(
            f"Block: {result['slug']}\n"
            f"  Title: {result['title']}\n"
            f"  Total links: {result['total_links']}\n"
            f"  Downloaded: {result['done']}\n"
            f"  Failed: {result['failed']}\n"
            f"  Queued: {result['queued']}"
        )
    
    logger.info("=" * 60)


def main():
    """Main entry point."""
    # Setup logger
    logger = setup_logger(
        name="loader",
        log_dir=Config.LOGS_DIR,
        level=Config.get_log_level(),
        max_bytes=Config.LOG_MAX_BYTES,
        backup_count=Config.LOG_BACKUP_COUNT
    )
    
    logger.info("=" * 60)
    logger.info("Image Loader Starting")
    logger.info("=" * 60)
    
    # Display configuration
    Config.display()
    
    # Initialize orchestrator
    orchestrator = Orchestrator(
        db_path=Config.DB_PATH,
        img_dir=Config.IMG_DIR,
        links_dir=Config.LINKS_DIR,
        logger=logger
    )
    
    try:
        # Parse command line arguments
        if len(sys.argv) < 2:
            # No arguments: Use URL mode from configuration
            asyncio.run(process_url_mode(orchestrator, logger))
        
        elif sys.argv[1] == "--html":
            # HTML string mode
            if len(sys.argv) < 3:
                logger.error("--html requires HTML string argument")
                sys.exit(1)
            
            html = sys.argv[2]
            asyncio.run(process_html_string(html, orchestrator, logger))
        
        else:
            # File mode
            html_file = Path(sys.argv[1])
            asyncio.run(process_html_file(html_file, orchestrator, logger))
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit(1)
    
    finally:
        orchestrator.close()
        logger.info("Image Loader finished")


if __name__ == "__main__":
    main()
