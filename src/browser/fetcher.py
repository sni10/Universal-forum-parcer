"""Browser-based HTML fetcher using undetected Chrome WebDriver.

This module provides functionality to fetch HTML from forum pages
using Chrome WebDriver with anti-bot protection bypass.
"""
import time
from typing import Optional
from pathlib import Path

try:
    import undetected_chromedriver as uc
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import TimeoutException, WebDriverException
except ImportError:
    uc = None
    By = None
    WebDriverWait = None
    EC = None
    TimeoutException = None
    WebDriverException = None


class ChromeFetcher:
    """
    Chrome-based HTML fetcher using undetected-chromedriver.
    
    Features:
    - Uses existing Chrome installation
    - Supports custom Chrome profiles (with ad blockers, etc.)
    - Anti-bot protection bypass
    - Explicit waits for content loading
    """
    
    def __init__(
        self,
        profile_path: Optional[str] = None,
        headless: bool = False,
        chrome_binary: Optional[str] = None,
        logger=None
    ):
        """
        Initialize Chrome fetcher.
        
        Args:
            profile_path: Path to Chrome user profile directory
            headless: Run in headless mode (default: False)
            chrome_binary: Path to Chrome binary (optional, auto-detected if None)
            logger: Logger instance
        """
        if uc is None:
            raise ImportError(
                "undetected-chromedriver is not installed. "
                "Install it with: pip install undetected-chromedriver"
            )
        
        self.profile_path = profile_path
        self.headless = headless
        self.chrome_binary = chrome_binary
        self.logger = logger
        self.driver: Optional[uc.Chrome] = None
        self._initialized = False
    
    def initialize(self) -> None:
        """
        Initialize Chrome WebDriver.
        
        Raises:
            WebDriverException: If Chrome initialization fails
        """
        if self._initialized and self.driver:
            return
        
        if self.logger:
            self.logger.info("Initializing Chrome WebDriver...")
        
        try:
            options = uc.ChromeOptions()
            
            # Use custom profile if specified
            if self.profile_path:
                profile_path = Path(self.profile_path).resolve()
                if profile_path.exists():
                    options.add_argument(f"--user-data-dir={profile_path}")
                    if self.logger:
                        self.logger.info(f"Using Chrome profile: {profile_path}")
                else:
                    if self.logger:
                        self.logger.warning(f"Profile path not found: {profile_path}")
            
            # Headless mode
            if self.headless:
                options.add_argument("--headless=new")
                if self.logger:
                    self.logger.info("Running in headless mode")
            
            # Additional options for stability
            options.add_argument("--disable-blink-features=AutomationControlled")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--no-sandbox")
            
            # Initialize driver
            driver_kwargs = {"options": options}
            if self.chrome_binary:
                driver_kwargs["browser_executable_path"] = self.chrome_binary
            
            self.driver = uc.Chrome(**driver_kwargs)
            self._initialized = True
            
            if self.logger:
                self.logger.info("Chrome WebDriver initialized successfully")
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to initialize Chrome WebDriver: {e}")
            raise
    
    def get_page_html(self, url: str, wait_for_selector: str = "div.postbody", timeout: int = 30) -> str:
        """
        Fetch HTML from a page URL.
        
        Args:
            url: Page URL to fetch
            wait_for_selector: CSS selector to wait for (ensures content is loaded)
            timeout: Maximum wait time in seconds
            
        Returns:
            HTML content as string
            
        Raises:
            WebDriverException: If page loading fails
            TimeoutException: If page loading times out
        """
        if not self._initialized or not self.driver:
            self.initialize()
        
        if self.logger:
            self.logger.info(f"Fetching page: {url}")
        
        try:
            # Load page
            self.driver.get(url)
            
            # Wait for content to load
            if self.logger:
                self.logger.debug(f"Waiting for selector: {wait_for_selector}")
            
            WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, wait_for_selector))
            )
            
            # Small additional delay to ensure all content is loaded
            time.sleep(1)
            
            # Get page source
            html = self.driver.page_source
            
            if self.logger:
                self.logger.info(f"Successfully fetched page ({len(html)} bytes)")
            
            return html
        
        except TimeoutException:
            if self.logger:
                self.logger.error(f"Timeout waiting for content: {url}")
            raise
        
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error fetching page {url}: {e}")
            raise
    
    def find_pagination_links(self, html: str) -> list[str]:
        """
        Extract pagination links from HTML.
        
        Args:
            html: HTML content
            
        Returns:
            List of pagination URLs (relative URLs)
        """
        from bs4 import BeautifulSoup
        
        soup = BeautifulSoup(html, "lxml")
        links = []
        
        # Find pagination container
        nav = soup.select_one("nav[aria-label='Page navigation']")
        if not nav:
            return links
        
        # Extract all links from pagination
        for a in nav.select("a.page-link"):
            href = a.get("href", "").strip()
            if href and href != "#":
                # Clean up href (remove ./ prefix)
                if href.startswith("./"):
                    href = href[2:]
                links.append(href)
        
        return list(dict.fromkeys(links))  # Remove duplicates while preserving order
    
    def close(self) -> None:
        """Close browser and clean up resources."""
        if self.driver:
            try:
                if self.logger:
                    self.logger.info("Closing Chrome WebDriver...")
                self.driver.quit()
                if self.logger:
                    self.logger.info("Chrome WebDriver closed")
            except Exception as e:
                if self.logger:
                    self.logger.warning(f"Error closing Chrome WebDriver: {e}")
            finally:
                self.driver = None
                self._initialized = False
    
    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
