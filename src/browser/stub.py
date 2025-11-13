"""Browser automation stub for future expansion.

Currently not needed as links lead directly to JPG files.
This stub is a placeholder for future scenarios where:
- Links lead to external pages requiring JavaScript rendering
- Pages require login/authentication
- Need to extract image URLs from dynamically loaded content
- Anti-bot protections require real browser behavior

Future implementation might use:
- Selenium with Chrome WebDriver
- undetected-chromedriver for anti-bot bypass
- Chrome user profile for persistent sessions
"""
from typing import Optional


class BrowserStub:
    """
    Stub for browser automation.
    
    This is a placeholder for future expansion when the parser
    needs to handle links that don't lead directly to images.
    """
    
    def __init__(
        self,
        profile_path: Optional[str] = None,
        headless: bool = False
    ):
        """
        Initialize browser stub.
        
        Args:
            profile_path: Path to Chrome profile (for future use)
            headless: Run in headless mode (for future use)
        """
        self.profile_path = profile_path
        self.headless = headless
        self._initialized = False
    
    def initialize(self) -> None:
        """
        Initialize browser (not implemented).
        
        Future implementation would:
        - Set up Selenium WebDriver
        - Configure Chrome options
        - Load user profile
        """
        # TODO: Implement when needed
        self._initialized = True
    
    def get_page_html(self, url: str) -> str:
        """
        Get page HTML (not implemented).
        
        Args:
            url: Page URL
            
        Returns:
            HTML content
            
        Raises:
            NotImplementedError: This functionality is not yet needed
        """
        raise NotImplementedError(
            "Browser functionality not implemented. "
            "Current implementation uses direct HTTP requests. "
            "Implement this when links require browser automation."
        )
    
    def get_image_from_page(self, page_url: str) -> Optional[str]:
        """
        Extract image URL from external page (not implemented).
        
        Args:
            page_url: External page URL
            
        Returns:
            Direct image URL or None
            
        Raises:
            NotImplementedError: This functionality is not yet needed
        """
        raise NotImplementedError(
            "Browser-based image extraction not implemented. "
            "Current implementation expects direct image links."
        )
    
    def close(self) -> None:
        """
        Close browser (not implemented).
        
        Future implementation would clean up WebDriver resources.
        """
        self._initialized = False
    
    def __enter__(self):
        """Context manager entry."""
        self.initialize()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
