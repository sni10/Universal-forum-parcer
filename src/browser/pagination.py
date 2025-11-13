"""Pagination manager for forum page navigation.

Handles URL generation and pagination state tracking.
"""
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


class PaginationManager:
    """
    Manages forum pagination logic.
    
    Supports different pagination parameter types:
    - "start": Incremental offset (e.g., start=0, start=15, start=30)
    - "page": Page number (e.g., page=1, page=2, page=3)
    - "p": Short page number (e.g., p=1, p=2, p=3)
    """
    
    def __init__(
        self,
        domain: str,
        start_url: str,
        page_param: str = "start",
        posts_per_page: int = 15,
        logger=None
    ):
        """
        Initialize pagination manager.
        
        Args:
            domain: Forum domain (e.g., "https://forum.com")
            start_url: Starting URL path (e.g., "/viewtopic.php?f=9&t=10160660")
            page_param: Pagination parameter name ("start", "page", or "p")
            posts_per_page: Number of posts per page (default: 15)
            logger: Logger instance
        """
        self.domain = domain.rstrip("/")
        self.start_url = start_url
        self.page_param = page_param
        self.posts_per_page = posts_per_page
        self.logger = logger
        
        # Parse base URL
        self.base_url = self._parse_base_url()
    
    def _parse_base_url(self) -> str:
        """
        Parse and construct base URL without pagination parameter.
        
        Returns:
            Base URL string
        """
        # Ensure start_url begins with /
        if not self.start_url.startswith("/"):
            url_path = "/" + self.start_url
        else:
            url_path = self.start_url
        
        # Construct full URL
        full_url = self.domain + url_path
        
        # Parse URL
        parsed = urlparse(full_url)
        query_params = parse_qs(parsed.query, keep_blank_values=True)
        
        # Remove pagination parameter if present
        query_params.pop(self.page_param, None)
        
        # Reconstruct URL without pagination parameter
        new_query = urlencode(query_params, doseq=True)
        base_url = urlunparse((
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            new_query,
            ""
        ))
        
        return base_url
    
    def get_page_url(self, page_number: int) -> str:
        """
        Generate URL for a specific page number.
        
        Args:
            page_number: Page number (0-indexed or 1-indexed depending on page_param)
            
        Returns:
            Full page URL
        """
        if page_number <= 0:
            # First page - no pagination parameter needed
            return self.base_url
        
        # Calculate parameter value based on type
        if self.page_param == "start":
            # Incremental offset: page 1 = start=15, page 2 = start=30, etc.
            param_value = page_number * self.posts_per_page
        else:
            # Page number: page 1 = page=2 or p=2 (assuming first page is implicit or =1)
            param_value = page_number + 1
        
        # Add pagination parameter to URL
        separator = "&" if "?" in self.base_url else "?"
        page_url = f"{self.base_url}{separator}{self.page_param}={param_value}"
        
        return page_url
    
    def extract_page_number(self, url: str) -> int:
        """
        Extract page number from a URL.
        
        Args:
            url: Full or relative URL
            
        Returns:
            Page number (0-indexed)
        """
        parsed = urlparse(url)
        query_params = parse_qs(parsed.query)
        
        # Get pagination parameter value
        param_value = query_params.get(self.page_param, [None])[0]
        
        if param_value is None:
            # No pagination parameter = first page
            return 0
        
        try:
            param_value = int(param_value)
        except (ValueError, TypeError):
            return 0
        
        # Convert to 0-indexed page number
        if self.page_param == "start":
            # Offset to page number: start=15 -> page 1, start=30 -> page 2
            page_number = param_value // self.posts_per_page
        else:
            # Page number to 0-indexed: page=2 or p=2 -> page 1
            page_number = param_value - 1
        
        return max(0, page_number)
    
    def find_next_page(self, current_url: str, pagination_links: list[str]) -> Optional[str]:
        """
        Find the next page URL from pagination links.
        
        Args:
            current_url: Current page URL
            pagination_links: List of pagination link URLs (relative or absolute)
            
        Returns:
            Next page URL or None if no next page
        """
        if not pagination_links:
            return None
        
        current_page = self.extract_page_number(current_url)
        
        # Find next page in links
        next_page_number = current_page + 1
        
        for link in pagination_links:
            page_num = self.extract_page_number(link)
            if page_num == next_page_number:
                # Construct full URL if link is relative
                if link.startswith("http"):
                    return link
                else:
                    # Remove leading ./ if present
                    if link.startswith("./"):
                        link = link[2:]
                    # Ensure link starts with /
                    if not link.startswith("/"):
                        link = "/" + link
                    return self.domain + link
        
        return None
    
    def find_last_page(self, pagination_links: list[str]) -> Optional[int]:
        """
        Find the last page number from pagination links.
        
        Args:
            pagination_links: List of pagination link URLs
            
        Returns:
            Last page number or None if not found
        """
        if not pagination_links:
            return None
        
        max_page = 0
        for link in pagination_links:
            page_num = self.extract_page_number(link)
            max_page = max(max_page, page_num)
        
        return max_page if max_page > 0 else None
