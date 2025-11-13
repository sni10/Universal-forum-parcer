"""Link extraction from HTML."""
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, parse_qs, urlencode

from bs4 import BeautifulSoup

from ..domain import PostBlock, ImageLink
from ..fs import slugify


# Supported image extensions
IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp"}


class LinkExtractor:
    """Extract post blocks and image links from forum HTML."""
    
    @staticmethod
    def is_preview(url: str) -> bool:
        """
        Check if URL is a preview/thumbnail.
        
        Pixhost pattern:
        - Preview: tNN.pixhost.to/thumbs/...
        - Original: imgNN.pixhost.to/images/...
        
        Args:
            url: URL to check
            
        Returns:
            True if URL appears to be a preview/thumbnail
        """
        parsed = urlparse(url)
        hostname = parsed.hostname or ""
        path = parsed.path.lower()
        
        # Check for "thumb" in path
        if "thumb" in path or "/thumbs/" in path:
            return True
        
        # Pixhost-specific: tNN.pixhost.to
        if hostname.endswith(".pixhost.to") and hostname.startswith("t"):
            return True
        
        return False
    
    @staticmethod
    def is_image_url(url: str) -> bool:
        """
        Check if URL points to an image.
        
        Args:
            url: URL to check
            
        Returns:
            True if URL has image extension
        """
        parsed = urlparse(url)
        path_lower = parsed.path.lower()
        extension = Path(path_lower).suffix
        
        return extension in IMAGE_EXTENSIONS
    
    @staticmethod
    def normalize_url(url: str) -> str:
        """
        Normalize URL for deduplication.
        
        - Convert hostname to lowercase
        - Sort query parameters
        - Remove common tracking parameters
        
        Args:
            url: URL to normalize
            
        Returns:
            Normalized URL
        """
        parsed = urlparse(url)
        
        # Lowercase hostname
        hostname = (parsed.hostname or "").lower()
        
        # Parse and filter query params (remove tracking)
        tracking_params = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content"}
        query_dict = parse_qs(parsed.query)
        filtered_query = {k: v for k, v in query_dict.items() if k not in tracking_params}
        
        # Sort params for consistency
        sorted_query = urlencode(sorted(filtered_query.items()), doseq=True)
        
        # Reconstruct URL
        normalized = f"{parsed.scheme}://{hostname}{parsed.path}"
        if sorted_query:
            normalized += f"?{sorted_query}"
        
        return normalized
    
    @staticmethod
    def extract_block_title(postbody_div) -> str:
        """
        Extract title from postbody div.
        
        Looks for the first <span class="font-weight-bold"> tag.
        Falls back to text before first <br> tag.
        
        Args:
            postbody_div: BeautifulSoup postbody div element
            
        Returns:
            Extracted title or empty string
        """
        # Try to find title in bold span first
        bold_span = postbody_div.select_one("span.font-weight-bold")
        if bold_span:
            title = bold_span.get_text(strip=True)
            if title:
                return title
        
        # Fallback: get text from first text node before first <br>
        # Look inside nested col div if present
        col_div = postbody_div.select_one("div.col")
        search_element = col_div if col_div else postbody_div
        
        pieces = []
        for child in search_element.children:
            # Stop at first <br>
            if getattr(child, "name", None) == "br":
                break
            
            # Skip nested tags, only get direct text
            if hasattr(child, "name") and child.name:
                continue
                
            text = str(child).strip()
            if text:
                pieces.append(text)
        
        return " ".join(pieces).strip()
    
    def extract_blocks(self, html: str, page_url: Optional[str] = None) -> list[PostBlock]:
        """
        Extract post blocks from forum HTML.
        
        Args:
            html: HTML content
            page_url: Optional page URL for referer
            
        Returns:
            List of PostBlock objects with image links
        """
        soup = BeautifulSoup(html, "html.parser")
        blocks = []
        
        # Find all post blocks
        for post_div in soup.select("div.list-row"):
            # Find postbody
            postbody = post_div.select_one("div.postbody")
            if not postbody:
                continue
            
            # Extract title
            title = self.extract_block_title(postbody)
            
            # Fallback to subject if title is empty
            if not title:
                subject = post_div.select_one(".postsubject")
                if subject:
                    title = subject.get_text(strip=True)
                    # Remove "Subject:" prefix if present
                    if title.startswith("Subject:"):
                        title = title[8:].strip()
            
            if not title:
                title = "Untitled"
            
            # Extract links from postbody
            links = []
            seen_urls = set()
            
            for a_tag in postbody.select("a[href]"):
                href = a_tag.get("href", "").strip()
                
                # Skip empty or relative links
                if not href or href.startswith("/"):
                    continue
                
                # Only http(s)
                if not (href.startswith("http://") or href.startswith("https://")):
                    continue
                
                # Skip previews
                if self.is_preview(href):
                    continue
                
                # Must be image URL
                if not self.is_image_url(href):
                    continue
                
                # Normalize for deduplication
                normalized = self.normalize_url(href)
                if normalized in seen_urls:
                    continue
                
                seen_urls.add(normalized)
                
                # Create ImageLink
                link = ImageLink(
                    url=href,
                    referer=page_url
                )
                links.append(link)
            
            # Only create block if it has links
            if links:
                slug = slugify(title)
                block = PostBlock(
                    title=title,
                    slug=slug,
                    links=links
                )
                blocks.append(block)
        
        return blocks
