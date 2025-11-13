"""Configuration management."""
import os
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # python-dotenv not installed, use environment variables directly
    pass


class Config:
    """Application configuration from environment variables."""
    
    # Forum settings (needed first for domain extraction)
    FORUM_DOMAIN: str = os.getenv("FORUM_DOMAIN", "")
    START_URL: str = os.getenv("START_URL", "")
    PAGE_PARAM: str = os.getenv("PAGE_PARAM", "start")  # or "page" or "p"
    
    # Extract domain name from FORUM_DOMAIN
    @staticmethod
    def _get_domain_name() -> str:
        """
        Extract clean domain name from FORUM_DOMAIN URL.
        Removes protocol and trailing slashes.
        
        Returns:
            Domain name (e.g., "www.porn-w.org") or empty string if not set
        """
        forum_domain = os.getenv("FORUM_DOMAIN", "")
        if not forum_domain:
            return ""
        
        parsed = urlparse(forum_domain)
        domain = parsed.netloc or parsed.path
        # Remove trailing slashes
        domain = domain.rstrip("/")
        return domain
    
    # Base directories from environment
    _base_db_path: str = os.getenv("DB_PATH", "data/loader.db")
    _base_img_dir: str = os.getenv("IMG_DIR", "img")
    _base_links_dir: str = os.getenv("LINKS_DIR", "links")
    
    # Compute actual paths with domain subdirectories
    _domain_name: str = _get_domain_name.__func__()  # type: ignore
    
    DB_PATH: str = (
        str(Path(_base_db_path).parent / _domain_name / Path(_base_db_path).name)
        if _domain_name else _base_db_path
    )
    IMG_DIR: str = (
        str(Path(_base_img_dir) / _domain_name)
        if _domain_name else _base_img_dir
    )
    LINKS_DIR: str = (
        str(Path(_base_links_dir) / _domain_name)
        if _domain_name else _base_links_dir
    )
    
    LOGS_DIR: str = os.getenv("LOGS_DIR", "logs")
    
    # Download settings
    GLOBAL_LIMIT: int = int(os.getenv("GLOBAL_LIMIT", "10"))
    PER_HOST_LIMIT: int = int(os.getenv("PER_HOST_LIMIT", "3"))
    MAX_RETRIES: int = int(os.getenv("MAX_RETRIES", "5"))
    DOWNLOAD_TIMEOUT: int = int(os.getenv("DOWNLOAD_TIMEOUT", "300"))
    
    # Logging
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_MAX_BYTES: int = int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024)))  # 10 MB
    LOG_BACKUP_COUNT: int = int(os.getenv("LOG_BACKUP_COUNT", "10"))
    
    # Chrome/Browser settings
    CHROME_PROFILE_PATH: str = os.getenv("CHROME_PROFILE_PATH", "")
    CHROME_HEADLESS: str = os.getenv("CHROME_HEADLESS", "false")
    CHROME_BINARY_PATH: str = os.getenv("CHROME_BINARY_PATH", "")
    
    @classmethod
    def get_log_level(cls) -> int:
        """
        Get logging level as integer.
        
        Returns:
            Logging level constant
        """
        import logging
        return getattr(logging, cls.LOG_LEVEL.upper(), logging.INFO)
    
    @classmethod
    def get_chrome_profile(cls) -> Optional[str]:
        """
        Get Chrome profile path.
        
        Returns:
            Chrome profile path or None if not set
        """
        return cls.CHROME_PROFILE_PATH if cls.CHROME_PROFILE_PATH else None
    
    @classmethod
    def get_chrome_headless(cls) -> bool:
        """
        Get Chrome headless mode setting.
        
        Returns:
            True if headless mode enabled, False otherwise
        """
        return cls.CHROME_HEADLESS.lower() in ("true", "1", "yes")
    
    @classmethod
    def validate(cls) -> list[str]:
        """
        Validate configuration.
        
        Returns:
            List of validation errors (empty if valid)
        """
        errors = []
        
        # Check required settings
        if not cls.FORUM_DOMAIN:
            errors.append("FORUM_DOMAIN is not set")
        
        if not cls.START_URL:
            errors.append("START_URL is not set")
        
        # Check numeric ranges
        if cls.GLOBAL_LIMIT < 1:
            errors.append("GLOBAL_LIMIT must be >= 1")
        
        if cls.PER_HOST_LIMIT < 1:
            errors.append("PER_HOST_LIMIT must be >= 1")
        
        if cls.MAX_RETRIES < 1:
            errors.append("MAX_RETRIES must be >= 1")
        
        if cls.DOWNLOAD_TIMEOUT < 1:
            errors.append("DOWNLOAD_TIMEOUT must be >= 1")
        
        return errors
    
    @classmethod
    def display(cls) -> None:
        """Display current configuration."""
        print("=== Configuration ===")
        print(f"DB_PATH: {cls.DB_PATH}")
        print(f"IMG_DIR: {cls.IMG_DIR}")
        print(f"LINKS_DIR: {cls.LINKS_DIR}")
        print(f"LOGS_DIR: {cls.LOGS_DIR}")
        print(f"GLOBAL_LIMIT: {cls.GLOBAL_LIMIT}")
        print(f"PER_HOST_LIMIT: {cls.PER_HOST_LIMIT}")
        print(f"MAX_RETRIES: {cls.MAX_RETRIES}")
        print(f"DOWNLOAD_TIMEOUT: {cls.DOWNLOAD_TIMEOUT}s")
        print(f"LOG_LEVEL: {cls.LOG_LEVEL}")
        print(f"FORUM_DOMAIN: {cls.FORUM_DOMAIN}")
        print(f"START_URL: {cls.START_URL}")
        print(f"PAGE_PARAM: {cls.PAGE_PARAM}")
        print(f"CHROME_PROFILE: {cls.CHROME_PROFILE_PATH or 'None'}")
        print(f"CHROME_HEADLESS: {cls.get_chrome_headless()}")
        print(f"CHROME_BINARY: {cls.CHROME_BINARY_PATH or 'Auto-detect'}")
        print("=" * 30)
