"""Domain models for the image loader."""
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class LinkStatus(str, Enum):
    """Status of an image link."""
    NEW = "new"
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    DONE = "done"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class ImageLink:
    """Represents a single image link to download."""
    url: str
    referer: Optional[str] = None
    status: LinkStatus = LinkStatus.NEW
    filename: Optional[str] = None
    size: Optional[int] = None
    etag: Optional[str] = None
    last_modified: Optional[str] = None
    retries: int = 0
    error: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    link_id: Optional[int] = None  # DB identifier


@dataclass
class PostBlock:
    """Represents a forum post block containing multiple image links."""
    title: str
    slug: str
    links: list[ImageLink] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    block_id: Optional[int] = None  # DB identifier
