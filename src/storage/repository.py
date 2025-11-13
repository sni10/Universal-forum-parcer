"""Repository for managing blocks and links."""
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Optional

from ..domain import PostBlock, ImageLink, LinkStatus
from ..fs import atomic_write_jsonl
from .database import init_database


class LinkRepository:
    """Repository for managing post blocks and image links."""
    
    def __init__(self, db_path: str | Path):
        """
        Initialize repository.
        
        Args:
            db_path: Path to SQLite database
        """
        self.db_path = Path(db_path)
        self.conn = init_database(db_path)
    
    def create_block(self, title: str, slug: str) -> PostBlock:
        """
        Create a new post block.
        
        Args:
            title: Block title
            slug: Filesystem-safe slug
            
        Returns:
            Created PostBlock with assigned ID
        """
        cursor = self.conn.execute(
            "INSERT INTO blocks (title, slug, created_at) VALUES (?, ?, ?)",
            (title, slug, datetime.now().isoformat())
        )
        self.conn.commit()
        
        block = PostBlock(
            title=title,
            slug=slug,
            block_id=cursor.lastrowid
        )
        return block
    
    def get_block_by_slug(self, slug: str) -> Optional[PostBlock]:
        """
        Get block by slug.
        
        Args:
            slug: Block slug
            
        Returns:
            PostBlock if found, None otherwise
        """
        cursor = self.conn.execute(
            "SELECT id, title, slug, created_at FROM blocks WHERE slug = ?",
            (slug,)
        )
        row = cursor.fetchone()
        
        if not row:
            return None
        
        block = PostBlock(
            title=row[1],
            slug=row[2],
            block_id=row[0],
            created_at=datetime.fromisoformat(row[3])
        )
        
        # Load links for this block
        block.links = self.get_links_by_block(block.block_id)
        
        return block
    
    def add_links(self, block_id: int, links: list[ImageLink]) -> None:
        """
        Add image links to a block.
        
        Args:
            block_id: Block ID
            links: List of ImageLink objects
        """
        for link in links:
            try:
                cursor = self.conn.execute(
                    """
                    INSERT INTO links (
                        block_id, url, referer, status, filename,
                        size, etag, last_modified, retries, error,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        block_id,
                        link.url,
                        link.referer,
                        link.status.value,
                        link.filename,
                        link.size,
                        link.etag,
                        link.last_modified,
                        link.retries,
                        link.error,
                        link.created_at.isoformat(),
                        link.updated_at.isoformat()
                    )
                )
                link.link_id = cursor.lastrowid
            except sqlite3.IntegrityError:
                # Link already exists (duplicate URL in block)
                pass
        
        self.conn.commit()
    
    def update_link_status(
        self,
        link_id: int,
        status: LinkStatus,
        filename: Optional[str] = None,
        size: Optional[int] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        error: Optional[str] = None,
        increment_retries: bool = False
    ) -> None:
        """
        Update link status and metadata.
        
        Args:
            link_id: Link ID
            status: New status
            filename: Downloaded filename
            size: File size
            etag: ETag header
            last_modified: Last-Modified header
            error: Error message if failed
            increment_retries: Whether to increment retry counter
        """
        updates = {
            "status": status.value,
            "updated_at": datetime.now().isoformat()
        }
        
        if filename is not None:
            updates["filename"] = filename
        if size is not None:
            updates["size"] = size
        if etag is not None:
            updates["etag"] = etag
        if last_modified is not None:
            updates["last_modified"] = last_modified
        if error is not None:
            updates["error"] = error
        
        set_clause = ", ".join(f"{k} = ?" for k in updates.keys())
        values = list(updates.values())
        
        if increment_retries:
            set_clause += ", retries = retries + 1"
        
        self.conn.execute(
            f"UPDATE links SET {set_clause} WHERE id = ?",
            values + [link_id]
        )
        self.conn.commit()
    
    def get_links_by_block(self, block_id: int) -> list[ImageLink]:
        """
        Get all links for a block.
        
        Args:
            block_id: Block ID
            
        Returns:
            List of ImageLink objects
        """
        cursor = self.conn.execute(
            """
            SELECT id, url, referer, status, filename, size, etag,
                   last_modified, retries, error, created_at, updated_at
            FROM links
            WHERE block_id = ?
            ORDER BY id
            """,
            (block_id,)
        )
        
        links = []
        for row in cursor.fetchall():
            link = ImageLink(
                url=row[1],
                referer=row[2],
                status=LinkStatus(row[3]),
                filename=row[4],
                size=row[5],
                etag=row[6],
                last_modified=row[7],
                retries=row[8],
                error=row[9],
                created_at=datetime.fromisoformat(row[10]),
                updated_at=datetime.fromisoformat(row[11]),
                link_id=row[0]
            )
            links.append(link)
        
        return links
    
    def get_links_by_status(self, block_id: int, status: LinkStatus) -> list[ImageLink]:
        """
        Get links by status for a block.
        
        Args:
            block_id: Block ID
            status: Link status
            
        Returns:
            List of ImageLink objects with specified status
        """
        cursor = self.conn.execute(
            """
            SELECT id, url, referer, status, filename, size, etag,
                   last_modified, retries, error, created_at, updated_at
            FROM links
            WHERE block_id = ? AND status = ?
            ORDER BY id
            """,
            (block_id, status.value)
        )
        
        links = []
        for row in cursor.fetchall():
            link = ImageLink(
                url=row[1],
                referer=row[2],
                status=LinkStatus(row[3]),
                filename=row[4],
                size=row[5],
                etag=row[6],
                last_modified=row[7],
                retries=row[8],
                error=row[9],
                created_at=datetime.fromisoformat(row[10]),
                updated_at=datetime.fromisoformat(row[11]),
                link_id=row[0]
            )
            links.append(link)
        
        return links
    
    def recover_downloading_links(self) -> int:
        """
        Reset links stuck in 'downloading' status back to 'queued'.
        
        This should be called on startup to recover from crashes.
        
        Returns:
            Number of links recovered
        """
        cursor = self.conn.execute(
            """
            UPDATE links
            SET status = ?, updated_at = ?
            WHERE status = ?
            """,
            (LinkStatus.QUEUED.value, datetime.now().isoformat(), LinkStatus.DOWNLOADING.value)
        )
        self.conn.commit()
        return cursor.rowcount
    
    def export_links_jsonl(self, block: PostBlock, output_dir: Path) -> None:
        """
        Export links to JSONL files.
        
        Creates:
        - all_links.jsonl: All links
        - done_links.jsonl: Successfully downloaded links
        - manifest.json: Block metadata
        
        Args:
            block: PostBlock to export
            output_dir: Output directory (e.g., links/<slug>/)
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Export all links
        all_links = [
            {
                "url": link.url,
                "referer": link.referer,
                "status": link.status.value,
                "filename": link.filename,
                "size": link.size,
                "etag": link.etag,
                "last_modified": link.last_modified,
                "retries": link.retries,
                "error": link.error,
                "created_at": link.created_at.isoformat(),
                "updated_at": link.updated_at.isoformat()
            }
            for link in block.links
        ]
        atomic_write_jsonl(output_dir / "all_links.jsonl", all_links)
        
        # Export done links
        done_links = [
            link_data for link_data in all_links
            if link_data["status"] == LinkStatus.DONE.value
        ]
        atomic_write_jsonl(output_dir / "done_links.jsonl", done_links)
        
        # Export manifest
        manifest = {
            "block_id": block.block_id,
            "title": block.title,
            "slug": block.slug,
            "created_at": block.created_at.isoformat(),
            "total_links": len(block.links),
            "done_links": len(done_links),
            "failed_links": sum(1 for link in block.links if link.status == LinkStatus.FAILED),
            "exported_at": datetime.now().isoformat()
        }
        atomic_write_jsonl(output_dir / "manifest.json", [manifest])
    
    def create_page(self, url: str, page_number: int, status: str = "new") -> int:
        """
        Create a new page record.
        
        Args:
            url: Page URL
            page_number: Page number (0-indexed)
            status: Page status (default: "new")
            
        Returns:
            Page ID
        """
        try:
            cursor = self.conn.execute(
                """
                INSERT INTO pages (url, page_number, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (url, page_number, status, datetime.now().isoformat(), datetime.now().isoformat())
            )
            self.conn.commit()
            return cursor.lastrowid
        except sqlite3.IntegrityError:
            # Page already exists
            cursor = self.conn.execute(
                "SELECT id FROM pages WHERE url = ?",
                (url,)
            )
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_page(self, url: str) -> Optional[dict]:
        """
        Get page record by URL.
        
        Args:
            url: Page URL
            
        Returns:
            Page dict or None if not found
        """
        cursor = self.conn.execute(
            """
            SELECT id, url, page_number, status, blocks_found, error,
                   created_at, updated_at
            FROM pages
            WHERE url = ?
            """,
            (url,)
        )
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return {
            "id": row[0],
            "url": row[1],
            "page_number": row[2],
            "status": row[3],
            "blocks_found": row[4],
            "error": row[5],
            "created_at": row[6],
            "updated_at": row[7]
        }
    
    def update_page_status(
        self,
        url: str,
        status: str,
        blocks_found: Optional[int] = None,
        error: Optional[str] = None
    ) -> None:
        """
        Update page status.
        
        Args:
            url: Page URL
            status: New status
            blocks_found: Number of blocks found (optional)
            error: Error message (optional)
        """
        if blocks_found is not None:
            self.conn.execute(
                """
                UPDATE pages
                SET status = ?, blocks_found = ?, error = ?, updated_at = ?
                WHERE url = ?
                """,
                (status, blocks_found, error, datetime.now().isoformat(), url)
            )
        else:
            self.conn.execute(
                """
                UPDATE pages
                SET status = ?, error = ?, updated_at = ?
                WHERE url = ?
                """,
                (status, error, datetime.now().isoformat(), url)
            )
        self.conn.commit()
    
    def get_last_processed_page(self) -> Optional[dict]:
        """
        Get the last successfully processed page.
        
        Returns:
            Page dict or None if no pages processed
        """
        cursor = self.conn.execute(
            """
            SELECT id, url, page_number, status, blocks_found, error,
                   created_at, updated_at
            FROM pages
            WHERE status = 'done'
            ORDER BY page_number DESC
            LIMIT 1
            """
        )
        row = cursor.fetchone()
        
        if not row:
            return None
        
        return {
            "id": row[0],
            "url": row[1],
            "page_number": row[2],
            "status": row[3],
            "blocks_found": row[4],
            "error": row[5],
            "created_at": row[6],
            "updated_at": row[7]
        }
    
    def recover_processing_pages(self) -> int:
        """
        Reset pages stuck in 'processing' status back to 'new'.
        
        This should be called on startup to recover from crashes.
        
        Returns:
            Number of pages recovered
        """
        cursor = self.conn.execute(
            """
            UPDATE pages
            SET status = ?, updated_at = ?
            WHERE status = ?
            """,
            ("new", datetime.now().isoformat(), "processing")
        )
        self.conn.commit()
        return cursor.rowcount
    
    def close(self) -> None:
        """Close database connection."""
        self.conn.close()
