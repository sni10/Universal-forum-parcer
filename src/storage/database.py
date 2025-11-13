"""Database schema and initialization."""
import sqlite3
from pathlib import Path


DATABASE_SCHEMA = """
-- Blocks table: represents forum post blocks
CREATE TABLE IF NOT EXISTS blocks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Links table: represents image links to download
CREATE TABLE IF NOT EXISTS links (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    block_id INTEGER NOT NULL,
    url TEXT NOT NULL,
    referer TEXT,
    status TEXT NOT NULL DEFAULT 'new',
    filename TEXT,
    size INTEGER,
    etag TEXT,
    last_modified TEXT,
    retries INTEGER DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (block_id) REFERENCES blocks(id) ON DELETE CASCADE,
    UNIQUE(block_id, url)
);

-- Index for faster status queries
CREATE INDEX IF NOT EXISTS idx_links_status ON links(status);

-- Index for faster block lookups
CREATE INDEX IF NOT EXISTS idx_links_block_id ON links(block_id);

-- Pages table: tracks forum pages processed
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT NOT NULL UNIQUE,
    page_number INTEGER NOT NULL,
    status TEXT NOT NULL DEFAULT 'new',
    blocks_found INTEGER DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster page status queries
CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status);

-- Index for faster page number lookups
CREATE INDEX IF NOT EXISTS idx_pages_page_number ON pages(page_number);
"""


def init_database(db_path: str | Path) -> sqlite3.Connection:
    """
    Initialize database with schema.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Database connection
    """
    db_path = Path(db_path)
    
    # Ensure data directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Connect and enable foreign keys
    conn = sqlite3.Connection(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Initialize schema
    conn.executescript(DATABASE_SCHEMA)
    conn.commit()
    
    return conn
