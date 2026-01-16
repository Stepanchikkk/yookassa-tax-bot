import aiosqlite
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


class Database:
    """SQLite database for deduplication and stats."""

    def __init__(self, db_path: Path):
        self.db_path = db_path

    async def init(self):
        """Initialize database schema."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id TEXT NOT NULL,
                    filename TEXT NOT NULL,
                    file_hash TEXT NOT NULL,
                    processed_at TEXT NOT NULL,
                    UNIQUE(message_id, filename, file_hash)
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_check TEXT,
                    emails_processed INTEGER DEFAULT 0,
                    files_processed INTEGER DEFAULT 0
                )
            """)

            # Insert default stats if not exists
            await db.execute("""
                INSERT OR IGNORE INTO stats (id, last_check, emails_processed, files_processed)
                VALUES (1, NULL, 0, 0)
            """)

            await db.commit()

        logger.info("Database initialized")

    async def is_processed(self, message_id: str, filename: str, file_hash: str) -> bool:
        """Check if file was already processed."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT 1 FROM processed_files WHERE message_id = ? AND filename = ? AND file_hash = ?",
                (message_id, filename, file_hash)
            )
            result = await cursor.fetchone()
            return result is not None

    async def mark_processed(self, message_id: str, filename: str, file_hash: str):
        """Mark file as processed."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR IGNORE INTO processed_files (message_id, filename, file_hash, processed_at) VALUES (?, ?, ?, ?)",
                (message_id, filename, file_hash, datetime.utcnow().isoformat())
            )
            await db.commit()

    async def update_stats(self, emails_count: int, files_count: int):
        """Update processing stats."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE stats SET last_check = ?, emails_processed = emails_processed + ?, files_processed = files_processed + ? WHERE id = 1",
                (datetime.utcnow().isoformat(), emails_count, files_count)
            )
            await db.commit()

    async def get_stats(self) -> Dict:
        """Get processing stats."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM stats WHERE id = 1")
            row = await cursor.fetchone()
            
            if row:
                return dict(row)
            
            return {
                "last_check": "Never",
                "emails_processed": 0,
                "files_processed": 0
            }

    async def close(self):
        """Close database connection."""
        # aiosqlite closes connections automatically
        pass