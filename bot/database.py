import aiosqlite
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Database:
    """SQLite database for deduplication, stats, and history."""

    def __init__(self, db_path: Path):
        self.db_path = db_path

    async def init(self):
        """Initialize database schema."""
        async with aiosqlite.connect(self.db_path) as db:
            # Processed files table
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

            # Stats table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_check TEXT,
                    emails_processed INTEGER DEFAULT 0,
                    files_processed INTEGER DEFAULT 0
                )
            """)

            # Registries table (history)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS registries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    date TEXT NOT NULL,
                    total_amount REAL NOT NULL,
                    commission REAL NOT NULL,
                    payments_count INTEGER NOT NULL,
                    tax_file TEXT,
                    payments_file TEXT,
                    created_at TEXT NOT NULL,
                    UNIQUE(date)
                )
            """)

            # Payments table (detailed)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    registry_id INTEGER NOT NULL,
                    payment_id TEXT NOT NULL,
                    amount REAL NOT NULL,
                    currency TEXT,
                    payment_time TEXT,
                    description TEXT,
                    payment_type TEXT,
                    FOREIGN KEY (registry_id) REFERENCES registries (id)
                )
            """)

            # Settings table
            await db.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
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
                "last_check": None,
                "emails_processed": 0,
                "files_processed": 0
            }

    async def save_registry(self, data: Dict) -> int:
        """Save registry to database. Returns registry_id."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """INSERT OR REPLACE INTO registries 
                   (date, total_amount, commission, payments_count, tax_file, payments_file, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    data["date"],
                    data["total_amount"],
                    data["commission"],
                    data["payments_count"],
                    data.get("tax_file"),
                    data.get("payments_file"),
                    datetime.utcnow().isoformat()
                )
            )
            registry_id = cursor.lastrowid

            # Save payments
            for payment in data.get("payments", []):
                await db.execute(
                    """INSERT INTO payments 
                       (registry_id, payment_id, amount, currency, payment_time, description, payment_type)
                       VALUES (?, ?, ?, ?, ?, ?, ?)""",
                    (
                        registry_id,
                        payment["payment_id"],
                        payment["amount"],
                        payment.get("currency", "RUB"),
                        payment.get("payment_time"),
                        payment.get("description"),
                        payment.get("payment_type")
                    )
                )

            await db.commit()
            return registry_id

    async def get_registry(self, date: str) -> Optional[Dict]:
        """Get registry by date."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM registries WHERE date = ?",
                (date,)
            )
            row = await cursor.fetchone()
            
            if not row:
                return None

            registry = dict(row)
            registry_id = registry["id"]

            # Get payments
            cursor = await db.execute(
                "SELECT * FROM payments WHERE registry_id = ?",
                (registry_id,)
            )
            payments = [dict(r) for r in await cursor.fetchall()]
            registry["payments"] = payments

            return registry

    async def get_history(self, limit: int = 10) -> List[Dict]:
        """Get recent registries."""
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM registries ORDER BY date DESC LIMIT ?",
                (limit,)
            )
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]

    async def get_monthly_stats(self, year: int, month: int) -> Dict:
        """Get stats for specific month."""
        async with aiosqlite.connect(self.db_path) as db:
            # Format: YYYY-MM-%
            date_pattern = f"{year:04d}-{month:02d}-%"
            
            cursor = await db.execute(
                """SELECT 
                    COUNT(*) as registries_count,
                    SUM(total_amount) as total_income,
                    SUM(commission) as total_commission,
                    SUM(payments_count) as total_payments,
                    COUNT(CASE WHEN total_amount > 0 THEN 1 END) as days_with_income
                   FROM registries 
                   WHERE date LIKE ?""",
                (date_pattern,)
            )
            row = await cursor.fetchone()
            
            if row:
                return {
                    "registries_count": row[0] or 0,
                    "total_income": row[1] or 0.0,
                    "total_commission": row[2] or 0.0,
                    "total_payments": row[3] or 0,
                    "days_with_income": row[4] or 0
                }
            
            return {
                "registries_count": 0,
                "total_income": 0.0,
                "total_commission": 0.0,
                "total_payments": 0,
                "days_with_income": 0
            }

    async def get_yearly_stats(self, year: int) -> Dict:
        """Get stats for entire year."""
        async with aiosqlite.connect(self.db_path) as db:
            date_pattern = f"{year:04d}-%"
            
            cursor = await db.execute(
                """SELECT 
                    SUM(total_amount) as total_income,
                    SUM(commission) as total_commission,
                    SUM(payments_count) as total_payments
                   FROM registries 
                   WHERE date LIKE ?""",
                (date_pattern,)
            )
            row = await cursor.fetchone()
            
            if row:
                return {
                    "total_income": row[0] or 0.0,
                    "total_commission": row[1] or 0.0,
                    "total_payments": row[2] or 0
                }
            
            return {
                "total_income": 0.0,
                "total_commission": 0.0,
                "total_payments": 0
            }

    async def get_all_time_stats(self) -> Dict:
        """Get all-time stats."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """SELECT 
                    COUNT(*) as registries_count,
                    SUM(total_amount) as total_income,
                    SUM(commission) as total_commission,
                    SUM(payments_count) as total_payments
                   FROM registries"""
            )
            row = await cursor.fetchone()
            
            if row:
                return {
                    "registries_count": row[0] or 0,
                    "total_income": row[1] or 0.0,
                    "total_commission": row[2] or 0.0,
                    "total_payments": row[3] or 0
                }
            
            return {
                "registries_count": 0,
                "total_income": 0.0,
                "total_commission": 0.0,
                "total_payments": 0
            }

    async def get_setting(self, key: str) -> Optional[str]:
        """Get setting value."""
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "SELECT value FROM settings WHERE key = ?",
                (key,)
            )
            row = await cursor.fetchone()
            return row[0] if row else None

    async def set_setting(self, key: str, value: str):
        """Set setting value."""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, value)
            )
            await db.commit()

    async def close(self):
        """Close database connection."""
        # aiosqlite closes connections automatically
        pass