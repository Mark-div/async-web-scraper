import csv
import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

from .core import ScrapedItem

logger = logging.getLogger(__name__)

DB_PATH = Path("data/scraper.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


@contextmanager
def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    with get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS scraped_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE NOT NULL,
                title TEXT,
                content TEXT,
                metadata TEXT,
                status TEXT DEFAULT 'success',
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_url ON scraped_items(url)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON scraped_items(status)")
    logger.info("Database initialized")


def save_item(item: ScrapedItem) -> bool:
    try:
        with get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO scraped_items
                    (url, title, content, metadata, status, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                item.url,
                item.title,
                item.content,
                json.dumps(item.metadata),
                item.status,
                item.scraped_at.isoformat(),
            ))
        return True
    except Exception as e:
        logger.error(f"Failed to save item {item.url}: {e}")
        return False


def save_many(items: list[ScrapedItem]) -> int:
    saved = sum(1 for item in items if save_item(item))
    logger.info(f"Saved {saved}/{len(items)} items")
    return saved


def get_all(status: Optional[str] = None, limit: int = 1000) -> list[dict]:
    with get_connection() as conn:
        query = "SELECT * FROM scraped_items"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += f" ORDER BY scraped_at DESC LIMIT {limit}"
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def export_csv(output_path: str = "data/export.csv") -> str:
    items = get_all()
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "url", "title", "content", "status", "scraped_at"])
        writer.writeheader()
        writer.writerows(items)
    logger.info(f"Exported {len(items)} items to {output_path}")
    return output_path


def get_stats() -> dict:
    with get_connection() as conn:
        total = conn.execute("SELECT COUNT(*) FROM scraped_items").fetchone()[0]
        success = conn.execute("SELECT COUNT(*) FROM scraped_items WHERE status='success'").fetchone()[0]
        failed = conn.execute("SELECT COUNT(*) FROM scraped_items WHERE status='failed'").fetchone()[0]
        last_run = conn.execute("SELECT MAX(scraped_at) FROM scraped_items").fetchone()[0]
    return {
        "total": total,
        "success": success,
        "failed": failed,
        "success_rate": round(success / total * 100, 2) if total else 0,
        "last_run": last_run,
    }
