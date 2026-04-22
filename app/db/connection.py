"""
Database connection pool using psycopg2 (sync) wrapped for FastAPI.
Uses a connection pool to handle concurrent requests safely.
"""

import psycopg2
from psycopg2 import pool as pg_pool
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
import logging
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

_pool: pg_pool.ThreadedConnectionPool | None = None

PROJECT_ROOT = Path(__file__).resolve().parents[2]
# Support both conventional .env and a custom env filename.
for env_name in (".env", "env"):
    env_path = PROJECT_ROOT / env_name
    if env_path.exists():
        load_dotenv(env_path, override=False)
        logger.info("Loaded environment from %s", env_path)
        break

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/assetflow"
)


async def init_pool():
    global _pool
    _pool = pg_pool.ThreadedConnectionPool(
        minconn=2,
        maxconn=20,
        dsn=DATABASE_URL,
        cursor_factory=RealDictCursor,
    )
    logger.info("Database connection pool initialized (min=2, max=20)")


async def close_pool():
    global _pool
    if _pool:
        _pool.closeall()
        logger.info("Database connection pool closed")


@contextmanager
def get_conn():
    """
    Context manager that yields a connection from the pool.
    Automatically returns the connection on exit.

    Usage:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(...)
    """
    global _pool
    if _pool is None:
        raise RuntimeError("Connection pool not initialized")

    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


@contextmanager
def get_cursor():
    """
    Convenience context manager yielding (conn, cursor).
    Handles transaction commit/rollback automatically.

    Usage:
        with get_cursor() as (conn, cur):
            cur.execute("SELECT deposit_gold(%s, %s)", [account_id, amount])
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            yield conn, cur