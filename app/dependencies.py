"""
FastAPI dependencies: authentication, session resolution.
"""

from fastapi import Request, HTTPException, status
from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)


def get_current_user(request: Request) -> dict:
    """
    Dependency: resolves session_token cookie → user dict.
    Raises 401 if session is missing, expired, or invalid.
    """
    session_token = request.cookies.get("session_token")
    if not session_token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated. Please log in.",
        )

    with get_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT s.user_id, s.expires_at, s.is_active,
                   u.username, u.email, u.is_active as user_active
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.session_token = %s
            """,
            [session_token],
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session not found. Please log in again.",
        )

    if not row["is_active"]:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session has been invalidated.",
        )

    if not row["user_active"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User account is inactive.",
        )

    # Check expiry in Python (avoids timezone edge cases with DB)
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc)
    expires_at = row["expires_at"]
    if expires_at.tzinfo is None:
        from datetime import timezone as tz
        expires_at = expires_at.replace(tzinfo=tz.utc)

    if now > expires_at:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Session expired. Please log in again.",
        )

    return {
        "user_id": row["user_id"],
        "username": row["username"],
        "email": row["email"],
    }


def get_account_id(user_id: int) -> int:
    """Resolves user_id → account_id."""
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT id FROM accounts WHERE user_id = %s",
            [user_id],
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No account found for user_id={user_id}",
        )
    return row["id"]
