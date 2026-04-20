"""
Auth service: registration, login, logout, session management.
Uses bcrypt for password hashing. No ORM — raw SQL only.
"""

import secrets
import hashlib
import hmac
import os
from datetime import datetime, timezone

import bcrypt
from psycopg2 import IntegrityError

from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)


def hash_password(password: str) -> str:
    """Hash password using bcrypt with auto-generated salt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(plain: str, hashed: str) -> bool:
    """Constant-time bcrypt comparison."""
    return bcrypt.checkpw(plain.encode("utf-8"), hashed.encode("utf-8"))


def register_user(username: str, email: str, password: str) -> dict:
    """
    Registers a new user and creates their gold account atomically.
    Returns user dict on success, raises ValueError on duplicate.
    """
    if len(password) < 8:
        raise ValueError("Password must be at least 8 characters.")
    if len(username) < 3:
        raise ValueError("Username must be at least 3 characters.")

    password_hash = hash_password(password)

    try:
        with get_cursor() as (conn, cur):
            # Insert user
            cur.execute(
                """
                INSERT INTO users (username, email, password_hash)
                VALUES (%s, %s, %s)
                RETURNING id, username, email, created_at
                """,
                [username.strip().lower(), email.strip().lower(), password_hash],
            )
            user = cur.fetchone()

            # Create associated gold account (1:1 with user)
            cur.execute(
                """
                INSERT INTO accounts (user_id)
                VALUES (%s)
                RETURNING id
                """,
                [user["id"]],
            )
            account = cur.fetchone()

            logger.info(
                "Registered user id=%s username=%s account_id=%s",
                user["id"], user["username"], account["id"]
            )
            return dict(user)

    except IntegrityError as e:
        err = str(e)
        if "users_username_key" in err or "users_username" in err:
            raise ValueError(f"Username '{username}' is already taken.")
        if "users_email_key" in err or "users_email" in err:
            raise ValueError(f"Email '{email}' is already registered.")
        raise ValueError("Registration failed due to a conflict.")


def login_user(username: str, password: str) -> str:
    """
    Authenticates user credentials, creates a session, returns session_token.
    Raises ValueError on bad credentials.
    """
    with get_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT id, username, email, password_hash, is_active
            FROM users
            WHERE username = %s
            """,
            [username.strip().lower()],
        )
        user = cur.fetchone()

    if not user or not verify_password(password, user["password_hash"]):
        # Constant-time: don't reveal whether username or password was wrong
        raise ValueError("Invalid username or password.")

    if not user["is_active"]:
        raise ValueError("Account is deactivated.")

    session_token = secrets.token_urlsafe(48)

    with get_cursor() as (conn, cur):
        cur.execute(
            """
            INSERT INTO sessions (session_token, user_id)
            VALUES (%s, %s)
            """,
            [session_token, user["id"]],
        )

    logger.info("Login: user_id=%s username=%s", user["id"], user["username"])
    return session_token


def logout_user(session_token: str) -> None:
    """Invalidates the session."""
    with get_cursor() as (conn, cur):
        cur.execute(
            "UPDATE sessions SET is_active = FALSE WHERE session_token = %s",
            [session_token],
        )
