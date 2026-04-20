"""
Account service: deposit, withdraw, balance queries.
All mutations delegate to PostgreSQL stored procedures.
Python handles only input validation and result mapping.
"""

from decimal import Decimal
from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)


def get_balance(account_id: int) -> dict:
    """Returns full account snapshot."""
    with get_cursor() as (conn, cur):
        cur.execute(
            """
            SELECT
                a.id,
                a.user_id,
                a.balance_grams,
                a.allocated_grams,
                a.total_deposited_grams,
                a.total_withdrawn_grams,
                a.updated_at,
                u.username
            FROM accounts a
            JOIN users u ON u.id = a.user_id
            WHERE a.id = %s
            """,
            [account_id],
        )
        row = cur.fetchone()

    if not row:
        raise ValueError(f"Account not found: id={account_id}")

    return dict(row)


def deposit(account_id: int, amount_grams: float, notes: str = None) -> dict:
    """
    Deposits gold into the account.
    Calls the deposit_gold() stored procedure which:
      - Locks the account row
      - Updates balance
      - Inserts an immutable ledger event
      - Syncs system_state via trigger

    Returns the new balance and ledger event id.
    """
    if amount_grams <= 0:
        raise ValueError("Deposit amount must be positive.")
    if amount_grams > 10_000_000:
        raise ValueError("Single deposit cannot exceed 10,000,000 grams.")

    amount = Decimal(str(amount_grams))

    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT deposit_gold(%s, %s, %s) AS ledger_id",
            [account_id, amount, notes],
        )
        result = cur.fetchone()
        ledger_id = result["ledger_id"]

        # Fetch updated balance in same transaction
        cur.execute(
            "SELECT balance_grams, total_deposited_grams FROM accounts WHERE id = %s",
            [account_id],
        )
        account = cur.fetchone()

    logger.info(
        "DEPOSIT account_id=%s amount=%s ledger_id=%s new_balance=%s",
        account_id, amount, ledger_id, account["balance_grams"]
    )

    return {
        "ledger_event_id": ledger_id,
        "amount_grams": float(amount),
        "new_balance_grams": float(account["balance_grams"]),
        "total_deposited_grams": float(account["total_deposited_grams"]),
    }


def withdraw(account_id: int, amount_grams: float, notes: str = None) -> dict:
    """
    Withdraws gold from the available balance.
    Calls withdraw_gold() stored procedure.
    Raises ValueError if insufficient available balance.
    """
    if amount_grams <= 0:
        raise ValueError("Withdrawal amount must be positive.")

    amount = Decimal(str(amount_grams))

    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT withdraw_gold(%s, %s, %s) AS ledger_id",
            [account_id, amount, notes],
        )
        result = cur.fetchone()
        ledger_id = result["ledger_id"]

        cur.execute(
            "SELECT balance_grams, total_withdrawn_grams FROM accounts WHERE id = %s",
            [account_id],
        )
        account = cur.fetchone()

    logger.info(
        "WITHDRAW account_id=%s amount=%s ledger_id=%s new_balance=%s",
        account_id, amount, ledger_id, account["balance_grams"]
    )

    return {
        "ledger_event_id": ledger_id,
        "amount_grams": float(amount),
        "new_balance_grams": float(account["balance_grams"]),
        "total_withdrawn_grams": float(account["total_withdrawn_grams"]),
    }


def get_system_state() -> dict:
    """Returns the system-wide gold state (admin/audit view)."""
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM system_state WHERE id = 1")
        row = cur.fetchone()
    return dict(row) if row else {}
