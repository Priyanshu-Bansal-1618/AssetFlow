"""
Allocation service: user-level allocate/deallocate/yield (legacy),
plus vault-level open/close via deal_service for v2 operations.
"""

from decimal import Decimal
from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)

VALID_ALLOCATION_TYPES = {"leasing", "collateral_lock"}


def allocate(account_id: int, amount_grams: float, allocation_type: str,
             yield_rate_bps: int = 0, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Allocation amount must be positive.")
    alloc_type = allocation_type.lower().replace(" ", "_")
    if alloc_type not in VALID_ALLOCATION_TYPES:
        raise ValueError(f"Invalid allocation type '{allocation_type}'. Must be one of: {', '.join(VALID_ALLOCATION_TYPES)}")
    db_type = alloc_type.upper()
    if yield_rate_bps < 0:
        raise ValueError("Yield rate cannot be negative.")
    if yield_rate_bps > 10000:
        raise ValueError("Yield rate cannot exceed 10000 bps (100%).")

    amount = Decimal(str(amount_grams))
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT ledger_event_id, allocation_id FROM allocate_gold(%s, %s, %s::allocation_type, %s, %s)",
            [account_id, amount, db_type, yield_rate_bps, notes],
        )
        result = cur.fetchone()
        cur.execute("SELECT balance_grams, allocated_grams FROM accounts WHERE id = %s", [account_id])
        account = cur.fetchone()

    logger.info("ALLOCATE account_id=%s amount=%s type=%s alloc_id=%s ledger_id=%s",
                account_id, amount, db_type, result["allocation_id"], result["ledger_event_id"])
    return {
        "ledger_event_id": result["ledger_event_id"],
        "allocation_id": result["allocation_id"],
        "amount_grams": float(amount),
        "allocation_type": alloc_type,
        "yield_rate_bps": yield_rate_bps,
        "new_available_balance_grams": float(account["balance_grams"]),
        "total_allocated_grams": float(account["allocated_grams"]),
    }


def deallocate(account_id: int, allocation_id: int, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT deallocate_gold(%s, %s, %s) AS ledger_id", [account_id, allocation_id, notes])
        result = cur.fetchone()
        ledger_id = result["ledger_id"]
        cur.execute("SELECT balance_grams, allocated_grams FROM accounts WHERE id = %s", [account_id])
        account = cur.fetchone()

    logger.info("DEALLOCATE account_id=%s allocation_id=%s ledger_id=%s", account_id, allocation_id, ledger_id)
    return {
        "ledger_event_id": ledger_id,
        "allocation_id": allocation_id,
        "new_available_balance_grams": float(account["balance_grams"]),
        "total_allocated_grams": float(account["allocated_grams"]),
    }


def credit_yield(allocation_id: int, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT credit_yield(%s, %s) AS ledger_id", [allocation_id, notes])
        result = cur.fetchone()
        ledger_id = result["ledger_id"]
        cur.execute("SELECT amount_grams, balance_after, account_id FROM ledger_events WHERE id = %s", [ledger_id])
        event = cur.fetchone()

    return {
        "ledger_event_id": ledger_id,
        "allocation_id": allocation_id,
        "yield_credited_grams": float(event["amount_grams"]),
        "new_balance_grams": float(event["balance_after"]),
    }


def get_allocations(account_id: int, status_filter: str = None) -> list[dict]:
    """Returns user-level (non-pooled) allocations for an account."""
    query = """
        SELECT
            a.id, a.allocation_type, a.status, a.amount_grams,
            a.yield_rate_bps, a.is_pooled, a.maturity_date,
            a.allocated_at, a.deallocated_at, a.notes,
            ad.id AS deal_id, ad.deal_reference,
            c.name AS counterparty_name
        FROM allocations a
        LEFT JOIN allocation_deals ad ON ad.allocation_id = a.id
        LEFT JOIN counterparties c    ON c.id = ad.counterparty_id
        WHERE a.account_id = %s AND a.is_pooled = FALSE
    """
    params = [account_id]
    if status_filter:
        query += " AND a.status = %s::allocation_status"
        params.append(status_filter.upper())
    query += " ORDER BY a.allocated_at DESC"

    with get_cursor() as (conn, cur):
        cur.execute(query, params)
        return [dict(r) for r in cur.fetchall()]
