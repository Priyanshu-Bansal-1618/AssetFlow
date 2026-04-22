"""
Deal service: vault-level lease deals, collateral locks, yield distribution.
All mutation delegates to PostgreSQL stored procedures.
"""

from decimal import Decimal
from app.db.connection import get_cursor
import logging

logger = logging.getLogger(__name__)


def list_counterparties(active_only: bool = True) -> list[dict]:
    query = """
        SELECT c.id, c.name, c.entity_type, c.credit_rating,
               c.max_exposure_grams, c.is_active, c.created_at,
               COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0) AS current_exposure_grams
        FROM counterparties c
        LEFT JOIN allocation_deals d ON d.counterparty_id = c.id
    """
    if active_only:
        query += " WHERE c.is_active = TRUE"
    query += " GROUP BY c.id ORDER BY c.name"
    with get_cursor() as (conn, cur):
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def create_counterparty(name: str, entity_type: str, credit_rating: str = None,
                        max_exposure_grams: float = 0) -> dict:
    valid = {"JEWELLER", "REFINER", "TRADING_DESK"}
    if entity_type.upper() not in valid:
        raise ValueError(f"entity_type must be one of {valid}")
    if max_exposure_grams < 0:
        raise ValueError("max_exposure_grams cannot be negative")
    with get_cursor() as (conn, cur):
        cur.execute(
            "INSERT INTO counterparties (name, entity_type, credit_rating, max_exposure_grams) "
            "VALUES (%s, %s::counterparty_entity_type, %s, %s) "
            "RETURNING id, name, entity_type, credit_rating, max_exposure_grams, created_at",
            [name, entity_type.upper(), credit_rating, Decimal(str(max_exposure_grams))],
        )
        return dict(cur.fetchone())


def open_lease_deal(amount_grams: float, counterparty_id: int, yield_rate_bps: int,
                    maturity_date: str = None, deal_reference: str = None, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Amount must be positive")
    if not 0 <= yield_rate_bps <= 10000:
        raise ValueError("yield_rate_bps must be 0–10000")

    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT allocation_id, deal_id, ledger_event_id FROM open_lease_deal(%s,%s,%s,%s::DATE,%s,%s)",
            [Decimal(str(amount_grams)), counterparty_id, yield_rate_bps, maturity_date, deal_reference, notes],
        )
        result = cur.fetchone()
        cur.execute(
            "SELECT d.*, c.name AS counterparty_name FROM allocation_deals d "
            "JOIN counterparties c ON c.id = d.counterparty_id WHERE d.id = %s",
            [result["deal_id"]],
        )
        deal = cur.fetchone()

    logger.info("LEASE OPENED: deal_id=%s counterparty=%s amount=%s", result["deal_id"], deal["counterparty_name"], amount_grams)
    return {"allocation_id": result["allocation_id"], "deal_id": result["deal_id"],
            "ledger_event_id": result["ledger_event_id"], "deal": dict(deal)}


def open_collateral_lock(amount_grams: float, yield_rate_bps: int,
                         maturity_date: str = None, notes: str = None) -> dict:
    if amount_grams <= 0:
        raise ValueError("Amount must be positive")
    if yield_rate_bps < 0:
        raise ValueError("yield_rate_bps cannot be negative")
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT allocation_id, ledger_event_id FROM open_collateral_lock(%s,%s,%s::DATE,%s)",
            [Decimal(str(amount_grams)), yield_rate_bps, maturity_date, notes],
        )
        return dict(cur.fetchone())


def close_deal(deal_id: int, is_default: bool = False, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT yield_event_id, gross_yield_grams, ledger_close_id, ledger_yield_id FROM close_deal(%s,%s,%s)",
            [deal_id, is_default, notes],
        )
        result = cur.fetchone()
        cur.execute("SELECT * FROM yield_events WHERE id = %s", [result["yield_event_id"]])
        ye = cur.fetchone()

    logger.info("DEAL CLOSED: deal_id=%s gross_yield=%s", deal_id, result["gross_yield_grams"])
    return {"yield_event_id": result["yield_event_id"], "gross_yield_grams": float(result["gross_yield_grams"]),
            "ledger_close_id": result["ledger_close_id"], "ledger_yield_id": result["ledger_yield_id"],
            "yield_event": dict(ye)}


def close_collateral_lock(allocation_id: int, notes: str = None) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT yield_event_id, gross_yield_grams, ledger_dealloc_id, ledger_yield_id FROM close_collateral_lock(%s,%s)",
            [allocation_id, notes],
        )
        return dict(cur.fetchone())


def distribute_yield(yield_event_id: int) -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT distribute_yield(%s) AS users_credited", [yield_event_id])
        result = cur.fetchone()
    logger.info("YIELD DISTRIBUTED: yield_event_id=%s users_credited=%s", yield_event_id, result["users_credited"])
    return {"yield_event_id": yield_event_id, "users_credited": result["users_credited"]}


def get_active_deals() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT d.id AS deal_id, d.deal_reference, d.status,
                   c.name AS counterparty, c.entity_type, c.credit_rating,
                   a.allocation_type, d.amount_grams, d.yield_rate_bps,
                   d.start_date, d.maturity_date,
                   CURRENT_DATE - d.start_date AS days_active,
                   ROUND(d.amount_grams * d.yield_rate_bps / 10000.0 * (CURRENT_DATE - d.start_date) / 365.0, 6) AS accrued_yield_grams,
                   d.created_at
            FROM allocation_deals d
            JOIN counterparties c ON c.id = d.counterparty_id
            JOIN allocations a    ON a.id = d.allocation_id
            WHERE d.status = 'ACTIVE'
            ORDER BY d.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_all_deals() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT d.id AS deal_id, d.deal_reference, d.status,
                   c.name AS counterparty, c.entity_type,
                   d.amount_grams, d.yield_rate_bps,
                   d.start_date, d.maturity_date, d.closed_at, d.created_at
            FROM allocation_deals d
            JOIN counterparties c ON c.id = d.counterparty_id
            ORDER BY d.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_active_collateral_locks() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT a.id AS allocation_id, a.amount_grams, a.yield_rate_bps,
                   a.allocated_at, a.maturity_date,
                   CURRENT_DATE - a.allocated_at::DATE AS days_held,
                   ROUND(a.amount_grams * a.yield_rate_bps / 10000.0 * (CURRENT_DATE - a.allocated_at::DATE) / 365.0, 6) AS accrued_yield_grams,
                   a.notes
            FROM allocations a
            WHERE a.allocation_type = 'COLLATERAL_LOCK' AND a.status = 'ACTIVE' AND a.is_pooled = TRUE
            ORDER BY a.allocated_at
        """)
        return [dict(r) for r in cur.fetchall()]


def get_counterparty_exposure() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT c.id, c.name, c.entity_type, c.credit_rating,
                   COUNT(d.id) FILTER (WHERE d.status='ACTIVE') AS active_deals,
                   COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0) AS gold_held_grams,
                   c.max_exposure_grams,
                   ROUND(COALESCE(SUM(d.amount_grams) FILTER (WHERE d.status='ACTIVE'), 0)
                         / NULLIF(c.max_exposure_grams, 0) * 100, 2) AS pct_of_limit
            FROM counterparties c
            LEFT JOIN allocation_deals d ON d.counterparty_id = c.id
            WHERE c.is_active = TRUE
            GROUP BY c.id, c.name, c.entity_type, c.credit_rating, c.max_exposure_grams
            ORDER BY gold_held_grams DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_pending_yield_events() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT ye.id AS yield_event_id, d.id AS deal_id, d.deal_reference,
                   c.name AS counterparty,
                   ye.gross_yield_grams, ye.system_fee_grams,
                   ye.distributable_yield_grams, ye.created_at AS earned_at
            FROM yield_events ye
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            LEFT JOIN counterparties c   ON c.id = d.counterparty_id
            WHERE ye.distribution_completed = FALSE AND ye.distributable_yield_grams > 0
            ORDER BY ye.created_at ASC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_yield_history() -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT ye.id, ye.gross_yield_grams, ye.system_fee_grams,
                   ye.distributable_yield_grams, ye.system_fee_bps,
                   ye.distribution_completed, ye.created_at, ye.distributed_at,
                   d.deal_reference, c.name AS counterparty
            FROM yield_events ye
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            LEFT JOIN counterparties c   ON c.id = d.counterparty_id
            ORDER BY ye.created_at DESC
        """)
        return [dict(r) for r in cur.fetchall()]


def get_user_yield_history(account_id: int) -> list[dict]:
    with get_cursor() as (conn, cur):
        cur.execute("""
            SELECT yd.id, yd.yield_event_id, ye.gross_yield_grams AS event_gross_yield,
                   yd.user_balance_snapshot, yd.share_fraction, yd.yield_grams,
                   yd.created_at, d.deal_reference
            FROM yield_distributions yd
            JOIN yield_events ye ON ye.id = yd.yield_event_id
            LEFT JOIN allocation_deals d ON d.id = ye.deal_id
            WHERE yd.account_id = %s
            ORDER BY yd.created_at DESC
        """, [account_id])
        return [dict(r) for r in cur.fetchall()]


def get_vault_balance() -> dict:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM system_state WHERE id = 1")
        return dict(cur.fetchone())
