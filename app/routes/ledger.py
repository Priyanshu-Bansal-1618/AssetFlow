"""
Ledger routes: read-only audit trail + consistency verification.
"""

from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.dependencies import get_current_user, get_account_id
from app.db.connection import get_cursor

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


def get_ledger_events(
    account_id: int,
    limit: int = 50,
    offset: int = 0,
    event_type: str = None,
) -> list[dict]:
    query = """
        SELECT
            le.id,
            le.event_type,
            le.amount_grams,
            le.balance_after,
            le.reference_id,
            le.notes,
            le.created_at
        FROM ledger_events le
        WHERE le.account_id = %s
    """
    params = [account_id]

    if event_type:
        query += " AND le.event_type = %s::ledger_event_type"
        params.append(event_type.upper())

    query += " ORDER BY le.created_at DESC, le.id DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    with get_cursor() as (conn, cur):
        cur.execute(query, params)
        rows = cur.fetchall()

    return [dict(r) for r in rows]


def get_ledger_count(account_id: int) -> int:
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ledger_events WHERE account_id = %s",
            [account_id],
        )
        return cur.fetchone()["cnt"]


@router.get("/", response_class=HTMLResponse)
async def ledger_page(
    request: Request,
    page: int = Query(1, ge=1),
    event_type: str = Query(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    limit = 20
    offset = (page - 1) * limit

    events = get_ledger_events(account_id, limit=limit, offset=offset, event_type=event_type)
    total = get_ledger_count(account_id)
    total_pages = (total + limit - 1) // limit

    return templates.TemplateResponse(
        "ledger.html",
        {
            "request": request,
            "user": current_user,
            "events": events,
            "page": page,
            "total_pages": total_pages,
            "total": total,
            "event_type_filter": event_type,
            "event_types": ["DEPOSIT", "WITHDRAW", "ALLOCATE", "DEALLOCATE", "YIELD_CREDIT"],
        },
    )


@router.get("/verify")
async def verify_ledger(current_user: dict = Depends(get_current_user)):
    """
    Reconstructs account balance from ledger and compares to stored balance.
    Use this to audit/verify ledger correctness.
    """
    account_id = get_account_id(current_user["user_id"])
    with get_cursor() as (conn, cur):
        cur.execute(
            "SELECT * FROM verify_ledger_consistency(%s)",
            [account_id],
        )
        result = cur.fetchone()
    return dict(result)


@router.get("/api/events")
async def api_ledger_events(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    event_type: str = Query(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    events = get_ledger_events(account_id, limit=limit, offset=offset, event_type=event_type)
    total = get_ledger_count(account_id)
    return {"total": total, "events": events}
