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

ALL_EVENT_TYPES = [
    "DEPOSIT", "WITHDRAW", "ALLOCATE", "DEALLOCATE",
    "YIELD_CREDIT", "DEAL_CLOSE", "YIELD_EARNED", "YIELD_DISTRIBUTED",
]


def get_ledger_events(account_id: int, limit: int = 50, offset: int = 0,
                      event_type: str = None) -> list[dict]:
    query = """
        SELECT le.id, le.event_type, le.amount_grams, le.balance_after,
               le.reference_id, le.deal_id, le.notes, le.created_at
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
        return [dict(r) for r in cur.fetchall()]


def get_ledger_count(account_id: int) -> int:
    with get_cursor() as (conn, cur):
        cur.execute("SELECT COUNT(*) AS cnt FROM ledger_events WHERE account_id = %s", [account_id])
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
    return templates.TemplateResponse("ledger.html", {
        "request": request,
        "user": current_user,
        "events": events,
        "page": page,
        "total_pages": (total + limit - 1) // limit,
        "total": total,
        "event_type_filter": event_type,
        "event_types": ALL_EVENT_TYPES,
    })


@router.get("/verify")
async def verify_ledger(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    with get_cursor() as (conn, cur):
        cur.execute("SELECT * FROM verify_ledger_consistency(%s)", [account_id])
        result = dict(cur.fetchone())

    events = get_ledger_events(account_id, limit=20, offset=0)
    total = get_ledger_count(account_id)
    is_ok = result.get("is_consistent")
    msg = (f"Audit {'PASSED' if is_ok else 'FAILED'}: "
           f"stored={result.get('stored_balance')}g ledger={result.get('ledger_balance')}g "
           f"consistent={is_ok}")
    ctx = {
        "request": request, "user": current_user, "events": events,
        "page": 1, "total_pages": (total + 19) // 20, "total": total,
        "event_type_filter": None, "event_types": ALL_EVENT_TYPES,
    }
    if is_ok:
        ctx["success"] = msg
    else:
        ctx["error"] = msg
    return templates.TemplateResponse("ledger.html", ctx)


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
