"""
Deal routes: counterparties, lease deals, collateral locks, yield distribution.
"""

from fastapi import APIRouter, Request, Form, Depends, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2

from app.dependencies import get_current_user
from app.services import deal_service, account_service
from app.dependencies import get_account_id

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


def _render(request, template, user, extra=None, status_code=200):
    ctx = {"request": request, "user": user}
    if extra:
        ctx.update(extra)
    return templates.TemplateResponse(template, ctx, status_code=status_code)


def _deals_context(extra=None):
    ctx = {
        "counterparties":  deal_service.list_counterparties(),
        "exposure":        deal_service.get_counterparty_exposure(),
        "active_deals":    deal_service.get_active_deals(),
        "all_deals":       deal_service.get_all_deals(),
        "pending_yield":   deal_service.get_pending_yield_events(),
        "yield_history":   deal_service.get_yield_history(),
    }
    if extra:
        ctx.update(extra)
    return ctx


def _collateral_context(extra=None):
    ctx = {
        "active_locks": deal_service.get_active_collateral_locks(),
        "vault":        deal_service.get_vault_balance(),
    }
    if extra:
        ctx.update(extra)
    return ctx


# =============================================================================
# HTML pages
# =============================================================================

@router.get("/", response_class=HTMLResponse)
async def deals_page(request: Request, current_user: dict = Depends(get_current_user)):
    return _render(request, "deals.html", current_user, _deals_context())


@router.get("/collateral", response_class=HTMLResponse)
async def collateral_page(request: Request, current_user: dict = Depends(get_current_user)):
    return _render(request, "collateral.html", current_user, _collateral_context())


@router.get("/yield", response_class=HTMLResponse)
async def yield_page(request: Request, current_user: dict = Depends(get_current_user)):
    account_id = get_account_id(current_user["user_id"])
    return _render(request, "yield.html", current_user, {
        "pending_yield":   deal_service.get_pending_yield_events(),
        "yield_history":   deal_service.get_yield_history(),
        "user_yield":      deal_service.get_user_yield_history(account_id),
        "vault":           deal_service.get_vault_balance(),
    })


# =============================================================================
# Counterparty actions
# =============================================================================

@router.post("/counterparties/create")
async def create_counterparty(
    request: Request,
    name: str = Form(...),
    entity_type: str = Form(...),
    credit_rating: str = Form(None),
    max_exposure_grams: float = Form(0),
    current_user: dict = Depends(get_current_user),
):
    try:
        deal_service.create_counterparty(name, entity_type, credit_rating, max_exposure_grams)
        return _render(request, "deals.html", current_user,
                       _deals_context({"success": f"Counterparty '{name}' created."}))
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "deals.html", current_user,
                       _deals_context({"error": str(e)}), status_code=400)


# =============================================================================
# Lease deal actions
# =============================================================================

@router.post("/open-lease")
async def open_lease(
    request: Request,
    amount_grams: float = Form(...),
    counterparty_id: int = Form(...),
    yield_rate_bps: int = Form(...),
    maturity_date: str = Form(None),
    deal_reference: str = Form(None),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = deal_service.open_lease_deal(amount_grams, counterparty_id, yield_rate_bps,
                                              maturity_date or None, deal_reference or None, notes or None)
        msg = f"Lease deal #{result['deal_id']} opened: {amount_grams}g at {yield_rate_bps} bps. Ledger #{result['ledger_event_id']}."
        return _render(request, "deals.html", current_user, _deals_context({"success": msg}))
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "deals.html", current_user, _deals_context({"error": str(e)}), status_code=400)


@router.post("/close-deal/{deal_id}")
async def close_deal(
    deal_id: int,
    request: Request,
    is_default: bool = Form(False),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = deal_service.close_deal(deal_id, is_default, notes or None)
        msg = (f"Deal #{deal_id} closed. Gross yield: {result['gross_yield_grams']}g. "
               f"Yield event #{result['yield_event_id']} ready for distribution.")
        return _render(request, "deals.html", current_user, _deals_context({"success": msg}))
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "deals.html", current_user, _deals_context({"error": str(e)}), status_code=400)


# =============================================================================
# Collateral lock actions
# =============================================================================

@router.post("/open-collateral")
async def open_collateral(
    request: Request,
    amount_grams: float = Form(...),
    yield_rate_bps: int = Form(...),
    maturity_date: str = Form(None),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = deal_service.open_collateral_lock(amount_grams, yield_rate_bps,
                                                   maturity_date or None, notes or None)
        msg = f"Collateral lock #{result['allocation_id']} opened: {amount_grams}g at {yield_rate_bps} bps."
        return _render(request, "collateral.html", current_user, _collateral_context({"success": msg}))
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "collateral.html", current_user, _collateral_context({"error": str(e)}), status_code=400)


@router.post("/close-collateral/{allocation_id}")
async def close_collateral(
    allocation_id: int,
    request: Request,
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    try:
        result = deal_service.close_collateral_lock(allocation_id, notes or None)
        msg = (f"Collateral lock #{allocation_id} closed. "
               f"Gross yield: {result['gross_yield_grams']}g. "
               f"Yield event #{result['yield_event_id']} ready for distribution.")
        return _render(request, "collateral.html", current_user, _collateral_context({"success": msg}))
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "collateral.html", current_user, _collateral_context({"error": str(e)}), status_code=400)


# =============================================================================
# Yield distribution
# =============================================================================

@router.post("/distribute/{yield_event_id}")
async def distribute_yield(
    yield_event_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        result = deal_service.distribute_yield(yield_event_id)
        msg = f"Yield event #{yield_event_id} distributed to {result['users_credited']} users."
        return _render(request, "yield.html", current_user, {
            "success": msg,
            "pending_yield": deal_service.get_pending_yield_events(),
            "yield_history": deal_service.get_yield_history(),
            "user_yield":    deal_service.get_user_yield_history(account_id),
            "vault":         deal_service.get_vault_balance(),
        })
    except (ValueError, psycopg2.Error) as e:
        return _render(request, "yield.html", current_user, {
            "error": str(e),
            "pending_yield": deal_service.get_pending_yield_events(),
            "yield_history": deal_service.get_yield_history(),
            "user_yield":    deal_service.get_user_yield_history(account_id),
            "vault":         deal_service.get_vault_balance(),
        }, status_code=400)


# =============================================================================
# JSON API
# =============================================================================

@router.post("/api/counterparties")
async def api_create_counterparty(name: str = Form(...), entity_type: str = Form(...),
                                   credit_rating: str = Form(None), max_exposure_grams: float = Form(0),
                                   current_user: dict = Depends(get_current_user)):
    try:
        return deal_service.create_counterparty(name, entity_type, credit_rating, max_exposure_grams)
    except (ValueError, psycopg2.Error) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/api/counterparties")
async def api_list_counterparties(current_user: dict = Depends(get_current_user)):
    return deal_service.list_counterparties()

@router.get("/api/deals/active")
async def api_active_deals(current_user: dict = Depends(get_current_user)):
    return deal_service.get_active_deals()

@router.get("/api/counterparties/exposure")
async def api_exposure(current_user: dict = Depends(get_current_user)):
    return deal_service.get_counterparty_exposure()

@router.get("/api/yield/pending")
async def api_pending_yield(current_user: dict = Depends(get_current_user)):
    return deal_service.get_pending_yield_events()
