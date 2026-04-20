"""
Allocation routes: allocate, deallocate, yield credit.
"""

from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import psycopg2

from app.dependencies import get_current_user, get_account_id
from app.services import allocation_service, account_service

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


@router.get("/", response_class=HTMLResponse)
async def allocations_page(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    account = account_service.get_balance(account_id)
    allocations = allocation_service.get_allocations(account_id)
    return templates.TemplateResponse(
        "allocations.html",
        {
            "request": request,
            "user": current_user,
            "account": account,
            "allocations": allocations,
        },
    )


@router.post("/allocate")
async def allocate(
    request: Request,
    amount: float = Form(...),
    allocation_type: str = Form(...),
    yield_rate_bps: int = Form(0),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        result = allocation_service.allocate(
            account_id, amount, allocation_type, yield_rate_bps, notes
        )
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "success": (
                    f"Allocated {amount}g to {allocation_type}. "
                    f"Allocation #{result['allocation_id']}, "
                    f"Ledger #{result['ledger_event_id']}."
                ),
            },
        )
    except (ValueError, psycopg2.Error) as e:
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "error": str(e),
            },
            status_code=400,
        )


@router.post("/deallocate/{allocation_id}")
async def deallocate(
    allocation_id: int,
    request: Request,
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        result = allocation_service.deallocate(account_id, allocation_id, notes)
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "success": f"Deallocated allocation #{allocation_id}. Ledger #{result['ledger_event_id']}.",
            },
        )
    except (ValueError, psycopg2.Error) as e:
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "error": str(e),
            },
            status_code=400,
        )


@router.post("/yield/{allocation_id}")
async def credit_yield(
    allocation_id: int,
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    """Credits yield for a specific allocation."""
    account_id = get_account_id(current_user["user_id"])
    try:
        result = allocation_service.credit_yield(allocation_id)
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "success": (
                    f"Yield credited: {result['yield_credited_grams']}g. "
                    f"New balance: {result['new_balance_grams']}g. "
                    f"Ledger #{result['ledger_event_id']}."
                ),
            },
        )
    except (ValueError, psycopg2.Error) as e:
        account = account_service.get_balance(account_id)
        allocations = allocation_service.get_allocations(account_id)
        return templates.TemplateResponse(
            "allocations.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "allocations": allocations,
                "error": str(e),
            },
            status_code=400,
        )


# --- JSON API ---

@router.post("/api/allocate")
async def api_allocate(
    amount: float = Form(...),
    allocation_type: str = Form(...),
    yield_rate_bps: int = Form(0),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        return allocation_service.allocate(account_id, amount, allocation_type, yield_rate_bps, notes)
    except (ValueError, psycopg2.Error) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/deallocate/{allocation_id}")
async def api_deallocate(
    allocation_id: int,
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        return allocation_service.deallocate(account_id, allocation_id, notes)
    except (ValueError, psycopg2.Error) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/list")
async def api_list_allocations(
    status: str = None,
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    return allocation_service.get_allocations(account_id, status_filter=status)
