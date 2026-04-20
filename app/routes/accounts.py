"""
Account routes: dashboard, deposit, withdraw, balance.
"""

from fastapi import APIRouter, Request, Form, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
import psycopg2

from app.dependencies import get_current_user, get_account_id
from app.services import account_service

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(
    request: Request,
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    account = account_service.get_balance(account_id)
    system = account_service.get_system_state()
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "user": current_user,
            "account": account,
            "system": system,
        },
    )


@router.post("/deposit")
async def deposit(
    request: Request,
    amount: float = Form(...),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        result = account_service.deposit(account_id, amount, notes)
        account = account_service.get_balance(account_id)
        system = account_service.get_system_state()
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "system": system,
                "success": f"Deposited {amount}g. Ledger event #{result['ledger_event_id']}.",
            },
        )
    except (ValueError, psycopg2.Error) as e:
        account = account_service.get_balance(account_id)
        system = account_service.get_system_state()
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "system": system,
                "error": str(e),
            },
            status_code=400,
        )


@router.post("/withdraw")
async def withdraw(
    request: Request,
    amount: float = Form(...),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        result = account_service.withdraw(account_id, amount, notes)
        account = account_service.get_balance(account_id)
        system = account_service.get_system_state()
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "system": system,
                "success": f"Withdrew {amount}g. Ledger event #{result['ledger_event_id']}.",
            },
        )
    except (ValueError, psycopg2.Error) as e:
        account = account_service.get_balance(account_id)
        system = account_service.get_system_state()
        return templates.TemplateResponse(
            "dashboard.html",
            {
                "request": request,
                "user": current_user,
                "account": account,
                "system": system,
                "error": str(e),
            },
            status_code=400,
        )


# --- JSON API ---

@router.get("/api/balance")
async def api_balance(current_user: dict = Depends(get_current_user)):
    account_id = get_account_id(current_user["user_id"])
    return account_service.get_balance(account_id)


@router.post("/api/deposit")
async def api_deposit(
    amount: float = Form(...),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        return account_service.deposit(account_id, amount, notes)
    except (ValueError, psycopg2.Error) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/withdraw")
async def api_withdraw(
    amount: float = Form(...),
    notes: str = Form(None),
    current_user: dict = Depends(get_current_user),
):
    account_id = get_account_id(current_user["user_id"])
    try:
        return account_service.withdraw(account_id, amount, notes)
    except (ValueError, psycopg2.Error) as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/system-state")
async def api_system_state(current_user: dict = Depends(get_current_user)):
    return account_service.get_system_state()
