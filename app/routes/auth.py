"""
Auth routes: register, login, logout (session-based, HTML forms).
"""

from fastapi import APIRouter, Request, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from app.services import auth_service

router = APIRouter()
templates = Jinja2Templates(directory="frontend/templates")


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@router.post("/register")
async def register(
    request: Request,
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
    confirm_password: str = Form(...),
):
    if password != confirm_password:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": "Passwords do not match."},
            status_code=400,
        )
    try:
        auth_service.register_user(username, email, password)
        return RedirectResponse(url="/auth/login?registered=1", status_code=302)
    except ValueError as e:
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "error": str(e)},
            status_code=400,
        )


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request, registered: str = None):
    context = {"request": request}
    if registered:
        context["success"] = "Account created successfully. Please log in."
    return templates.TemplateResponse("login.html", context)


@router.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    try:
        session_token = auth_service.login_user(username, password)
        response = RedirectResponse(url="/accounts/dashboard", status_code=302)
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=True,      # prevent JS access
            samesite="lax",     # CSRF protection
            max_age=86400,      # 24h
            secure=False,       # set True in prod with HTTPS
        )
        return response
    except ValueError as e:
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "error": str(e)},
            status_code=401,
        )


@router.post("/logout")
async def logout(request: Request):
    session_token = request.cookies.get("session_token")
    if session_token:
        auth_service.logout_user(session_token)
    response = RedirectResponse(url="/auth/login", status_code=302)
    response.delete_cookie("session_token")
    return response


# --- JSON API endpoints (for programmatic access) ---

@router.post("/api/register")
async def api_register(
    username: str = Form(...),
    email: str = Form(...),
    password: str = Form(...),
):
    try:
        user = auth_service.register_user(username, email, password)
        return {"success": True, "user_id": user["id"], "username": user["username"]}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/api/login")
async def api_login(
    username: str = Form(...),
    password: str = Form(...),
):
    try:
        token = auth_service.login_user(username, password)
        return {"success": True, "session_token": token}
    except ValueError as e:
        raise HTTPException(status_code=401, detail=str(e))
