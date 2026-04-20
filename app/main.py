"""
Gold Monetization Infrastructure Simulator
FastAPI Application Entry Point
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import RedirectResponse
from contextlib import asynccontextmanager
import logging

from app.db.connection import init_pool, close_pool
from app.routes import auth, accounts, allocations, ledger

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing database connection pool...")
    await init_pool()
    yield
    logger.info("Closing database connection pool...")
    await close_pool()


app = FastAPI(
    title="Gold Monetization Infrastructure Simulator",
    description="A ledger-based gold banking system demonstrating transactional safety and DB correctness.",
    version="1.0.0",
    lifespan=lifespan,
)

templates = Jinja2Templates(directory="frontend/templates")

# Register routers
app.include_router(auth.router,        prefix="/auth",        tags=["Authentication"])
app.include_router(accounts.router,    prefix="/accounts",    tags=["Accounts"])
app.include_router(allocations.router, prefix="/allocations", tags=["Allocations"])
app.include_router(ledger.router,      prefix="/ledger",      tags=["Ledger"])


@app.get("/", include_in_schema=False)
async def root(request: Request):
    """Redirect root to dashboard or login."""
    session_token = request.cookies.get("session_token")
    if session_token:
        return RedirectResponse(url="/accounts/dashboard", status_code=302)
    return RedirectResponse(url="/auth/login", status_code=302)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gold-simulator"}
