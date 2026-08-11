import hmac
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

from api.routes import dashboard, holdings, prices, scores, screen

API_TOKEN = os.environ.get("AFV_API_TOKEN", "")
if not API_TOKEN:
    raise RuntimeError("AFV_API_TOKEN environment variable must be set")

limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

app = FastAPI(title="AFV API", version="0.1.0", docs_url=None, redoc_url=None, openapi_url=None)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

_EXPECTED_HEADER = f"Bearer {API_TOKEN}"


@app.middleware("http")
async def auth(request: Request, call_next):
    if request.url.path.startswith("/api/"):
        bearer = request.headers.get("Authorization", "")
        if not hmac.compare_digest(bearer, _EXPECTED_HEADER):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return await call_next(request)


app.include_router(dashboard.router, prefix="/api")
app.include_router(holdings.router, prefix="/api")
app.include_router(prices.router, prefix="/api")
app.include_router(scores.router, prefix="/api")
app.include_router(screen.router, prefix="/api")
