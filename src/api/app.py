import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api.routes import dashboard, holdings, prices, scores, screen

API_TOKEN = os.environ.get("AFV_API_TOKEN", "")

app = FastAPI(title="AFV API", version="0.1.0")


@app.middleware("http")
async def auth(request: Request, call_next):
    if API_TOKEN and request.url.path.startswith("/api/"):
        bearer = request.headers.get("Authorization", "")
        if bearer != f"Bearer {API_TOKEN}":
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
    return await call_next(request)


app.include_router(dashboard.router, prefix="/api")
app.include_router(holdings.router, prefix="/api")
app.include_router(prices.router, prefix="/api")
app.include_router(scores.router, prefix="/api")
app.include_router(screen.router, prefix="/api")
