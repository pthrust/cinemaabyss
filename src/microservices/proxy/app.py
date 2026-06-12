import asyncio
import logging
import os
import random
from contextlib import asynccontextmanager
from typing import Dict, List, Optional

import httpx
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from urllib.parse import urlencode

# ---------- Configuration ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://localhost:8080").rstrip("/")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://localhost:8081").rstrip("/")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://localhost:8082").rstrip("/")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "false").lower() == "true"
MOVIES_MIGRATION_PERCENT = max(0, min(100, int(os.getenv("MOVIES_MIGRATION_PERCENT", "0"))))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "10"))

# ---------- Logging ----------
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

# ---------- HTTP Client (shared) ----------
@asynccontextmanager
async def get_http_client():
    async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT_SECONDS, follow_redirects=False) as client:
        yield client

# ---------- Upstream Selection ----------
def select_upstream(path: str) -> str:
    if path.startswith("/api/movies"):
        if not GRADUAL_MIGRATION:
            return MONOLITH_URL

        route_to_movies = random.randint(1, 100) <= MOVIES_MIGRATION_PERCENT
        return MOVIES_SERVICE_URL if route_to_movies else MONOLITH_URL

    if path.startswith("/api/events"):
        return EVENTS_SERVICE_URL

    return MONOLITH_URL

def build_upstream_url(base_url: str, path: str, query_params: Dict[str, List[str]]) -> str:
    query_string = urlencode(query_params, doseq=True)
    url = f"{base_url}{path}"
    return f"{url}?{query_string}" if query_string else url

# ---------- Proxy Core ----------
async def proxy_request(request: Request, path: str) -> Response:
    # Normalize path (ensure leading slash)
    if not path.startswith("/"):
        path = "/" + path

    upstream = select_upstream(path)

    # Extract query parameters as list-of-values dict
    query_params: Dict[str, List[str]] = {}
    for key, values in request.query_params.multi_items():
        if key not in query_params:
            query_params[key] = []
        query_params[key].append(values)

    upstream_url = build_upstream_url(upstream, path, query_params)

    headers = request.headers
    body = await request.body()
    cookies = request.cookies

    logger.info("proxy %s %s -> %s", request.method, path, upstream_url)

    try:
        async with get_http_client() as client:
            upstream_response = await client.request(
                method=request.method,
                url=upstream_url,
                headers=headers,
                content=body,
                cookies=cookies,
            )
    except httpx.RequestError as exc:
        logger.exception("upstream request failed")
        return JSONResponse(
            status_code=502,
            content={
                "error": "Bad Gateway",
                "message": str(exc),
                "upstream": upstream,
            }
        )

    # Build FastAPI response
    response_headers = upstream_response.headers
    return Response(
        content=upstream_response.content,
        status_code=upstream_response.status_code,
        headers=response_headers,
    )

# ---------- FastAPI App ----------
app = FastAPI(title="Proxy Service")

@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse({
        "status": True,
        "service": "proxy-service",
        "gradual_migration": GRADUAL_MIGRATION,
        "movies_migration_percent": MOVIES_MIGRATION_PERCENT,
    })

# Catch-all route for all HTTP methods
@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def catch_all(request: Request, path: str = "") -> Response:
    return await proxy_request(request, path)

# ---------- Entry Point ----------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)