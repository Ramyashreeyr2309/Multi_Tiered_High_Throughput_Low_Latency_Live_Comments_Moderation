import asyncio
import time
import httpx
import websockets
from pathlib import Path
from urllib.parse import urlencode
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect, HTTPException, Depends, status
from fastapi.responses import FileResponse, StreamingResponse
from starlette.websockets import WebSocketState
import redis.asyncio as redis
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from Token_Util.token_util import TokenValidationError, decode_access_token

app = FastAPI(version="1.0.0")
security = HTTPBearer(auto_error=False)

# Connect to Redis container on localhost:6379
# decode_responses=True ensures Redis returns string instead of bytes
redis_client = redis.Redis(host="host.docker.internal", port=6379, db=0, decode_responses=True)

# Shared HTTPX Async Client for Reverse Proxying (Reuses Connection Pool)
http_client = httpx.AsyncClient(timeout=10.0)


@app.on_event("shutdown")
async def shutdown_event():
    await http_client.aclose()
    await redis_client.close()


# 2. Rate Limiting Middleware / Dependency (Token Bucket Pattern)
async def check_rate_limit(request: Request):
    client_ip = request.client.host
    current_minute = int(time.time() // 60)
    key = f"rate_limit:{client_ip}:{current_minute}"
    
    # Allow max 60 requests per minute per IP
    requests_count = await redis_client.incr(key)
    if requests_count == 1:
        await redis_client.expire(key, 60)
        
    if requests_count > 60:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded. Try again in a minute."
        )

BROWSER_ROUTE_MAP = {
    ("GET", "/login"): ("user_service", "login"),
    ("GET", "/register"): ("user_service", "register"),
    ("GET", "/forgot_password"): ("user_service", "forgot_password"),
    ("GET", "/events"): ("live_event_service", "live_events_page"),
    ("GET", "/events/{event_id}"): ("live_event_service", "live_events/{event_id}/page"),
    ("GET", "/admin/dashboard"): ("monitoring_service", "admin_dashboard_page"),
    ("GET", "/media/{asset_path:path}"): ("live_event_service", "{asset_path}"),
}


def resolve_browser_route(method: str, path: str):
    route = BROWSER_ROUTE_MAP.get((method, path))
    if route is not None:
        return route

    path_parts = path.strip("/").split("/") if path.strip("/") else []
    for (route_method, route_template), route in BROWSER_ROUTE_MAP.items():
        if route_method != method or "{" not in route_template:
            continue

        template_parts = route_template.strip("/").split("/")
        has_path_parameter = template_parts[-1].endswith(":path}")
        if len(path_parts) < len(template_parts) - int(has_path_parameter):
            continue
        if not has_path_parameter and len(template_parts) != len(path_parts):
            continue

        parameters = {}
        for index, template_part in enumerate(template_parts):
            if template_part.startswith("{") and template_part.endswith("}"):
                parameter_name = template_part[1:-1]
                if parameter_name.endswith(":path"):
                    parameters[parameter_name[:-5]] = "/".join(path_parts[index:])
                    continue
                parameters[parameter_name] = path_parts[index]
            elif template_part != path_parts[index]:
                break
        else:
            service, target_path = route
            for name, value in parameters.items():
                target_path = target_path.replace(f"{{{name}}}", value)
            return service, target_path

    return None


PUBLIC_ENDPOINTS = {
    "user_service": {
        ("POST", "user/session"),
        ("GET", "user/registration"),
        ("POST", "user/registration"),
        ("POST", "refresh"),
        ("GET", "user/reset_password"),
        ("PATCH", "user/reset_password"),
    },
}


async def verify_auth_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials | None = Depends(security),
):
    if "service" not in request.path_params:
        browser_route = resolve_browser_route(request.method, request.url.path)
        if browser_route is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Route not found")
        return None
    else:
        endpoint = (
            request.method,
            request.path_params["path"].strip("/"),
        )
        service = request.path_params.get("service", "user_service")
        if endpoint in PUBLIC_ENDPOINTS.get(service, set()):
            return None

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials or token expired",
            headers={"WWW-Authenticate": "Bearer"},
        )

    token = credentials.credentials
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials or token expired",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload = decode_access_token(token)
        user_id: str = payload.get("user_id")
        mail_id: str = payload.get("mail_id")
        user_name: str = payload.get("user_name")
        role = payload.get("role")
        if service == "monitoring_service" and role != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Admin role required",
            )
        return {"user_id": user_id, "mail_id": mail_id, "role": role, "user_name": user_name}
    except TokenValidationError as error:
        raise credentials_exception from error



# 4. Service Registry Routing Map
MICROSERVICE_ROUTES = {
    "user_service": "http://host.docker.internal:8001",
    "live_event_service": "http://host.docker.internal:8002",      # Ingestion API / RabbitMQ Producer
    "tier_1_service": "http://host.docker.internal:8003",
    "tier_2_service": "http://host.docker.internal:8004",
    "monitoring_service": "http://host.docker.internal:8005",
}


async def forward_request(
    service: str,
    path: str,
    request: Request,
    current_user: dict | None,
):
    if service not in MICROSERVICE_ROUTES:
        raise HTTPException(status_code=404, detail=f"Service '{service}' not found")

    target_base_url = MICROSERVICE_ROUTES[service]
    target_url = f"{target_base_url}/{path}"
    
    # Forward headers (Inject trace ID and extracted user ID)
    headers = dict(request.headers)
    headers.pop("x-user-id", None)
    if current_user is not None:
        headers["x-user-id"] = current_user["user_id"]
        headers["x-user-mail"] = current_user["mail_id"]
        headers["x-user-role"] = current_user.get("role", "")
        headers["x-user-name"] = current_user.get("user_name", "")
    headers["x-forwarded-for"] = request.client.host

    # Read incoming request body
    body = await request.body()

    try:
        # Asynchronously forward the request to target microservice
        proxy_res = http_client.build_request(
            method=request.method,
            url=target_url,
            headers=headers,
            params=request.query_params,
            content=body
        )
        response = await http_client.send(proxy_res, stream=True)

        # Stream response back to client without blocking memory
        return StreamingResponse(
            response.aiter_raw(),
            status_code=response.status_code,
            headers=dict(response.headers)
        )

    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Backend microservice error: {str(exc)}"
        )

@app.get("/")
def health():
    return {"status": "ok"}


# 5. Reverse Proxy Route Handlers
@app.api_route(
    "/api/v1/{service}/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    dependencies=[Depends(check_rate_limit)]
)
async def api_gateway_proxy(
    service: str,
    path: str,
    request: Request,
    current_user: dict | None = Depends(verify_auth_token),
):
    return await forward_request(service, path, request, current_user)


@app.websocket("/api/v1/{service}/ws/{path:path}")
async def api_gateway_websocket_proxy(service: str, path: str, websocket: WebSocket):
    if service != "live_event_service":
        await websocket.close(code=1008, reason="WebSocket service not found")
        return

    access_token = websocket.query_params.get("access_token")
    if not access_token:
        await websocket.close(code=1008, reason="Authentication required")
        return
    try:
        current_user = decode_access_token(access_token)
    except TokenValidationError:
        await websocket.close(code=1008, reason="Invalid or expired access token")
        return

    backend_url = MICROSERVICE_ROUTES[service].replace("http://", "ws://", 1)
    backend_url = f"{backend_url}/ws/{path}"
    backend_query = [
        (key, value)
        for key, value in websocket.query_params.multi_items()
        if key != "access_token"
    ]
    if backend_query:
        backend_url = f"{backend_url}?{urlencode(backend_query)}"

    headers = {}
    headers["x-user-id"] = current_user["user_id"]
    origin = websocket.headers.get("origin")
    if origin:
        headers["Origin"] = origin

    try:
        async with websockets.connect(backend_url, additional_headers=headers) as backend:
            await websocket.accept()

            async def browser_to_backend():
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    if message.get("text") is not None:
                        await backend.send(message["text"])
                    elif message.get("bytes") is not None:
                        await backend.send(message["bytes"])

            async def backend_to_browser():
                async for message in backend:
                    if isinstance(message, bytes):
                        await websocket.send_bytes(message)
                    else:
                        await websocket.send_text(message)

            tasks = {
                asyncio.create_task(browser_to_backend()),
                asyncio.create_task(backend_to_browser()),
            }
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            for task in done:
                task.result()
            if (
                websocket.client_state != WebSocketState.DISCONNECTED
                and websocket.application_state != WebSocketState.DISCONNECTED
            ):
                await websocket.close(code=1000)
    except WebSocketDisconnect:
        pass
    except (OSError, websockets.exceptions.WebSocketException):
        if websocket.application_state != WebSocketState.DISCONNECTED:
            await websocket.close(code=1013, reason="WebSocket backend unavailable")


@app.get("/media/site-header.html", include_in_schema=False)
async def shared_site_header():
    return FileResponse(
        Path(__file__).with_name("site-header.html"),
        media_type="text/html",
    )


@app.get("/media/site-header.js", include_in_schema=False)
async def shared_site_header_loader():
    return FileResponse(
        Path(__file__).with_name("site-header.js"),
        media_type="application/javascript",
    )


@app.api_route(
    "/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    dependencies=[Depends(check_rate_limit)]
)
async def user_service_alias(
    path: str,
    request: Request,
    current_user: dict | None = Depends(verify_auth_token),
):
    browser_route = resolve_browser_route(request.method, request.url.path)
    if browser_route is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Route not found")

    service, target_path = browser_route
    return await forward_request(service, target_path, request, current_user)