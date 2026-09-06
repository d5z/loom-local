#!/usr/bin/env python3
"""Hearth Gateway prototype. Config: /tmp/gateway-routes.json or GATEWAY_ROUTES_CONFIG."""
import asyncio, contextlib, json, logging, os, time
from collections import namedtuple

import aiohttp
from aiohttp import WSMsgType, web

CONFIG_PATH = os.environ.get("GATEWAY_ROUTES_CONFIG", "/tmp/gateway-routes.json")
HTTP_TIMEOUT = float(os.environ.get("GATEWAY_HTTP_TIMEOUT", "30"))
SSE_TIMEOUT = float(os.environ.get("GATEWAY_SSE_TIMEOUT", "300"))
HEALTH_TIMEOUT = float(os.environ.get("GATEWAY_HEALTH_TIMEOUT", "5"))
SHUTDOWN_TIMEOUT = float(os.environ.get("GATEWAY_SHUTDOWN_TIMEOUT", "30"))
WS_HEARTBEAT = float(os.environ.get("GATEWAY_WS_HEARTBEAT", "30"))
HOP_HEADERS = {"connection", "content-length", "keep-alive", "proxy-authenticate",
               "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade"}
WS_HEADERS = {"sec-websocket-accept", "sec-websocket-extensions", "sec-websocket-key",
              "sec-websocket-protocol", "sec-websocket-version"}
GatewayConfig = namedtuple("GatewayConfig", "path beings default gateway_port")
Route = namedtuple("Route", "being port backend_path")
logger = logging.getLogger("gateway")

class ConfigError(ValueError):
    pass

def reject_duplicate_keys(pairs):
    data = {}
    for key, value in pairs:
        if key in data:
            raise ConfigError(f"duplicate config key: {key}")
        data[key] = value
    return data

def validate_port(value, label):
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConfigError(f"{label} must be an integer port")
    if not 1 <= value <= 65535:
        raise ConfigError(f"{label} must be in range 1..65535")
    return value

def load_config(path=CONFIG_PATH):
    try:
        with open(path, "r", encoding="utf-8") as config_file:
            raw = json.load(config_file, object_pairs_hook=reject_duplicate_keys)
    except FileNotFoundError as exc:
        raise ConfigError(f"config file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("config root must be an object")
    beings_raw = raw.get("beings")
    if not isinstance(beings_raw, dict) or not beings_raw:
        raise ConfigError("config.beings must be a non-empty object")
    beings, ports = {}, {}
    for name, entry in beings_raw.items():
        if not isinstance(name, str) or not name or "/" in name:
            raise ConfigError(f"invalid being name: {name!r}")
        if name == "gateway":
            raise ConfigError("being name is reserved for gateway API: gateway")
        if not isinstance(entry, dict):
            raise ConfigError(f"beings.{name} must be an object")
        port = validate_port(entry.get("port"), f"beings.{name}.port")
        if port in ports:
            raise ConfigError(f"duplicate backend port :{port} for {ports[port]} and {name}")
        beings[name], ports[port] = port, name
    default = raw.get("default")
    if not isinstance(default, str) or default not in beings:
        raise ConfigError("config.default must name a configured being")
    gateway_port = validate_port(raw.get("gateway_port"), "gateway_port")
    if gateway_port in ports:
        raise ConfigError(f"gateway_port :{gateway_port} conflicts with being {ports[gateway_port]}")
    return GatewayConfig(path, beings, default, gateway_port)

def route_summary(config):
    routes = ", ".join(f"/{name}/*→:{port}" for name, port in sorted(config.beings.items()))
    return f"{routes}; default={config.default}; config={config.path}"

def resolve_route(path, config):
    parts = path.strip("/").split("/", 1)
    if not parts or not parts[0]:
        return Route(config.default, config.beings[config.default], "/")
    if parts[0] in config.beings:
        return Route(parts[0], config.beings[parts[0]], "/" + parts[1] if len(parts) > 1 else "/")
    # Preserve default routing for unprefixed API paths like /api/status.
    return Route(config.default, config.beings[config.default], path)

def backend_url(route, request, scheme="http"):
    query = request.raw_path.split("?", 1)[1] if "?" in request.raw_path else ""
    target = f"{scheme}://127.0.0.1:{route.port}{route.backend_path}"
    return f"{target}?{query}" if query else target

def filtered_request_headers(request, websocket=False):
    blocked = HOP_HEADERS | (WS_HEADERS if websocket else set())
    headers = {k: v for k, v in request.headers.items() if k.lower() not in blocked and k.lower() != "host"}
    remote = request.headers.get("X-Real-IP") or request.remote or ""
    prior = request.headers.get("X-Forwarded-For")
    if remote:
        headers["X-Forwarded-For"] = f"{prior}, {remote}" if prior else remote
        headers["X-Real-IP"] = remote
    elif prior:
        headers["X-Forwarded-For"] = prior
    headers["X-Forwarded-Proto"] = request.headers.get("X-Forwarded-Proto", request.scheme)
    return headers

def filtered_response_headers(headers):
    return {k: v for k, v in headers.items() if k.lower() not in HOP_HEADERS}

def json_error(status, error, route=None):
    payload = {"error": error}
    if route:
        payload["being"] = route.being
    return web.json_response(payload, status=status)

def log_request(request, route, status, started_at):
    logger.info("[gateway] %s %s → :%s %s %sms", request.method, request.path_qs,
                route.port, status, int((time.monotonic() - started_at) * 1000))

@web.middleware
async def cors_middleware(request, handler):
    response = web.Response(status=204) if request.method == "OPTIONS" else await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = request.headers.get(
        "Access-Control-Request-Headers", "Content-Type, Authorization, X-Requested-With")
    return response

@web.middleware
async def inflight_middleware(request, handler):
    app = request.app
    if app["closing"]:
        return web.json_response({"error": "gateway shutting down"}, status=503)
    app["inflight"] += 1
    app["inflight_drained"].clear()
    try:
        return await handler(request)
    finally:
        app["inflight"] -= 1
        if app["inflight"] == 0:
            app["inflight_drained"].set()

async def handle_request(request):
    started_at = time.monotonic()
    route = resolve_route(request.path, request.app["config"])
    status = 500

    try:
        if request.headers.get("Upgrade", "").lower() == "websocket":
            response = await handle_ws_proxy(request, route)
            status = response.status or 101
            return response

        response = await handle_http_proxy(request, route)
        status = response.status
        return response
    except asyncio.TimeoutError:
        status = 504
        logger.error("[gateway] timeout being=%s backend=:%s path=%s", route.being, route.port, request.path_qs)
        return json_error(504, "backend timeout", route)
    except aiohttp.ClientError as exc:
        status = 502
        logger.error("[gateway] backend unavailable being=%s backend=:%s path=%s error=%s",
                     route.being, route.port, request.path_qs, exc)
        return json_error(502, "backend unavailable", route)
    except Exception as exc:
        status = 500
        logger.exception("[gateway] proxy failure being=%s backend=:%s path=%s error=%s",
                         route.being, route.port, request.path_qs, exc)
        return json_error(500, "gateway proxy failure", route)
    finally:
        log_request(request, route, status, started_at)


async def handle_http_proxy(request, route):
    session = request.app["session"]
    body = await request.read() if request.can_read_body else None

    async with session.request(
        request.method,
        backend_url(route, request),
        headers=filtered_request_headers(request),
        data=body,
        timeout=aiohttp.ClientTimeout(
            total=SSE_TIMEOUT if "text/event-stream" in request.headers.get("Accept", "") else HTTP_TIMEOUT),
        allow_redirects=False,
    ) as response:
        content_type = response.headers.get("Content-Type", "")
        if "text/event-stream" in content_type:
            return await stream_sse(request, response, route)

        proxied_headers = filtered_response_headers(response.headers)
        return web.Response(
            status=response.status,
            body=await response.read(),
            headers=proxied_headers,
        )


async def stream_sse(request, backend_response, route):
    headers = filtered_response_headers(backend_response.headers)
    headers["Content-Type"] = backend_response.headers.get("Content-Type", "text/event-stream")
    headers.setdefault("Cache-Control", "no-cache")

    response = web.StreamResponse(status=backend_response.status, headers=headers)
    await response.prepare(request)

    try:
        async for chunk in backend_response.content.iter_any():
            await response.write(chunk)
    except (ConnectionResetError, asyncio.CancelledError):
        logger.info("[gateway] SSE client disconnected being=%s backend=:%s", route.being, route.port)
    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
        logger.error(
            "[gateway] SSE backend closed with error being=%s backend=:%s error=%s",
            route.being,
            route.port,
            exc,
        )
    finally:
        with contextlib.suppress(ConnectionResetError, RuntimeError):
            await response.write_eof()

    return response


async def handle_ws_proxy(request, route):
    session = request.app["session"]
    ws_client = await session.ws_connect(
        backend_url(route, request, scheme="ws"),
        headers=filtered_request_headers(request, websocket=True),
        heartbeat=WS_HEARTBEAT,
        timeout=HTTP_TIMEOUT,
        autoping=True,
    )

    ws_server = web.WebSocketResponse(heartbeat=WS_HEARTBEAT, autoping=True)
    await ws_server.prepare(request)

    async def forward(source, target):
        async for msg in source:
            if target.closed:
                break
            if msg.type == WSMsgType.TEXT:
                await target.send_str(msg.data)
            elif msg.type == WSMsgType.BINARY:
                await target.send_bytes(msg.data)
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                break
            elif msg.type == WSMsgType.ERROR:
                raise source.exception() or ConnectionError("websocket relay error")

    try:
        tasks = [
            asyncio.create_task(forward(ws_client, ws_server)),
            asyncio.create_task(forward(ws_server, ws_client)),
        ]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            if task.cancelled():
                continue
            error = task.exception()
            if error:
                logger.error("[gateway] WS relay error being=%s backend=:%s error=%s",
                             route.being, route.port, error)
    except Exception as exc:
        logger.error("[gateway] WS unexpected disconnect being=%s backend=:%s error=%s", route.being, route.port, exc)
    finally:
        for ws in (ws_client, ws_server):
            with contextlib.suppress(Exception):
                await ws.close()

    return ws_server


async def check_backend_health(session, name, port):
    started_at = time.monotonic()
    url = f"http://127.0.0.1:{port}/health"
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=HEALTH_TIMEOUT)) as response:
            ok = 200 <= response.status < 300
            return name, {
                "ok": ok,
                "status": "healthy" if ok else "unhealthy",
                "port": port,
                "http_status": response.status,
                "duration_ms": int((time.monotonic() - started_at) * 1000),
            }
    except asyncio.TimeoutError:
        status = "timeout"
    except aiohttp.ClientError:
        status = "unavailable"

    return name, {
        "ok": False,
        "status": status,
        "port": port,
        "duration_ms": int((time.monotonic() - started_at) * 1000),
    }


async def handle_gateway_health(request):
    config = request.app["config"]
    session = request.app["session"]
    checks = [check_backend_health(session, name, port) for name, port in sorted(config.beings.items())]
    backends = dict(await asyncio.gather(*checks))
    ok = all(item["ok"] for item in backends.values())
    return web.json_response({"ok": ok, "status": "healthy" if ok else "degraded",
                              "gateway_port": config.gateway_port, "default": config.default,
                              "config": config.path, "backends": backends})


async def handle_gateway_reload(request):
    async with request.app["reload_lock"]:
        current = request.app["config"]
        try:
            config = load_config(current.path)
            if config.gateway_port != current.gateway_port:
                raise ConfigError("gateway_port changes require restart")
        except ConfigError as exc:
            logger.error("[gateway] reload failed config=%s error=%s", current.path, exc)
            return web.json_response({"ok": False, "error": str(exc)}, status=400)

        request.app["config"] = config
        logger.info("[gateway] reloaded routes: %s", route_summary(config))
        beings = {name: {"port": port} for name, port in sorted(config.beings.items())}
        return web.json_response({"ok": True, "gateway_port": config.gateway_port,
                                  "default": config.default, "beings": beings})


async def on_startup(app):
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    app["session"] = aiohttp.ClientSession(connector=connector, auto_decompress=False)


async def on_shutdown(app):
    app["closing"] = True
    deadline = time.monotonic() + SHUTDOWN_TIMEOUT
    while app["inflight"] > 0 and time.monotonic() < deadline:
        timeout = min(1, max(0.01, deadline - time.monotonic()))
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(app["inflight_drained"].wait(), timeout=timeout)

    if app["inflight"] > 0:
        logger.warning("[gateway] shutdown timed out with %s in-flight requests", app["inflight"])
    else:
        logger.info("[gateway] drained in-flight requests")


async def on_cleanup(app):
    await app["session"].close()


def create_app(config):
    app = web.Application(middlewares=[cors_middleware, inflight_middleware])
    app["config"], app["closing"], app["inflight"] = config, False, 0
    app["inflight_drained"] = asyncio.Event()
    app["inflight_drained"].set()
    app["reload_lock"] = asyncio.Lock()
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/gateway/health", handle_gateway_health)
    app.router.add_post("/gateway/reload", handle_gateway_reload)
    app.router.add_route("*", "/{path_info:.*}", handle_request)
    return app


def main():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        config = load_config()
    except ConfigError as exc:
        logger.error("[gateway] startup failed: %s", exc)
        raise SystemExit(1) from exc

    logger.info("[gateway] starting on :%s", config.gateway_port)
    logger.info("[gateway] routes: %s", route_summary(config))
    web.run_app(create_app(config), port=config.gateway_port, print=None,
                shutdown_timeout=SHUTDOWN_TIMEOUT)


if __name__ == "__main__":
    main()
