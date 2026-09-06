#!/usr/bin/env python3
"""Mock heart-core for gateway testing. Run multiple on different ports."""

import asyncio
import sys
import json
import time
from aiohttp import web, WSMsgType

NAME = sys.argv[1] if len(sys.argv) > 1 else "test"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 4001

async def handle_health(request):
    return web.json_response({"status": "healthy", "being": NAME, "port": PORT})

async def handle_status(request):
    return web.json_response({
        "being_name": NAME, "status": "active", "model": "test",
        "surface_tokens": 1234, "uptime_seconds": 999
    })

async def handle_history(request):
    limit = int(request.query.get("limit", "10"))
    msgs = [
        {"role": "user", "content": "hello", "seq": 1, "at": "2026-06-15T20:00:00Z"},
        {"role": "assistant", "content": f"hi from {NAME}!", "seq": 2, "at": "2026-06-15T20:00:01Z"},
    ]
    return web.json_response({"messages": msgs[:limit]})

async def handle_chat_stream(request):
    """SSE streaming response — simulates thinking + reply."""
    data = await request.json()
    msg = data.get("message", "")
    
    response = web.StreamResponse(
        headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"}
    )
    await response.prepare(request)
    
    # Simulate thinking
    await response.write(b"event: thinking\ndata: {}\n\n")
    await asyncio.sleep(0.5)
    
    # Stream reply
    reply = f"[{NAME}] received: {msg}"
    for i, ch in enumerate(reply):
        chunk = json.dumps({"text": ch, "seq": i})
        await response.write(f"event: delta\ndata: {chunk}\n\n".encode())
        await asyncio.sleep(0.05)
    
    # Done
    await response.write(b"event: done\ndata: {}\n\n")
    await response.write_eof()
    return response

async def handle_ws_relay(request):
    """WebSocket echo — simulates relay endpoint."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    print(f"[{NAME}] WS relay connected")
    
    async for msg in ws:
        if msg.type == WSMsgType.TEXT:
            print(f"[{NAME}] WS recv: {msg.data}")
            await ws.send_str(f"[{NAME}] echo: {msg.data}")
        elif msg.type == WSMsgType.CLOSE:
            break
    
    print(f"[{NAME}] WS relay disconnected")
    return ws

async def handle_loom(request):
    return web.Response(text=f"<html><body><h1>Loom: {NAME}</h1></body></html>",
                       content_type="text/html")

app = web.Application()
app.router.add_get("/health", handle_health)
app.router.add_get("/api/status", handle_status)
app.router.add_get("/api/history", handle_history)
app.router.add_post("/api/chat/stream", handle_chat_stream)
app.router.add_get("/_relay", handle_ws_relay)
app.router.add_get("/", handle_loom)

if __name__ == "__main__":
    print(f"[mock-{NAME}] starting on :{PORT}")
    web.run_app(app, port=PORT, print=None)
