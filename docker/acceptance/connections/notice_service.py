"""Disposable MCP service with one static tool and aggregate observations."""

import sys

import uvicorn
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.responses import JSONResponse


def create_app(name: str):
    if name not in {"notices-a", "notices-b", "notices-unlisted"}:
        raise ValueError("Unknown fixture service")

    counts = {"requests": 0, "discoveries": 0, "calls": 0}

    class NoticeServer(FastMCP):
        async def list_tools(self):
            counts["discoveries"] += 1
            return await super().list_tools()

    service = NoticeServer(
        name,
        stateless_http=True,
        json_response=True,
        transport_security=TransportSecuritySettings(allowed_hosts=[f"{name}.example.test:8080"]),
    )

    @service.tool()
    async def read_notice() -> str:
        """Read the notice published by this service."""
        counts["calls"] += 1
        return f"Notice from {name}."

    @service.custom_route("/observations", methods=["GET"])
    async def observations(_request):
        return JSONResponse(counts.copy())

    app = service.streamable_http_app()

    async def counted(scope, receive, send):
        if scope["type"] == "http" and scope["path"] != "/observations":
            counts["requests"] += 1
        await app(scope, receive, send)

    return counted


if __name__ == "__main__":
    uvicorn.run(create_app(sys.argv[1]), host="0.0.0.0", port=8080, ssl_certfile="/fixture/notice.pem", ssl_keyfile="/fixture/notice.key", access_log=False, log_level="warning")
