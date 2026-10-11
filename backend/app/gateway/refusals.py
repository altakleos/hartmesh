"""Application-wide structured host refusals, independent of route wrappers."""

from fastapi import Request
from fastapi.responses import JSONResponse

from deerflow.agent_instances.contract import Refusal


async def refusal_handler(request: Request, exc: Refusal) -> JSONResponse:
    return JSONResponse(status_code=exc.status, content={"detail": exc.detail, "reason": exc.reason, **exc.extra})
