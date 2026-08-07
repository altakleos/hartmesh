"""Shared HTTP envelope primitives for the versioned runtime namespace."""

from __future__ import annotations

from typing import Any

from deerflow_runtime_api import API_VERSION, FailureCode
from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from starlette.responses import JSONResponse

RUNTIME_API_PREFIX = "/api/runtime/v1"


def is_runtime_api_path(path: str) -> bool:
    return path == RUNTIME_API_PREFIX or path.startswith(f"{RUNTIME_API_PREFIX}/")


def runtime_error_response(
    status_code: int,
    code: FailureCode | str,
    *,
    details: dict[str, Any] | None = None,
) -> JSONResponse:
    content: dict[str, Any] = {
        "api_version": API_VERSION,
        "kind": "runtime.error",
        "code": str(code),
    }
    if details:
        content["details"] = details
    return JSONResponse(status_code=status_code, content=content)


async def _runtime_request_validation_error(
    request: Request,
    exc: RequestValidationError,
):
    if is_runtime_api_path(request.url.path):
        return runtime_error_response(422, FailureCode.invalid_request)
    return await request_validation_exception_handler(request, exc)


def install_runtime_error_handlers(app: FastAPI) -> None:
    """Keep framework validation failures inside the runtime wire contract."""

    app.add_exception_handler(
        RequestValidationError,
        _runtime_request_validation_error,
    )


__all__ = [
    "RUNTIME_API_PREFIX",
    "install_runtime_error_handlers",
    "is_runtime_api_path",
    "runtime_error_response",
]
