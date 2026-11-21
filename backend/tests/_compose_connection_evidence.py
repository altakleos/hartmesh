"""Measure initial browser reads and correlate its final notice with the durable run."""

from contextlib import contextmanager
from time import monotonic

_BROWSER_FAILURES = {
    "net::ERR_ABORTED": "aborted",
    "net::ERR_BLOCKED_BY_CLIENT": "blocked-by-client",
    "net::ERR_BLOCKED_BY_RESPONSE": "blocked-by-response",
    "net::ERR_CONNECTION_CLOSED": "connection-closed",
    "net::ERR_CONNECTION_REFUSED": "connection-refused",
    "net::ERR_CONNECTION_RESET": "connection-reset",
    "net::ERR_NAME_NOT_RESOLVED": "name-not-resolved",
    "net::ERR_TIMED_OUT": "timed-out",
}


@contextmanager
def observe_initial_thread_reads(page, thread_url, evidence):
    """Observe only the two canonical reads between navigation and the first edit."""
    endpoints = {("GET", thread_url): "metadata", ("POST", thread_url + "/history"): "history"}
    attempts = {"metadata": [], "history": []}
    tracked = {}
    start = monotonic()

    def elapsed_ms():
        return round((monotonic() - start) * 1000)

    def issued(request):
        kind = endpoints.get((request.method, request.url.partition("?")[0]))
        if kind is not None:
            row = {"state": "pending", "started_ms": elapsed_ms()}
            attempts[kind].append(row)
            tracked[request] = row

    def headers(response):
        if response.request in tracked:
            tracked[response.request]["http_status"] = response.status

    def ended(request, state):
        if request in tracked:
            row = tracked.pop(request)
            row.update(state=state, elapsed_ms=elapsed_ms() - row["started_ms"])
            if state == "failed":
                row["failure_kind"] = _BROWSER_FAILURES.get(request.failure, "other")

    listeners = {"request": issued, "response": headers, "requestfinished": lambda req: ended(req, "completed"), "requestfailed": lambda req: ended(req, "failed")}
    for event, handler in listeners.items():
        page.on(event, handler)
    try:
        yield
    finally:
        duration = elapsed_ms()
        for row in tracked.values():
            row["elapsed_ms"] = duration - row["started_ms"]
        evidence["initial_reads"] = {
            "observation_ms": duration,
            **{kind: {"state": rows[-1]["state"] if rows else "not-issued", "attempts": rows} for kind, rows in attempts.items()},
        }
        for event, handler in listeners.items():
            page.remove_listener(event, handler)


def _answer_text(message):
    content = message.get("content")
    if not isinstance(content, list):
        return content
    parts = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") == "text" and isinstance(block.get("text"), str):
            parts.append(block["text"])
    return "".join(parts)


def confirmed_browser_notice(thread_id, run_id, expected, messages, events):
    final = next((message for message in reversed(messages) if message.get("type") == "ai" and not message.get("additional_kwargs", {}).get("hide_from_ui")), None)
    if not final or not final.get("id") or final.get("tool_calls") or _answer_text(final) != expected:
        return None
    for event in events:
        if event.get("thread_id") != thread_id or event.get("run_id") != run_id or event.get("event_type") != "llm.ai.response" or event.get("category") != "message":
            continue
        message = event.get("content")
        if isinstance(message, dict) and message.get("type") == "ai" and message.get("id") == final["id"] and _answer_text(message) == expected:
            return final["id"]
    return None
