"""Correlate the browser's final notice with its thread checkpoint and durable run journal."""


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
