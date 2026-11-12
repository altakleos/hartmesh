"""Detached result media plans; only current-recipient exports authorize bytes."""

from app.telegram_bots.render import render_plain
from app.telegram_bots.work_requests import WorkRequestPresentation, present_request
from deerflow.agent_instances.work_contract import WorkOutcome, WorkSource


def result_options(value):
    if type(value) is not dict or set(value) != {"version", "capacity", "max_bytes"} or type(value["version"]) is not int or value["version"] != 1:
        raise ValueError("invalid-work-result-options")
    if type(value["capacity"]) is not int or type(value["max_bytes"]) is not int or not 0 < value["max_bytes"] <= min(value["capacity"], 50_000_000):
        raise ValueError("invalid-work-result-options")
    return {key: value[key] for key in ("capacity", "max_bytes")}


def result_files(record, *, locale):
    """Keep outcome indices and Space identities, including redacted entries."""
    outcome = record.get("outcome")
    if type(outcome) is not dict or type(outcome.get("sources")) is not list or len(outcome["sources"]) > 16:
        raise ValueError("invalid-work-result")
    checked = WorkOutcome.model_validate({**outcome, "sources": []})
    if (checked.id, checked.assignment_revision) != (record.get("outcome_id"), record.get("assignment_revision")):
        raise ValueError("invalid-work-result")
    files = []
    for index, reference in enumerate(outcome["sources"]):
        if reference == {"kind": "unavailable", "current_contents": "not_checked"}:
            source, filename = None, render_plain("label.file", locale)
        else:
            if type(reference) is not dict or reference.get("current_contents") != "not_checked":
                raise ValueError("invalid-work-result-source")
            source = WorkSource.model_validate({key: value for key, value in reference.items() if key != "current_contents"}).model_dump()
            filename = source["path"].rsplit("/", 1)[-1]
        files.append({"source_index": index, "filename": filename, "source": source})
    return files


def result_media(files, *, capacity, max_bytes, protected):
    """Partition unknown-size exports before opening a file or reserving bytes."""
    if type(capacity) is not int or type(max_bytes) is not int or not 0 < max_bytes <= min(capacity, 50_000_000) or type(protected) is not bool:
        raise ValueError("invalid-work-result-limits")
    parts, photos = [], []

    def append(kind, members):
        parts.append({"kind": kind, "files": list(members), "silent": True, "protect_content": protected})

    def flush():
        if photos:
            append("album" if len(photos) > 1 else "photo", photos)
            photos.clear()

    for file in files:
        if file["source"] is None or not file["filename"].lower().endswith((".jpg", ".jpeg", ".png")):
            flush()
            append("document", [file])
        else:
            if len(photos) == min(10, capacity // max_bytes):
                flush()
            photos.append(file)
    flush()
    return tuple(parts)


def present_review_files(source, *, locale, capacity, max_bytes, concealed=False, outbox_id=None):
    if source.request["purpose"] != "review":
        raise ValueError("work-review-required")
    files = result_files(source.work, locale=locale)
    media = result_media(files, capacity=capacity, max_bytes=max_bytes, protected=source.protected)
    card = present_request(source, locale=locale, concealed=concealed, outbox_id=outbox_id, files_above=tuple(file["filename"] for file in files))
    return WorkRequestPresentation((() if concealed else media) + card.parts, card.keyboard)


def classify_result_media(files, photos):
    if type(photos) is not list or len(files) != len(photos) or any(type(value) is not bool for value in photos):
        raise ValueError("invalid-result-photo-classification")
    result, group = [], []

    def flush():
        nonlocal group
        if group:
            result.append(("album" if len(group) > 1 else "photo", group))
            group = []

    for file, photo in zip(files, photos, strict=True):
        if photo:
            group.append(file)
            if len(group) == 10:
                flush()
        else:
            flush()
            result.append(("document", [file]))
    flush()
    return tuple(result)
