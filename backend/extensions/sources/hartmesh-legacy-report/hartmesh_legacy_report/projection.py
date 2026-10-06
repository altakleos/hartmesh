"""Display-only field projection over the host's authorized immutable byte snapshot."""

import json

CARD_FIELDS = ("version", "meta", "kpis", "sections", "charts", "checks", "notes")
META_FIELDS = ("title", "period", "draft", "brand", "company", "currency", "inputs")


def _reject_nonfinite(value: str) -> None:
    raise ValueError("Non-finite JSON number")


def project_report(raw: bytes) -> bytes:
    report = json.loads(raw.decode("utf-8"), parse_constant=_reject_nonfinite)
    if not isinstance(report, dict) or not isinstance(report.get("meta"), dict):
        raise ValueError("Not a report object")
    projection = {key: report[key] for key in CARD_FIELDS if key in report}
    projection["meta"] = {key: report["meta"][key] for key in META_FIELDS if key in report["meta"]}
    return json.dumps(projection, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("utf-8")
