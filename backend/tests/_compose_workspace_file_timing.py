"""Retain successful and failed file-navigation observations separately."""

import math
import time


def summary(values):
    result = {"sample_count": len(values), "maximum_seconds": max(values)}
    if len(values) >= 20:
        result["p95_seconds"] = sorted(values)[math.ceil(len(values) * 0.95) - 1]
    return result


def observe(record, key, procedure):
    started = time.monotonic()
    try:
        procedure()
    except Exception:
        record[key]["failed_after_seconds"] = round(time.monotonic() - started, 5)
        raise
    times = record[key].setdefault("seconds", [])
    times.append(round(time.monotonic() - started, 5))
    record[key].update(summary(times))
