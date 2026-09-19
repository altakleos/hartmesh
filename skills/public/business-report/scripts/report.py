#!/usr/bin/env python3
"""Turn a tabular export (CSV, XLSX, XLS) into a management report.

Commands (see SKILL.md for the workflow):

    report.py inspect <files…>                       schema, roles, period and currency guesses
    report.py build   <files…> --period P --out DIR  report.json, charts/*.png, checks.json
    report.py show    <report.json>                  the figures, checks and notes, without the rows
    report.py prose   <report.json> --from prose.json  model-written text, numbers verified
    report.py render  <report.json> --to pdf,docx,xlsx   every named format in one run
    report.py checks  <report.json> <files…>         re-run the checks on their own

Everything the renderers show comes from one report.json, so the HTML, PDF,
DOCX and XLSX agree by construction. The script runs on the libraries the
sandbox image ships (docker/sandbox/Dockerfile), installs nothing, never
calls a network service, never shells out and never modifies an input file.
A file is addressed as ``path`` or ``path::Sheet`` for workbooks.
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import hashlib
import io
import json
import math
import os
import re
import sys
import warnings
from dataclasses import dataclass
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

try:
    import openpyxl  # noqa: F401  (pandas' xlsx engine; asserted so a missing engine fails here, not mid-build)
    import pandas as pd
    from business_report_common import (
        CURRENCY_CODES,
        DEFAULT_PROFILE,
        DEFAULT_REPORTS_DIR,
        DEFAULT_TENANT_DIR,
        EXIT_DECISION_NEEDED,
        EXIT_MISSING_LIBRARY,
        EXIT_OK,
        EXIT_WITHHELD,
        PRESENTED_TARGETS,
        PROFILES_DIR,
        RENDER_TARGETS,
        RENDERS_MANIFEST,
        REPORT_SUFFIX,
        REQUIRED_ROLES,
        ROLES,
        SYMBOL_TO_CODE,
        TARGET_ORDER,
        TEXT_ROLES,
        BuildContext,
        BuildOptions,
        DecisionNeeded,
        InputError,
        LoadedTable,
        Mapping,
        checks_line,
        format_value,
        in_period,
        is_are,
        is_color,
        is_missing,
        load_brand,
        one_line,
        parse_period,
        plural,
        previous_period,
        same_period_last_year,
        slugify,
        suggest_period,
        to_text,
        utc_now,
        vocab,
    )
    from business_report_render import draw_charts, fetch_inline_only, render
    from business_report_sections import SECTION_BUILDERS, TABLE_ROW_LIMIT, build_report
except ImportError as error:
    from business_report_common import MISSING_LIBRARY_MESSAGE  # stdlib-only module, always importable

    sys.stderr.write(f"{MISSING_LIBRARY_MESSAGE} ({error})\n")
    sys.exit(EXIT_MISSING_LIBRARY if "EXIT_MISSING_LIBRARY" in dir() else 2)

__all__ = ["DEFAULT_REPORTS_DIR", "SECTION_BUILDERS", "TABLE_ROW_LIMIT", "checks_line", "fetch_inline_only", "format_value", "main", "parse_period", "previous_period", "same_period_last_year"]

SAMPLE_ROWS = 200
NUMBER_TOKEN = re.compile(r"(?<![A-Za-z0-9.])([$€£]?)(-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|-?\d+(?:\.\d+)?)([%kK])?(?![A-Za-z0-9])")
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
DMY_PATTERN = re.compile(r"^\s*(\d{1,2})[/.\-](\d{1,2})[/.\-](\d{2,4})\b")
LETTERS = re.compile(r"[A-Za-z]")


# --- profile, preferences ------------------------------------------------------


def load_profile(name_or_path: str | None, tenant_dir: str | None) -> dict:
    """A profile by path, by name from the tenant bundle, or by name from the skill."""

    if name_or_path and name_or_path.endswith(".json"):
        candidates = [Path(name_or_path)]
    else:
        name = name_or_path or DEFAULT_PROFILE
        candidates = []
        if tenant_dir:
            candidates.append(Path(tenant_dir) / "report-profiles" / f"{name}.json")
        candidates.append(PROFILES_DIR / f"{name}.json")
    for candidate in candidates:
        if candidate.is_file():
            profile = json.loads(candidate.read_text(encoding="utf-8"))
            for key in ("name", "vocabulary", "aliases", "sections"):
                if key not in profile:
                    raise InputError(f"Profile {candidate} has no '{key}' key.")
            missing_roles = [role for role in ROLES if role not in profile["aliases"]]
            if missing_roles:
                raise InputError(f"Profile {candidate} has no aliases for: {', '.join(missing_roles)}.")
            profile.setdefault("status_groups", {})
            profile.setdefault("top_n", 10)
            profile.setdefault("title", "{period} Business Review")
            return profile
    raise InputError(f"No report profile named {name_or_path or DEFAULT_PROFILE!r} (looked in {', '.join(str(c) for c in candidates)}).")


def resolve_tenant_dir(tenant_dir: str | None) -> str | None:
    if tenant_dir:
        return tenant_dir
    return DEFAULT_TENANT_DIR if Path(DEFAULT_TENANT_DIR).is_dir() else None


def apply_preferences(options: BuildOptions, prefs: dict) -> tuple[BuildOptions, list[str]]:
    """Merge preferences.json into build options; pure, returns what was applied."""

    merged = copy.deepcopy(options)
    applied: list[str] = []
    brand = prefs.get("brand")
    if isinstance(brand, dict):
        for key in ("primary", "secondary"):
            if is_color(brand.get(key)):
                merged.brand[key] = brand[key]
                applied.append(f"brand.{key}")
    exclusions = prefs.get("exclusions")
    if isinstance(exclusions, list):
        for index, exclusion in enumerate(exclusions):
            normalized = normalize_exclusion(exclusion)
            if normalized is not None:
                merged.exclusions.append(normalized)
                applied.append(f"exclusions[{index}]")
    if prefs.get("summary_length") in ("short", "standard"):
        merged.summary_length = prefs["summary_length"]
        applied.append("summary_length")
    comparisons = prefs.get("comparisons")
    if isinstance(comparisons, list) and all(item in ("previous_period", "same_period_last_year") for item in comparisons):
        merged.comparisons = list(comparisons)
        applied.append("comparisons")
    charts = prefs.get("charts")
    if isinstance(charts, list) and all(isinstance(item, str) for item in charts):
        merged.charts = list(charts)
        applied.append("charts")
    currency = prefs.get("currency")
    if isinstance(currency, str) and re.fullmatch(r"[A-Z]{3}", currency):
        merged.currency = currency
        applied.append("currency")
    return merged, applied


def normalize_exclusion(value) -> dict | None:
    if isinstance(value, str):
        key, separator, equals = value.partition("=")
        if not separator:
            return None
        key = key.strip()
        target = "role" if key in ROLES else "column"
        return {target: key, "equals": equals.strip()}
    if isinstance(value, dict) and "equals" in value and ("role" in value or "column" in value):
        target = "role" if "role" in value else "column"
        return {target: str(value[target]), "equals": str(value["equals"])}
    return None


# --- reading inputs ------------------------------------------------------------


def parse_source(source: str) -> tuple[Path, str | None]:
    path_text, separator, sheet = source.rpartition("::")
    if separator and path_text:
        return Path(path_text), sheet
    return Path(source), None


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def usable_header(name) -> bool:
    text = to_text(name)
    return bool(text) and re.fullmatch(r"Unnamed: \d+", text) is None


def _read_csv(path: Path) -> pd.DataFrame:
    raw = path.read_bytes()
    for encoding in ("utf-8-sig", "cp1252"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise InputError(f"{path.name} is not UTF-8 or Windows-1252 text.")
    try:
        delimiter = csv.Sniffer().sniff(text[:8192], delimiters=",;\t|").delimiter
    except csv.Error:
        delimiter = ","
    frame = pd.read_csv(io.StringIO(text), sep=delimiter, dtype=str, keep_default_na=False, skip_blank_lines=True)
    frame.columns = [str(column) for column in frame.columns]
    return frame


def _read_workbook(path: Path) -> dict[str, pd.DataFrame]:
    try:
        sheets = pd.read_excel(path, sheet_name=None)
    except Exception as error:  # xlrd/openpyxl raise their own families
        raise InputError(f"{path.name} could not be read as a workbook: {error}") from error
    result: dict[str, pd.DataFrame] = {}
    for name, frame in sheets.items():
        frame.columns = [str(column) for column in frame.columns]
        result[str(name)] = frame
    return result


def read_tables(path: Path, profile: dict) -> tuple[list[tuple[str | None, pd.DataFrame]], list[dict], bool]:
    """Every data table in a file, the sheets skipped and why, and whether it is a workbook."""

    if not path.is_file():
        raise InputError(f"No such file: {path}")
    if path.suffix.lower() in (".csv", ".txt", ".tsv"):
        return [(None, _read_csv(path))], [], False
    if path.suffix.lower() not in (".xlsx", ".xlsm", ".xls"):
        raise InputError(f"{path.name}: only .csv, .xlsx, .xlsm and .xls files are supported.")
    tables: list[tuple[str | None, pd.DataFrame]] = []
    skipped: list[dict] = []
    for sheet, frame in _read_workbook(path).items():
        if frame.shape[0] == 0 or frame.shape[1] == 0:
            skipped.append({"sheet": sheet, "reason": "empty"})
            continue
        mapping = suggest_mapping(frame, profile)
        if mapping.missing:
            skipped.append({"sheet": sheet, "reason": f"no {' and '.join(mapping.missing)} column"})
            continue
        tables.append((sheet, frame))
    return tables, skipped, True


def read_table(source: str, profile: dict | None = None) -> LoadedTable:
    """One data table from ``path`` or ``path::Sheet``."""

    profile = profile or load_profile(None, None)
    path, sheet = parse_source(source)
    tables, skipped, is_workbook = read_tables(path, profile)
    if sheet is not None:
        chosen = [table for table in tables if table[0] == sheet]
        if not chosen:
            reason = next((entry["reason"] for entry in skipped if entry["sheet"] == sheet), None)
            if reason:
                raise InputError(f"Sheet {sheet!r} in {path.name} has no data table ({reason}).")
            names = [table[0] for table in tables] + [entry["sheet"] for entry in skipped]
            raise InputError(f"No sheet named {sheet!r} in {path.name} (sheets: {', '.join(names)}).")
        sheet, frame = chosen[0]
    elif len(tables) == 1:
        sheet, frame = tables[0]
    elif not tables:
        detail = "; ".join(f"{entry['sheet']}: {entry['reason']}" for entry in skipped)
        raise InputError(f"{path.name} has no sheet with a date and an amount column ({detail}).")
    else:
        names = [table[0] for table in tables]
        raise DecisionNeeded(
            f"{path.name} has several sheets with data: {', '.join(names)}. Which one is the export? Pass it as {path.name}::<sheet>.",
            {"file": path.name, "sheets": names},
        )
    return LoadedTable(
        name=path.name,
        path=str(path),
        sheet=sheet,
        frame=frame,
        sha256=sha256_of(path),
        rows=int(frame.shape[0]),
        uploaded=dt.date.fromtimestamp(path.stat().st_mtime).isoformat(),
        is_workbook=is_workbook,
        skipped_sheets=skipped,
    )


# --- column roles --------------------------------------------------------------


def normalize_header(name) -> str:
    return re.sub(r"[^a-z0-9]+", " ", to_text(name).lower()).strip()


def _sample(series: pd.Series) -> list:
    return [value for value in series.head(SAMPLE_ROWS).tolist() if not is_missing(value) and to_text(value) != ""]


def looks_like_dates(series: pd.Series) -> bool:
    if pd.api.types.is_datetime64_any_dtype(series):
        return True
    values = _sample(series)
    if not values:
        return False
    texts = [to_text(value) for value in values]
    if sum(1 for text in texts if re.fullmatch(r"-?\d+(\.\d+)?", text)) > len(texts) / 2:
        return False
    parsed = parse_dates(pd.Series(texts))
    return int(parsed.notna().sum()) >= 0.9 * len(texts)


def _strip_currency_words(text: str) -> str:
    text = re.sub(r"\b(" + "|".join(CURRENCY_CODES) + r")\b", "", text.upper())
    for symbol in SYMBOL_TO_CODE:
        text = text.replace(symbol, "")
    return text


def looks_numeric(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
        return True
    values = _sample(series)
    if not values:
        return False
    texts = [to_text(value) for value in values]
    if any(LETTERS.search(_strip_currency_words(text)) for text in texts):
        return False
    style = detect_number_style(texts)
    parsed = sum(1 for text in texts if not math.isnan(parse_amount(text, style)))
    return parsed >= 0.9 * len(texts)


def suggest_mapping(frame: pd.DataFrame, profile: dict) -> Mapping:
    """Header names first, then value types; two equally good columns are a question, not a guess."""

    columns = [column for column in frame.columns if usable_header(column)]
    normalized = {column: normalize_header(column) for column in columns}
    date_like = {column: looks_like_dates(frame[column]) for column in columns}
    roles: dict[str, str | None] = {role: None for role in ROLES}
    confidence: dict[str, str] = {}
    ambiguous: dict[str, list[str]] = {}
    alternatives: dict[str, list[str]] = {}
    assigned: set[str] = set()

    def free(candidates: list[str]) -> list[str]:
        return [column for column in candidates if column not in assigned]

    for role in ROLES:
        aliases = [normalize_header(alias) for alias in profile["aliases"][role]]
        pool = [column for column in columns if column not in assigned and (role == "date" or not date_like[column])]
        exact = [column for column in pool if normalized[column] in aliases]
        if len(exact) > 1:
            ambiguous[role] = exact
            continue
        if exact:
            roles[role], confidence[role] = exact[0], "high"
            assigned.add(exact[0])
            continue
        partial = [column for column in pool if any(re.search(rf"(^| ){re.escape(alias)}( |$)", normalized[column]) for alias in aliases)]
        if len(partial) > 1:
            ambiguous[role] = partial
            continue
        if partial:
            roles[role], confidence[role] = partial[0], "medium"
            assigned.add(partial[0])

    if roles["date"] is None and "date" not in ambiguous:
        candidates = free([column for column in columns if date_like[column]])
        if len(candidates) == 1:
            roles["date"], confidence["date"] = candidates[0], "low"
            assigned.add(candidates[0])
        elif candidates:
            ambiguous["date"] = candidates
    if roles["amount"] is None and "amount" not in ambiguous:
        candidates = free([column for column in columns if not date_like[column] and looks_numeric(frame[column])])
        if len(candidates) == 1:
            roles["amount"], confidence["amount"] = candidates[0], "low"
            assigned.add(candidates[0])
        elif candidates:
            ambiguous["amount"] = candidates
    for role in ("date", "amount"):
        if roles[role] is not None:
            others = free([column for column in columns if (date_like[column] if role == "date" else looks_numeric(frame[column]))])
            if others:
                alternatives[role] = others
    missing = [role for role in REQUIRED_ROLES if roles[role] is None and role not in ambiguous]
    return Mapping(roles=roles, confidence=confidence, ambiguous=ambiguous, missing=missing, alternatives=alternatives)


def resolve_mapping(frame: pd.DataFrame, profile: dict, override: dict | None, table_name: str) -> Mapping:
    """The suggested mapping with the agent's mapping file applied; an explicit column displaces a guessed one."""

    mapping = suggest_mapping(frame, profile)
    if not override:
        return mapping
    explicit: dict[str, str] = {}
    for role, column in override.items():
        if role not in ROLES:
            raise InputError(f"Mapping names an unknown role {role!r}; roles are {', '.join(ROLES)}.")
        mapping.ambiguous.pop(role, None)
        if column is None:
            mapping.roles[role] = None
            mapping.confidence.pop(role, None)
            continue
        if column not in frame.columns:
            raise InputError(f"Mapping puts {role!r} on column {column!r}, which {table_name} does not have.")
        if column in explicit.values():
            other = next(name for name, taken in explicit.items() if taken == column)
            raise InputError(f"Mapping uses column {column!r} for two roles, {other} and {role}; give one of them another column or null.")
        for other, taken in list(mapping.roles.items()):
            if taken == column and other != role and other not in explicit:
                mapping.roles[other] = None  # the guess yields to the explicit choice
                mapping.confidence.pop(other, None)
        mapping.roles[role] = column
        mapping.confidence[role] = "mapping"
        explicit[role] = column
    mapping.missing = [role for role in REQUIRED_ROLES if mapping.roles[role] is None and role not in mapping.ambiguous]
    return mapping


def mapping_question(mapping: Mapping, table_name: str, profile: dict) -> str | None:
    """One question that covers every open role, so the user is asked once."""

    vocabulary = profile["vocabulary"]

    def describe(role: str) -> str:
        return {"date": "date", "amount": vocabulary.get("amount", "amount")}.get(role, vocabulary.get(role, role))

    parts = [f"the {describe(role)} ({' or '.join(candidates)})" for role, candidates in mapping.ambiguous.items()]
    parts += [f"the {describe(role)} (no column matched)" for role in mapping.missing]
    if not parts:
        return None
    what = "column holds" if len(parts) == 1 else "columns hold"
    return f"Which {what} {', and '.join(parts)} in {table_name}?"


# --- parsing values ------------------------------------------------------------


EU_STRONG = re.compile(r"\d,\d{1,2}$")
EU_WEAK = re.compile(r"\d\.\d{3}$")
US_STRONG = re.compile(r"\d\.\d{1,2}$")
US_GROUPED = re.compile(r"\d,\d{3}(?:\D|$)")


def detect_number_style(values) -> str | None:
    """Decide once per column whether ',' or '.' is the decimal separator ("us", "eu", or None when nothing says)."""

    eu_strong = us_strong = eu_weak = us_grouped = plain = double_dot = 0
    for value in values:
        text = to_text(value)
        if not text:
            continue
        digits = re.sub(r"[^\d.,]", "", text)
        if EU_STRONG.search(digits):
            eu_strong += 1
        elif US_STRONG.search(digits):
            us_strong += 1
        elif EU_WEAK.search(digits) and "," not in digits:
            eu_weak += 1
        if US_GROUPED.search(digits) and "." in digits:
            us_grouped += 1
        if re.fullmatch(r"\d+", digits):
            plain += 1
        if re.search(r"\d\.\d{3}\.\d{3}", digits):
            double_dot += 1
    if eu_strong > us_strong + us_grouped:
        return "eu"
    if us_strong or us_grouped:
        return "us"
    # "3.000" alone could be three thousand or three with three decimals; a column
    # that also holds plain integers ("850") or "1.234.567" is grouping thousands,
    # one where every value carries three decimals is not.
    if eu_weak and (double_dot or plain):
        return "eu"
    if eu_weak:
        return "us"
    return None


def _digits_to_float_text(digits: str, style: str | None) -> str:
    if style == "eu":
        return digits.replace(".", "").replace(",", ".")
    if style == "us":
        return digits.replace(",", "")
    if "," in digits and "." in digits:
        return digits.replace(".", "").replace(",", ".") if digits.rfind(",") > digits.rfind(".") else digits.replace(",", "")
    if "," in digits:
        groups = digits.split(",")
        if all(len(group) == 3 for group in groups[1:]) and len(groups[0]) <= 3:
            return digits.replace(",", "")
        return digits.replace(",", ".")
    return digits


def parse_amount(value, style: str | None = None) -> float:
    """A money cell as a float; NaN when it cannot be read (the caller counts those)."""

    if is_missing(value) or isinstance(value, bool):
        return math.nan
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return math.nan
    negative = False
    if text.startswith("(") and text.endswith(")"):
        negative, text = True, text[1:-1]
    if text.endswith("-"):
        negative, text = True, text[:-1]
    if text.startswith("-"):
        negative, text = True, text[1:]
    digits = re.sub(r"[^\d.,]", "", text)
    if not re.search(r"\d", digits):
        return math.nan
    try:
        result = float(_digits_to_float_text(digits, style))
    except ValueError:
        return math.nan
    return -result if negative else result


def _decimal_from_cell(value, style: str | None = None) -> Decimal | None:
    """A second reading of a money cell with Decimal arithmetic, used only by the reconciliation check."""

    if is_missing(value) or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    text = str(value).strip()
    sign = -1 if (text.startswith("(") and text.endswith(")")) or text.startswith("-") or text.endswith("-") else 1
    digits = "".join(char for char in text if char.isdigit() or char in ".,")
    if not any(char.isdigit() for char in digits):
        return None
    try:
        return sign * Decimal(_digits_to_float_text(digits, style))
    except InvalidOperation:
        return None


def independent_amount_total(raw_values: list, style: str | None = None) -> float:
    total = Decimal("0")
    for value in raw_values:
        parsed = _decimal_from_cell(value, style)
        if parsed is not None:
            total += parsed
    return float(total)


def detect_date_order(texts: list[str]) -> str | None:
    """Day-first or month-first, decided once per column from the numeric d/m/y values in it."""

    first_over_12 = second_over_12 = seen = 0
    for text in texts:
        match = DMY_PATTERN.match(text) if isinstance(text, str) else None
        if not match:
            continue
        seen += 1
        first, second = int(match.group(1)), int(match.group(2))
        if first > 12:
            first_over_12 += 1
        if second > 12:
            second_over_12 += 1
    if not seen:
        return None
    if first_over_12 and not second_over_12:
        return "day-first"
    if second_over_12 and not first_over_12:
        return "month-first"
    if first_over_12 and second_over_12:
        return "mixed"
    return "ambiguous"


def _naive(parsed: pd.Series) -> pd.Series:
    if isinstance(parsed.dtype, pd.DatetimeTZDtype):
        return parsed.dt.tz_convert(None)
    return parsed


def _to_datetime(texts: pd.Series, **kwargs) -> pd.Series | None:
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # the inferred-format attempt warns when it falls back; the next attempt handles that
            parsed = pd.to_datetime(texts, errors="coerce", **kwargs)
    except (ValueError, TypeError):
        return None
    if not pd.api.types.is_datetime64_any_dtype(parsed):
        return None  # pandas 2 hands back object dtype for mixed offsets instead of raising
    return _naive(parsed)


def parse_dates(series: pd.Series, order: str | None = None) -> pd.Series:
    """Dates for a whole column with one reading of the day/month order, never one guess per row."""

    if pd.api.types.is_datetime64_any_dtype(series):
        return _naive(pd.to_datetime(series, errors="coerce"))
    texts = series.map(lambda value: to_text(value) or None)
    present = int(texts.notna().sum())
    if present == 0:
        return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    order = order or detect_date_order([text for text in texts.tolist() if isinstance(text, str)])
    dayfirst = order == "day-first"
    attempts = (
        {"format": "ISO8601"},
        {"format": "ISO8601", "utc": True},
        {"dayfirst": dayfirst},
        {"dayfirst": dayfirst, "utc": True},
        {"format": "mixed", "dayfirst": dayfirst},
        {"format": "mixed", "dayfirst": dayfirst, "utc": True},
    )
    best = None
    for kwargs in attempts:
        parsed = _to_datetime(texts, **kwargs)
        if parsed is None:
            continue
        if int(parsed.notna().sum()) >= 0.9 * present:
            return parsed
        if best is None or int(parsed.notna().sum()) > int(best.notna().sum()):
            best = parsed
    if best is None:
        return pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")
    return best


def detect_currency(frame: pd.DataFrame, amount_column: str | None) -> tuple[str, str]:
    if amount_column is None:
        return "USD", "assumed"
    header = to_text(amount_column)
    code = re.search(r"\b(" + "|".join(CURRENCY_CODES) + r")\b", header.upper())
    if code:
        return code.group(1), "header"
    for symbol, symbol_code in SYMBOL_TO_CODE.items():
        if symbol in header:
            return symbol_code, "header"
    for value in _sample(frame[amount_column]):
        text = to_text(value)
        for symbol, symbol_code in SYMBOL_TO_CODE.items():
            if symbol in text:
                return symbol_code, "values"
        code = re.search(r"\b(" + "|".join(CURRENCY_CODES) + r")\b", text.upper())
        if code:
            return code.group(1), "values"
    return "USD", "assumed"


@dataclass
class CleanFrame:
    frame: pd.DataFrame
    unparsed_dates: int
    unparsed_amounts: int
    parsed_amounts: int
    unmapped_columns: list[str]
    number_style: str | None
    date_order: str | None
    ambiguous_date_example: str | None


def apply_mapping(frame: pd.DataFrame, roles: dict[str, str | None]) -> CleanFrame:
    """Role columns with parsed values, plus the amount cells as read, for the checks."""

    columns: dict[str, pd.Series] = {}
    date_column, amount_column = roles.get("date"), roles.get("amount")
    if date_column is None or amount_column is None:
        raise InputError("A date column and an amount column are required.")
    date_texts = [to_text(value) for value in _sample(frame[date_column])] if not pd.api.types.is_datetime64_any_dtype(frame[date_column]) else []
    date_order = detect_date_order(date_texts)
    ambiguous_example = next((text for text in date_texts if DMY_PATTERN.match(text)), None) if date_order == "ambiguous" else None
    dates = parse_dates(frame[date_column], date_order)
    columns["date"] = dates
    style = detect_number_style(_sample(frame[amount_column])) if not pd.api.types.is_numeric_dtype(frame[amount_column]) else None
    amounts = frame[amount_column].map(lambda value: parse_amount(value, style)).astype(float)
    columns["amount"] = amounts
    columns["amount_raw"] = frame[amount_column]
    for role in TEXT_ROLES:
        column = roles.get(role)
        if column is not None:
            columns[role] = frame[column].map(to_text)
    if roles.get("quantity") is not None:
        columns["quantity"] = frame[roles["quantity"]].map(lambda value: parse_amount(value, style)).astype(float)
    clean = pd.DataFrame(columns, index=frame.index)
    non_empty = frame[amount_column].map(lambda value: to_text(value) != "")
    used = {column for column in roles.values() if column}
    unmapped = [column for column in frame.columns if usable_header(column) and column not in used]
    return CleanFrame(
        frame=clean,
        unparsed_dates=int(dates.isna().sum()),
        unparsed_amounts=int((amounts.isna() & non_empty).sum()),
        parsed_amounts=int(amounts.notna().sum()),
        unmapped_columns=unmapped,
        number_style=style,
        date_order=date_order,
        ambiguous_date_example=ambiguous_example,
    )


# --- periods -------------------------------------------------------------------


# --- building the report -------------------------------------------------------


def prepare(sources: list[str], period_text: str, options: BuildOptions, mapping_override: dict | None, profile: dict) -> BuildContext:
    """Read, map, clean and filter the inputs; raises DecisionNeeded when a question is due."""

    if not sources:
        raise InputError("Give at least one CSV, XLSX or XLS file.")
    profile = copy.deepcopy(profile)
    tables = [read_table(source, profile) for source in sources]
    mappings: list[Mapping] = []
    for table in tables:
        mapping = resolve_mapping(table.frame, profile, mapping_override, table.name)
        question = mapping_question(mapping, table.name, profile)
        if question:
            # The columns the file does have travel with the question. Without
            # them a missing role reads as "no column matched" and the only way
            # to see what the file holds is a second read of it, which is the
            # round trip this exit exists to avoid.
            raise DecisionNeeded(
                question,
                {
                    "file": table.name,
                    "sheet": table.sheet,
                    "columns": [column for column in table.frame.columns if usable_header(column)],
                    "ambiguous": mapping.ambiguous,
                    "missing": mapping.missing,
                    "mapping": mapping.roles,
                },
            )
        mappings.append(mapping)
    for role, column in (mapping_override or {}).items():
        if isinstance(column, str) and role in ("category", "person", "customer", "source", "status"):
            profile["vocabulary"][role] = column.strip().lower()  # the user's own word for it heads the section
    cleaned = [apply_mapping(table.frame, mapping.roles) for table, mapping in zip(tables, mappings)]
    frames = []
    unmapped: list[str] = []
    for table, clean in zip(tables, cleaned):
        extra = table.frame[clean.unmapped_columns].map(to_text) if clean.unmapped_columns else pd.DataFrame(index=table.frame.index)
        extra.columns = [f"extra:{column}" for column in extra.columns]
        frames.append(pd.concat([clean.frame, extra], axis=1).assign(source_file=table.name))
        for column in clean.unmapped_columns:
            if column not in unmapped:
                unmapped.append(column)
    all_rows = pd.concat(frames, ignore_index=True) if len(frames) > 1 else frames[0].reset_index(drop=True)
    for role in TEXT_ROLES:
        if role in all_rows.columns:
            all_rows[role] = all_rows[role].fillna("").astype(str)
    period = parse_period(period_text)
    currency, currency_source = detect_currency(tables[0].frame, mappings[0].roles["amount"])
    if options.currency:
        currency, currency_source = options.currency, "preferences"
    within = in_period(all_rows["date"], period)
    excluded_mask = pd.Series(False, index=all_rows.index)
    exclusion_texts: list[str] = []
    records, record = vocab(profile, "records", "rows"), vocab(profile, "record", "row")
    for exclusion in options.exclusions:
        column = exclusion.get("role") if "role" in exclusion else f"extra:{exclusion.get('column')}"
        if column not in all_rows.columns:
            mapped = [role for role, name in mappings[0].roles.items() if name == exclusion.get("column")]
            column = mapped[0] if mapped else column
        if column not in all_rows.columns:
            raise InputError(f"Exclusion names {exclusion.get('role') or exclusion.get('column')!r}, which the files do not have.")
        matches = (all_rows[column].astype(str).str.strip().str.lower() == str(exclusion["equals"]).strip().lower()) & ~excluded_mask
        counted = matches & within
        label = vocab(profile, exclusion["role"], exclusion["role"]) if "role" in exclusion else exclusion["column"]
        count = int(counted.sum())
        amount = float(all_rows.loc[counted, "amount"].fillna(0).sum())
        exclusion_texts.append(f"Excluded {count} {plural(count, record, records)} where {label} = {exclusion['equals']} ({format_value(amount, 'currency', currency)})")
        excluded_mask |= matches
    excluded_in_period = int((excluded_mask & within).sum())
    kept = all_rows.loc[~excluded_mask].reset_index(drop=True)
    if not int(in_period(kept["date"], period).sum()):
        valid = all_rows["date"].dropna()
        covered = f"the files cover {valid.min().strftime('%Y-%m-%d')} to {valid.max().strftime('%Y-%m-%d')}" if len(valid) else "no row has a usable date"
        raise InputError(f"No rows fall in {period.label}; {covered}.")
    return BuildContext(
        tables=tables,
        mappings=mappings,
        all_rows=kept,
        unmapped_columns=unmapped,
        period=period,
        profile=profile,
        options=options,
        currency=currency,
        currency_source=currency_source,
        number_style=cleaned[0].number_style,
        date_order=cleaned[0].date_order,
        ambiguous_date_example=next((clean.ambiguous_date_example for clean in cleaned if clean.ambiguous_date_example), None),
        excluded_rows=excluded_in_period,
        unparsed_dates=sum(clean.unparsed_dates for clean in cleaned),
        unparsed_amounts=sum(clean.unparsed_amounts for clean in cleaned),
        parsed_amounts=sum(clean.parsed_amounts for clean in cleaned),
        exclusion_texts=exclusion_texts,
    )


# --- sections ------------------------------------------------------------------


# --- checks --------------------------------------------------------------------


SUM_TOLERANCE_PER_ROW = 0.005  # each rounded table cell may be half a cent off its unrounded value


def compute_checks(ctx: BuildContext, rows: pd.DataFrame, revenue: float, count: int, sections: list[dict]) -> list[dict]:
    profile, period, currency = ctx.profile, ctx.period, ctx.currency
    records, record = vocab(profile, "records", "rows"), vocab(profile, "record", "row")
    checks: list[dict] = []
    total_rows = sum(table.rows for table in ctx.tables)
    outside = total_rows - count - ctx.unparsed_dates - ctx.excluded_rows
    used_text = (
        f"Used {format_value(count, 'integer')} of {format_value(total_rows, 'integer')} rows: {format_value(outside, 'integer')} {is_are(outside)} outside {period.label}, {format_value(ctx.unparsed_dates, 'integer')} had no usable date"
    )
    used_text += f", {format_value(ctx.excluded_rows, 'integer')} {'was' if ctx.excluded_rows == 1 else 'were'} excluded." if ctx.excluded_rows else "."
    checks.append({"id": "rows_used", "status": "pass", "text": used_text})

    independent = independent_amount_total(rows["amount_raw"].tolist(), ctx.number_style)
    mismatches = []
    if abs(independent - revenue) > 0.01:
        mismatches.append(f"the amount column sums to {format_value(independent, 'currency', currency)}")
    for section in sections:
        table = section.get("table")
        if not table or not table.get("totals") or "Revenue" not in table["columns"] or section["id"] == "customers":
            continue
        index = table["columns"].index("Revenue")
        section_total = sum(row[index] or 0 for row in table["rows"])
        if abs(section_total - revenue) > 0.01 + SUM_TOLERANCE_PER_ROW * len(table["rows"]):
            mismatches.append(f"the {section['heading'].lower()} table sums to {format_value(section_total, 'currency', currency)}")
    if mismatches:
        checks.append({"id": "totals_reconcile", "status": "fail", "text": f"Totals do not match your file: the report says {format_value(revenue, 'currency', currency)} but {', '.join(mismatches)}. The report was withheld."})
    else:
        checks.append({"id": "totals_reconcile", "status": "pass", "text": f"Totals match your file: {format_value(revenue, 'currency', currency)} across {format_value(count, 'integer')} {plural(count, record, records)}."})

    if ctx.date_order == "ambiguous" and ctx.ambiguous_date_example:
        checks.append({"id": "date_order", "status": "warn", "text": f"Dates such as {ctx.ambiguous_date_example} were read as month/day/year; say if they are day/month/year."})
    elif ctx.date_order == "mixed":
        checks.append({"id": "date_order", "status": "warn", "text": "The date column mixes day/month and month/day values; rows were read one by one and some may sit in the wrong month."})

    if "id" in rows.columns:
        ids = rows.loc[rows["id"].str.strip() != "", "id"]
        duplicates = int((ids.value_counts() > 1).sum())
        if duplicates:
            checks.append({"id": "duplicate_ids", "status": "warn", "text": f"{format_value(duplicates, 'integer')} {record} {'ID appears' if duplicates == 1 else 'IDs appear'} more than once."})
        else:
            checks.append({"id": "duplicate_ids", "status": "pass", "text": f"No {record} ID appears more than once."})
    else:
        checks.append({"id": "duplicate_ids", "status": "not_checked", "text": f"No ID column, so duplicate {records} were not checked."})

    unmapped_parts = []
    if "person" in rows.columns:
        unassigned = int((rows["person"] == "Unassigned").sum())
        if unassigned:
            unmapped_parts.append(f"{format_value(unassigned, 'integer')} {plural(unassigned, record, records)} had no {vocab(profile, 'person', 'person')} and {is_are(unassigned)} listed as Unassigned")
    if "category" in rows.columns:
        uncategorized = int((rows["category"] == "Uncategorized").sum())
        if uncategorized:
            unmapped_parts.append(f"{format_value(uncategorized, 'integer')} had no {vocab(profile, 'category', 'category')} and {is_are(uncategorized)} listed as Uncategorized")
    if "person" in rows.columns or "category" in rows.columns:
        if unmapped_parts:
            checks.append({"id": "unmapped_rows", "status": "warn", "text": "; ".join(unmapped_parts) + "."})
        else:
            checks.append({"id": "unmapped_rows", "status": "pass", "text": f"Every {record} has a {vocab(profile, 'person', 'person') if 'person' in rows.columns else vocab(profile, 'category', 'category')}."})

    if ctx.parsed_amounts == 0:
        checks.append({"id": "unparsed_amounts", "status": "warn", "text": f"No row has a usable amount, so every total is {format_value(0, 'currency', currency)}; check the amount column."})
    elif ctx.unparsed_amounts:
        checks.append(
            {
                "id": "unparsed_amounts",
                "status": "warn",
                "text": f"{format_value(ctx.unparsed_amounts, 'integer')} {plural(ctx.unparsed_amounts, 'row')} had no usable amount and {'counts' if ctx.unparsed_amounts == 1 else 'count'} as {format_value(0, 'currency', currency)}.",
            }
        )
    else:
        checks.append({"id": "unparsed_amounts", "status": "pass", "text": "Every row has a usable amount."})

    if ctx.currency_source == "assumed":
        checks.append({"id": "currency", "status": "warn", "text": f"Amounts are assumed to be in {currency}; say if they are not."})
    else:
        origin = {"header": "from the column header", "values": "from the values", "preferences": "from your preferences", "mapping": "from the mapping", "profile": "from the profile"}[ctx.currency_source]
        checks.append({"id": "currency", "status": "pass", "text": f"Amounts are in {currency} ({origin})."})

    if ctx.exclusion_texts:
        checks.append({"id": "exclusions", "status": "pass", "text": "; ".join(ctx.exclusion_texts) + "."})
    if any(table.is_workbook for table in ctx.tables):
        checks.append({"id": "external_links", "status": "not_checked", "text": "External workbook links were not checked."})
    return checks


# --- prose verification --------------------------------------------------------


def _numbers_in(value, found: set[float]) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, (int, float)):
        if not (isinstance(value, float) and math.isnan(value)):
            found.add(float(value))
    elif isinstance(value, dict):
        for item in value.values():
            _numbers_in(item, found)
    elif isinstance(value, list):
        for item in value:
            _numbers_in(item, found)


def allowed_numbers(report: dict) -> set[float]:
    """The figures a summary may cite: KPIs, tables and checks, the periods named, never individual rows."""

    found: set[float] = set()
    for key in ("kpis", "sections"):
        _numbers_in(report.get(key), found)
    for check in report.get("checks", []):
        for match in NUMBER_TOKEN.finditer(check["text"]):
            found.add(float(match.group(2).replace(",", "")))
    period = parse_period(report["meta"]["period"]["key"])
    for named in (period, previous_period(period), same_period_last_year(period)):
        found.add(float(named.start.year))
        found.add(float(named.end.year))
    found.update(float(day) for day in range(1, min(31, (period.end - period.start).days + 1) + 1))
    found.add(float(report["meta"]["draft"]))
    for entry in report["meta"]["inputs"]:
        found.add(float(entry["rows"]))
    return found


def _token_matches(text: str, suffix: str | None, allowed: set[float]) -> bool:
    value = Decimal(text.replace(",", ""))
    decimals = len(text.split(".")[1]) if "." in text else 0
    quantum = Decimal(1).scaleb(-decimals)
    scale = Decimal(1000) if suffix in ("k", "K") else Decimal(1)
    for candidate in allowed:
        scaled = Decimal(str(candidate)) / scale
        # A delta of -8.2 is written "8.2% below", so the sign is not compared.
        if scaled.quantize(quantum, rounding=ROUND_HALF_UP) == value or (-scaled).quantize(quantum, rounding=ROUND_HALF_UP) == value:
            return True
    return False


def verify_prose_numbers(report: dict, text: str) -> tuple[str, list[str]]:
    """Keep only sentences whose every number is in the report; return the text and what was removed."""

    allowed = allowed_numbers(report)
    kept: list[str] = []
    removed: list[str] = []
    for sentence in SENTENCE_SPLIT.split(text.strip()):
        if not sentence:
            continue
        bad = [match.group(0) for match in NUMBER_TOKEN.finditer(sentence) if not _token_matches(match.group(2), match.group(3), allowed)]
        if bad:
            removed.extend(bad)
        else:
            kept.append(sentence)
    return " ".join(kept), removed


def apply_prose(report: dict, summary: list[str] | None, actions: list[str] | None) -> tuple[dict, list[str], list[str]]:
    """Model-written text into the report, numbers verified; pure. Returns the report, what was removed, and notes."""

    updated = copy.deepcopy(report)
    removed_all: list[str] = []
    notes: list[str] = []
    sections = {section["id"]: section for section in updated["sections"]}
    if summary is not None:
        paragraphs = []
        for paragraph in summary:
            clean, removed = verify_prose_numbers(updated, paragraph)
            removed_all += removed
            if clean:
                paragraphs.append(clean)
        section = sections.get("summary")
        if section is None:
            section = {"id": "summary", "heading": "Summary"}
            updated["sections"].insert(0, section)
        if paragraphs:
            section["paragraphs"] = paragraphs
        else:
            notes.append("No sentence of the new summary survived the number check; kept the built summary.")
    if actions is not None:
        bullets = []
        for bullet in actions:
            clean, removed = verify_prose_numbers(updated, bullet)
            removed_all += removed
            if clean and not removed:
                bullets.append(clean)
        section = sections.get("actions")
        if section is None:
            section = {"id": "actions", "heading": "What to act on"}
            updated["sections"].append(section)
        if bullets:
            section["bullets"] = bullets
        else:
            notes.append("No action survived the number check; kept the built actions.")
    if removed_all:
        check = {"id": "prose_numbers", "status": "warn", "text": f"Removed {len(removed_all)} {plural(len(removed_all), 'number')} from the written text that {is_are(len(removed_all))} not in the report: {', '.join(removed_all)}."}
    else:
        check = {"id": "prose_numbers", "status": "pass", "text": "Every number in the written text matches the report."}
    updated["checks"] = [entry for entry in updated["checks"] if entry["id"] != "prose_numbers"] + [check]
    updated["meta"]["draft"] = int(updated["meta"]["draft"]) + 1
    updated["meta"]["generated_at"] = utc_now()
    return updated, removed_all, notes


# --- inspect and show ----------------------------------------------------------


def inspect_sources(sources: list[str], profile: dict) -> dict:
    files = []
    question = None
    for source in sources:
        path, sheet = parse_source(source)
        tables, skipped, is_workbook = read_tables(path, profile)
        if sheet is not None:
            tables = [table for table in tables if table[0] == sheet]
        entries = []
        for sheet_name, frame in tables:
            mapping = suggest_mapping(frame, profile)
            date_texts = [to_text(value) for value in _sample(frame[mapping.roles["date"]])] if mapping.roles["date"] and not pd.api.types.is_datetime64_any_dtype(frame[mapping.roles["date"]]) else []
            date_order = detect_date_order(date_texts)
            dates = parse_dates(frame[mapping.roles["date"]], date_order) if mapping.roles["date"] else pd.Series(dtype="datetime64[ns]")
            suggestion = suggest_period(dates)
            valid = dates.dropna()
            columns = []
            for column in frame.columns:
                if not usable_header(column):
                    continue
                series = frame[column]
                kind = "date" if looks_like_dates(series) else "number" if looks_numeric(series) else "text"
                columns.append({"name": column, "type": kind, "sample": [to_text(value) for value in _sample(series)[:3]], "distinct": int(series.map(to_text).nunique())})
            entries.append(
                {
                    "sheet": sheet_name,
                    "rows": int(frame.shape[0]),
                    "columns": columns,
                    "mapping": mapping.roles,
                    "confidence": mapping.confidence,
                    "ambiguous": mapping.ambiguous,
                    "missing": mapping.missing,
                    "alternatives": mapping.alternatives,
                    "period_suggestion": {"key": suggestion.key, "label": suggestion.label, "rows": int(in_period(dates, suggestion).sum())} if suggestion else None,
                    "date_range": {"start": valid.min().strftime("%Y-%m-%d"), "end": valid.max().strftime("%Y-%m-%d")} if len(valid) else None,
                    "date_order": date_order,
                    "months": {str(key): int(value) for key, value in sorted(valid.dt.to_period("M").value_counts().items())} if len(valid) else {},
                    "currency": dict(zip(("code", "source"), detect_currency(frame, mapping.roles["amount"]))),
                }
            )
            if question is None:
                question = mapping_question(mapping, f"{path.name}::{sheet_name}" if sheet_name else path.name, profile)
        if question is None and len(entries) > 1:
            question = f"{path.name} has several sheets with data: {', '.join(entry['sheet'] for entry in entries)}. Which one is the export?"
        files.append({"name": path.name, "path": str(path), "sha256": sha256_of(path), "uploaded": dt.date.fromtimestamp(path.stat().st_mtime).isoformat(), "workbook": is_workbook, "tables": entries, "skipped_sheets": skipped})
    return {"profile": profile["name"], "files": files, "question": question}


def show_report(report: dict) -> str:
    """The figures, tables, checks and notes as text for the agent; the rows stay in the file."""

    currency = report["meta"]["currency"]["code"]
    meta = report["meta"]
    lines = [f"{meta['title']} (draft {meta['draft']}, {meta['period']['label']}, {currency})"]
    for kpi in report["kpis"]:
        delta = kpi.get("delta")
        change = f" ({'+' if (delta['pct'] or 0) > 0 else ''}{format_value(delta['pct'], 'percent')} vs {delta['vs']})" if delta and delta.get("pct") is not None else ""
        lines.append(f"  {kpi['label']}: {format_value(kpi['value'], kpi['format'], currency)}{change}")
    for section in report["sections"]:
        lines.append("")
        lines.append(section["heading"])
        for paragraph in section.get("paragraphs", []):
            lines.append(f"  {paragraph}")
        for bullet in section.get("bullets", []):
            lines.append(f"  - {bullet}")
        table = section.get("table")
        if table:
            lines.append("  " + " | ".join(table["columns"]))
            for row_index, row in enumerate(table["rows"][:12]):
                lines.append("  " + " | ".join(format_value(value, table["formats"][index] if not table.get("row_formats") else table["row_formats"][row_index][index], currency) for index, value in enumerate(row)))
            if len(table["rows"]) > 12:
                lines.append(f"  … {len(table['rows']) - 12} more rows in the file")
            if table.get("totals"):
                lines.append("  " + " | ".join(format_value(value, table["formats"][index], currency) for index, value in enumerate(table["totals"])))
    lines.append("")
    lines.append(f"Checks: {checks_line(report)}")
    for check in report["checks"]:
        lines.append(f"  [{check['status']}] {check['text']}")
    lines.append("")
    lines.append("Not included: " + (" ".join(report["notes"]) if report["notes"] else "nothing; every section the profile lists is in the report."))
    inputs = ", ".join(f"{entry['name']} (uploaded {entry['uploaded']}, {entry['rows']} rows)" for entry in meta["inputs"])
    lines.append(f"Inputs: {inputs}")
    # What the report did not read. The rows table carries these columns, but
    # `show` never prints rows, so without this line the only way to learn that
    # a `Treatment` column existed is to read the file again.
    unused = meta.get("build", {}).get("unmapped") or []
    if unused:
        lines.append("Unused columns: " + ", ".join(unused))
    # A cell, a column name or a profile word must not be able to start a line:
    # the model is told to act on whole lines of this digest.
    return "\n".join(one_line(line) for line in lines)


# --- command line --------------------------------------------------------------


def _report_base_name(out_dir: Path, override: str | None) -> str:
    """The report is named after its directory, so every path a run will write
    is known before the run: the model names them under the bash tool's
    ``present`` argument in the same call. ``--name`` overrides."""

    if override:
        return slugify(override)
    return slugify(out_dir.resolve().name) or "report"


def _existing_report(path: Path) -> dict | None:
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        int(data["meta"]["draft"])
        return data
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return None


def _write_json(path: Path, data) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    tmp.replace(path)


def _load_json_file(path: str, what: str) -> dict:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise InputError(f"Could not read {what} {path}: {error}") from error
    if not isinstance(data, dict):
        raise InputError(f"{what} {path} must hold a JSON object.")
    return data


def _load_report(path: Path) -> dict:
    report = _load_json_file(str(path), "report")
    if report.get("version") != 1 or not isinstance(report.get("meta"), dict):
        raise InputError(f"{path.name} is not a version 1 report produced by this skill.")
    return report


def parse_targets(values: list[str] | None, flag: str) -> list[str]:
    """`pdf,docx,xlsx`, repeated flags, or `all`; validated before anything is written."""

    named: list[str] = []
    for value in values or []:
        for item in str(value).split(","):
            item = item.strip().lower()
            if not item:
                continue
            if item == "all":
                named.extend(PRESENTED_TARGETS)
            else:
                named.append(item)
    unknown = [item for item in named if item not in RENDER_TARGETS]
    if unknown:
        raise InputError(f"{flag} does not know {', '.join(sorted(set(unknown)))}; use {', '.join(RENDER_TARGETS)}, a comma-separated list of them, or all.")
    return [target for target in TARGET_ORDER if target in named]


def _render_base(report_path: Path) -> tuple[Path, str]:
    name = report_path.name
    return report_path.parent, name[: -len(REPORT_SUFFIX)] if name.endswith(REPORT_SUFFIX) else report_path.stem


def _render_name(report_path: Path, target: str) -> str:
    return f"{_render_base(report_path)[1]}.{target}"


def read_render_manifest(report_path: Path) -> list[str]:
    """The renders this skill recorded writing beside *report_path*.

    Ownership is recorded rather than inferred from the file name, because the
    name says nothing about who wrote the file. A report directory with no
    manifest — one this skill has not rendered into, or a bundle the user
    re-uploaded — owns nothing, so nothing in it is ever removed.
    """

    directory, base = _render_base(report_path)
    path = directory / RENDERS_MANIFEST
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    if not isinstance(data, dict) or data.get("base") != base:
        return []
    names = data.get("files")
    if not isinstance(names, list):
        return []
    known = [f"{base}.{target}" for target in TARGET_ORDER]
    return [name for name in known if name in names]


def write_render_manifest(report_path: Path, names: list[str]) -> None:
    directory, base = _render_base(report_path)
    _write_json(directory / RENDERS_MANIFEST, {"version": 1, "base": base, "files": [name for name in (f"{base}.{target}" for target in TARGET_ORDER) if name in set(names)]})


def unowned_renders(report_path: Path) -> list[str]:
    """Files named like renders of this report that this skill did not write."""

    directory, base = _render_base(report_path)
    owned = set(read_render_manifest(report_path))
    return [f"{base}.{target}" for target in TARGET_ORDER if f"{base}.{target}" not in owned and (directory / f"{base}.{target}").is_file()]


def remove_stale_renders(report_path: Path, keep: list[str]) -> list[str]:
    """Drop this skill's renders of the draft just replaced, except the ones rewritten."""

    directory, _ = _render_base(report_path)
    kept = {_render_name(report_path, target) for target in keep}
    removed = []
    for name in read_render_manifest(report_path):
        if name in kept:
            continue
        path = directory / name
        if path.is_file():
            path.unlink()
        removed.append(name)
    return removed


def render_targets(report: dict, report_path: Path, targets: list[str], tenant_dir: str | None) -> list[Path]:
    """Every named format from one already-loaded report, in one process."""

    written = []
    for target in targets:
        path = render(report, report_path, target, None, tenant_dir)
        print(f"Rendered {target}: {path}")
        written.append(path)
    return written


def publish_renders(report: dict, report_path: Path, targets: list[str], tenant_dir: str | None) -> None:
    """Write this draft's renders, then drop the ones it did not replace.

    In that order: a render that fails leaves the previous draft's files where
    they are rather than deleting them first and then raising, and the removal
    notice is printed the moment the removal happens rather than after work
    that might not finish.
    """

    written = render_targets(report, report_path, targets, tenant_dir)
    removed = remove_stale_renders(report_path, targets)
    write_render_manifest(report_path, [path.name for path in written])
    if removed:
        print("Removed stale renders from the previous draft: " + ", ".join(removed) + ". Render again.")


def print_unowned_renders(report_path: Path) -> None:
    """Warn about a file named like one of this report's renders that this skill did not write."""

    for name in unowned_renders(report_path):
        print(f"Note: {name} sits beside this report but was not written by it; it may show a different draft.")


def command_inspect(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(args.profile, tenant_dir)
    print(json.dumps(inspect_sources(args.files, profile), indent=2, ensure_ascii=False))
    return EXIT_OK


def command_build(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(args.profile, tenant_dir)
    options = BuildOptions(brand=load_brand(tenant_dir))
    applied: list[str] = []
    if args.prefs:
        options, applied = apply_preferences(options, _load_json_file(args.prefs, "preferences file"))
    for text in args.exclude or []:
        exclusion = normalize_exclusion(text)
        if exclusion is None:
            raise InputError(f"Exclusion {text!r} must look like role=value or column=value.")
        options.exclusions.append(exclusion)
    options.title = args.title
    options.company = args.company
    options.currency = args.currency or options.currency
    options.draft = args.draft
    options.name = args.name
    if args.short:
        options.summary_length = "short"
    mapping_override = _load_json_file(args.mapping, "mapping file") if args.mapping else None
    targets = parse_targets(args.render, "--render")
    ctx = prepare(args.files, args.period, options, mapping_override, profile)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    base = _report_base_name(out_dir, options.name)
    report_path = out_dir / f"{base}{REPORT_SUFFIX}"
    previous = _existing_report(report_path)
    if previous and previous["meta"]["period"]["key"] != ctx.period.key:
        # The report is named after its directory, so a second period built
        # here would silently replace the first and call itself its next draft.
        raise InputError(f"{out_dir} holds the {previous['meta']['period']['label']} report; build {ctx.period.label} into its own --out directory.")
    report, charts = build_report(ctx, int(previous["meta"]["draft"]) if previous else 0, compute_checks)
    report["meta"]["preferences_applied"] = applied
    _write_json(out_dir / "checks.json", report["checks"])
    if any(check["status"] == "fail" for check in report["checks"]):
        failed = next(check for check in report["checks"] if check["status"] == "fail")
        sys.stderr.write(f"Report withheld: {failed['text']}\n")
        return EXIT_WITHHELD
    draw_charts(charts, out_dir, options.brand, ctx.currency)
    _write_json(report_path, report)
    print(f"Built draft {report['meta']['draft']}: {report['meta']['title']} -> {report_path}")
    if previous and any(check["id"] == "prose_numbers" for check in previous.get("checks", [])):
        print("The previous draft carried written text (summary or actions); this rebuild replaced it with the computed text. Run prose again if it still applies.")
    # The figures the next step is written from, so reading them is not a second
    # run: this is exactly what `show` prints, and the rows stay in the file.
    print()
    print(show_report(report))
    if report["charts"]:
        print("Charts: " + ", ".join(chart["png"] for chart in report["charts"]))
    publish_renders(report, report_path, targets, tenant_dir)
    print_unowned_renders(report_path)
    return EXIT_OK


def command_show(args) -> int:
    print(show_report(_load_report(Path(args.report))))
    return EXIT_OK


def command_prose(args) -> int:
    report_path = Path(args.report)
    report = _load_report(report_path)
    summary: list[str] | None = None
    actions: list[str] | None = None
    if args.source:
        data = _load_json_file(args.source, "prose file")
        if "summary" in data:
            summary = [str(item) for item in data["summary"]] if isinstance(data["summary"], list) else [str(data["summary"])]
        if "actions" in data:
            actions = [str(item) for item in data["actions"]]
    if args.summary:
        summary = list(args.summary)
    if args.action:
        actions = list(args.action)
    if summary is None and actions is None:
        raise InputError("Give --from prose.json, --summary text or --action text.")
    targets = parse_targets(args.render, "--render")
    updated, removed, notes = apply_prose(report, summary, actions)
    _write_json(report_path, updated)
    _write_json(report_path.parent / "checks.json", updated["checks"])
    print(f"Draft {updated['meta']['draft']}: {report_path}")
    if removed:
        print(f"Removed {len(removed)} {plural(len(removed), 'number')} not in the report: {', '.join(removed)}. Sentences with them were dropped; say so to the user.")
    else:
        print("Every number in the written text matches the report.")
    for note in notes:
        print(note)
    # The report as it now stands, read out of the draft that was just written
    # rather than out of the text that was handed in: a sentence the number
    # check dropped is not in here, and the KPI strip, the checks line and the
    # not-included items are what the user has to be told about this draft.
    print()
    print(show_report(updated))
    publish_renders(updated, report_path, targets, resolve_tenant_dir(args.tenant))
    print_unowned_renders(report_path)
    return EXIT_OK


def command_render(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    report_path = Path(args.report)
    targets = parse_targets(args.to, "--to")
    if not targets:
        raise InputError("--to needs at least one of html, pdf, docx, xlsx, or all.")
    if args.out and len(targets) > 1:
        raise InputError("--out names one file, so it goes with one --to target.")
    report = _load_report(report_path)
    if args.out:
        # A name the caller chose: this skill did not place it beside the
        # report, so it is not recorded as one of the report's own renders.
        path = render(report, report_path, targets[0], Path(args.out), tenant_dir)
        print(f"Rendered {targets[0]}: {path}")
        os.utime(report_path, None)
        return EXIT_OK
    written = render_targets(report, report_path, targets, tenant_dir)
    write_render_manifest(report_path, read_render_manifest(report_path) + [path.name for path in written])
    # The report is handed over with its renders, and the bash tool attaches
    # only files the call wrote: a render in a later turn touches the report
    # so it can be named under `present` beside the new renders. A touch, not
    # a rewrite: the renders are already on disk, and nothing that can fail
    # should follow them.
    os.utime(report_path, None)
    print_unowned_renders(report_path)
    return EXIT_OK


def command_checks(args) -> int:
    report_path = Path(args.report)
    report = _load_report(report_path)
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(report["meta"].get("profile"), tenant_dir)
    build = report["meta"]["build"]
    options = BuildOptions(exclusions=list(build.get("exclusions", [])), currency=report["meta"]["currency"]["code"] if report["meta"]["currency"]["source"] == "preferences" else None)
    ctx = prepare(args.files or build["sources"], report["meta"]["period"]["key"], options, build.get("mapping"), profile)
    rebuilt, _charts = build_report(ctx, int(report["meta"]["draft"]) - 1, compute_checks)
    checks = rebuilt["checks"] + [check for check in report.get("checks", []) if check["id"] == "prose_numbers"]
    _write_json(report_path.parent / "checks.json", checks)
    print(checks_line({"checks": checks}))
    return EXIT_WITHHELD if any(check["status"] == "fail" for check in checks) else EXIT_OK


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="report.py", description="Turn a tabular export into a management report (see SKILL.md).")
    commands = parser.add_subparsers(dest="command", required=True)

    inspect_parser = commands.add_parser("inspect", help="Describe the files: columns, suggested roles, period and currency.")
    inspect_parser.add_argument("files", nargs="+", help="CSV, XLSX or XLS files; a workbook sheet as path::Sheet")
    inspect_parser.add_argument("--profile", help="Profile name or path (default: services-generic)")
    inspect_parser.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    inspect_parser.set_defaults(handler=command_inspect)

    build_parser_ = commands.add_parser("build", help="Compute the report: report.json, charts and checks.")
    build_parser_.add_argument("files", nargs="+", help="CSV, XLSX or XLS files; earlier periods in the same files or extra files feed the comparisons")
    build_parser_.add_argument("--period", required=True, help="YYYY-MM, YYYY-Qn, YYYY or YYYY-MM-DD..YYYY-MM-DD")
    build_parser_.add_argument("--out", required=True, help="Directory for the report, its charts and checks.json")
    build_parser_.add_argument("--mapping", help="JSON file: {role: column or null} to settle a question from inspect")
    build_parser_.add_argument("--profile", help="Profile name or path (default: services-generic)")
    build_parser_.add_argument("--prefs", help="preferences.json to apply")
    build_parser_.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    build_parser_.add_argument("--exclude", action="append", help="role=value or column=value; repeatable")
    build_parser_.add_argument("--title", help="Report title (default from the profile)")
    build_parser_.add_argument("--company", help="Company name shown on the report")
    build_parser_.add_argument("--currency", help="Three-letter code when the file does not say")
    build_parser_.add_argument("--name", help="Base file name (default: the name of the --out directory)")
    build_parser_.add_argument("--draft", type=int, help="Draft number (default: previous draft in --out plus one)")
    build_parser_.add_argument("--short", action="store_true", help="One-sentence summary")
    build_parser_.add_argument("--render", action="append", help="Render in the same run: pdf,docx,xlsx or all; repeatable")
    build_parser_.set_defaults(handler=command_build)

    show_parser = commands.add_parser("show", help="Print the figures, tables, checks and notes of a report (not its rows).")
    show_parser.add_argument("report", help="The .report.json to show")
    show_parser.set_defaults(handler=command_show)

    prose_parser = commands.add_parser("prose", help="Put model-written summary and actions into the report; numbers are verified.")
    prose_parser.add_argument("report", help="The .report.json to update")
    prose_parser.add_argument("--from", dest="source", help='JSON file: {"summary": [paragraphs], "actions": [bullets]}')
    prose_parser.add_argument("--summary", action="append", help="A summary paragraph; repeatable")
    prose_parser.add_argument("--action", action="append", help="An action bullet; repeatable")
    prose_parser.add_argument("--render", action="append", help="Render the new draft in the same run: pdf,docx,xlsx or all; repeatable")
    prose_parser.add_argument("--tenant", help="Tenant bundle directory for the logo and colours (default: /mnt/tenant when present)")
    prose_parser.set_defaults(handler=command_prose)

    render_parser = commands.add_parser("render", help="Render report.json to html, pdf, docx or xlsx; several formats in one run.")
    render_parser.add_argument("report", help="The .report.json to render")
    render_parser.add_argument("--to", required=True, action="append", help="html, pdf, docx, xlsx, a comma-separated list of them, or all (pdf, docx and xlsx); repeatable")
    render_parser.add_argument("--out", help="Output path (default: next to the report)")
    render_parser.add_argument("--tenant", help="Tenant bundle directory for the logo and colours (default: /mnt/tenant when present)")
    render_parser.set_defaults(handler=command_render)

    checks_parser = commands.add_parser("checks", help="Re-run the checks against the inputs and write checks.json.")
    checks_parser.add_argument("report", help="The .report.json whose checks to re-run")
    checks_parser.add_argument("files", nargs="*", help="The input files (default: the ones recorded in the report)")
    checks_parser.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    checks_parser.set_defaults(handler=command_checks)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except DecisionNeeded as decision:
        sys.stderr.write(f"Decision needed: {decision.question}\n{json.dumps(decision.details, indent=2, ensure_ascii=False)}\n")
        return EXIT_DECISION_NEEDED
    except InputError as error:
        sys.stderr.write(f"Error: {error}\n")
        return EXIT_WITHHELD


if __name__ == "__main__":
    sys.exit(main())
