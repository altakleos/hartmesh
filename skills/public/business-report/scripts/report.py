#!/usr/bin/env python3
"""Turn a tabular export (CSV, XLSX, XLS) into a management report.

Commands (see SKILL.md for the workflow):

    report.py inspect <files…>                       schema, roles, period and currency guesses
    report.py build   <files…> --period P --out DIR  report.json, charts/*.png, checks.json
    report.py prose   <report.json> --from prose.json  model-written text, numbers verified
    report.py render  <report.json> --to html|pdf|docx|xlsx
    report.py checks  <report.json> <files…>         re-run the checks on their own

Everything the renderers show comes from one report.json, so the HTML, PDF,
DOCX and XLSX agree by construction. The script runs on the libraries the
sandbox image ships (docker/sandbox/Dockerfile), installs nothing, never
calls a network service, never shells out and never modifies an input file.
A file is addressed as ``path`` or ``path::Sheet`` for workbooks.
"""

from __future__ import annotations

import argparse
import base64
import copy
import csv
import datetime as dt
import hashlib
import io
import json
import math
import re
import sys
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from pathlib import Path

MISSING_LIBRARY_MESSAGE = (
    "business-report needs pandas, python-docx, jinja2, xlsxwriter, openpyxl and matplotlib, "
    "which the sandbox image built from docker/sandbox/Dockerfile provides. "
    "This sandbox does not have them, so it is not the image this skill is built for. "
    "Do not install packages inside the sandbox; tell the user which image is running."
)

try:
    import docx
    import jinja2
    import openpyxl  # noqa: F401  (pandas' xlsx engine; asserted so a missing engine fails here, not mid-build)
    import pandas as pd
    import xlsxwriter
    from docx.enum.text import WD_ALIGN_PARAGRAPH
    from docx.shared import Inches
except ImportError as error:
    sys.stderr.write(f"{MISSING_LIBRARY_MESSAGE} ({error})\n")
    sys.exit(2)

EXIT_OK = 0
EXIT_WITHHELD = 1
EXIT_MISSING_LIBRARY = 2
EXIT_DECISION_NEEDED = 3

SKILL_DIR = Path(__file__).resolve().parent.parent
TEMPLATES_DIR = SKILL_DIR / "templates"
PROFILES_DIR = SKILL_DIR / "profiles"
DEFAULT_PROFILE = "services-generic"
DEFAULT_TENANT_DIR = "/mnt/tenant"
DEFAULT_REPORTS_DIR = "/mnt/user-data/outputs/reports"
LANG = "en-US"
REPORT_SUFFIX = ".report.json"

ROLES = ("date", "amount", "id", "customer", "category", "person", "status", "source", "quantity", "location")
REQUIRED_ROLES = ("date", "amount")
TEXT_ROLES = ("id", "customer", "category", "person", "status", "source", "location")
ROLE_HEADINGS = {"date": "Date", "amount": "Amount", "id": "ID", "customer": "Customer", "category": "Category", "person": "Person", "status": "Status", "source": "Source", "quantity": "Quantity", "location": "Location"}

CURRENCY_SYMBOLS = {"USD": "$", "EUR": "€", "GBP": "£", "CAD": "CA$", "AUD": "A$", "NZD": "NZ$", "JPY": "¥", "INR": "₹"}
SYMBOL_TO_CODE = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY", "₹": "INR"}
CURRENCY_CODES = ("USD", "EUR", "GBP", "CAD", "AUD", "NZD", "JPY", "INR", "CHF", "SEK", "NOK", "DKK", "MXN", "BRL", "ZAR", "SGD", "HKD")
DEFAULT_BRAND = {"company": "", "primary": "#1F4E79", "secondary": "#8FA9C8", "logo": None}
MONTHS = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
MONTHS_SHORT = [name[:3] for name in MONTHS]
WEEK_BUCKET_LIMIT_DAYS = 62
SAMPLE_ROWS = 200
CHART_SIZE_INCHES = (6.4, 2.9)
CHART_DPI = 200
NUMBER_TOKEN = re.compile(r"(?<![A-Za-z0-9.])([$€£]?)(-?\d{1,3}(?:,\d{3})+(?:\.\d+)?|-?\d+(?:\.\d+)?)([%kK])?(?![A-Za-z0-9])")
SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")


class InputError(Exception):
    """A problem with the inputs or arguments that the agent can explain to the user."""


class DecisionNeeded(Exception):
    """The mapping needs one answer from the user before the report can be built."""

    def __init__(self, question: str, details: dict):
        super().__init__(question)
        self.question = question
        self.details = details


# --- small helpers -----------------------------------------------------------


def _is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        return bool(pd.isna(value))
    except (TypeError, ValueError):
        return False


def to_text(value) -> str:
    """Render any cell as the text a person would read (no '10001.0' for an integer id)."""

    if _is_missing(value):
        return ""
    if isinstance(value, bool):
        return "Yes" if value else "No"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else repr(value)
    if isinstance(value, (dt.datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d") if value.hour == 0 and value.minute == 0 and value.second == 0 else value.strftime("%Y-%m-%d %H:%M")
    if isinstance(value, dt.date):
        return value.isoformat()
    return str(value).strip()


def _round(value, places: int) -> float:
    return float(Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def _number(value):
    """A JSON-safe number: int when integral, else float rounded to cents."""

    if _is_missing(value):
        return None
    number = float(value)
    if number.is_integer():
        return int(number)
    return _round(number, 2)


def format_value(value, fmt: str, currency: str = "USD") -> str:
    """The one formatting function every renderer uses."""

    if fmt == "text":
        return "" if value is None else to_text(value)
    if _is_missing(value):
        return "—"
    if fmt == "date":
        return to_text(value)
    number = Decimal(str(value))
    if fmt == "currency":
        symbol = CURRENCY_SYMBOLS.get(currency)
        prefix = symbol if symbol else f"{currency} "
        sign = "-" if number < 0 else ""
        magnitude = abs(number)
        body = f"{int(magnitude):,}" if magnitude == magnitude.to_integral_value() else f"{magnitude.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f}"
        return f"{sign}{prefix}{body}"
    if fmt == "integer":
        return f"{int(number.to_integral_value(rounding=ROUND_HALF_UP)):,}"
    if fmt == "percent":
        return f"{number.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP):,.1f}%"
    if number == number.to_integral_value():
        return f"{int(number):,}"
    return f"{number.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f}"


def _plural(count: int, singular: str, plural: str | None = None) -> str:
    return singular if count == 1 else (plural or singular + "s")


def _slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "report"


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utc_now() -> str:
    # dt.UTC needs Python 3.11; the sandbox image runs 3.10.
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")  # noqa: UP017


# --- profile, brand, preferences ---------------------------------------------


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


def load_brand(tenant_dir: str | None) -> dict:
    """brand.json from the tenant bundle; a missing logo degrades to the name alone."""

    brand = dict(DEFAULT_BRAND)
    if not tenant_dir:
        return brand
    path = Path(tenant_dir) / "brand.json"
    if not path.is_file():
        return brand
    data = json.loads(path.read_text(encoding="utf-8"))
    if data.get("company_name"):
        brand["company"] = str(data["company_name"])
    colors = data.get("colors") or {}
    for key in ("primary", "secondary"):
        if _is_color(colors.get(key)):
            brand[key] = colors[key]
    logo = data.get("logo")
    if logo:
        logo_path = Path(tenant_dir) / str(logo)
        if logo_path.is_file():
            brand["logo"] = str(logo_path)
    return brand


def _is_color(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", value) is not None


@dataclass
class BuildOptions:
    title: str | None = None
    company: str | None = None
    exclusions: list[dict] = field(default_factory=list)
    summary_length: str = "standard"
    comparisons: list[str] = field(default_factory=lambda: ["previous_period", "same_period_last_year"])
    charts: list[str] | None = None
    brand: dict = field(default_factory=lambda: dict(DEFAULT_BRAND))
    top_n: int | None = None
    currency: str | None = None
    draft: int | None = None
    name: str | None = None


def apply_preferences(options: BuildOptions, prefs: dict) -> tuple[BuildOptions, list[str]]:
    """Merge preferences.json into build options; pure, returns what was applied."""

    merged = copy.deepcopy(options)
    applied: list[str] = []
    brand = prefs.get("brand")
    if isinstance(brand, dict):
        for key in ("primary", "secondary"):
            if _is_color(brand.get(key)):
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


# --- reading inputs ----------------------------------------------------------


@dataclass
class LoadedTable:
    name: str
    path: str
    sheet: str | None
    frame: pd.DataFrame
    sha256: str
    rows: int
    uploaded: str
    is_workbook: bool
    skipped_sheets: list[dict] = field(default_factory=list)


def parse_source(source: str) -> tuple[Path, str | None]:
    path_text, separator, sheet = source.rpartition("::")
    if separator and path_text:
        return Path(path_text), sheet
    return Path(source), None


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
    sample = text[:8192]
    try:
        delimiter = csv.Sniffer().sniff(sample, delimiters=",;\t|").delimiter
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


# --- column roles ------------------------------------------------------------


@dataclass
class Mapping:
    roles: dict[str, str | None]
    confidence: dict[str, str]
    ambiguous: dict[str, list[str]]
    missing: list[str]
    alternatives: dict[str, list[str]]


def normalize_header(name) -> str:
    return re.sub(r"[^a-z0-9]+", " ", to_text(name).lower()).strip()


def _sample(series: pd.Series) -> list:
    values = [value for value in series.head(SAMPLE_ROWS).tolist() if not _is_missing(value) and to_text(value) != ""]
    return values


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


def looks_numeric(series: pd.Series) -> bool:
    if pd.api.types.is_numeric_dtype(series) and not pd.api.types.is_bool_dtype(series):
        return True
    values = _sample(series)
    if not values:
        return False
    parsed = sum(1 for value in values if not math.isnan(parse_amount(value)))
    return parsed >= 0.9 * len(values)


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
    """The suggested mapping with the agent's mapping file applied and checked against the table."""

    mapping = suggest_mapping(frame, profile)
    if override:
        for role, column in override.items():
            if role not in ROLES:
                raise InputError(f"Mapping names an unknown role {role!r}; roles are {', '.join(ROLES)}.")
            if column is None:
                mapping.roles[role] = None
                mapping.confidence.pop(role, None)
            else:
                if column not in frame.columns:
                    raise InputError(f"Mapping puts {role!r} on column {column!r}, which {table_name} does not have.")
                mapping.roles[role] = column
                mapping.confidence[role] = "mapping"
            mapping.ambiguous.pop(role, None)
        taken = [column for column in mapping.roles.values() if column]
        duplicates = sorted({column for column in taken if taken.count(column) > 1})
        if duplicates:
            raise InputError(f"Mapping uses the same column for two roles: {', '.join(duplicates)}.")
        mapping.missing = [role for role in REQUIRED_ROLES if mapping.roles[role] is None and role not in mapping.ambiguous]
    return mapping


def mapping_question(mapping: Mapping, table_name: str, profile: dict) -> str | None:
    vocabulary = profile["vocabulary"]
    if mapping.ambiguous:
        role, candidates = next(iter(mapping.ambiguous.items()))
        what = {"date": "date", "amount": vocabulary.get("amount", "amount")}.get(role, vocabulary.get(role, role))
        return f"Which column in {table_name} is the {what}: {' or '.join(candidates)}?"
    if mapping.missing:
        role = mapping.missing[0]
        what = {"date": "date of each row", "amount": f"{vocabulary.get('amount', 'amount')} of each row"}[role]
        return f"Which column in {table_name} holds the {what}? I could not find one."
    return None


# --- parsing values ----------------------------------------------------------


def parse_amount(value) -> float:
    """A money cell as a float; NaN when it cannot be read (the caller counts those)."""

    if _is_missing(value):
        return math.nan
    if isinstance(value, bool):
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
    text = re.sub(r"[^\d.,]", "", text)
    if not re.search(r"\d", text):
        return math.nan
    if "," in text and "." in text:
        if text.rfind(",") > text.rfind("."):
            text = text.replace(".", "").replace(",", ".")
        else:
            text = text.replace(",", "")
    elif "," in text:
        groups = text.split(",")
        if all(len(group) == 3 for group in groups[1:]) and len(groups[0]) <= 3:
            text = text.replace(",", "")
        else:
            text = text.replace(",", ".")
    try:
        number = float(text)
    except ValueError:
        return math.nan
    return -number if negative else number


def _decimal_from_cell(value) -> Decimal | None:
    """A second, independent reading of a money cell, used only by the reconciliation check."""

    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return Decimal(str(value)) if not (isinstance(value, float) and math.isnan(value)) else None
    text = str(value).strip()
    sign = -1 if (text.startswith("(") and text.endswith(")")) or text.startswith("-") or text.endswith("-") else 1
    digits = "".join(char for char in text if char.isdigit() or char in ".,")
    if not any(char.isdigit() for char in digits):
        return None
    if "," in digits and "." in digits and digits.rfind(",") > digits.rfind("."):
        digits = digits.replace(".", "").replace(",", ".")
    elif "," in digits and "." not in digits and not all(len(group) == 3 for group in digits.split(",")[1:]):
        digits = digits.replace(",", ".")
    else:
        digits = digits.replace(",", "")
    try:
        return sign * Decimal(digits)
    except InvalidOperation:
        return None


def independent_amount_total(raw_values: list) -> float:
    total = Decimal("0")
    for value in raw_values:
        parsed = _decimal_from_cell(value)
        if parsed is not None:
            total += parsed
    return float(total)


def parse_dates(series: pd.Series) -> pd.Series:
    if pd.api.types.is_datetime64_any_dtype(series):
        parsed = pd.to_datetime(series, errors="coerce")
    else:
        texts = series.map(lambda value: to_text(value) or None)
        parsed = pd.to_datetime(texts, errors="coerce", format="mixed")
    if getattr(parsed.dt, "tz", None) is not None:
        parsed = parsed.dt.tz_localize(None)
    return parsed


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
    unmapped_columns: list[str]


def apply_mapping(frame: pd.DataFrame, roles: dict[str, str | None]) -> CleanFrame:
    """Role columns with parsed values, plus the amount cells as read, for the checks."""

    columns: dict[str, pd.Series] = {}
    date_column, amount_column = roles.get("date"), roles.get("amount")
    if date_column is None or amount_column is None:
        raise InputError("A date column and an amount column are required.")
    dates = parse_dates(frame[date_column])
    columns["date"] = dates
    amounts = frame[amount_column].map(parse_amount).astype(float)
    columns["amount"] = amounts
    columns["amount_raw"] = frame[amount_column]
    for role in TEXT_ROLES:
        column = roles.get(role)
        if column is not None:
            columns[role] = frame[column].map(to_text)
    if roles.get("quantity") is not None:
        columns["quantity"] = frame[roles["quantity"]].map(parse_amount).astype(float)
    clean = pd.DataFrame(columns, index=frame.index)
    unparsed_dates = int(dates.isna().sum())
    non_empty = frame[amount_column].map(lambda value: to_text(value) != "")
    unparsed_amounts = int((amounts.isna() & non_empty).sum())
    used = {column for column in roles.values() if column}
    unmapped = [column for column in frame.columns if usable_header(column) and column not in used]
    return CleanFrame(frame=clean, unparsed_dates=unparsed_dates, unparsed_amounts=unparsed_amounts, unmapped_columns=unmapped)


# --- periods -----------------------------------------------------------------


@dataclass(frozen=True)
class Period:
    start: dt.date
    end: dt.date
    label: str
    key: str
    kind: str


def _month_end(year: int, month: int) -> dt.date:
    following = dt.date(year + (month == 12), (month % 12) + 1, 1)
    return following - dt.timedelta(days=1)


def month_period(year: int, month: int) -> Period:
    return Period(dt.date(year, month, 1), _month_end(year, month), f"{MONTHS[month - 1]} {year}", f"{year:04d}-{month:02d}", "month")


def quarter_period(year: int, quarter: int) -> Period:
    first_month = (quarter - 1) * 3 + 1
    return Period(dt.date(year, first_month, 1), _month_end(year, first_month + 2), f"Q{quarter} {year}", f"{year:04d}-Q{quarter}", "quarter")


def year_period(year: int) -> Period:
    return Period(dt.date(year, 1, 1), dt.date(year, 12, 31), str(year), f"{year:04d}", "year")


def custom_period(start: dt.date, end: dt.date) -> Period:
    if end < start:
        raise InputError(f"Period ends before it starts: {start} to {end}.")
    if start.year == end.year and start.month == end.month:
        label = f"{start.day} to {end.day} {MONTHS[start.month - 1]} {start.year}"
    elif start.year == end.year:
        label = f"{start.day} {MONTHS[start.month - 1]} to {end.day} {MONTHS[end.month - 1]} {start.year}"
    else:
        label = f"{start.day} {MONTHS[start.month - 1]} {start.year} to {end.day} {MONTHS[end.month - 1]} {end.year}"
    return Period(start, end, label, f"{start.isoformat()}..{end.isoformat()}", "custom")


def parse_period(text: str) -> Period:
    text = text.strip()
    if match := re.fullmatch(r"(\d{4})-(\d{2})", text):
        year, month = int(match.group(1)), int(match.group(2))
        if not 1 <= month <= 12:
            raise InputError(f"Not a month: {text}")
        return month_period(year, month)
    if match := re.fullmatch(r"(\d{4})-Q([1-4])", text, flags=re.IGNORECASE):
        return quarter_period(int(match.group(1)), int(match.group(2)))
    if match := re.fullmatch(r"\d{4}", text):
        return year_period(int(text))
    if match := re.fullmatch(r"(\d{4}-\d{2}-\d{2})\.\.(\d{4}-\d{2}-\d{2})", text):
        try:
            return custom_period(dt.date.fromisoformat(match.group(1)), dt.date.fromisoformat(match.group(2)))
        except ValueError as error:
            raise InputError(f"Not a date range: {text} ({error})") from error
    raise InputError(f"Period {text!r} is not one of YYYY-MM, YYYY-Qn, YYYY or YYYY-MM-DD..YYYY-MM-DD.")


def previous_period(period: Period) -> Period:
    if period.kind == "month":
        year, month = (period.start.year - 1, 12) if period.start.month == 1 else (period.start.year, period.start.month - 1)
        return month_period(year, month)
    if period.kind == "quarter":
        quarter = (period.start.month - 1) // 3 + 1
        return quarter_period(period.start.year - 1, 4) if quarter == 1 else quarter_period(period.start.year, quarter - 1)
    if period.kind == "year":
        return year_period(period.start.year - 1)
    length = (period.end - period.start).days
    end = period.start - dt.timedelta(days=1)
    return custom_period(end - dt.timedelta(days=length), end)


def _shift_year(date: dt.date, years: int) -> dt.date:
    try:
        return date.replace(year=date.year + years)
    except ValueError:  # 29 February
        return date.replace(year=date.year + years, day=28)


def same_period_last_year(period: Period) -> Period:
    if period.kind == "month":
        return month_period(period.start.year - 1, period.start.month)
    if period.kind == "quarter":
        return quarter_period(period.start.year - 1, (period.start.month - 1) // 3 + 1)
    if period.kind == "year":
        return year_period(period.start.year - 1)
    return custom_period(_shift_year(period.start, -1), _shift_year(period.end, -1))


def suggest_period(dates: pd.Series) -> Period | None:
    valid = dates.dropna()
    if valid.empty:
        return None
    counts = valid.dt.to_period("M").value_counts()
    best = counts.idxmax()
    return month_period(best.year, best.month)


def _in_period(dates: pd.Series, period: Period) -> pd.Series:
    start = pd.Timestamp(period.start)
    end = pd.Timestamp(period.end) + pd.Timedelta(days=1)
    return (dates >= start) & (dates < end)


# --- building the report -----------------------------------------------------


@dataclass
class BuildContext:
    tables: list[LoadedTable]
    mappings: list[Mapping]
    all_rows: pd.DataFrame
    unmapped_columns: list[str]
    period: Period
    profile: dict
    options: BuildOptions
    currency: str
    currency_source: str
    excluded_rows: int
    excluded_amount: float
    unparsed_dates: int
    unparsed_amounts: int
    exclusion_texts: list[str]


def _vocab(profile: dict, key: str, default: str) -> str:
    return str(profile.get("vocabulary", {}).get(key, default))


def _cap(text: str) -> str:
    return text[:1].upper() + text[1:]


def prepare(sources: list[str], period_text: str, options: BuildOptions, mapping_override: dict | None, profile: dict) -> BuildContext:
    """Read, map, clean and filter the inputs; raises DecisionNeeded when a question is due."""

    if not sources:
        raise InputError("Give at least one CSV, XLSX or XLS file.")
    tables = [read_table(source, profile) for source in sources]
    mappings: list[Mapping] = []
    for table in tables:
        mapping = resolve_mapping(table.frame, profile, mapping_override, table.name)
        question = mapping_question(mapping, table.name, profile)
        if question:
            raise DecisionNeeded(question, {"file": table.name, "sheet": table.sheet, "ambiguous": mapping.ambiguous, "missing": mapping.missing, "mapping": mapping.roles})
        mappings.append(mapping)
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
    excluded_mask = pd.Series(False, index=all_rows.index)
    exclusion_texts: list[str] = []
    for exclusion in options.exclusions:
        column = exclusion.get("role") if "role" in exclusion else f"extra:{exclusion.get('column')}"
        if column not in all_rows.columns:
            mapped = [role for role, name in mappings[0].roles.items() if name == exclusion.get("column")]
            column = mapped[0] if mapped else column
        if column not in all_rows.columns:
            raise InputError(f"Exclusion names {exclusion.get('role') or exclusion.get('column')!r}, which the files do not have.")
        matches = all_rows[column].astype(str).str.strip().str.lower() == str(exclusion["equals"]).strip().lower()
        in_period_matches = matches & _in_period(all_rows["date"], period)
        label = _vocab(profile, exclusion["role"], exclusion["role"]) if "role" in exclusion else exclusion["column"]
        records = _vocab(profile, "records", "rows")
        excluded_amount = float(all_rows.loc[in_period_matches, "amount"].fillna(0).sum())
        exclusion_texts.append(f"Excluded {int(in_period_matches.sum())} {records} where {label} = {exclusion['equals']} ({format_value(excluded_amount, 'currency', options.currency or 'USD')})")
        excluded_mask |= matches
    excluded_in_period = int((excluded_mask & _in_period(all_rows["date"], period)).sum())
    excluded_amount = float(all_rows.loc[excluded_mask & _in_period(all_rows["date"], period), "amount"].fillna(0).sum())
    kept = all_rows.loc[~excluded_mask].reset_index(drop=True)
    currency, currency_source = detect_currency(tables[0].frame, mappings[0].roles["amount"])
    if options.currency:
        currency, currency_source = options.currency, "preferences"
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
        excluded_rows=excluded_in_period,
        excluded_amount=excluded_amount,
        unparsed_dates=sum(clean.unparsed_dates for clean in cleaned),
        unparsed_amounts=sum(clean.unparsed_amounts for clean in cleaned),
        exclusion_texts=exclusion_texts,
    )


def _period_rows(ctx: BuildContext, period: Period) -> pd.DataFrame:
    return ctx.all_rows.loc[_in_period(ctx.all_rows["date"], period)]


def _revenue(rows: pd.DataFrame) -> float:
    return float(rows["amount"].fillna(0).sum())


def _pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return _round((current - previous) / abs(previous) * 100, 1)


def _group_table(rows: pd.DataFrame, key: str, currency: str, first_heading: str, records_heading: str, unpaid: bool, share: bool, top_n: int | None = None) -> dict:
    grouped = rows.groupby(key, sort=False, dropna=False)
    summary = pd.DataFrame({"count": grouped.size(), "revenue": grouped["amount"].sum(min_count=0)})
    summary["revenue"] = summary["revenue"].fillna(0)
    if unpaid:
        summary["unpaid"] = rows.loc[rows["status_group"] == "unpaid"].groupby(key, sort=False, dropna=False)["amount"].sum(min_count=0)
        summary["unpaid"] = summary["unpaid"].fillna(0)
    summary = summary.sort_values(["revenue", "count"], ascending=[False, False])
    total_revenue = float(summary["revenue"].sum())
    total_count = int(summary["count"].sum())
    table_rows = []
    for name, entry in summary.iterrows():
        count = int(entry["count"])
        revenue = float(entry["revenue"])
        row = [str(name), count, _number(revenue), _number(revenue / count) if count else None]
        if unpaid:
            row.append(_number(entry["unpaid"]))
        if share:
            row.append(_number(revenue / total_revenue * 100) if total_revenue else None)
        table_rows.append(row)
    if top_n is not None and len(table_rows) > top_n:
        table_rows = table_rows[:top_n]
    columns = [first_heading, records_heading, "Revenue", "Avg ticket"]
    formats = ["text", "integer", "currency", "currency"]
    totals = ["Total", total_count, _number(total_revenue), _number(total_revenue / total_count) if total_count else None]
    if unpaid:
        columns.append("Unpaid")
        formats.append("currency")
        totals.append(_number(float(summary["unpaid"].sum())))
    if share:
        columns.append("Share")
        formats.append("percent")
        totals.append(100 if total_revenue else None)
    return {"columns": columns, "formats": formats, "rows": table_rows, "totals": totals}


def _bucket_labels(rows: pd.DataFrame, period: Period) -> pd.Series:
    span = (period.end - period.start).days + 1
    dates = rows["date"].dt.normalize()
    if span <= WEEK_BUCKET_LIMIT_DAYS:
        starts = dates - pd.to_timedelta(dates.dt.weekday, unit="D")

        def label(start: pd.Timestamp) -> str:
            first = max(start.date(), period.start)
            last = min((start + pd.Timedelta(days=6)).date(), period.end)
            if first == last:
                return f"{first.day} {MONTHS_SHORT[first.month - 1]}"
            if first.month == last.month:
                return f"{first.day}–{last.day} {MONTHS_SHORT[first.month - 1]}"
            return f"{first.day} {MONTHS_SHORT[first.month - 1]}–{last.day} {MONTHS_SHORT[last.month - 1]}"

        return starts.map(label)
    return dates.map(lambda date: f"{MONTHS_SHORT[date.month - 1]} {date.year}")


def build_report(ctx: BuildContext, previous_draft: int) -> tuple[dict, dict]:
    """The report document and the data behind its charts."""

    profile, options, period, currency = ctx.profile, ctx.options, ctx.period, ctx.currency
    vocab = lambda key, default: _vocab(profile, key, default)  # noqa: E731
    records, record = vocab("records", "rows"), vocab("record", "row")
    rows = _period_rows(ctx, period).copy()
    has = {role: role in rows.columns for role in ROLES}
    status_groups = profile.get("status_groups", {})
    if has["status"]:
        lookup = {alias.lower(): group for group, aliases in status_groups.items() for alias in aliases}
        rows["status_group"] = rows["status"].str.strip().str.lower().map(lambda value: lookup.get(value, "other"))
        has_status_groups = bool((rows["status_group"] != "other").any())
    else:
        rows["status_group"] = "other"
        has_status_groups = False
    if has["person"]:
        rows["person"] = rows["person"].where(rows["person"].str.strip() != "", "Unassigned")
    if has["category"]:
        rows["category"] = rows["category"].where(rows["category"].str.strip() != "", "Uncategorized")

    revenue = _revenue(rows)
    count = int(len(rows))
    average = revenue / count if count else None
    notes: list[str] = []
    kpis: list[dict] = [
        {"id": "revenue", "label": _cap(vocab("amount", "revenue")), "value": _number(revenue), "format": "currency"},
        {"id": "jobs", "label": _cap(records), "value": count, "format": "integer"},
        {"id": "average_ticket", "label": "Avg ticket", "value": _number(average), "format": "currency"},
    ]

    previous = previous_period(period)
    previous_rows = _period_rows(ctx, previous) if "previous_period" in options.comparisons else rows.iloc[0:0]
    last_year = same_period_last_year(period)
    last_year_rows = _period_rows(ctx, last_year) if "same_period_last_year" in options.comparisons else rows.iloc[0:0]
    if len(previous_rows):
        previous_revenue = _revenue(previous_rows)
        kpis[0]["delta"] = {"vs": previous.label, "pct": _pct_change(revenue, previous_revenue), "previous": _number(previous_revenue)}
        kpis[1]["delta"] = {"vs": previous.label, "pct": _pct_change(count, len(previous_rows)), "previous": len(previous_rows)}
    elif "previous_period" in options.comparisons:
        notes.append(f"Comparison with {previous.label}: not included, the files have no rows for that period.")
    if not len(last_year_rows) and "same_period_last_year" in options.comparisons:
        notes.append(f"Comparison with {last_year.label}: not included, the files have no rows for that period.")

    if has_status_groups:
        unpaid = float(rows.loc[rows["status_group"] == "unpaid", "amount"].fillna(0).sum())
        kpis.append({"id": "unpaid", "label": "Unpaid", "value": _number(unpaid), "format": "currency"})
    new_customers_text = None
    if has["customer"]:
        known = ctx.all_rows.loc[ctx.all_rows["customer"].str.strip() != ""]
        earlier = known.loc[known["date"] < pd.Timestamp(period.start)]
        active = set(rows.loc[rows["customer"].str.strip() != "", "customer"])
        if len(earlier) and active:
            seen_before = set(earlier["customer"])
            new = {customer for customer in active if customer not in seen_before}
            pct = _round(len(new) / len(active) * 100, 1)
            kpis.append({"id": "new_customers", "label": "New customers", "value": pct, "format": "percent"})
            new_customers_text = f"{len(new)} of {len(active)} {vocab('customers', 'customers')} were new in {period.label} ({format_value(pct, 'percent')}), based on the files given."
        else:
            notes.append(f"New {vocab('customers', 'customers')}: not shown, the files have no rows earlier than {period.label} to tell new {vocab('customers', 'customers')} from returning ones.")

    sections: list[dict] = []
    charts: dict[str, dict] = {}
    section_order = profile.get("sections", list(SECTION_BUILDERS))
    state = {
        "rows": rows,
        "has": has,
        "has_status_groups": has_status_groups,
        "revenue": revenue,
        "count": count,
        "average": average,
        "previous": previous,
        "previous_rows": previous_rows,
        "last_year": last_year,
        "last_year_rows": last_year_rows,
        "kpis": kpis,
        "notes": notes,
        "charts": charts,
        "new_customers_text": new_customers_text,
        "records": records,
        "record": record,
    }
    for section_id in section_order:
        builder = SECTION_BUILDERS.get(section_id)
        if builder is None:
            continue
        section = builder(ctx, state)
        if section is not None:
            sections.append(section)
    wanted = options.charts
    if wanted is not None:
        for section in sections:
            section["charts"] = [chart for chart in section.get("charts", []) if chart in wanted]
        charts = {chart_id: data for chart_id, data in charts.items() if chart_id in wanted}
    chart_entries = [{"id": chart_id, "png": f"charts/{chart_id}.png", "spec": {"type": data["type"], "x": data["x"], "y": data["y"], "title": data["title"], "format": data["format"]}} for chart_id, data in charts.items()]

    title = options.title or profile.get("title", "{period} Business Review").replace("{period}", period.label)
    company = options.company if options.company is not None else options.brand.get("company", "")
    report = {
        "version": 1,
        "meta": {
            "title": title,
            "company": company,
            "period": {"start": period.start.isoformat(), "end": period.end.isoformat(), "label": period.label, "key": period.key},
            "generated_at": _utc_now(),
            "draft": options.draft or previous_draft + 1,
            "inputs": [{"name": table.name, "sha256": table.sha256, "rows": table.rows, "uploaded": table.uploaded, "sheet": table.sheet} for table in ctx.tables],
            "profile": profile["name"],
            "preferences_applied": [],
            "lang": LANG,
            "currency": {"code": currency, "source": ctx.currency_source},
            "build": {"sources": [f"{table.path}::{table.sheet}" if table.sheet else table.path for table in ctx.tables], "mapping": dict(ctx.mappings[0].roles), "exclusions": list(options.exclusions)},
            "brand": {"company": company, "primary": options.brand["primary"], "secondary": options.brand["secondary"], "logo": options.brand.get("logo")},
        },
        "kpis": kpis,
        "sections": sections,
        "charts": chart_entries,
        "checks": compute_checks(ctx, rows, revenue, count, sections),
        "notes": notes,
        "rows": _rows_table(ctx, rows),
    }
    return report, charts


def _rows_table(ctx: BuildContext, rows: pd.DataFrame) -> dict:
    vocab = ctx.profile.get("vocabulary", {})
    columns, formats, keys = [], [], []
    for role in ("date", "amount", *[role for role in ROLES if role not in ("date", "amount")]):
        if role in rows.columns:
            heading = {"date": "Date", "amount": _cap(vocab.get("amount", "amount")), "quantity": "Quantity"}.get(role) or _cap(vocab.get(role, ROLE_HEADINGS[role]))
            columns.append(heading)
            formats.append({"date": "date", "amount": "currency", "quantity": "number"}.get(role, "text"))
            keys.append(role)
    for column in ctx.unmapped_columns:
        columns.append(column)
        formats.append("text")
        keys.append(f"extra:{column}")
    table_rows = []
    for _, entry in rows[keys].iterrows():
        row = []
        for key, fmt in zip(keys, formats):
            value = entry[key]
            if fmt == "date":
                row.append(to_text(value) if not _is_missing(value) else None)
            elif fmt in ("currency", "number"):
                row.append(_number(value))
            else:
                row.append(to_text(value))
        table_rows.append(row)
    return {"columns": columns, "formats": formats, "rows": table_rows, "totals": None}


# --- sections ----------------------------------------------------------------


def _section_summary(ctx: BuildContext, state: dict) -> dict:
    period, currency, options = ctx.period, ctx.currency, ctx.options
    records, record = state["records"], state["record"]
    rows = state["rows"]
    sentences = [f"{period.label}: {format_value(state['revenue'], 'currency', currency)} in {_vocab(ctx.profile, 'amount', 'revenue')} across {format_value(state['count'], 'integer')} {records if state['count'] != 1 else record}"]
    if state["average"] is not None:
        sentences[0] += f", an average of {format_value(state['average'], 'currency', currency)} per {record}."
    else:
        sentences[0] += "."
    if len(state["previous_rows"]):
        pct = _pct_change(state["revenue"], _revenue(state["previous_rows"]))
        if pct is not None:
            direction = "above" if pct >= 0 else "below"
            sentences.append(f"That is {format_value(abs(pct), 'percent')} {direction} {state['previous'].label} ({format_value(_revenue(state['previous_rows']), 'currency', currency)}).")
    if options.summary_length != "short":
        if state["has"]["category"] and state["revenue"]:
            by_category = rows.groupby("category")["amount"].sum().sort_values(ascending=False)
            top = by_category.index[0]
            share = _round(float(by_category.iloc[0]) / state["revenue"] * 100, 1)
            sentences.append(f"{top} was the largest {_vocab(ctx.profile, 'category', 'category')} at {format_value(share, 'percent')} of {_vocab(ctx.profile, 'amount', 'revenue')}.")
        if state["has"]["person"] and state["revenue"]:
            by_person = rows.groupby("person").agg(revenue=("amount", "sum"), count=("amount", "size")).sort_values("revenue", ascending=False)
            leader = by_person.index[0]
            sentences.append(f"{leader} led with {format_value(float(by_person['revenue'].iloc[0]), 'currency', currency)} across {format_value(int(by_person['count'].iloc[0]), 'integer')} {records}.")
        if state["has_status_groups"]:
            unpaid_rows = rows.loc[rows["status_group"] == "unpaid"]
            if len(unpaid_rows):
                sentences.append(f"{format_value(_revenue(unpaid_rows), 'currency', currency)} across {format_value(len(unpaid_rows), 'integer')} {records} is unpaid.")
    return {"id": "summary", "heading": "Summary", "paragraphs": [" ".join(sentences)]}


def _section_comparison(ctx: BuildContext, state: dict) -> dict | None:
    previous_rows, last_year_rows = state["previous_rows"], state["last_year_rows"]
    if not len(previous_rows) and not len(last_year_rows):
        return None
    period = ctx.period
    columns, formats = ["Metric", period.label], ["text", "number"]
    metrics = [("Revenue", state["revenue"], _revenue), ("Jobs", state["count"], len), ("Average ticket", state["average"], lambda rows: _revenue(rows) / len(rows) if len(rows) else None)]
    metrics[1] = (_cap(state["records"]), state["count"], len)
    table_rows = [[label, _number(value)] for label, value, _ in metrics]
    for label, comparison_rows in ((state["previous"].label, previous_rows), (state["last_year"].label, last_year_rows)):
        if not len(comparison_rows):
            continue
        change_heading = "Change" if label == state["previous"].label else "Change vs last year"
        columns += [label, change_heading]
        formats += ["number", "percent"]
        for row, (_, value, compute) in zip(table_rows, metrics):
            other = compute(comparison_rows)
            row += [_number(other), _pct_change(value, other) if value is not None and other not in (None, 0) else None]
    return {"id": "comparison", "heading": "Compared with earlier periods", "table": {"columns": columns, "formats": formats, "rows": table_rows, "totals": None}}


def _section_by_period(ctx: BuildContext, state: dict) -> dict | None:
    rows = state["rows"]
    if not len(rows):
        return None
    labelled = rows.assign(bucket=_bucket_labels(rows, ctx.period))
    order = labelled.groupby("bucket", sort=False)["date"].min().sort_values().index.tolist()
    grouped = labelled.groupby("bucket", sort=False)
    table_rows = []
    for bucket in order:
        subset = grouped.get_group(bucket)
        revenue = _revenue(subset)
        table_rows.append([bucket, int(len(subset)), _number(revenue), _number(revenue / len(subset))])
    span = (ctx.period.end - ctx.period.start).days + 1
    unit = "Week" if span <= WEEK_BUCKET_LIMIT_DAYS else "Month"
    state["charts"]["revenue_by_period"] = {
        "type": "bar",
        "x": unit.lower(),
        "y": "revenue",
        "title": f"{_cap(_vocab(ctx.profile, 'amount', 'revenue'))} by {unit.lower()}",
        "format": "currency",
        "labels": [row[0] for row in table_rows],
        "values": [row[2] or 0 for row in table_rows],
    }
    return {
        "id": "by_period",
        "heading": f"By {unit.lower()}",
        "table": {
            "columns": [unit, _cap(state["records"]), "Revenue", "Avg ticket"],
            "formats": ["text", "integer", "currency", "currency"],
            "rows": table_rows,
            "totals": ["Total", state["count"], _number(state["revenue"]), _number(state["average"])],
        },
        "charts": ["revenue_by_period"],
    }


def _section_by_category(ctx: BuildContext, state: dict) -> dict | None:
    if not state["has"]["category"]:
        state["notes"].append(f"By {_vocab(ctx.profile, 'category', 'category')}: not included, the file has no {_vocab(ctx.profile, 'category', 'category')} column.")
        return None
    table = _group_table(state["rows"], "category", ctx.currency, _cap(_vocab(ctx.profile, "category", "category")), _cap(state["records"]), unpaid=False, share=True)
    top_n = ctx.options.top_n or int(ctx.profile.get("top_n", 10))
    labels = [row[0] for row in table["rows"][:top_n]]
    values = [row[2] or 0 for row in table["rows"][:top_n]]
    if len(table["rows"]) > top_n:
        labels.append("Other")
        values.append(sum(row[2] or 0 for row in table["rows"][top_n:]))
    category = _vocab(ctx.profile, "category", "category")
    state["charts"]["revenue_by_category"] = {
        "type": "barh",
        "x": category,
        "y": "revenue",
        "title": f"{_cap(_vocab(ctx.profile, 'amount', 'revenue'))} by {category}",
        "format": "currency",
        "labels": labels,
        "values": values,
    }
    return {"id": "by_category", "heading": f"By {_vocab(ctx.profile, 'category', 'category')}", "table": table, "charts": ["revenue_by_category"]}


def _section_by_person(ctx: BuildContext, state: dict) -> dict | None:
    person = _vocab(ctx.profile, "person", "person")
    if not state["has"]["person"]:
        state["notes"].append(f"By {person}: not included, the file has no {person} column.")
        return None
    table = _group_table(state["rows"], "person", ctx.currency, _cap(person), _cap(state["records"]), unpaid=state["has_status_groups"], share=False)
    state["charts"]["jobs_by_person"] = {
        "type": "bar",
        "x": person,
        "y": state["records"],
        "title": f"{_cap(state['records'])} by {person}",
        "format": "integer",
        "labels": [row[0] for row in table["rows"]],
        "values": [row[1] for row in table["rows"]],
    }
    return {"id": "by_person", "heading": f"By {person}", "table": table, "charts": ["jobs_by_person"]}


def _section_customers(ctx: BuildContext, state: dict) -> dict | None:
    customer = _vocab(ctx.profile, "customer", "customer")
    if not state["has"]["customer"]:
        state["notes"].append(f"{_cap(_vocab(ctx.profile, 'customers', 'customers'))}: not included, the file has no {customer} column.")
        return None
    rows = state["rows"]
    rows = rows.assign(customer=rows["customer"].where(rows["customer"].str.strip() != "", "Unknown"))
    top_n = ctx.options.top_n or int(ctx.profile.get("top_n", 10))
    table = _group_table(rows, "customer", ctx.currency, _cap(customer), _cap(state["records"]), unpaid=False, share=False, top_n=top_n)
    table["totals"] = None
    section = {"id": "customers", "heading": f"Top {min(top_n, len(table['rows']))} {_vocab(ctx.profile, 'customers', 'customers')}", "table": table}
    if state["new_customers_text"]:
        section["paragraphs"] = [state["new_customers_text"]]
    return section


def _section_status(ctx: BuildContext, state: dict) -> dict | None:
    if not state["has"]["status"]:
        state["notes"].append("Status: not included, the file has no status column.")
        return None
    rows = state["rows"]
    rows = rows.assign(status=rows["status"].where(rows["status"].str.strip() != "", "Blank"))
    table = _group_table(rows, "status", ctx.currency, "Status", _cap(state["records"]), unpaid=False, share=True)
    return {"id": "status", "heading": "By status", "table": table}


def _section_source(ctx: BuildContext, state: dict) -> dict | None:
    source = _vocab(ctx.profile, "source", "source")
    if not state["has"]["source"]:
        state["notes"].append(f"{_cap(source)}: not included, the file has no {source} column.")
        return None
    rows = state["rows"]
    rows = rows.assign(source=rows["source"].where(rows["source"].str.strip() != "", "Unknown"))
    table = _group_table(rows, "source", ctx.currency, _cap(source), _cap(state["records"]), unpaid=False, share=True)
    return {"id": "source", "heading": f"By {source}", "table": table}


def _section_actions(ctx: BuildContext, state: dict) -> dict:
    rows, currency, records = state["rows"], ctx.currency, state["records"]
    bullets: list[str] = []
    if state["has_status_groups"]:
        unpaid_rows = rows.loc[rows["status_group"] == "unpaid"]
        if len(unpaid_rows):
            bullets.append(f"Collect the {format_value(_revenue(unpaid_rows), 'currency', currency)} still unpaid across {format_value(len(unpaid_rows), 'integer')} {records}.")
    if len(state["previous_rows"]):
        pct = _pct_change(state["revenue"], _revenue(state["previous_rows"]))
        if pct is not None and pct < 0:
            bullets.append(f"{_cap(_vocab(ctx.profile, 'amount', 'revenue'))} was {format_value(abs(pct), 'percent')} below {state['previous'].label}; check whether fewer {records} or smaller tickets drove it.")
    if state["has"]["category"] and state["revenue"]:
        by_category = rows.groupby("category")["amount"].sum().sort_values(ascending=False)
        share = _round(float(by_category.iloc[0]) / state["revenue"] * 100, 1)
        if share >= 50:
            bullets.append(f"{by_category.index[0]} is {format_value(share, 'percent')} of {_vocab(ctx.profile, 'amount', 'revenue')}; a slow month there moves the whole business.")
    if state["has"]["person"]:
        unassigned = int((rows["person"] == "Unassigned").sum())
        if unassigned:
            person = _vocab(ctx.profile, "person", "person")
            bullets.append(f"Assign the {format_value(unassigned, 'integer')} {records if unassigned != 1 else state['record']} with no {person} so the by-{person} figures are complete.")
    if state["has"]["customer"] and state["new_customers_text"] is None:
        bullets.append(f"Add earlier months next time to see new versus returning {_vocab(ctx.profile, 'customers', 'customers')}.")
    if not bullets:
        bullets.append(f"Review the {_vocab(ctx.profile, 'category', 'category')} and {_vocab(ctx.profile, 'person', 'person')} tables for anything that looks off; nothing in the checks needs action.")
    return {"id": "actions", "heading": "Three things to act on", "bullets": bullets[:3]}


SECTION_BUILDERS = {
    "summary": _section_summary,
    "comparison": _section_comparison,
    "by_period": _section_by_period,
    "by_category": _section_by_category,
    "by_person": _section_by_person,
    "customers": _section_customers,
    "status": _section_status,
    "source": _section_source,
    "actions": _section_actions,
}


# --- checks ------------------------------------------------------------------


def compute_checks(ctx: BuildContext, rows: pd.DataFrame, revenue: float, count: int, sections: list[dict]) -> list[dict]:
    profile, period, currency = ctx.profile, ctx.period, ctx.currency
    records, record = _vocab(profile, "records", "rows"), _vocab(profile, "record", "row")
    checks: list[dict] = []
    total_rows = sum(table.rows for table in ctx.tables)
    outside = total_rows - count - ctx.unparsed_dates - ctx.excluded_rows
    used_text = f"Used {format_value(count, 'integer')} of {format_value(total_rows, 'integer')} rows: {format_value(outside, 'integer')} are outside {period.label}, {format_value(ctx.unparsed_dates, 'integer')} had no usable date"
    used_text += f", {format_value(ctx.excluded_rows, 'integer')} were excluded." if ctx.excluded_rows else "."
    checks.append({"id": "rows_used", "status": "pass", "text": used_text})

    independent = independent_amount_total(rows["amount_raw"].tolist())
    mismatches = []
    if abs(independent - revenue) > 0.01:
        mismatches.append(f"the amount column sums to {format_value(independent, 'currency', currency)}")
    for section in sections:
        table = section.get("table")
        if not table or not table.get("totals") or "Revenue" not in table["columns"] or section["id"] in ("customers",):
            continue
        index = table["columns"].index("Revenue")
        section_total = sum(row[index] or 0 for row in table["rows"])
        if abs(section_total - revenue) > 0.01:
            mismatches.append(f"the {section['heading'].lower()} table sums to {format_value(section_total, 'currency', currency)}")
    if mismatches:
        checks.append({"id": "totals_reconcile", "status": "fail", "text": f"Totals do not match your file: the report says {format_value(revenue, 'currency', currency)} but {', '.join(mismatches)}. The report was withheld."})
    else:
        checks.append({"id": "totals_reconcile", "status": "pass", "text": f"Totals match your file: {format_value(revenue, 'currency', currency)} across {format_value(count, 'integer')} {records if count != 1 else record}."})
    span = f"{period.start.day} {MONTHS[period.start.month - 1]} {period.start.year} and {period.end.day} {MONTHS[period.end.month - 1]} {period.end.year}"
    checks.append({"id": "dates_in_period", "status": "pass", "text": f"Every {record} counted falls between {span}."})

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
            unmapped_parts.append(f"{format_value(unassigned, 'integer')} {records if unassigned != 1 else record} had no {_vocab(profile, 'person', 'person')} and are listed as Unassigned")
    if "category" in rows.columns:
        uncategorized = int((rows["category"] == "Uncategorized").sum())
        if uncategorized:
            unmapped_parts.append(f"{format_value(uncategorized, 'integer')} had no {_vocab(profile, 'category', 'category')} and are listed as Uncategorized")
    if "person" in rows.columns or "category" in rows.columns:
        if unmapped_parts:
            checks.append({"id": "unmapped_rows", "status": "warn", "text": "; ".join(unmapped_parts) + "."})
        else:
            checks.append({"id": "unmapped_rows", "status": "pass", "text": f"Every {record} has a {_vocab(profile, 'person', 'person') if 'person' in rows.columns else _vocab(profile, 'category', 'category')}."})

    if ctx.unparsed_amounts:
        rows_had = "row had" if ctx.unparsed_amounts == 1 else "rows had"
        checks.append({"id": "unparsed_amounts", "status": "warn", "text": f"{format_value(ctx.unparsed_amounts, 'integer')} {rows_had} no usable amount and count as {format_value(0, 'currency', currency)}."})
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


def checks_line(report: dict) -> str:
    """The one line the card shows: the reconciliation, then every warning or failure."""

    checks = report.get("checks", [])
    parts = [check["text"] for check in checks if check["id"] == "totals_reconcile"]
    parts += [check["text"] for check in checks if check["status"] in ("warn", "fail") and check["id"] != "totals_reconcile"]
    return " ".join(parts)


# --- prose verification ------------------------------------------------------


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
    found: set[float] = set()
    for key in ("kpis", "sections", "rows"):
        _numbers_in(report.get(key), found)
    for check in report.get("checks", []):
        for match in NUMBER_TOKEN.finditer(check["text"]):
            found.add(float(match.group(2).replace(",", "")))
    period = report["meta"]["period"]
    for date_text in (period["start"], period["end"]):
        found.add(float(date_text[:4]))
    found.update(float(day) for day in range(1, 32))
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


def apply_prose(report: dict, summary: list[str] | None, actions: list[str] | None) -> tuple[dict, list[str]]:
    """Model-written text into the report, numbers verified; pure."""

    updated = copy.deepcopy(report)
    removed_all: list[str] = []
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
        section["paragraphs"] = paragraphs
    if actions is not None:
        bullets = []
        for bullet in actions:
            clean, removed = verify_prose_numbers(updated, bullet)
            removed_all += removed
            if clean and not removed:
                bullets.append(clean)
        section = sections.get("actions")
        if section is None:
            section = {"id": "actions", "heading": "Three things to act on"}
            updated["sections"].append(section)
        section["bullets"] = bullets
    if removed_all:
        verb = "is" if len(removed_all) == 1 else "are"
        check = {"id": "prose_numbers", "status": "warn", "text": f"Removed {len(removed_all)} {_plural(len(removed_all), 'number')} from the written text that {verb} not in the report: {', '.join(removed_all)}."}
    else:
        check = {"id": "prose_numbers", "status": "pass", "text": "Every number in the written text matches the report."}
    updated["checks"] = [entry for entry in updated["checks"] if entry["id"] != "prose_numbers"] + [check]
    updated["meta"]["draft"] = int(updated["meta"]["draft"]) + 1
    updated["meta"]["generated_at"] = _utc_now()
    return updated, removed_all


# --- charts ------------------------------------------------------------------


def draw_charts(charts: dict[str, dict], out_dir: Path, brand: dict, currency: str) -> list[Path]:
    if not charts:
        return []
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.ticker import FuncFormatter
    except ImportError as error:
        sys.stderr.write(f"{MISSING_LIBRARY_MESSAGE} ({error})\n")
        sys.exit(EXIT_MISSING_LIBRARY)
    charts_dir = out_dir / "charts"
    charts_dir.mkdir(parents=True, exist_ok=True)
    written = []
    for chart_id, data in charts.items():
        figure, axis = plt.subplots(figsize=CHART_SIZE_INCHES, dpi=CHART_DPI)
        labels, values = data["labels"], data["values"]
        formatter = FuncFormatter(lambda value, _pos, fmt=data["format"]: format_value(value, fmt, currency))
        if data["type"] == "barh":
            axis.barh(labels[::-1], values[::-1], color=brand["primary"])
            axis.xaxis.set_major_formatter(formatter)
            axis.grid(axis="x", color="#dddddd", linewidth=0.6)
        else:
            axis.bar(labels, values, color=brand["primary"])
            axis.yaxis.set_major_formatter(formatter)
            axis.grid(axis="y", color="#dddddd", linewidth=0.6)
            if len(labels) > 6 or max((len(label) for label in labels), default=0) > 8:
                plt.setp(axis.get_xticklabels(), rotation=30, ha="right")
        axis.set_axisbelow(True)
        axis.set_title(data["title"], loc="left", fontsize=11, color="#222222")
        for side in ("top", "right"):
            axis.spines[side].set_visible(False)
        axis.tick_params(labelsize=8, colors="#333333")
        figure.tight_layout()
        path = charts_dir / f"{chart_id}.png"
        figure.savefig(path, dpi=CHART_DPI)
        plt.close(figure)
        written.append(path)
    return written


# --- renderers ---------------------------------------------------------------


def _data_uri(path: Path) -> str:
    suffix = path.suffix.lower()
    mime = {".png": "image/png", ".svg": "image/svg+xml", ".jpg": "image/jpeg", ".jpeg": "image/jpeg"}.get(suffix, "application/octet-stream")
    return f"data:{mime};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def effective_brand(report: dict, tenant_dir: str | None) -> dict:
    """The brand the report was built with, with the tenant bundle on top when given."""

    brand = dict(DEFAULT_BRAND)
    stored = report["meta"].get("brand", {})
    brand["company"] = str(stored.get("company") or "")
    brand["logo"] = stored.get("logo") if isinstance(stored.get("logo"), str) else None
    for key in ("primary", "secondary"):
        if _is_color(stored.get(key)):
            brand[key] = stored[key]
    if tenant_dir:
        tenant = load_brand(tenant_dir)
        for key in ("company", "primary", "secondary", "logo"):
            if tenant.get(key) not in (None, "") and (key != "company" or not brand.get("company")):
                brand[key] = tenant[key]
        if tenant.get("company") and not report["meta"].get("company"):
            brand["company"] = tenant["company"]
    if brand.get("company") == "":
        brand["company"] = report["meta"].get("company", "")
    return brand


def render_html(report: dict, report_path: Path, brand: dict) -> str:
    environment = jinja2.Environment(loader=jinja2.FileSystemLoader(str(TEMPLATES_DIR)), autoescape=jinja2.select_autoescape(["html", "j2"]), undefined=jinja2.StrictUndefined)
    template = environment.get_template("report.html.j2")
    currency = report["meta"]["currency"]["code"]
    charts = {}
    for chart in report["charts"]:
        png = report_path.parent / chart["png"]
        if png.is_file():
            charts[chart["id"]] = _data_uri(png)
    logo = None
    if brand.get("logo") and Path(brand["logo"]).is_file():
        logo = _data_uri(Path(brand["logo"]))
    company = brand.get("company") or report["meta"].get("company", "")
    return template.render(
        report=report,
        brand=brand,
        company=company,
        logo=logo,
        charts=charts,
        css=(TEMPLATES_DIR / "report.css").read_text(encoding="utf-8"),
        fmt=lambda value, fmt: format_value(value, fmt, currency),
        checks_line=checks_line(report),
    )


def render_pdf(html: str, path: Path) -> None:
    try:
        import weasyprint
    except ImportError as error:
        sys.stderr.write(f"{MISSING_LIBRARY_MESSAGE} ({error})\n")
        sys.exit(EXIT_MISSING_LIBRARY)
    weasyprint.HTML(string=html).write_pdf(str(path))


def render_docx(report: dict, report_path: Path, brand: dict, path: Path) -> None:
    currency = report["meta"]["currency"]["code"]
    document = docx.Document()
    if brand.get("logo") and Path(brand["logo"]).is_file() and Path(brand["logo"]).suffix.lower() in (".png", ".jpg", ".jpeg"):
        document.add_picture(brand["logo"], width=Inches(1.5))
    document.add_heading(report["meta"]["title"], level=0)
    company = brand.get("company") or report["meta"].get("company", "")
    subtitle = " · ".join(part for part in (company, report["meta"]["period"]["label"], f"draft {report['meta']['draft']}") if part)
    document.add_paragraph(subtitle)
    kpi_table = document.add_table(rows=2, cols=max(1, len(report["kpis"])))
    kpi_table.style = "Table Grid"
    for index, kpi in enumerate(report["kpis"]):
        kpi_table.cell(0, index).text = kpi["label"]
        value = format_value(kpi["value"], kpi["format"], currency)
        delta = kpi.get("delta")
        if delta and delta.get("pct") is not None:
            value += f" ({'+' if delta['pct'] >= 0 else ''}{format_value(delta['pct'], 'percent')} vs {delta['vs']})"
        kpi_table.cell(1, index).text = value
    for section in report["sections"]:
        document.add_heading(section["heading"], level=1)
        for paragraph in section.get("paragraphs", []):
            document.add_paragraph(paragraph)
        for bullet in section.get("bullets", []):
            document.add_paragraph(bullet, style="List Bullet")
        table = section.get("table")
        if table:
            rows = list(table["rows"]) + ([table["totals"]] if table.get("totals") else [])
            word_table = document.add_table(rows=len(rows) + 1, cols=len(table["columns"]))
            word_table.style = "Table Grid"
            for index, column in enumerate(table["columns"]):
                cell = word_table.cell(0, index)
                cell.text = column
                cell.paragraphs[0].runs[0].bold = True
            for row_index, row in enumerate(rows, start=1):
                for index, (value, fmt) in enumerate(zip(row, table["formats"])):
                    cell = word_table.cell(row_index, index)
                    cell.text = format_value(value, fmt, currency)
                    if fmt != "text":
                        cell.paragraphs[0].alignment = WD_ALIGN_PARAGRAPH.RIGHT
                    if row_index == len(rows) and table.get("totals"):
                        cell.paragraphs[0].runs[0].bold = True
        for chart_id in section.get("charts", []):
            png = report_path.parent / f"charts/{chart_id}.png"
            if png.is_file():
                document.add_picture(str(png), width=Inches(6.0))
        if section.get("note"):
            document.add_paragraph(section["note"])
    document.add_heading("Checks", level=1)
    for check in report["checks"]:
        document.add_paragraph(f"{check['status'].replace('_', ' ')}: {check['text']}", style="List Bullet")
    for note in report["notes"]:
        document.add_paragraph(note)
    inputs = ", ".join(f"{entry['name']} (uploaded {entry['uploaded']})" for entry in report["meta"]["inputs"])
    document.add_paragraph(f"Built from {inputs}. Checked by the report script.")
    document.save(str(path))


def _sheet_name(heading: str, used: set[str]) -> str:
    name = re.sub(r"[\[\]:*?/\\]", "", heading).strip()[:31] or "Sheet"
    candidate, counter = name, 2
    while candidate in used:
        suffix = f" {counter}"
        candidate = name[: 31 - len(suffix)] + suffix
        counter += 1
    used.add(candidate)
    return candidate


def render_xlsx(report: dict, path: Path) -> None:
    currency = report["meta"]["currency"]["code"]
    workbook = xlsxwriter.Workbook(str(path), {"strings_to_formulas": False, "strings_to_numbers": False, "strings_to_urls": False})
    symbol = CURRENCY_SYMBOLS.get(currency)
    money_format = f'"{symbol}"#,##0.00' if symbol else f'"{currency} "#,##0.00'
    formats = {
        "currency": workbook.add_format({"num_format": money_format}),
        "integer": workbook.add_format({"num_format": "#,##0"}),
        "number": workbook.add_format({"num_format": "#,##0.00"}),
        "percent": workbook.add_format({"num_format": '0.0"%"'}),
        "date": workbook.add_format({"num_format": "yyyy-mm-dd"}),
        "header": workbook.add_format({"bold": True, "bg_color": "#EEEEEE", "border": 1}),
        "total": workbook.add_format({"bold": True, "top": 1}),
        "total_currency": workbook.add_format({"bold": True, "top": 1, "num_format": money_format}),
        "total_integer": workbook.add_format({"bold": True, "top": 1, "num_format": "#,##0"}),
        "total_number": workbook.add_format({"bold": True, "top": 1, "num_format": "#,##0.00"}),
        "total_percent": workbook.add_format({"bold": True, "top": 1, "num_format": '0.0"%"'}),
    }
    used_names: set[str] = {"Summary", "Rows"}

    def write_cell(sheet, row: int, column: int, value, fmt: str, cell_format=None) -> None:
        if fmt == "text":
            sheet.write_string(row, column, "" if value is None else str(value), cell_format)
        elif value is None or (isinstance(value, float) and math.isnan(value)):
            sheet.write_blank(row, column, None, cell_format)
        elif fmt == "date":
            try:
                parsed = dt.datetime.fromisoformat(str(value))
            except ValueError:
                sheet.write_string(row, column, str(value), cell_format)
            else:
                sheet.write_datetime(row, column, parsed, cell_format or formats["date"])
        else:
            sheet.write_number(row, column, float(value), cell_format or formats[fmt])

    def write_table(sheet, table: dict, live_totals: bool) -> None:
        for column, heading in enumerate(table["columns"]):
            sheet.write_string(0, column, heading, formats["header"])
            sheet.set_column(column, column, max(12, min(40, len(heading) + 2)))
        for row_index, row in enumerate(table["rows"], start=1):
            for column, (value, fmt) in enumerate(zip(row, table["formats"])):
                write_cell(sheet, row_index, column, value, fmt)
        if table.get("totals"):
            total_row = len(table["rows"]) + 1
            first_data, last_data = 2, len(table["rows"]) + 1
            for column, (value, fmt) in enumerate(zip(table["totals"], table["formats"])):
                letter = xlsxwriter.utility.xl_col_to_name(column)
                if live_totals and fmt in ("currency", "integer", "number") and column > 0 and table["rows"]:
                    sheet.write_formula(total_row, column, f"=SUM({letter}{first_data}:{letter}{last_data})", formats[f"total_{fmt}"], float(value or 0))
                elif fmt == "text":
                    sheet.write_string(total_row, column, "" if value is None else str(value), formats["total"])
                else:
                    write_cell(sheet, total_row, column, value, fmt, formats.get(f"total_{fmt}", formats["total"]))
        sheet.freeze_panes(1, 0)

    summary = workbook.add_worksheet("Summary")
    rows_table = report["rows"]
    row_count = len(rows_table["rows"])
    amount_index = rows_table["formats"].index("currency") if "currency" in rows_table["formats"] else None
    amount_letter = xlsxwriter.utility.xl_col_to_name(amount_index) if amount_index is not None else None
    summary.set_column(0, 0, 28)
    summary.set_column(1, 1, 22)
    summary.write_string(0, 0, "Metric", formats["header"])
    summary.write_string(1 - 1, 1, "Value", formats["header"])
    line = 1
    kpi_cells: dict[str, int] = {}
    for kpi in report["kpis"]:
        summary.write_string(line, 0, kpi["label"])
        if kpi["id"] == "revenue" and amount_letter and row_count:
            summary.write_formula(line, 1, f"=SUM(Rows!{amount_letter}2:{amount_letter}{row_count + 1})", formats["currency"], float(kpi["value"] or 0))
        elif kpi["id"] == "jobs" and row_count:
            summary.write_formula(line, 1, f"=COUNTA(Rows!A2:A{row_count + 1})", formats["integer"], float(kpi["value"] or 0))
        elif kpi["id"] == "average_ticket" and "revenue" in kpi_cells and "jobs" in kpi_cells:
            revenue_cell, jobs_cell = f"B{kpi_cells['revenue'] + 1}", f"B{kpi_cells['jobs'] + 1}"
            summary.write_formula(line, 1, f"=IF({jobs_cell}=0,0,{revenue_cell}/{jobs_cell})", formats["currency"], float(kpi["value"] or 0))
        else:
            write_cell(summary, line, 1, kpi["value"], kpi["format"])
        kpi_cells[kpi["id"]] = line
        line += 1
    line += 1
    meta = report["meta"]
    details = (
        ("Title", meta["title"]),
        ("Company", meta["company"]),
        ("Period", meta["period"]["label"]),
        ("Generated", meta["generated_at"]),
        ("Draft", str(meta["draft"])),
        ("Inputs", ", ".join(entry["name"] for entry in meta["inputs"])),
        ("Checks", checks_line(report)),
    )
    for label, value in details:
        summary.write_string(line, 0, label)
        summary.write_string(line, 1, value)
        line += 1

    for section in report["sections"]:
        table = section.get("table")
        if not table:
            continue
        sheet = workbook.add_worksheet(_sheet_name(section["heading"], used_names))
        write_table(sheet, table, live_totals=True)
    rows_sheet = workbook.add_worksheet("Rows")
    write_table(rows_sheet, rows_table, live_totals=False)
    workbook.close()


def render(report_path: Path, target: str, out_path: Path | None, tenant_dir: str | None) -> Path:
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if report.get("version") != 1:
        raise InputError(f"{report_path.name} is not a version 1 report.")
    brand = effective_brand(report, tenant_dir)
    base = report_path.name[: -len(REPORT_SUFFIX)] if report_path.name.endswith(REPORT_SUFFIX) else report_path.stem
    path = out_path or report_path.with_name(f"{base}.{target}")
    path.parent.mkdir(parents=True, exist_ok=True)
    if target == "html":
        path.write_text(render_html(report, report_path, brand), encoding="utf-8")
    elif target == "pdf":
        render_pdf(render_html(report, report_path, brand), path)
    elif target == "docx":
        render_docx(report, report_path, brand, path)
    elif target == "xlsx":
        render_xlsx(report, path)
    else:
        raise InputError(f"Unknown render target {target!r}; use html, pdf, docx or xlsx.")
    return path


# --- inspect -----------------------------------------------------------------


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
            dates = parse_dates(frame[mapping.roles["date"]]) if mapping.roles["date"] else pd.Series(dtype="datetime64[ns]")
            suggestion = suggest_period(dates)
            valid = dates.dropna()
            columns = []
            for column in frame.columns:
                if not usable_header(column):
                    continue
                series = frame[column]
                kind = "date" if looks_like_dates(series) else "number" if looks_numeric(series) else "text"
                columns.append({"name": column, "type": kind, "sample": [to_text(value) for value in _sample(series)[:3]], "distinct": int(series.map(to_text).nunique())})
            entry = {
                "sheet": sheet_name,
                "rows": int(frame.shape[0]),
                "columns": columns,
                "mapping": mapping.roles,
                "confidence": mapping.confidence,
                "ambiguous": mapping.ambiguous,
                "missing": mapping.missing,
                "alternatives": mapping.alternatives,
                "period_suggestion": {"key": suggestion.key, "label": suggestion.label, "rows": int(_in_period(dates, suggestion).sum())} if suggestion else None,
                "date_range": {"start": valid.min().strftime("%Y-%m-%d"), "end": valid.max().strftime("%Y-%m-%d")} if len(valid) else None,
                "months": {str(key): int(value) for key, value in sorted(valid.dt.to_period("M").value_counts().items())} if len(valid) else {},
                "currency": dict(zip(("code", "source"), detect_currency(frame, mapping.roles["amount"]))),
            }
            entries.append(entry)
            if question is None:
                question = mapping_question(mapping, f"{path.name}::{sheet_name}" if sheet_name else path.name, profile)
        if question is None and len(entries) > 1:
            question = f"{path.name} has several sheets with data: {', '.join(entry['sheet'] for entry in entries)}. Which one is the export?"
        files.append({"name": path.name, "path": str(path), "sha256": sha256_of(path), "uploaded": dt.date.fromtimestamp(path.stat().st_mtime).isoformat(), "workbook": is_workbook, "tables": entries, "skipped_sheets": skipped})
    return {"profile": profile["name"], "files": files, "question": question}


# --- command line ------------------------------------------------------------


def _report_base_name(period: Period, title: str, override: str | None) -> str:
    if override:
        return _slugify(override)
    remainder = title.replace(period.label, "").strip(" -–:")
    return f"{period.key}-{_slugify(remainder or 'report')}"


def _existing_draft(path: Path) -> int:
    if not path.is_file():
        return 0
    try:
        return int(json.loads(path.read_text(encoding="utf-8"))["meta"]["draft"])
    except (ValueError, KeyError, TypeError, json.JSONDecodeError):
        return 0


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


def command_inspect(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(args.profile, tenant_dir)
    result = inspect_sources(args.files, profile)
    print(json.dumps(result, indent=2, ensure_ascii=False))
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
    ctx = prepare(args.files, args.period, options, mapping_override, profile)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    title = options.title or profile.get("title", "{period} Business Review").replace("{period}", ctx.period.label)
    base = _report_base_name(ctx.period, title, options.name)
    report_path = out_dir / f"{base}{REPORT_SUFFIX}"
    report, charts = build_report(ctx, _existing_draft(report_path))
    report["meta"]["preferences_applied"] = applied
    _write_json(out_dir / "checks.json", report["checks"])
    if any(check["status"] == "fail" for check in report["checks"]):
        failed = next(check for check in report["checks"] if check["status"] == "fail")
        sys.stderr.write(f"Report withheld: {failed['text']}\n")
        return EXIT_WITHHELD
    draw_charts(charts, out_dir, options.brand, ctx.currency)
    _write_json(report_path, report)
    print(f"Built draft {report['meta']['draft']}: {report['meta']['title']} -> {report_path}")
    print(f"Checks: {checks_line(report)}")
    if report["notes"]:
        print("Not included: " + " ".join(report["notes"]))
    if report["charts"]:
        print("Charts: " + ", ".join(chart["png"] for chart in report["charts"]))
    return EXIT_OK


def command_prose(args) -> int:
    report_path = Path(args.report)
    report = _load_json_file(str(report_path), "report")
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
    updated, removed = apply_prose(report, summary, actions)
    _write_json(report_path, updated)
    _write_json(report_path.parent / "checks.json", updated["checks"])
    print(f"Draft {updated['meta']['draft']}: {report_path}")
    if removed:
        print(f"Removed {len(removed)} {_plural(len(removed), 'number')} not in the report: {', '.join(removed)}. Sentences with them were dropped; say so to the user.")
    else:
        print("Every number in the written text matches the report.")
    return EXIT_OK


def command_render(args) -> int:
    tenant_dir = resolve_tenant_dir(args.tenant)
    path = render(Path(args.report), args.to, Path(args.out) if args.out else None, tenant_dir)
    print(f"Rendered {args.to}: {path}")
    return EXIT_OK


def command_checks(args) -> int:
    report_path = Path(args.report)
    report = _load_json_file(str(report_path), "report")
    tenant_dir = resolve_tenant_dir(args.tenant)
    profile = load_profile(report["meta"].get("profile"), tenant_dir)
    build = report["meta"]["build"]
    options = BuildOptions(exclusions=list(build.get("exclusions", [])), currency=report["meta"]["currency"]["code"] if report["meta"]["currency"]["source"] == "preferences" else None)
    ctx = prepare(args.files or build["sources"], report["meta"]["period"]["key"], options, build.get("mapping"), profile)
    rebuilt, _charts = build_report(ctx, int(report["meta"]["draft"]) - 1)
    checks = rebuilt["checks"]
    prose = [check for check in report.get("checks", []) if check["id"] == "prose_numbers"]
    checks += prose
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
    build_parser_.add_argument("--mapping", help="JSON file: {role: column} to settle a question from inspect")
    build_parser_.add_argument("--profile", help="Profile name or path (default: services-generic)")
    build_parser_.add_argument("--prefs", help="preferences.json to apply")
    build_parser_.add_argument("--tenant", help="Tenant bundle directory (default: /mnt/tenant when present)")
    build_parser_.add_argument("--exclude", action="append", help="role=value or column=value; repeatable")
    build_parser_.add_argument("--title", help="Report title (default from the profile)")
    build_parser_.add_argument("--company", help="Company name shown on the report")
    build_parser_.add_argument("--currency", help="Three-letter code when the file does not say")
    build_parser_.add_argument("--name", help="Base file name (default: <period>-<title slug>)")
    build_parser_.add_argument("--draft", type=int, help="Draft number (default: previous draft in --out plus one)")
    build_parser_.add_argument("--short", action="store_true", help="One-sentence summary")
    build_parser_.set_defaults(handler=command_build)

    prose_parser = commands.add_parser("prose", help="Put model-written summary and actions into the report; numbers are verified.")
    prose_parser.add_argument("report", help="The .report.json to update")
    prose_parser.add_argument("--from", dest="source", help='JSON file: {"summary": [paragraphs], "actions": [bullets]}')
    prose_parser.add_argument("--summary", action="append", help="A summary paragraph; repeatable")
    prose_parser.add_argument("--action", action="append", help="An action bullet; repeatable")
    prose_parser.set_defaults(handler=command_prose)

    render_parser = commands.add_parser("render", help="Render report.json to html, pdf, docx or xlsx.")
    render_parser.add_argument("report", help="The .report.json to render")
    render_parser.add_argument("--to", required=True, choices=["html", "pdf", "docx", "xlsx"])
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
