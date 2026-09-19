"""Shared constants and helpers for the business-report skill (report.py and its renderers).

Kept separate so the analysis code and the renderers stay small enough for the
repository's skill scanner to analyse each file completely.
"""

from __future__ import annotations

import datetime as dt
import json
import math
import os
import re
from dataclasses import dataclass, field
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

import pandas as pd

MISSING_LIBRARY_MESSAGE = (
    "business-report needs pandas, python-docx, jinja2, xlsxwriter, openpyxl and matplotlib, "
    "which the sandbox image built from docker/sandbox/Dockerfile provides. "
    "This sandbox does not have them, so it is not the image this skill is built for. "
    "Do not install packages inside the sandbox; tell the user which image is running."
)

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
RENDER_TARGETS = ("html", "pdf", "docx", "xlsx")
# The formats the user is handed, in the order they are offered. HTML is the
# sheet the PDF is printed from, so it is rendered only when named outright and
# comes last wherever a run lists files.
PRESENTED_TARGETS = ("pdf", "docx", "xlsx")
TARGET_ORDER = PRESENTED_TARGETS + ("html",)
# What this skill has written beside a report, so a later draft removes its own
# renders and never a file it did not make.
RENDERS_MANIFEST = "renders.json"
CHART_PNG_PATTERN = re.compile(r"^charts/[a-z0-9_]+\.png$")
PICTURE_SUFFIXES = (".png", ".jpg", ".jpeg")

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

MAX_CELL_CHARS = 32767  # the Excel cell limit; the other renders keep the same text so the formats agree
CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


class InputError(Exception):
    """A problem with the inputs or arguments that the agent can explain to the user."""


class DecisionNeeded(Exception):
    """The mapping needs one answer from the user before the report can be built."""

    def __init__(self, question: str, details: dict):
        super().__init__(question)
        self.question = question
        self.details = details


def is_missing(value) -> bool:
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

    if is_missing(value):
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
    text = CONTROL_CHARS.sub("", str(value)).strip()
    return text[:MAX_CELL_CHARS]


def round_half_up(value, places: int) -> float:
    return float(Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP))


def number(value):
    """A JSON-safe number: int when integral, else float rounded to cents."""

    if is_missing(value):
        return None
    as_float = float(value)
    if as_float.is_integer():
        return int(as_float)
    return round_half_up(as_float, 2)


def format_value(value, fmt: str, currency: str = "USD") -> str:
    """The one formatting function every renderer uses."""

    if fmt == "text":
        return "" if value is None else to_text(value)
    if is_missing(value):
        return "—"
    if fmt == "date":
        return to_text(value)
    amount = Decimal(str(value))
    if fmt == "currency":
        symbol = CURRENCY_SYMBOLS.get(currency)
        prefix = symbol if symbol else f"{currency} "
        sign = "-" if amount < 0 else ""
        return f"{sign}{prefix}{abs(amount).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f}"
    if fmt == "integer":
        return f"{int(amount.to_integral_value(rounding=ROUND_HALF_UP)):,}"
    if fmt == "percent":
        return f"{amount.quantize(Decimal('0.1'), rounding=ROUND_HALF_UP):,.1f}%"
    if amount == amount.to_integral_value():
        return f"{int(amount):,}"
    return f"{amount.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP):,.2f}"


def axis_label(value, fmt: str, currency: str = "USD") -> str:
    """Compact tick labels for charts: whole units, no cents."""

    if fmt == "currency":
        symbol = CURRENCY_SYMBOLS.get(currency)
        prefix = symbol if symbol else f"{currency} "
        sign = "-" if value < 0 else ""
        return f"{sign}{prefix}{abs(value):,.0f}"
    if fmt == "percent":
        return f"{value:,.0f}%"
    return f"{value:,.0f}"


def plural(count: int, singular: str, plural_form: str | None = None) -> str:
    return singular if count == 1 else (plural_form or singular + "s")


def is_are(count: int) -> str:
    return "is" if count == 1 else "are"


CONTROL_CHARACTERS = re.compile(r"[\x00-\x1f\x7f]")


def one_line(text: str) -> str:
    """A line of agent-facing output, with anything that could start a new one removed.

    Cell values, column names and profile vocabulary all reach the digest, and
    the digest is read back by a model that is told to act on whole lines. A
    value carrying a newline would otherwise write its own line at column 0 and
    could forge any line the skill documents.
    """

    return CONTROL_CHARACTERS.sub(" ", text)


def slugify(text: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "report"


def capitalize(text: str) -> str:
    return text[:1].upper() + text[1:]


def is_color(value) -> bool:
    return isinstance(value, str) and re.fullmatch(r"#[0-9a-fA-F]{6}", value) is not None


def cell_format(table: dict, row_index: int | None, column_index: int) -> str:
    """The format of one cell: the row's own list when the table has one, else the column's."""

    row_formats = table.get("row_formats")
    if row_index is not None and row_formats and row_index < len(row_formats) and column_index < len(row_formats[row_index]):
        return row_formats[row_index][column_index]
    return table["formats"][column_index]


MAX_COMPANY_NAME_CHARS = 80


def load_brand(tenant_dir: str | None) -> dict:
    """brand.json from the tenant bundle, under the rules the Gateway reads it by; every field degrades on its own.

    A file that cannot be read or is not an object is the product's brand, not
    a failed report: the workspace header applies the same rule, so the name a
    report carries is the name the person sees.
    """

    brand = dict(DEFAULT_BRAND)
    if not tenant_dir:
        return brand
    path = Path(tenant_dir) / "brand.json"
    try:
        if not path.is_file():
            return brand
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return brand
    if not isinstance(data, dict):
        return brand
    name = data.get("company_name")
    if isinstance(name, str):
        name = name.strip()
        if name and len(name) <= MAX_COMPANY_NAME_CHARS and _one_plain_line(name):
            brand["company"] = name
    colors = data.get("colors") or {}
    for key in ("primary", "secondary"):
        if is_color(colors.get(key)):
            brand[key] = colors[key]
    logo = data.get("logo")
    if isinstance(logo, str) and logo:
        candidate = (Path(tenant_dir) / logo).resolve()
        if _inside(candidate, Path(tenant_dir).resolve()) and candidate.is_file() and candidate.suffix.lower() in PICTURE_SUFFIXES and os.access(candidate, os.R_OK):
            brand["logo"] = str(candidate)
    return brand


#: The Gateway's rule for the header, character for character.
_REORDERING_CHARS = frozenset("\u202a\u202b\u202c\u202d\u202e\u2066\u2067\u2068\u2069\u200b\u200e\u200f\ufeff")


def _one_plain_line(value: str) -> bool:
    """No control or bidi-reordering characters, so the name reads as what it is."""
    return not any(ord(char) < 32 or ord(char) == 127 or char in _REORDERING_CHARS for char in value)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def picture_inside(path_text: str | None, roots: list[Path]) -> Path | None:
    """A PNG or JPEG that exists under one of the roots; anything else is ignored."""

    if not isinstance(path_text, str) or not path_text:
        return None
    try:
        candidate = Path(path_text).resolve()
    except OSError:
        return None
    if candidate.suffix.lower() not in PICTURE_SUFFIXES or not candidate.is_file():
        return None
    for root in roots:
        if _inside(candidate, root.resolve()):
            return candidate
    return None


def checks_line(report: dict) -> str:
    """The one line the agent repeats: the reconciliation, then every warning or failure."""

    checks = report.get("checks", [])
    parts = [check["text"] for check in checks if check["id"] == "totals_reconcile"]
    parts += [check["text"] for check in checks if check["status"] in ("warn", "fail") and check["id"] != "totals_reconcile"]
    return " ".join(parts)


# --- shared data classes -------------------------------------------------------


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


@dataclass
class Mapping:
    roles: dict[str, str | None]
    confidence: dict[str, str]
    ambiguous: dict[str, list[str]]
    missing: list[str]
    alternatives: dict[str, list[str]]


@dataclass(frozen=True)
class Period:
    start: dt.date
    end: dt.date
    label: str
    key: str
    kind: str


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
    number_style: str | None
    date_order: str | None
    ambiguous_date_example: str | None
    excluded_rows: int
    unparsed_dates: int
    unparsed_amounts: int
    parsed_amounts: int
    exclusion_texts: list[str]


# --- periods -------------------------------------------------------------------


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
    if re.fullmatch(r"\d{4}", text):
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
    best = valid.dt.to_period("M").value_counts().idxmax()
    return month_period(best.year, best.month)


def in_period(dates: pd.Series, period: Period) -> pd.Series:
    start = pd.Timestamp(period.start)
    end = pd.Timestamp(period.end) + pd.Timedelta(days=1)
    return (dates >= start) & (dates < end)


def vocab(profile: dict, key: str, default: str) -> str:
    return str(profile.get("vocabulary", {}).get(key, default))


def utc_now() -> str:
    # dt.UTC needs Python 3.11; the sandbox image runs 3.10.
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")  # noqa: UP017
