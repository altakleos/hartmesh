"""Preview visible spreadsheet cells in workbook order using stored values."""

import re
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation

from .package import MAX_MEMBERS, OfficePackage
from .view import View

_CELL = re.compile(r"([A-Z]{1,3})([1-9][0-9]{0,6})")
_MAX_ROWS = 200
_MAX_COLS = 12


def _styles(package, path):
    formats = {}
    styles = []
    in_formats = False
    for event, tag, data in package.events(path):
        if event == "start" and tag == "x:numFmt":
            if len(formats) >= MAX_MEMBERS:
                raise ValueError("Too many number formats")
            formats[data["numFmtId"]] = data.get("formatCode", "")[:512]
        elif tag == "x:cellXfs":
            in_formats = event == "start"
        elif event == "start" and in_formats and tag == "x:xf":
            if len(styles) >= MAX_MEMBERS:
                raise ValueError("Too many cell formats")
            num = int(data.get("numFmtId", "0"))
            pattern = formats.get(str(num), "").split(";")[0]
            pattern = re.sub(r'"[^"]*"|\\.', "", pattern.lower())
            elapsed = re.search(r"\[([hms])\1?\]", pattern)
            pattern = re.sub(r"\[[^\]]*\]", "", pattern)
            if "%" in pattern or num in (9, 10):
                styles.append("percent")
            elif elapsed or num == 46:
                styles.append("duration")
            elif num == 22 or (re.search(r"[yd]", pattern) and re.search(r"[hs]", pattern)):
                styles.append("datetime")
            elif 18 <= num <= 21 or num in {45, 47} or re.search(r"[hs]", pattern):
                styles.append("time")
            elif 14 <= num <= 17 or 27 <= num <= 36 or re.search(r"[ymd]", pattern):
                styles.append("date")
            else:
                styles.append("")
    return styles


def _format_number(value, style, date1904):
    try:
        number = Decimal(value)
        if not number.is_finite() or abs(number.adjusted()) > 308:
            return value
        if style == "percent":
            return format((number * 100).normalize(), "f") + "%"
        if style in {"duration", "time"}:
            seconds = int((abs(number) * 86400).to_integral_value())
            if style == "time":
                seconds %= 86400
            hours, remainder = divmod(seconds, 3600)
            minutes, seconds = divmod(remainder, 60)
            return ("-" if number < 0 else "") + f"{hours:02}:{minutes:02}:{seconds:02}"
        if style in {"date", "datetime"}:
            epoch = datetime(1904, 1, 1) if date1904 else datetime(1899, 12, 30 if number >= 60 else 31)
            result = epoch + timedelta(days=float(number))
            calendar_day = result.date().isoformat()
            if not date1904 and 60 <= number < 61:
                calendar_day = "1900-02-29"  # The 1900 date system includes this fictitious day.
            return calendar_day + " " + result.time().isoformat(timespec="seconds") if style == "datetime" else calendar_day
    except (OverflowError, ValueError, InvalidOperation):
        pass
    return value


def _sheet(package, part, view):
    rows = []
    row = col = 0
    hidden_columns = set()
    hidden_row = False
    cell = None
    field = None
    phonetic = False
    root_seen = False
    for event, tag, data in package.events(part):
        if event == "text":
            if cell is not None and field is not None:
                cell[field] = view.text(cell[field] + data)
            continue
        if event == "start":
            if not root_seen:
                if tag != "x:worksheet":
                    raise ValueError("Expected a worksheet")
                root_seen = True
            if tag == "x:col" and data.get("hidden") in {"true", "1"}:
                hidden_columns.update(range(max(1, int(data["min"])), min(12, int(data["max"])) + 1))
                view.notices.add("Hidden columns were left out.")
            elif tag == "x:row":
                row, col = int(data.get("r", row + 1)), 0
                hidden_row = data.get("hidden") in {"true", "1"}
                if hidden_row:
                    view.notices.add("Hidden rows were left out.")
                if row > _MAX_ROWS:
                    view.notices.add("Only the first 200 rows are shown.")
                    break
            elif tag == "x:c":
                ref = data.get("r")
                col += 1
                if ref:
                    match = _CELL.fullmatch(ref)
                    if match is None or int(match[2]) != row:
                        raise ValueError("Invalid cell reference")
                    col = 0
                    for letter in match[1]:
                        col = col * 26 + ord(letter) - 64
                if col > _MAX_COLS:
                    view.notices.add("Only the first 12 columns are shown.")
                elif 1 <= row <= _MAX_ROWS and not hidden_row and col not in hidden_columns:
                    cell = {"type": data.get("t", "n"), "style": int(data.get("s", 0)), "value": "", "formula": "", "inline": ""}
            elif tag == "x:rPh":
                phonetic = True
            elif tag in {"x:f", "x:v", "x:t"} and not phonetic:
                field = {"x:f": "formula", "x:v": "value", "x:t": "inline"}[tag]
            elif tag == "x:mergeCell":
                view.notices.add("Merged cells show their stored value in the original cell.")
        elif tag == "x:rPh":
            phonetic = False
        elif tag in {"x:f", "x:v", "x:t"}:
            field = None
        elif tag == "x:c" and cell is not None:
            while len(rows) < row:
                rows.append([])
            while len(rows[row - 1]) < col:
                rows[row - 1].append(None)
            rows[row - 1][col - 1] = cell
            cell = None
    return rows


def _shared(package, part, wanted, view):
    values = {}
    index = -1
    keep = capture = False
    phonetic = False
    for event, tag, data in package.events(part):
        if event == "start" and tag == "x:si":
            index += 1
            keep = index in wanted
            if keep:
                values[index] = ""
        elif tag == "x:rPh":
            phonetic = event == "start"
        elif tag == "x:t":
            capture = event == "start" and keep and not phonetic
        elif event == "text" and capture:
            values[index] = view.text(values[index] + data)
        elif event == "end" and tag == "x:si" and len(values) == len(wanted):
            break
    return values


def _display(cell, shared, styles, date1904, view):
    if cell is None:
        return ""
    kind, value, formula = cell["type"], cell["value"], cell["formula"]
    if formula and not value:
        view.notices.add("Some cells show formulas because the file has no stored results.")
        return "=" + formula
    if kind == "s":
        index = int(value)
        if index not in shared:
            raise ValueError("Missing shared string")
        return shared[index]
    if kind == "inlineStr":
        return cell["inline"]
    if kind == "b":
        return {"1": "TRUE", "0": "FALSE"}.get(value, value)
    index = cell["style"]
    if kind == "n" and value and 0 <= index < len(styles) and styles[index]:
        return _format_number(value, styles[index], date1904)
    return value


def project_workbook(source: bytes) -> bytes:
    package = OfficePackage(source)
    view = View("Workbook", package.notices)
    main = package.main_part()
    rels = package.relationships(main)
    styles = []
    strings_part = None
    for kind, part in rels.values():
        if kind == "styles":
            styles = _styles(package, part)
        if kind == "sharedStrings":
            strings_part = part
    sheets = []
    date1904 = False
    cells_left = view.cells_left
    root_seen = False
    wanted = set()
    for event, tag, data in package.events(main):
        if event != "start":
            continue
        if not root_seen:
            if tag != "x:workbook":
                raise ValueError("Expected a workbook")
            root_seen = True
        if tag == "x:workbookPr":
            date1904 = data.get("date1904") in {"true", "1"}
        if tag != "x:sheet":
            continue
        if data.get("state", "visible") != "visible":
            view.notices.add("Hidden sheets were left out.")
            continue
        kind, part = rels[data["r:id"]]
        if kind != "worksheet":
            view.notices.add("Non-worksheet sheets were left out.")
            continue
        if cells_left <= 0 or len(sheets) >= 63:
            view.notices.add("Additional sheets were left out.")
            break
        rows = _sheet(package, part, view)
        width = max((len(row) for row in rows), default=0)
        if not width:
            rows = [[None]]
            width = 1
        count = max(0, cells_left // width - 1)
        if len(rows) > count:
            view.notices.add("Additional table rows were left out.")
            rows = rows[:count]
        cells_left -= width * (len(rows) + 1)
        for row in rows:
            wanted.update(int(cell["value"]) for cell in row if cell and cell["type"] == "s")
        sheets.append((data.get("name", "Unnamed sheet"), rows))
    shared = _shared(package, strings_part, wanted, view) if strings_part and wanted else {}
    for name, rows in sheets:
        values = [[_display(cell, shared, styles, date1904, view) for cell in row] for row in rows]
        if not view.table(values, "Sheet: " + name):
            break
    view.notices.add("Pictures, charts, embedded objects and workbook layout are outside this preview.")
    return view.finish()
