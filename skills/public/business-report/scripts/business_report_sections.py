"""Sections, tables and charts of the business report: from the prepared rows to the document.

Kept apart from report.py so each file stays within the skill scanner's analysis budget.
"""

from __future__ import annotations

from decimal import Decimal

import pandas as pd
from business_report_common import (
    LANG,
    MONTHS_SHORT,
    ROLE_HEADINGS,
    ROLES,
    BuildContext,
    Period,
    capitalize,
    format_value,
    in_period,
    is_are,
    is_missing,
    number,
    plural,
    previous_period,
    round_half_up,
    same_period_last_year,
    to_text,
    utc_now,
    vocab,
)

WEEK_BUCKET_LIMIT_DAYS = 62


TABLE_ROW_LIMIT = 25  # group tables list this many entries, then one "Other" row


CHART_BAR_LIMIT = 12  # bars per chart, then one "Other" bar


def _period_rows(ctx: BuildContext, period: Period) -> pd.DataFrame:
    return ctx.all_rows.loc[in_period(ctx.all_rows["date"], period)]


def _revenue(rows: pd.DataFrame) -> float:
    # Summed as decimals of each value's shortest repr, so five rows of 1.005 total 5.025, not 5.02499…
    return float(sum((Decimal(repr(value)) for value in rows["amount"].fillna(0).tolist()), Decimal(0)))


def _pct_change(current: float, previous: float) -> float | None:
    if previous == 0:
        return None
    return round_half_up((current - previous) / abs(previous) * 100, 1)


def _group_table(rows: pd.DataFrame, key: str, first_heading: str, records_heading: str, plural_noun: str, unpaid: bool, share: bool, limit: int | None = TABLE_ROW_LIMIT, other_row: bool = True) -> dict:
    """One row per distinct value, biggest revenue first, the long tail folded into one 'Other' row."""

    grouped = rows.groupby(key, sort=False, dropna=False)
    summary = pd.DataFrame({"count": grouped.size(), "revenue": grouped["amount"].sum(min_count=0)})
    summary["revenue"] = summary["revenue"].fillna(0)
    if unpaid:
        summary["unpaid"] = rows.loc[rows["status_group"] == "unpaid"].groupby(key, sort=False, dropna=False)["amount"].sum(min_count=0)
        summary["unpaid"] = summary["unpaid"].fillna(0)
    summary = summary.sort_values(["revenue", "count"], ascending=[False, False])
    total_revenue = float(summary["revenue"].sum())
    total_count = int(summary["count"].sum())
    if limit is not None and len(summary) > limit:
        head, tail = summary.iloc[:limit], summary.iloc[limit:]
        if other_row:
            other = pd.DataFrame({"count": [int(tail["count"].sum())], "revenue": [float(tail["revenue"].sum())]}, index=[f"Other ({len(tail)} {plural_noun})"])
            if unpaid:
                other["unpaid"] = [float(tail["unpaid"].sum())]
            summary = pd.concat([head, other])
        else:
            summary = head
    table_rows = []
    for name, entry in summary.iterrows():
        count = int(entry["count"])
        revenue = float(entry["revenue"])
        row = [str(name), count, number(revenue), number(revenue / count) if count else None]
        if unpaid:
            row.append(number(entry["unpaid"]))
        if share:
            row.append(number(revenue / total_revenue * 100) if total_revenue else None)
        table_rows.append(row)
    columns = [first_heading, records_heading, "Revenue", "Avg ticket"]
    formats = ["text", "integer", "currency", "currency"]
    totals = ["Total", total_count, number(total_revenue), number(total_revenue / total_count) if total_count else None]
    if unpaid:
        columns.append("Unpaid")
        formats.append("currency")
        totals.append(number(float(summary["unpaid"].sum())))
    if share:
        columns.append("Share")
        formats.append("percent")
        totals.append(100 if total_revenue else None)
    return {"columns": columns, "formats": formats, "rows": table_rows, "totals": totals, "ratios": [[3, 2, 1]]}


def _chart_bars(rows: list[list], label_index: int, value_index: int, plural_noun: str) -> tuple[list[str], list[float]]:
    """At most CHART_BAR_LIMIT bars plus one 'Other'; table rows already folded stay as they are."""

    ordered = sorted(rows, key=lambda row: row[value_index] or 0, reverse=True)
    labels = [str(row[label_index]) for row in ordered[:CHART_BAR_LIMIT]]
    values = [float(row[value_index] or 0) for row in ordered[:CHART_BAR_LIMIT]]
    tail = ordered[CHART_BAR_LIMIT:]
    if tail:
        labels.append(f"Other ({len(tail)} {plural_noun})")
        values.append(sum(float(row[value_index] or 0) for row in tail))
    return labels, values


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


def build_report(ctx: BuildContext, previous_draft: int, compute_checks) -> tuple[dict, dict]:
    """The report document and the data behind its charts."""

    profile, options, period, currency = ctx.profile, ctx.options, ctx.period, ctx.currency
    records, record = vocab(profile, "records", "rows"), vocab(profile, "record", "row")
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
        {"id": "revenue", "label": capitalize(vocab(profile, "amount", "revenue")), "value": number(revenue), "format": "currency"},
        {"id": "jobs", "label": capitalize(records), "value": count, "format": "integer"},
        {"id": "average_ticket", "label": "Avg ticket", "value": number(average), "format": "currency"},
    ]

    previous = previous_period(period)
    previous_rows = _period_rows(ctx, previous) if "previous_period" in options.comparisons else rows.iloc[0:0]
    last_year = same_period_last_year(period)
    compare_last_year = "same_period_last_year" in options.comparisons and last_year.label != previous.label
    last_year_rows = _period_rows(ctx, last_year) if compare_last_year else rows.iloc[0:0]
    if len(previous_rows):
        previous_revenue = _revenue(previous_rows)
        kpis[0]["delta"] = {"vs": previous.label, "pct": _pct_change(revenue, previous_revenue), "previous": number(previous_revenue)}
        kpis[1]["delta"] = {"vs": previous.label, "pct": _pct_change(count, len(previous_rows)), "previous": len(previous_rows)}
    elif "previous_period" in options.comparisons:
        notes.append(f"Comparison with {previous.label}: not included, the files have no rows for that period.")
    if compare_last_year and not len(last_year_rows):
        notes.append(f"Comparison with {last_year.label}: not included, the files have no rows for that period.")

    if has_status_groups:
        unpaid = float(rows.loc[rows["status_group"] == "unpaid", "amount"].fillna(0).sum())
        kpis.append({"id": "unpaid", "label": "Unpaid", "value": number(unpaid), "format": "currency"})
    cancelled = int((rows["status_group"] == "cancelled").sum()) if has_status_groups else 0
    zero_amount = int((rows["amount"].fillna(0) == 0).sum())
    included_zero_check = None
    if cancelled or zero_amount:
        parts = []
        if cancelled:
            parts.append(f"{format_value(cancelled, 'integer')} cancelled {plural(cancelled, record, records)}")
        if zero_amount:
            parts.append(f"{format_value(zero_amount, 'integer')} {plural(zero_amount, record, records)} at {format_value(0, 'currency', currency)}")
        included_zero_check = {"id": "included_zero_rows", "status": "warn", "text": f"{' and '.join(parts)} {is_are(cancelled + zero_amount)} included in the {record} count and the average ticket."}
    new_customers_text = None
    if has["customer"]:
        known = ctx.all_rows.loc[ctx.all_rows["customer"].str.strip() != ""]
        earlier = known.loc[known["date"] < pd.Timestamp(period.start)]
        active = set(rows.loc[rows["customer"].str.strip() != "", "customer"])
        customers_word = vocab(profile, "customers", "customers")
        if len(earlier) and active:
            seen_before = set(earlier["customer"])
            new = {customer for customer in active if customer not in seen_before}
            pct = round_half_up(len(new) / len(active) * 100, 1)
            kpis.append({"id": "new_customers", "label": "New customer share", "value": pct, "format": "percent"})
            customer_noun = plural(len(active), vocab(profile, "customer", "customer"), customers_word)
            new_customers_text = f"{len(new)} of {len(active)} {customer_noun} {'was' if len(new) == 1 else 'were'} new in {period.label} ({format_value(pct, 'percent')}), based on the files given."
        else:
            notes.append(f"New {customers_word}: not shown, the files have no rows earlier than {period.label} to tell new {customers_word} from returning ones.")

    sections: list[dict] = []
    charts: dict[str, dict] = {}
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
    for section_id in profile.get("sections", list(SECTION_BUILDERS)):
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
            "generated_at": utc_now(),
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
        "checks": compute_checks(ctx, rows, revenue, count, sections) + ([included_zero_check] if included_zero_check else []),
        "notes": notes,
        "rows": _rows_table(ctx, rows),
    }
    return report, charts


def _rows_table(ctx: BuildContext, rows: pd.DataFrame) -> dict:
    vocab = ctx.profile.get("vocabulary", {})
    columns, formats, keys = [], [], []
    for role in ("date", "amount", *[role for role in ROLES if role not in ("date", "amount")]):
        if role in rows.columns:
            heading = {"date": "Date", "amount": capitalize(vocab.get("amount", "amount")), "quantity": "Quantity"}.get(role) or capitalize(vocab.get(role, ROLE_HEADINGS[role]))
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
                row.append(to_text(value) if not is_missing(value) else None)
            elif fmt in ("currency", "number"):
                row.append(number(value))
            else:
                row.append(to_text(value))
        table_rows.append(row)
    return {"columns": columns, "formats": formats, "rows": table_rows, "totals": None}


def _section_summary(ctx: BuildContext, state: dict) -> dict:
    period, currency, options = ctx.period, ctx.currency, ctx.options
    records, record = state["records"], state["record"]
    rows = state["rows"]
    count = state["count"]
    sentences = [f"{period.label}: {format_value(state['revenue'], 'currency', currency)} in {vocab(ctx.profile, 'amount', 'revenue')} across {format_value(count, 'integer')} {plural(count, record, records)}"]
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
            share = round_half_up(float(by_category.iloc[0]) / state["revenue"] * 100, 1)
            sentences.append(f"{by_category.index[0]} was the largest {vocab(ctx.profile, 'category', 'category')} at {format_value(share, 'percent')} of {vocab(ctx.profile, 'amount', 'revenue')}.")
        if state["has"]["person"] and state["revenue"]:
            by_person = rows.groupby("person").agg(revenue=("amount", "sum"), count=("amount", "size")).sort_values("revenue", ascending=False)
            leader_count = int(by_person["count"].iloc[0])
            sentences.append(f"{by_person.index[0]} led with {format_value(float(by_person['revenue'].iloc[0]), 'currency', currency)} across {format_value(leader_count, 'integer')} {plural(leader_count, record, records)}.")
        if state["has_status_groups"]:
            unpaid_rows = rows.loc[rows["status_group"] == "unpaid"]
            if len(unpaid_rows):
                sentences.append(f"{format_value(_revenue(unpaid_rows), 'currency', currency)} across {format_value(len(unpaid_rows), 'integer')} {plural(len(unpaid_rows), record, records)} is unpaid.")
    return {"id": "summary", "heading": "Summary", "paragraphs": [" ".join(sentences)]}


def _section_comparison(ctx: BuildContext, state: dict) -> dict | None:
    previous_rows, last_year_rows = state["previous_rows"], state["last_year_rows"]
    if not len(previous_rows) and not len(last_year_rows):
        return None
    period = ctx.period
    columns, formats = ["Metric", period.label], ["text", "number"]
    metrics = [
        ("Revenue", state["revenue"], _revenue, "currency"),
        (capitalize(state["records"]), state["count"], len, "integer"),
        ("Average ticket", state["average"], lambda rows: _revenue(rows) / len(rows) if len(rows) else None, "currency"),
    ]
    table_rows = [[label, number(value)] for label, value, _, _ in metrics]
    row_formats = [["text", fmt] for _, _, _, fmt in metrics]
    for label, comparison_rows in ((state["previous"].label, previous_rows), (state["last_year"].label, last_year_rows)):
        if not len(comparison_rows):
            continue
        columns += [label, "Change" if label == state["previous"].label else "Change vs last year"]
        formats += ["number", "percent"]
        for row, row_format, (_, value, compute, fmt) in zip(table_rows, row_formats, metrics):
            other = compute(comparison_rows)
            row += [number(other), _pct_change(value, other) if value is not None and other not in (None, 0) else None]
            row_format += [fmt, "percent"]
    return {"id": "comparison", "heading": "Compared with earlier periods", "table": {"columns": columns, "formats": formats, "rows": table_rows, "totals": None, "row_formats": row_formats}}


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
        table_rows.append([bucket, int(len(subset)), number(revenue), number(revenue / len(subset))])
    span = (ctx.period.end - ctx.period.start).days + 1
    unit = "Week" if span <= WEEK_BUCKET_LIMIT_DAYS else "Month"
    state["charts"]["revenue_by_period"] = {
        "type": "bar",
        "x": unit.lower(),
        "y": "revenue",
        "title": f"{capitalize(vocab(ctx.profile, 'amount', 'revenue'))} by {unit.lower()}",
        "format": "currency",
        "labels": [row[0] for row in table_rows],
        "values": [row[2] or 0 for row in table_rows],
    }
    return {
        "id": "by_period",
        "heading": f"By {unit.lower()}",
        "table": {
            "columns": [unit, capitalize(state["records"]), "Revenue", "Avg ticket"],
            "formats": ["text", "integer", "currency", "currency"],
            "rows": table_rows,
            "totals": ["Total", state["count"], number(state["revenue"]), number(state["average"])],
            "ratios": [[3, 2, 1]],
        },
        "charts": ["revenue_by_period"],
    }


def _section_by_category(ctx: BuildContext, state: dict) -> dict | None:
    category = vocab(ctx.profile, "category", "category")
    categories = vocab(ctx.profile, "categories", category + "s")
    if not state["has"]["category"]:
        state["notes"].append(f"By {category}: not included, no column matched {category}.")
        return None
    table = _group_table(state["rows"], "category", capitalize(category), capitalize(state["records"]), categories, unpaid=False, share=True)
    labels, values = _chart_bars(table["rows"], 0, 2, categories)
    state["charts"]["revenue_by_category"] = {
        "type": "barh",
        "x": category,
        "y": "revenue",
        "title": f"{capitalize(vocab(ctx.profile, 'amount', 'revenue'))} by {category}",
        "format": "currency",
        "labels": labels,
        "values": values,
    }
    return {"id": "by_category", "heading": f"By {category}", "table": table, "charts": ["revenue_by_category"]}


def _section_by_person(ctx: BuildContext, state: dict) -> dict | None:
    person = vocab(ctx.profile, "person", "person")
    people = vocab(ctx.profile, "people", person + "s")
    if not state["has"]["person"]:
        state["notes"].append(f"By {person}: not included, no column matched {person}.")
        return None
    table = _group_table(state["rows"], "person", capitalize(person), capitalize(state["records"]), people, unpaid=state["has_status_groups"], share=False)
    labels, values = _chart_bars(table["rows"], 0, 1, people)
    state["charts"]["jobs_by_person"] = {
        "type": "bar",
        "x": person,
        "y": state["records"],
        "title": f"{capitalize(state['records'])} by {person}",
        "format": "integer",
        "labels": labels,
        "values": values,
    }
    return {"id": "by_person", "heading": f"By {person}", "table": table, "charts": ["jobs_by_person"]}


def _section_customers(ctx: BuildContext, state: dict) -> dict | None:
    customer = vocab(ctx.profile, "customer", "customer")
    customers = vocab(ctx.profile, "customers", customer + "s")
    if not state["has"]["customer"]:
        state["notes"].append(f"{capitalize(customers)}: not included, no column matched {customer}.")
        return None
    rows = state["rows"]
    rows = rows.assign(customer=rows["customer"].where(rows["customer"].str.strip() != "", "Unknown"))
    top_n = ctx.options.top_n or int(ctx.profile.get("top_n", 10))
    table = _group_table(rows, "customer", capitalize(customer), capitalize(state["records"]), customers, unpaid=False, share=False, limit=top_n, other_row=False)
    table["totals"] = None
    shown = len(table["rows"])
    section = {"id": "customers", "heading": f"Top {shown} {customers}" if shown > 1 else capitalize(customers), "table": table}
    if state["new_customers_text"]:
        section["paragraphs"] = [state["new_customers_text"]]
    return section


def _section_status(ctx: BuildContext, state: dict) -> dict | None:
    if not state["has"]["status"]:
        state["notes"].append("By status: not included, no column matched status.")
        return None
    rows = state["rows"]
    rows = rows.assign(status=rows["status"].where(rows["status"].str.strip() != "", "Blank"))
    table = _group_table(rows, "status", "Status", capitalize(state["records"]), "statuses", unpaid=False, share=True)
    return {"id": "status", "heading": "By status", "table": table}


def _section_source(ctx: BuildContext, state: dict) -> dict | None:
    source = vocab(ctx.profile, "source", "source")
    if not state["has"]["source"]:
        state["notes"].append(f"By {source}: not included, no column matched {source}.")
        return None
    rows = state["rows"]
    rows = rows.assign(source=rows["source"].where(rows["source"].str.strip() != "", "Unknown"))
    table = _group_table(rows, "source", capitalize(source), capitalize(state["records"]), source + "s", unpaid=False, share=True)
    return {"id": "source", "heading": f"By {source}", "table": table}


def _section_actions(ctx: BuildContext, state: dict) -> dict:
    rows, currency, records, record = state["rows"], ctx.currency, state["records"], state["record"]
    person = vocab(ctx.profile, "person", "person")
    bullets: list[str] = []
    if state["has_status_groups"]:
        unpaid_rows = rows.loc[rows["status_group"] == "unpaid"]
        if len(unpaid_rows):
            bullets.append(f"Collect the {format_value(_revenue(unpaid_rows), 'currency', currency)} still unpaid across {format_value(len(unpaid_rows), 'integer')} {plural(len(unpaid_rows), record, records)}.")
    if len(state["previous_rows"]):
        pct = _pct_change(state["revenue"], _revenue(state["previous_rows"]))
        if pct is not None and pct < 0:
            bullets.append(f"{capitalize(vocab(ctx.profile, 'amount', 'revenue'))} was {format_value(abs(pct), 'percent')} below {state['previous'].label}; check whether fewer {records} or smaller tickets drove it.")
    if state["has"]["category"] and state["revenue"]:
        by_category = rows.groupby("category")["amount"].sum().sort_values(ascending=False)
        share = round_half_up(float(by_category.iloc[0]) / state["revenue"] * 100, 1)
        if share >= 50:
            bullets.append(f"{by_category.index[0]} is {format_value(share, 'percent')} of {vocab(ctx.profile, 'amount', 'revenue')}; a slow month there moves the whole business.")
    if state["has"]["person"]:
        unassigned = int((rows["person"] == "Unassigned").sum())
        if unassigned:
            bullets.append(f"Assign the {format_value(unassigned, 'integer')} {plural(unassigned, record, records)} with no {person} so the by-{person} figures are complete.")
    if state["has"]["customer"] and state["new_customers_text"] is None:
        bullets.append(f"Add earlier months next time to see new versus returning {vocab(ctx.profile, 'customers', 'customers')}.")
    if not bullets:
        tables = [name for flag, name in ((state["has"]["category"], vocab(ctx.profile, "category", "category")), (state["has"]["person"], person)) if flag]
        if tables:
            bullets.append(f"Review the {' and '.join(tables)} {plural(len(tables), 'table')} for anything that looks off; nothing in the checks needs action.")
        else:
            bullets.append("Nothing in the checks needs action; add a column for the team or the type of work to see where the month came from.")
    return {"id": "actions", "heading": "What to act on", "bullets": bullets[:3]}


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
