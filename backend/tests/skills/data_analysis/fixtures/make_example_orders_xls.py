"""Regenerate ``example_orders.xls``, the legacy-format workbook the data-analysis tests load.

The backend environment has no writer for the legacy BIFF format, so the file is
committed and rebuilt on demand with xlwt:

    uv run --no-project --with xlwt python make_example_orders_xls.py example_orders.xls

The data is fictional. Sheet ``Orders`` has twelve rows, one blank amount and
one month of dates; sheet ``Payments 2026`` (a space and a numeric suffix in
its name) joins to nine of them. Paid order amounts sum to 5,345.00.
"""

from __future__ import annotations

import datetime as dt
import sys

import xlwt

ORDER_HEADERS = ["order_id", "date", "customer", "item", "amount", "rep", "status"]
ORDERS = [
    (1001, dt.date(2026, 8, 1), "Cedar Lane LLC", "Service plan", 240.0, "Ana", "paid"),
    (1002, dt.date(2026, 8, 1), "M. Okafor", "Starter kit", 1450.0, "Luis", "paid"),
    (1003, dt.date(2026, 8, 3), "Pine St Co", "Support hours", 185.0, "Ana", "paid"),
    (1004, dt.date(2026, 8, 4), "J. Weber", "Widget B", 320.5, "Luis", "open"),
    (1005, dt.date(2026, 8, 6), "Cedar Lane LLC", "Service plan", 240.0, "Sam", "paid"),
    (1006, dt.date(2026, 8, 9), "R. Ito", "Gadget", 610.0, "Sam", "paid"),
    (1007, dt.date(2026, 8, 12), "Harbor Cafe", "Widget A", 395.0, "Ana", "paid"),
    (1008, dt.date(2026, 8, 15), "M. Okafor", "Support hours", 95.0, "Luis", "cancelled"),
    (1009, dt.date(2026, 8, 18), "T. Nguyen", "Starter kit", 1390.0, "Sam", "paid"),
    (1010, dt.date(2026, 8, 22), "Pine St Co", "Widget B", None, "Ana", "open"),
    (1011, dt.date(2026, 8, 27), "J. Weber", "Service plan", 260.0, "Luis", "paid"),
    (1012, dt.date(2026, 8, 30), "Harbor Cafe", "Gadget", 575.0, "Sam", "paid"),
]
PAYMENT_HEADERS = ["payment_id", "order_id", "paid_on", "method", "amount"]
PAYMENTS = [
    (1, 1001, dt.date(2026, 8, 2), "card", 240.0),
    (2, 1002, dt.date(2026, 8, 5), "transfer", 1450.0),
    (3, 1003, dt.date(2026, 8, 3), "card", 185.0),
    (4, 1005, dt.date(2026, 8, 7), "cash", 240.0),
    (5, 1006, dt.date(2026, 8, 10), "card", 610.0),
    (6, 1007, dt.date(2026, 8, 14), "transfer", 395.0),
    (7, 1009, dt.date(2026, 8, 20), "card", 1390.0),
    (8, 1011, dt.date(2026, 8, 28), "card", 260.0),
    (9, 1012, dt.date(2026, 8, 31), "cash", 575.0),
]


def write_sheet(workbook: xlwt.Workbook, name: str, headers: list[str], rows: list[tuple]) -> None:
    date_style = xlwt.easyxf(num_format_str="YYYY-MM-DD")
    money_style = xlwt.easyxf(num_format_str="#,##0.00")
    sheet = workbook.add_sheet(name)
    for column, header in enumerate(headers):
        sheet.write(0, column, header)
    for row_index, row in enumerate(rows, start=1):
        for column, value in enumerate(row):
            if value is None:
                continue
            if isinstance(value, dt.date):
                sheet.write(row_index, column, value, date_style)
            elif isinstance(value, float):
                sheet.write(row_index, column, value, money_style)
            else:
                sheet.write(row_index, column, value)


def main(output: str) -> None:
    workbook = xlwt.Workbook(encoding="utf-8")
    write_sheet(workbook, "Orders", ORDER_HEADERS, ORDERS)
    write_sheet(workbook, "Payments 2026", PAYMENT_HEADERS, PAYMENTS)
    workbook.save(output)
    paid = sum(row[4] for row in ORDERS if row[6] == "paid")
    print(f"wrote {output}: {len(ORDERS)} orders ({paid:,.2f} paid), {len(PAYMENTS)} payments")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "example_orders.xls")
