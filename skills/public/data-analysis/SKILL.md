---
name: data-analysis
description: Use this skill when the user uploads Excel (.xlsx/.xlsm/.xls) or CSV files and wants to perform data analysis, generate statistics, create summaries, pivot tables, SQL queries, or any form of structured data exploration. Supports multi-sheet Excel workbooks, aggregation, filtering, joins, and exporting results to CSV/JSON/Markdown.
---

# Data Analysis Skill

## Overview

This skill analyzes user-uploaded Excel/CSV files using DuckDB — an in-process analytical SQL engine. It supports schema inspection, SQL-based querying, statistical summaries, and result export, all through a single Python script.

**Script paths.** `$SKILL_DIR` is this skill's own directory — the one holding this `SKILL.md`, which `describe_skill` reports as `Directory` (`Location` is the file inside it). Set `SKILL_DIR` to that directory at the start of each command that runs one of these scripts. Where a skill is mounted differs between deployments, so no absolute path can be written here.

## Core Capabilities

- Inspect Excel/CSV file structure (sheets, columns, types, row counts)
- Execute arbitrary SQL queries against uploaded data
- Generate statistical summaries (mean, median, stddev, percentiles, nulls)
- Support multi-sheet Excel workbooks (each non-empty sheet becomes a table; empty or unreadable sheets are skipped with a warning and do not appear in `inspect`)
- Export query results to CSV, JSON, or Markdown
- Handle large CSV files efficiently with DuckDB's columnar engine
- Need no network access: the script installs nothing at runtime. If it exits saying `duckdb` is missing, the sandbox image is not the one this skill is built for; tell the user and do not install packages inside the sandbox

## Workflow

### Step 1: Understand Requirements

When a user uploads data files and requests analysis, identify:

- **File location**: Path(s) to uploaded Excel/CSV files under `/mnt/user-data/uploads/`
- **Analysis goal**: What insights the user wants (summary, filtering, aggregation, comparison, etc.)
- **Output format**: How results should be presented (table, CSV export, JSON, etc.)
- You don't need to check the folder under `/mnt/user-data`

### Step 2: Inspect File Structure

First, inspect the uploaded file to understand its schema:

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/data.xlsx \
  --action inspect
```

This returns:
- Sheet names (for Excel) or filename (for CSV)
- Column names, data types, and non-null counts
- Row count per sheet/file
- Sample data (first 5 rows)

### Step 3: Perform Analysis

Based on the schema, construct SQL queries to answer the user's questions.

#### Run SQL Query

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/data.xlsx \
  --action query \
  --sql "SELECT category, COUNT(*) as count, AVG(amount) as avg_amount FROM Sheet1 GROUP BY category ORDER BY count DESC"
```

#### Generate Statistical Summary

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/data.xlsx \
  --action summary \
  --table Sheet1
```

This returns for each numeric column: count, mean, std, min, 25%, 50%, 75%, max, null_count.
For string columns: count, unique, top value, frequency, null_count.

#### Export Results

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/data.xlsx \
  --action query \
  --sql "SELECT * FROM Sheet1 WHERE amount > 1000" \
  --output-file /mnt/user-data/outputs/filtered-results.csv
```

Supported output formats (auto-detected from extension):
- `.csv` — Comma-separated values
- `.json` — JSON array of records
- `.md` — Markdown table

### Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `--files` | Yes | Space-separated paths to Excel/CSV files |
| `--action` | Yes | One of: `inspect`, `query`, `summary` |
| `--sql` | For `query` | SQL query to execute |
| `--table` | For `summary` | Table/sheet name to summarize |
| `--output-file` | No | Path to export results (CSV/JSON/MD) |

> [!NOTE]
> Do NOT read the Python file, just call it with the parameters.

## Table Naming Rules

- **Excel files**: Each sheet becomes a table named after the sheet (e.g., `Sheet1`, `Sales`, `Revenue`)
- **CSV files**: Table name is the filename without extension (e.g., `data.csv` → `data`)
- **Multiple files**: All tables from all files are available in the same query context, enabling cross-file joins
- **Special characters**: Sheet/file names with spaces or special characters are auto-sanitized (spaces → underscores). Use double quotes for names that start with numbers or contain special characters, e.g., `"2024_Sales"`

## When an Export Does Not Load Cleanly

- **The script prints `Failed to read workbook` and exits 1.** The file is not a spreadsheet, whatever its extension says. Check `head -c 8 <file>`: `<html` or `<!DOCTYPE` means the tool exported an HTML table (load it with `pandas.read_html`, save each table as CSV under `/mnt/user-data/workspace/`, then analyze the CSV); plain delimited text is CSV/TSV (copy it to a `.csv` name in the workspace). Tell the user what the file really was.
- **`inspect` shows columns named `Unnamed: 1`, `Unnamed: 2`, ...** The header row is not the first row; tool exports often start with a title. Convert that sheet first, then analyze the CSV:

```bash
python -c "import pandas as pd; pd.read_excel('/mnt/user-data/uploads/<file>', sheet_name='<sheet>', header=<row>).to_csv('/mnt/user-data/workspace/<sheet>.csv', index=False)"
```

## Analysis Patterns

### Basic Exploration
```sql
-- Row count
SELECT COUNT(*) FROM Sheet1

-- Distinct values in a column
SELECT DISTINCT category FROM Sheet1

-- Value distribution
SELECT category, COUNT(*) as cnt FROM Sheet1 GROUP BY category ORDER BY cnt DESC

-- Date range
SELECT MIN(date_col), MAX(date_col) FROM Sheet1
```

### Aggregation & Grouping
```sql
-- Revenue by category and month
SELECT category, DATE_TRUNC('month', order_date) as month,
       SUM(revenue) as total_revenue
FROM Sales
GROUP BY category, month
ORDER BY month, total_revenue DESC

-- Top 10 customers by spend
SELECT customer_name, SUM(amount) as total_spend
FROM Orders GROUP BY customer_name
ORDER BY total_spend DESC LIMIT 10
```

### Cross-file Joins
```sql
-- Join sales with customer info from different files
SELECT s.order_id, s.amount, c.customer_name, c.region
FROM sales s
JOIN customers c ON s.customer_id = c.id
WHERE s.amount > 500
```

### Window Functions
```sql
-- Running total and rank
SELECT order_date, amount,
       SUM(amount) OVER (ORDER BY order_date) as running_total,
       RANK() OVER (ORDER BY amount DESC) as amount_rank
FROM Sales
```

### Pivot-style Analysis
```sql
-- Pivot: monthly revenue by category
SELECT category,
       SUM(CASE WHEN MONTH(date) = 1 THEN revenue END) as Jan,
       SUM(CASE WHEN MONTH(date) = 2 THEN revenue END) as Feb,
       SUM(CASE WHEN MONTH(date) = 3 THEN revenue END) as Mar
FROM Sales
GROUP BY category
```

## Complete Example

User uploads `sales_2024.xlsx` (with sheets: `Orders`, `Products`, `Customers`) and asks: "Analyze my sales data — show top products by revenue and monthly trends."

### Step 1: Inspect the file

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/sales_2024.xlsx \
  --action inspect
```

### Step 2: Top products by revenue

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/sales_2024.xlsx \
  --action query \
  --sql "SELECT p.product_name, SUM(o.quantity * o.unit_price) as total_revenue, SUM(o.quantity) as total_units FROM Orders o JOIN Products p ON o.product_id = p.id GROUP BY p.product_name ORDER BY total_revenue DESC LIMIT 10"
```

### Step 3: Monthly revenue trends

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/sales_2024.xlsx \
  --action query \
  --sql "SELECT DATE_TRUNC('month', order_date) as month, SUM(quantity * unit_price) as revenue FROM Orders GROUP BY month ORDER BY month" \
  --output-file /mnt/user-data/outputs/monthly-trends.csv
```

### Step 4: Statistical summary

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/sales_2024.xlsx \
  --action summary \
  --table Orders
```

Present results to the user with clear explanations of findings, trends, and actionable insights.

## Multi-file Example

User uploads `orders.csv` and `customers.xlsx` and asks: "Which region has the highest average order value?"

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/analyze.py" \
  --files /mnt/user-data/uploads/orders.csv /mnt/user-data/uploads/customers.xlsx \
  --action query \
  --sql "SELECT c.region, AVG(o.amount) as avg_order_value, COUNT(*) as order_count FROM orders o JOIN Customers c ON o.customer_id = c.id GROUP BY c.region ORDER BY avg_order_value DESC"
```

## Output Handling

After analysis:

- Present query results directly in conversation as formatted tables
- For large results, export to file and share via `present_files` tool
- Always explain findings in plain language with key takeaways
- Suggest follow-up analyses when patterns are interesting
- Offer to export results if the user wants to keep them

## Caching

The script automatically caches loaded data to avoid re-parsing files on every call:

- On first load, files are parsed and stored in a persistent DuckDB database under `/mnt/user-data/workspace/.cache/data-analysis/` (the chat's workspace, so the cache survives a sandbox restart and stays private to the chat; `DATA_ANALYSIS_CACHE_DIR` overrides the location)
- Older caches in the same chat are removed automatically (the newest three are kept); nothing there needs cleaning up
- The cache key is a SHA256 hash of all input file contents — if files change, a new cache is created
- Subsequent calls with the same files will use the cached database directly (near-instant startup)
- Cache is transparent — no extra parameters needed

This is especially useful when running multiple queries against the same data files (inspect → query → summary).

## Notes

- DuckDB supports full SQL including window functions, CTEs, subqueries, and advanced aggregations
- Excel date columns are automatically parsed; use DuckDB date functions (`DATE_TRUNC`, `EXTRACT`, etc.)
- Legacy `.xls` workbooks are read with xlrd and `.xlsx`/`.xlsm` workbooks with openpyxl, chosen from the file's content rather than its name; each sheet is copied into DuckDB, so a whole workbook is read into memory once. The sandbox has a fixed memory limit: for a workbook of tens of MB, convert only the sheets you need to CSV in the workspace first (see the header-row note above) or ask the user for a CSV export
- For very large CSV files (100MB+), DuckDB streams them efficiently without loading everything into memory
- Column names with spaces are accessible using double quotes: `"Column Name"`
