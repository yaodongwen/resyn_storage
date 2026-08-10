# Retrieve Research Reports

Query one day's Naver research report indexes from PostgreSQL.

## Command Line

All report types for one day:

```bash
PGPASSWORD='Stock2026Secure!' python3 retrieve_research_reports.py \
  --date 2026-08-07 \
  --pg-host localhost \
  --pg-database stock_data \
  --pg-user stock
```

Specific report types:

```bash
PGPASSWORD='Stock2026Secure!' python3 retrieve_research_reports.py \
  --date 2026-08-07 \
  --types company debenture economy industry invest market \
  --pg-host localhost \
  --pg-database stock_data \
  --pg-user stock
```

CSV output:

```bash
PGPASSWORD='Stock2026Secure!' python3 retrieve_research_reports.py \
  --date 2026-08-07 \
  --types company market \
  --format csv
```

## Python

```python
from datetime import date
import psycopg2

from retrieve_research_reports import fetch_reports_by_date

conn = psycopg2.connect(
    host="localhost",
    dbname="stock_data",
    user="stock",
    password="Stock2026Secure!",
)

reports = fetch_reports_by_date(
    conn,
    date(2026, 8, 7),
    ["company", "market"],
)

print(reports)
```
