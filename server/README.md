# Server Scripts

These files are meant to be copied to the server.

## Research Reports

Create or update the PostgreSQL table:

```bash
psql -U stock -d stock_data -f research_schema.sql
```

Load indexes from uploaded Parquet files:

```bash
PGPASSWORD='Stock2026Secure!' python3 load_research_reports.py \
  --stocklake-root /mnt/nas-intern/homes/dwyao/Data/stocklake \
  --pg-host localhost \
  --pg-database stock_data \
  --pg-user stock
```

The loader scans:

```text
/mnt/nas-intern/homes/dwyao/Data/stocklake/research/**/*.parquet
```

It also rewrites local PDF paths from the crawler into server paths under:

```text
/mnt/nas-intern/homes/dwyao/Data/stocklake/research_pdf/
```
