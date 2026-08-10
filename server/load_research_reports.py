#!/usr/bin/env python3
"""
Load Naver research parquet files into PostgreSQL.

Example:
    python3 load_research_reports.py \
        --stocklake-root /mnt/nas-intern/homes/dwyao/Data/stocklake
"""

from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import psycopg2
from psycopg2.extras import execute_batch


INSERT_SQL = """
INSERT INTO research_reports
(
    report_type,
    report_id,
    title,
    securities_company,
    analyst,
    stock_code,
    stock_name,
    classification,
    published_date,
    views,
    investment_opinion,
    target_price,
    target_price_text,
    content,
    pdf_url,
    pdf_path,
    detail_url,
    list_url,
    crawl_time,
    parquet_path,
    schema_version
)
VALUES
(
    %(report_type)s,
    %(report_id)s,
    %(title)s,
    %(securities_company)s,
    %(analyst)s,
    %(stock_code)s,
    %(stock_name)s,
    %(classification)s,
    %(published_date)s,
    %(views)s,
    %(investment_opinion)s,
    %(target_price)s,
    %(target_price_text)s,
    %(content)s,
    %(pdf_url)s,
    %(pdf_path)s,
    %(detail_url)s,
    %(list_url)s,
    %(crawl_time)s,
    %(parquet_path)s,
    %(schema_version)s
)
ON CONFLICT (report_type, report_id)
DO UPDATE SET
    title = EXCLUDED.title,
    securities_company = EXCLUDED.securities_company,
    analyst = EXCLUDED.analyst,
    stock_code = EXCLUDED.stock_code,
    stock_name = EXCLUDED.stock_name,
    classification = EXCLUDED.classification,
    published_date = EXCLUDED.published_date,
    views = EXCLUDED.views,
    investment_opinion = EXCLUDED.investment_opinion,
    target_price = EXCLUDED.target_price,
    target_price_text = EXCLUDED.target_price_text,
    content = EXCLUDED.content,
    pdf_url = EXCLUDED.pdf_url,
    pdf_path = EXCLUDED.pdf_path,
    detail_url = EXCLUDED.detail_url,
    list_url = EXCLUDED.list_url,
    crawl_time = EXCLUDED.crawl_time,
    parquet_path = EXCLUDED.parquet_path,
    schema_version = EXCLUDED.schema_version,
    updated_at = now();
"""


def parse_date(value: Any) -> date | None:
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    return datetime.strptime(str(value), "%Y-%m-%d").date()


def parse_timestamp(value: Any) -> datetime | None:
    if value in (None, ""):
        return None

    if isinstance(value, datetime):
        return value

    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def normalize_pdf_path(
    pdf_path: Any,
    stocklake_root: Path,
) -> str | None:
    if not pdf_path:
        return None

    value = str(pdf_path)
    marker = "/research_spool/pdf/"

    if marker in value:
        relative_pdf = value.split(marker, 1)[1]
        return str(stocklake_root / "research_pdf" / relative_pdf)

    return value


def normalize_row(
    row: dict[str, Any],
    parquet_path: Path,
    stocklake_root: Path,
) -> dict[str, Any]:
    return {
        "report_type": row.get("report_type"),
        "report_id": str(row.get("report_id")),
        "title": row.get("title"),
        "securities_company": row.get("securities_company"),
        "analyst": row.get("analyst"),
        "stock_code": row.get("stock_code"),
        "stock_name": row.get("stock_name"),
        "classification": row.get("classification"),
        "published_date": parse_date(row.get("published_date")),
        "views": row.get("views"),
        "investment_opinion": row.get("investment_opinion"),
        "target_price": row.get("target_price"),
        "target_price_text": row.get("target_price_text"),
        "content": row.get("content"),
        "pdf_url": row.get("pdf_url"),
        "pdf_path": normalize_pdf_path(
            row.get("pdf_path"),
            stocklake_root,
        ),
        "detail_url": row.get("detail_url"),
        "list_url": row.get("list_url"),
        "crawl_time": parse_timestamp(row.get("crawl_time")),
        "parquet_path": str(parquet_path),
        "schema_version": row.get("schema_version"),
    }


def iter_parquet_rows(stocklake_root: Path):
    research_root = stocklake_root / "research"

    for parquet_path in sorted(research_root.rglob("*.parquet")):
        table = pq.read_table(parquet_path)

        for row in table.to_pylist():
            yield normalize_row(
                row,
                parquet_path,
                stocklake_root,
            )


def connect(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.pg_host or os.getenv("PGHOST", "localhost"),
        port=args.pg_port or os.getenv("PGPORT", "5432"),
        dbname=args.pg_database or os.getenv("PGDATABASE", "stock_data"),
        user=args.pg_user or os.getenv("PGUSER", "stock"),
        password=args.pg_password or os.getenv("PGPASSWORD"),
    )


def load_reports(args: argparse.Namespace) -> None:
    stocklake_root = Path(args.stocklake_root)
    batch: list[dict[str, Any]] = []
    inserted = 0

    with connect(args) as conn:
        with conn.cursor() as cur:
            for row in iter_parquet_rows(stocklake_root):
                if not row["report_type"] or not row["report_id"]:
                    continue

                batch.append(row)

                if len(batch) >= args.batch_size:
                    execute_batch(cur, INSERT_SQL, batch)
                    inserted += len(batch)
                    print(f"upserted {inserted} rows")
                    batch.clear()

            if batch:
                execute_batch(cur, INSERT_SQL, batch)
                inserted += len(batch)

        conn.commit()

    print(f"done, upserted {inserted} rows")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan stocklake/research parquet files and upsert PostgreSQL indexes."
    )
    parser.add_argument(
        "--stocklake-root",
        default="/mnt/nas-intern/homes/dwyao/Data/stocklake",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--pg-host")
    parser.add_argument("--pg-port")
    parser.add_argument("--pg-database")
    parser.add_argument("--pg-user")
    parser.add_argument("--pg-password")
    return parser.parse_args()


if __name__ == "__main__":
    load_reports(parse_args())
