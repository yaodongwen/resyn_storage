#!/usr/bin/env python3
"""
Retrieve Naver research report indexes from PostgreSQL.

Examples:
    python retrieve/retrieve_research_reports.py --date 2026-08-07
    python retrieve/retrieve_research_reports.py --date 2026-08-07 --types company market
    python retrieve/retrieve_research_reports.py --date 2026-08-07 --format csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from datetime import date, datetime
from decimal import Decimal
from typing import Any

import psycopg2
from psycopg2.extras import RealDictCursor


VALID_REPORT_TYPES = {
    "company",
    "debenture",
    "economy",
    "industry",
    "invest",
    "market",
}

REPORT_TYPE_ALIASES = {
    "inverst": "invest",
}

DEFAULT_COLUMNS = [
    "report_type",
    "report_id",
    "title",
    "securities_company",
    "analyst",
    "stock_code",
    "stock_name",
    "published_date",
    "views",
    "investment_opinion",
    "target_price",
    "target_price_text",
    "pdf_url",
    "pdf_path",
    "detail_url",
    "parquet_path",
]


def normalize_report_types(report_types: list[str] | None) -> list[str]:
    if not report_types:
        return sorted(VALID_REPORT_TYPES)

    normalized = []

    for report_type in report_types:
        value = REPORT_TYPE_ALIASES.get(
            report_type.lower(),
            report_type.lower(),
        )

        if value not in VALID_REPORT_TYPES:
            valid = ", ".join(sorted(VALID_REPORT_TYPES))
            raise ValueError(f"不支持的 report_type: {report_type}; 可选: {valid}")

        normalized.append(value)

    return sorted(set(normalized))


def parse_query_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def connect(args: argparse.Namespace):
    return psycopg2.connect(
        host=args.pg_host or os.getenv("PGHOST", "localhost"),
        port=args.pg_port or os.getenv("PGPORT", "5432"),
        dbname=args.pg_database or os.getenv("PGDATABASE", "stock_data"),
        user=args.pg_user or os.getenv("PGUSER", "stock"),
        password=args.pg_password or os.getenv("PGPASSWORD"),
    )


def fetch_reports_by_date(
    conn,
    query_date: date,
    report_types: list[str] | None = None,
    include_content: bool = False,
) -> list[dict[str, Any]]:
    columns = DEFAULT_COLUMNS.copy()

    if include_content:
        columns.append("content")

    report_types = normalize_report_types(report_types)
    sql = f"""
        SELECT {", ".join(columns)}
        FROM research_reports
        WHERE published_date = %s
          AND report_type = ANY(%s)
        ORDER BY report_type, published_date DESC, report_id;
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cursor:
        cursor.execute(sql, (query_date, report_types))
        return [dict(row) for row in cursor.fetchall()]


def json_default(value: Any):
    if isinstance(value, (date, datetime)):
        return value.isoformat()

    if isinstance(value, Decimal):
        return float(value)

    return str(value)


def print_json(rows: list[dict[str, Any]]) -> None:
    print(
        json.dumps(
            rows,
            ensure_ascii=False,
            indent=2,
            default=json_default,
        )
    )


def print_csv(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return

    writer = csv.DictWriter(
        sys.stdout,
        fieldnames=list(rows[0].keys()),
    )
    writer.writeheader()

    for row in rows:
        writer.writerow(
            {
                key: json_default(value) if value is not None else ""
                for key, value in row.items()
            }
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="查询某一天的 Naver research 报告索引。"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="发布日期，格式 YYYY-MM-DD。",
    )
    parser.add_argument(
        "--types",
        nargs="+",
        help="报告类型：company debenture economy industry invest market。",
    )
    parser.add_argument(
        "--include-content",
        action="store_true",
        help="返回正文 content 字段。",
    )
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="输出格式。",
    )
    parser.add_argument("--pg-host")
    parser.add_argument("--pg-port")
    parser.add_argument("--pg-database")
    parser.add_argument("--pg-user")
    parser.add_argument("--pg-password")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    with connect(args) as conn:
        rows = fetch_reports_by_date(
            conn,
            parse_query_date(args.date),
            args.types,
            include_content=args.include_content,
        )

    if args.format == "csv":
        print_csv(rows)
    else:
        print_json(rows)


if __name__ == "__main__":
    main()
