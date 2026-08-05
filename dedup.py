"""
dedup.py

负责两级去重：

1. 当前批次内去重
2. 与服务器历史分区 ID 索引进行跨批次去重

跨批次去重只读取：
    _indexes/{data_type}/year=.../month=.../day=.../stock=.../*.parquet

不会扫描新闻或评论正文。
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import pyarrow.parquet as pq


logger = logging.getLogger(__name__)


def get_data_type(record: dict[str, Any]) -> str | None:
    """
    评论记录可能同时包含 comment_id 和 news_id，
    所以必须先判断 comment_id。
    """
    if record.get("comment_id"):
        return "comments"

    if record.get("news_id"):
        return "news"

    return None


def get_record_id(record: dict[str, Any]) -> str | None:
    data_type = get_data_type(record)

    if data_type == "comments":
        value = record.get("comment_id")
    elif data_type == "news":
        value = record.get("news_id")
    else:
        value = record.get("hash")

    if value is None:
        return None

    value = str(value).strip()
    return value or None


def get_record_datetime(record: dict[str, Any]) -> datetime:
    value = record.get("publish_time")

    if isinstance(value, datetime):
        return value

    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time())

    if isinstance(value, str) and value.strip():
        try:
            return datetime.fromisoformat(
                value.strip().replace("Z", "+00:00")
            )
        except ValueError:
            pass

    # 没有发布时间的数据不应长期使用当前日期分区。
    # 第一版暂时使用 crawl_time，最后才退回当前时间。
    crawl_time = record.get("crawl_time")

    if isinstance(crawl_time, datetime):
        return crawl_time

    if isinstance(crawl_time, str) and crawl_time.strip():
        try:
            return datetime.fromisoformat(
                crawl_time.strip().replace("Z", "+00:00")
            )
        except ValueError:
            pass

    return datetime.now()


def get_partition_key(
    record: dict[str, Any],
) -> tuple[str, str, date] | None:
    data_type = get_data_type(record)
    stock_code = str(record.get("stock_code") or "").strip()

    if not data_type or not stock_code:
        return None

    record_date = get_record_datetime(record).date()
    return data_type, stock_code, record_date


def build_index_partition_path(
    cache_root: str | Path,
    data_type: str,
    stock_code: str,
    data_date: date,
) -> Path:
    return (
        Path(cache_root)
        / data_type
        / f"year={data_date.year:04d}"
        / f"month={data_date.month:02d}"
        / f"day={data_date.day:02d}"
        / f"stock={stock_code}"
    )


def deduplicate_in_batch(
    records: Iterable[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    当前一次运行内部去重。
    """
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []

    duplicate_count = 0
    invalid_count = 0

    for record in records:
        data_type = get_data_type(record)
        record_id = get_record_id(record)

        if not data_type or not record_id:
            invalid_count += 1
            logger.warning("记录缺少数据类型或唯一ID，跳过：%s", record)
            continue

        key = data_type, record_id

        if key in seen:
            duplicate_count += 1
            continue

        seen.add(key)
        result.append(record)

    stats = {
        "input": len(seen) + duplicate_count + invalid_count,
        "output": len(result),
        "batch_duplicates": duplicate_count,
        "invalid": invalid_count,
    }

    logger.info(
        "批次内去重完成：输入=%s，保留=%s，重复=%s，无效=%s",
        stats["input"],
        stats["output"],
        duplicate_count,
        invalid_count,
    )

    return result, stats


def load_existing_ids(index_partition_dir: str | Path) -> set[str]:
    """
    读取某股票某天的所有 ID 索引文件。

    索引文件只有一列：
        record_id
    """
    index_dir = Path(index_partition_dir)

    if not index_dir.exists():
        return set()

    existing_ids: set[str] = set()

    for file_path in sorted(index_dir.glob("ids-*.parquet")):
        try:
            table = pq.read_table(
                file_path,
                columns=["record_id"],
            )

            for value in table.column("record_id").to_pylist():
                if value is not None:
                    existing_ids.add(str(value))

        except Exception:
            logger.exception("读取ID索引失败：%s", file_path)
            raise

    return existing_ids


def deduplicate_cross_batch(
    records: Iterable[dict[str, Any]],
    index_cache_root: str | Path,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """
    按股票、日期、类型分组，只读取对应分区的已有ID索引。
    """
    grouped: dict[
        tuple[str, str, date],
        list[dict[str, Any]],
    ] = defaultdict(list)

    invalid_count = 0

    for record in records:
        partition_key = get_partition_key(record)

        if partition_key is None:
            invalid_count += 1
            logger.warning("无法确定记录分区，跳过：%s", record)
            continue

        grouped[partition_key].append(record)

    result: list[dict[str, Any]] = []
    historical_duplicate_count = 0

    for (data_type, stock_code, data_date), partition_records in grouped.items():
        index_dir = build_index_partition_path(
            index_cache_root,
            data_type,
            stock_code,
            data_date,
        )

        existing_ids = load_existing_ids(index_dir)

        logger.info(
            "加载历史ID：类型=%s，股票=%s，日期=%s，已有=%s",
            data_type,
            stock_code,
            data_date,
            len(existing_ids),
        )

        for record in partition_records:
            record_id = get_record_id(record)

            if not record_id:
                invalid_count += 1
                continue

            if record_id in existing_ids:
                historical_duplicate_count += 1
                continue

            # 同一分区当前新记录之间也立即加入，防止再次重复。
            existing_ids.add(record_id)
            result.append(record)

    stats = {
        "input": len(result) + historical_duplicate_count + invalid_count,
        "output": len(result),
        "historical_duplicates": historical_duplicate_count,
        "invalid": invalid_count,
    }

    logger.info(
        "跨批次去重完成：保留=%s，历史重复=%s，无效=%s",
        len(result),
        historical_duplicate_count,
        invalid_count,
    )

    return result, stats


def get_touched_partitions(
    records: Iterable[dict[str, Any]],
) -> list[tuple[str, str, date]]:
    """
    返回本次记录涉及的所有分区，供 uploader.py
    从服务器拉取相应 ID 索引。
    """
    partitions = {
        key
        for record in records
        if (key := get_partition_key(record)) is not None
    }

    return sorted(partitions)