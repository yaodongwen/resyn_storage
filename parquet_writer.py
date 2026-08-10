"""
parquet_writer.py

把标准化且去重后的记录写成：

1. 正文 Parquet
2. 对应的 ID 索引 Parquet

正文：
comments/year=.../month=.../day=.../stock=.../part-<token>.parquet

ID索引：
_indexes/comments/year=.../month=.../day=.../stock=.../ids-<token>.parquet
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

try:
    from tqdm import tqdm
except ImportError:
    def tqdm(iterable, **kwargs):
        return iterable

from dedup import (
    get_data_type,
    get_partition_key,
    get_record_id,
)


logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def create_file_token() -> str:
    """
    示例：
    20260805T063202123456Z-a1b2c3d4e5f6
    """
    timestamp = datetime.now(timezone.utc).strftime(
        "%Y%m%dT%H%M%S%fZ"
    )
    random_part = uuid4().hex[:12]
    return f"{timestamp}-{random_part}"


def build_partition_relative_path(
    data_type: str,
    stock_code: str,
    data_date: date,
) -> Path:
    return (
        Path(data_type)
        / f"year={data_date.year:04d}"
        / f"month={data_date.month:02d}"
        / f"day={data_date.day:02d}"
        / f"stock={stock_code}"
    )


def build_index_relative_path(
    data_type: str,
    stock_code: str,
    data_date: date,
) -> Path:
    return (
        Path("_indexes")
        / data_type
        / f"year={data_date.year:04d}"
        / f"month={data_date.month:02d}"
        / f"day={data_date.day:02d}"
        / f"stock={stock_code}"
    )


def calculate_sha256(file_path: str | Path) -> str:
    digest = hashlib.sha256()

    with Path(file_path).open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def normalize_arrow_values(
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    删除内部辅助字段，避免写入 Parquet。
    """
    clean_records: list[dict[str, Any]] = []

    for record in records:
        clean_record = {
            key: value
            for key, value in record.items()
            if not key.startswith("_")
        }
        clean_records.append(clean_record)

    return clean_records


def write_data_file(
    records: list[dict[str, Any]],
    output_path: Path,
    compression: str,
) -> None:
    clean_records = normalize_arrow_values(records)
    table = pa.Table.from_pylist(clean_records)

    pq.write_table(
        table,
        output_path,
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
    )


def write_id_index_file(
    records: list[dict[str, Any]],
    output_path: Path,
    compression: str,
) -> None:
    record_ids = []

    for record in records:
        record_id = get_record_id(record)

        if record_id:
            record_ids.append(record_id)

    # 只保存一列，文件会非常小。
    table = pa.table(
        {
            "record_id": pa.array(
                record_ids,
                type=pa.string(),
            )
        }
    )

    pq.write_table(
        table,
        output_path,
        compression=compression,
        use_dictionary=True,
        write_statistics=True,
    )


def write_partition_batches(
    records: list[dict[str, Any]],
    local_warehouse: Path,
    data_type: str,
    stock_code: str,
    data_date: date,
    max_rows: int,
    compression: str,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []

    data_relative_dir = build_partition_relative_path(
        data_type,
        stock_code,
        data_date,
    )

    index_relative_dir = build_index_relative_path(
        data_type,
        stock_code,
        data_date,
    )

    data_local_dir = local_warehouse / data_relative_dir
    index_local_dir = local_warehouse / index_relative_dir

    data_local_dir.mkdir(parents=True, exist_ok=True)
    index_local_dir.mkdir(parents=True, exist_ok=True)

    total_batches = (len(records) + max_rows - 1) // max_rows

    for start in tqdm(
        range(0, len(records), max_rows),
        total=total_batches,
        desc=f"写Parquet {data_type}/{stock_code}/{data_date}",
        unit="file",
    ):
        batch = records[start : start + max_rows]
        token = create_file_token()

        data_filename = f"part-{token}.parquet"
        index_filename = f"ids-{token}.parquet"

        data_relative_path = data_relative_dir / data_filename
        index_relative_path = index_relative_dir / index_filename

        data_local_path = local_warehouse / data_relative_path
        index_local_path = local_warehouse / index_relative_path

        write_data_file(
            batch,
            data_local_path,
            compression,
        )

        write_id_index_file(
            batch,
            index_local_path,
            compression,
        )

        metadata = {
            "data_type": data_type,
            "stock_code": stock_code,
            "data_date": data_date.isoformat(),

            # PostgreSQL保存这些相对路径。
            "file_path": data_relative_path.as_posix(),
            "id_index_path": index_relative_path.as_posix(),

            # 上传程序使用这些本地路径。
            "local_file_path": str(data_local_path),
            "local_id_index_path": str(index_local_path),

            "record_count": len(batch),
            "file_size": data_local_path.stat().st_size,
            "file_sha256": calculate_sha256(data_local_path),
        }

        results.append(metadata)

        logger.info(
            "生成Parquet：%s，记录=%s",
            data_relative_path,
            len(batch),
        )

    return results


def write_parquet(
    records: list[dict[str, Any]],
    config_path: str = "config.yaml",
) -> list[dict[str, Any]]:
    if not records:
        return []

    config = load_config(config_path)

    local_warehouse = Path(
        config["local"]["warehouse_dir"]
    )

    max_rows = int(
        config.get("storage", {})
        .get("partition", {})
        .get("max_rows", 50000)
    )

    compression = (
        config.get("storage", {})
        .get("compression", {})
        .get("codec", "zstd")
    )

    grouped: dict[
        tuple[str, str, date],
        list[dict[str, Any]],
    ] = defaultdict(list)

    for record in records:
        partition_key = get_partition_key(record)

        if partition_key is None:
            logger.warning("无法识别记录分区，跳过：%s", record)
            continue

        grouped[partition_key].append(record)

    results: list[dict[str, Any]] = []

    for (
        data_type,
        stock_code,
        data_date,
    ), partition_records in tqdm(
        grouped.items(),
        desc="处理Parquet分区",
        unit="partition",
    ):
        results.extend(
            write_partition_batches(
                records=partition_records,
                local_warehouse=local_warehouse,
                data_type=data_type,
                stock_code=stock_code,
                data_date=data_date,
                max_rows=max_rows,
                compression=compression,
            )
        )

    logger.info(
        "Parquet写入完成：文件=%s，记录=%s",
        len(results),
        sum(item["record_count"] for item in results),
    )

    return results
