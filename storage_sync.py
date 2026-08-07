"""
storage_sync.py


股票数据同步主程序


流程:

scanner

↓

normalizer

↓

dedup

    1. 当前批次去重

    2. 历史分区ID去重


↓

parquet_writer

↓

uploader

↓

postgres

↓

cleaner



"""


import argparse
import logging
import time
from pathlib import Path
import yaml


from scanner import scan_outputs

from normalizer import normalize_records


from dedup import (
    deduplicate_in_batch,
    deduplicate_cross_batch,
    get_touched_partitions,
)


from parquet_writer import write_parquet


from postgres import PostgreSQL


from uploader import (
    rsync_upload,
    fetch_partition_indexes,
)


from cleaner import clean_files




logger = logging.getLogger(__name__)





# =====================================================
# 主流程
# =====================================================


def run(
    files=None
):


    logging.basicConfig(

        level=logging.INFO,

        format=
        "%(asctime)s %(levelname)s %(message)s"

    )



    logger.info(
        "===== 数据同步开始 ====="
    )



    # --------------------------------
    # 读取配置
    # --------------------------------


    with open(

        "config.yaml",

        "r",

        encoding="utf-8"

    ) as f:

        config = yaml.safe_load(f)




    output_dir = config["local"]["output_dir"]


    warehouse_dir = config["local"]["warehouse_dir"]


    index_cache_dir = config["local"].get(

        "index_cache_dir",

        "../index_cache"

    )




    # --------------------------------
    # 1.扫描JSON
    # --------------------------------


    if files is None:

        files = scan_outputs(

            output_dir

        )



    if not files:


        logger.info(

            "没有发现JSON"

        )

        return




    logger.info(

        f"发现JSON:{len(files)}"

    )





    # --------------------------------
    # 2.JSON标准化
    # --------------------------------


    records = normalize_records(

        files

    )



    if not records:


        logger.warning(

            "没有有效数据"

        )

        return




    logger.info(

        f"标准化完成:{len(records)}"

    )





    # --------------------------------
    # 3.当前批次去重
    # --------------------------------


    records, batch_stats = deduplicate_in_batch(

        records

    )


    logger.info(

        f"批次去重后:{len(records)}"

    )



    if not records:


        logger.info(

            "批次内全部重复"

        )


        clean_files(

            files,

            output_dir

        )


        return





    # --------------------------------
    # 4.获取历史分区ID索引
    # --------------------------------


    partitions = get_touched_partitions(

        records

    )



    logger.info(

        f"涉及分区:{len(partitions)}"

    )



    success = fetch_partition_indexes(

        partitions,

        "config.yaml"

    )


    if not success:


        logger.error(

            "历史索引同步失败，停止执行"

        )


        return





    # --------------------------------
    # 5.跨批次去重
    # --------------------------------


    records, cross_stats = deduplicate_cross_batch(

        records,

        index_cache_dir

    )



    logger.info(

        f"跨批次去重后:{len(records)}"

    )



    if not records:


        logger.info(

            "全部数据已经存在"

        )


        clean_files(

            files,

            output_dir

        )


        return





    # --------------------------------
    # 6.生成Parquet
    # --------------------------------


    parquet_files = write_parquet(

        records,

        "config.yaml"

    )



    if not parquet_files:


        logger.warning(

            "没有生成Parquet"

        )

        return




    logger.info(

        f"生成Parquet文件:{len(parquet_files)}"

    )






    # --------------------------------
    # 7.上传服务器
    # --------------------------------


    success = rsync_upload(

        warehouse_dir,

        "config.yaml"

    )



    if not success:


        logger.error(

            "上传失败"

        )


        logger.error(

            "不写数据库，不删除JSON"

        )


        return




    logger.info(

        "服务器上传成功"

    )






    # --------------------------------
    # 8.写PostgreSQL
    # --------------------------------


    db = PostgreSQL()



    try:


        for f in parquet_files:


            db.insert_data_file(

                data_type=f["data_type"],


                stock_code=f["stock_code"],


                data_date=f["data_date"],


                file_path=f["file_path"],


                id_index_path=f["id_index_path"],


                record_count=f["record_count"],


                file_size=f["file_size"],


                file_sha256=f["file_sha256"]

            )



        logger.info(

            "PostgreSQL索引写入完成"

        )



    except Exception:


        logger.exception(

            "数据库写入失败"

        )


        raise



    finally:


        db.close()






    # --------------------------------
    # 9.清理JSON
    # --------------------------------


    if config.get("sync", {}).get("delete_after_upload", True):

        clean_files(

            files,

            output_dir

        )



        logger.info(

            "JSON清理完成"

        )



    logger.info(

        "===== 数据同步完成 ====="

    )







def get_file_size(file_item):
    try:
        return Path(file_item["file_path"]).stat().st_size
    except OSError:
        return 0


def select_ready_files(files, quiet_seconds, max_files):
    """
    只选择一段时间内没有再修改的 JSON，避免读到爬虫正在写入的半截文件。
    """
    now = time.time()
    ready = []

    for item in sorted(files, key=lambda value: value["file_path"]):
        path = Path(item["file_path"])

        try:
            stat = path.stat()
        except OSError:
            continue

        if now - stat.st_mtime < quiet_seconds:
            continue

        ready.append(item)

        if max_files and len(ready) >= max_files:
            break

    return ready


def batch_reached_threshold(files, min_files, min_bytes):
    if not files:
        return False

    total_bytes = sum(get_file_size(item) for item in files)
    return len(files) >= min_files or total_bytes >= min_bytes


def watch():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    with open("config.yaml", "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    output_dir = config["local"]["output_dir"]
    watch_config = config.get("watch", {})

    interval_seconds = int(watch_config.get("interval_seconds", 20))
    quiet_seconds = int(watch_config.get("quiet_seconds", 30))
    min_files = int(watch_config.get("min_files", 100))
    min_bytes = int(watch_config.get("min_bytes", 10 * 1024 * 1024))
    max_files = int(watch_config.get("max_files_per_batch", 5000))
    run_on_exit = bool(watch_config.get("run_on_exit", True))

    logger.info(
        "===== 持续同步启动：interval=%ss quiet=%ss min_files=%s min_bytes=%s max_files=%s =====",
        interval_seconds,
        quiet_seconds,
        min_files,
        min_bytes,
        max_files,
    )

    try:
        while True:
            files = scan_outputs(output_dir)
            ready_files = select_ready_files(
                files,
                quiet_seconds,
                max_files,
            )
            total_bytes = sum(get_file_size(item) for item in ready_files)

            logger.info(
                "扫描完成：全部JSON=%s，可处理=%s，可处理大小=%.2fMB",
                len(files),
                len(ready_files),
                total_bytes / 1024 / 1024,
            )

            if batch_reached_threshold(
                ready_files,
                min_files,
                min_bytes,
            ):
                try:
                    run(ready_files)
                except Exception:
                    logger.exception("本批同步失败，继续监控下一轮")

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("收到停止信号")

        if not run_on_exit:
            return

        files = scan_outputs(output_dir)
        ready_files = select_ready_files(
            files,
            quiet_seconds,
            max_files,
        )

        if ready_files:
            logger.info("退出前处理最后一批：%s", len(ready_files))
            run(ready_files)


def parse_args():
    parser = argparse.ArgumentParser(
        description="同步 outputs JSON 到 Parquet、服务器和 PostgreSQL。"
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="持续监控 output_dir，达到阈值后分批同步。",
    )
    return parser.parse_args()


# =====================================================
# 入口
# =====================================================


if __name__=="__main__":


    args = parse_args()

    if args.watch:
        watch()
    else:
        run()
