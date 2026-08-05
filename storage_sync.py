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


import logging
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


def run():


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







# =====================================================
# 入口
# =====================================================


if __name__=="__main__":


    run()
