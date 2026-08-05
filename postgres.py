"""
postgres.py

PostgreSQL 数据访问层

负责：
1. PostgreSQL连接
2. 数据文件索引写入
3. 新闻索引写入
4. 评论索引写入
5. 同步日志

适配：
PostgreSQL 15+

"""

import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_batch
from contextlib import contextmanager
import yaml
import os
import logging


logger = logging.getLogger(__name__)


# =====================================================
# 读取配置
# =====================================================

def load_config(config_path="config.yaml"):

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"找不到配置文件: {config_path}"
        )

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as f:

        return yaml.safe_load(f)



# =====================================================
# PostgreSQL连接池
# =====================================================

class PostgreSQL:


    def __init__(self, config_path="config.yaml"):


        config = load_config(config_path)


        pg = config["postgres"]


        self.pool = pool.SimpleConnectionPool(

            1,

            10,

            host=pg["host"],

            port=pg["port"],

            database=pg["database"],

            user=pg["user"],

            password=pg["password"]

        )


        logger.info(
            "PostgreSQL连接池创建成功"
        )



    # ---------------------------------
    # 获取连接
    # ---------------------------------

    @contextmanager
    def connection(self):

        conn = self.pool.getconn()


        try:

            yield conn


        finally:

            self.pool.putconn(conn)



    # ---------------------------------
    # 执行SQL
    # ---------------------------------

    def execute(
        self,
        sql,
        params=None
    ):


        with self.connection() as conn:


            with conn.cursor() as cur:


                cur.execute(
                    sql,
                    params
                )


            conn.commit()



    # =================================================
    # stocks
    # =================================================


    def insert_stock(
        self,
        code,
        name,
        market
    ):


        sql = """

        INSERT INTO stocks
        (
            code,
            name,
            market
        )

        VALUES
        (
            %s,%s,%s
        )

        ON CONFLICT(code)
        DO UPDATE SET

        name=EXCLUDED.name,

        market=EXCLUDED.market;

        """


        self.execute(
            sql,
            (
                code,
                name,
                market
            )
        )



    # =================================================
    # data_files
    # =================================================


    def insert_data_file(
        self,
        data_type,
        stock_code,
        data_date,
        file_path,
        id_index_path,
        record_count,
        file_size,
        file_sha256,
    ):
        sql = """
        INSERT INTO data_files
        (
            data_type,
            stock_code,
            data_date,
            file_path,
            id_index_path,
            record_count,
            file_size,
            file_sha256
        )
        VALUES
        (
            %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (file_path)
        DO NOTHING
        RETURNING id;
        """

        with self.connection() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        sql,
                        (
                            data_type,
                            stock_code,
                            data_date,
                            file_path,
                            id_index_path,
                            record_count,
                            file_size,
                            file_sha256,
                        ),
                    )

                    row = cur.fetchone()

                conn.commit()
                return row[0] if row else None

            except Exception:
                conn.rollback()
                raise


    # =================================================
    # 批量写入新闻索引
    # =================================================


    def batch_insert_news(
        self,
        records
    ):


        """
        records:

        [
          {
          news_id:"",
          stock_code:"",
          publish_time:"",
          title:"",
          file_id:"",
          row_number:1
          }
        ]

        """


        if not records:
            return



        sql = """

        INSERT INTO news_index
        (
            news_id,
            stock_code,
            publish_time,
            title,
            file_id,
            row_number
        )

        VALUES
        (
            %s,%s,%s,%s,%s,%s
        )


        ON CONFLICT(news_id)
        DO NOTHING;


        """



        values = [

            (

                r["news_id"],

                r["stock_code"],

                r["publish_time"],

                r["title"],

                r["file_id"],

                r["row_number"]

            )

            for r in records

        ]



        with self.connection() as conn:


            with conn.cursor() as cur:


                execute_batch(
                    cur,
                    sql,
                    values,
                    page_size=5000
                )


            conn.commit()



    # =================================================
    # 批量写入评论索引
    # =================================================


    def batch_insert_comments(
        self,
        records
    ):


        if not records:
            return



        sql = """

        INSERT INTO comments_index
        (
            comment_id,
            news_id,
            stock_code,
            publish_time,
            file_id,
            row_number
        )

        VALUES
        (
            %s,%s,%s,%s,%s,%s
        )


        ON CONFLICT(comment_id)
        DO NOTHING;


        """



        values=[

            (

                r["comment_id"],

                r["news_id"],

                r["stock_code"],

                r["publish_time"],

                r["file_id"],

                r["row_number"]

            )

            for r in records

        ]



        with self.connection() as conn:


            with conn.cursor() as cur:


                execute_batch(

                    cur,

                    sql,

                    values,

                    page_size=10000

                )


            conn.commit()



    # =================================================
    # 同步日志
    # =================================================


    def insert_sync_log(
        self,
        filename,
        md5,
        status
    ):


        sql="""

        INSERT INTO sync_log
        (
            filename,
            md5,
            status
        )

        VALUES
        (
            %s,%s,%s
        );

        """

        self.execute(
            sql,
            (
                filename,
                md5,
                status
            )
        )



    # =================================================
    # 关闭连接池
    # =================================================


    def close(self):

        self.pool.closeall()



# =====================================================
# 测试
# =====================================================

if __name__ == "__main__":


    logging.basicConfig(
        level=logging.INFO
    )


    db = PostgreSQL()


    print(
        "PostgreSQL连接成功"
    )


    db.insert_stock(

        "005930",

        "삼성전자",

        "KOSPI"

    )


    print(
        "测试写入完成"
    )


    db.close()
