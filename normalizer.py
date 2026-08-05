"""
normalizer.py


负责：

JSON原始数据

        ↓

标准数据结构


支持：

news

comments


"""

import json
import hashlib
from pathlib import Path
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


# =====================================================
# hash生成
# =====================================================

def make_hash(text):

    if text is None:

        text = ""


    return hashlib.sha256(

        str(text).encode(
            "utf-8"
        )

    ).hexdigest()



# =====================================================
# 时间标准化
# =====================================================

def normalize_time(value):

    """
    不同来源时间统一

    """

    if not value:

        return None


    if isinstance(value, datetime):

        return value



    formats = [

        "%Y-%m-%dT%H:%M:%S",

        "%Y-%m-%d %H:%M:%S",

        "%Y.%m.%d %H:%M:%S",

        "%Y-%m-%d",

        "%Y/%m/%d %H:%M:%S"

    ]


    for fmt in formats:

        try:

            return datetime.strptime(
                value,
                fmt
            )

        except:

            pass



    # 无法解析
    return None



# =====================================================
# 新闻标准化
# =====================================================


def normalize_news(
    raw,
    stock_code,
    stock_name
):


    news_id = (

        raw.get("nid")

        or

        raw.get("news_id")

        or

        raw.get("id")

    )


    title = (

        raw.get("title")

        or

        raw.get("subject")

        or ""

    )


    content = (

        raw.get("content")

        or

        raw.get("body")

        or ""

    )



    publish_time = normalize_time(

        raw.get("publish_time")

        or

        raw.get("time")

        or

        raw.get("date")

    )



    return {


        "news_id":
            str(news_id),


        "stock_code":
            stock_code,


        "stock_name":
            stock_name,


        "source":
            "naver",


        "publish_time":
            publish_time,


        "title":
            title,


        "content":
            content,


        "url":
            raw.get("url",""),


        "crawl_time":
            datetime.now(),


        "hash":
            make_hash(

                str(news_id)

                +

                title

            )

    }



# =====================================================
# 评论标准化
# =====================================================


def normalize_comment(
    raw,
    stock_code,
    stock_name
):

    """
    Naver股票讨论评论标准化

    原始:

    {
      code,
      nid,
      title,
      content,
      written_at,
      nickname
    }

    """

    comment_id = str(
        raw.get("nid", "")
    )


    # 股票讨论区没有新闻ID
    news_id = None


    content = raw.get(
        "content",
        ""
    )


    title = raw.get(
        "title",
        ""
    )


    publish_time = normalize_time(
        raw.get(
            "written_at"
        )
    )


    return {

        "comment_id":
            comment_id,


        "news_id":
            news_id,


        "stock_code":
            raw.get(
                "code",
                stock_code
            ),


        "stock_name":
            stock_name,


        "source":
            "naver_board",


        "publish_time":
            publish_time,


        "user_id":
            raw.get(
                "nickname",
                ""
            ),


        "title":
            title,


        "content":
            content,


        "views":
            int(
                raw.get(
                    "view_count",
                    0
                )
            ),


        "likes":
            int(
                raw.get(
                    "recommend_count",
                    0
                )
            ),


        "dislikes":
            int(
                raw.get(
                    "not_recommend_count",
                    0
                )
            ),


        "url":
            raw.get(
                "url",
                ""
            ),


        "detail_url":
            raw.get(
                "detail_url",
                ""
            ),


        "crawl_time":
            datetime.now(),


        "hash":
            make_hash(

                comment_id

                +

                content

            )

    }


# =====================================================
# 单文件处理
# =====================================================


def normalize_file(

    item

):


    path = Path(

        item["file_path"]

    )


    with open(

        path,

        "r",

        encoding="utf-8"

    ) as f:


        raw=json.load(f)



    if item["data_type"]=="news":


        return normalize_news(

            raw,

            item["stock_code"],

            item["stock_name"]

        )



    elif item["data_type"]=="comments":


        return normalize_comment(

            raw,

            item["stock_code"],

            item["stock_name"]

        )



    else:

        return None



# =====================================================
# 批量处理
# =====================================================


def normalize_records(

    files

):


    result=[]



    for item in files:


        try:


            record=normalize_file(

                item

            )


            if record:

                result.append(

                    record

                )


        except Exception as e:


            logger.error(

                f"{item['file_path']}失败:{e}"

            )



    logger.info(

        f"标准化完成:{len(result)}条"

    )


    return result



# =====================================================
# 测试
# =====================================================


if __name__=="__main__":


    logging.basicConfig(

        level=logging.INFO

    )


    from scanner import scan_outputs


    import yaml
    
    
    
    with open(
        "config.yaml",
        "r",
        encoding="utf-8"
    ) as f:

        config = yaml.safe_load(f)

    files=scan_outputs(

        config["local"]["output_dir"]

    )


    records=normalize_records(

        files[:10]

    )


    for r in records:

        print(r)
