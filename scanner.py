"""
scanner.py

扫描爬虫输出目录

输入:

outputs/

    股票代码_股票名称/

        news/

            *.json


        comments/

            *.json



输出:

[
    {
        "data_type": "news",
        "stock_code": "005930",
        "stock_name": "삼성전자",
        "file_path": "/xxx/news/xxx.json"
    }
]


注意:

这里只扫描文件

不读取JSON内容

不连接数据库

"""

import os
from pathlib import Path
import logging


logger = logging.getLogger(__name__)


# ======================================================
# 股票目录解析
# ======================================================

def parse_stock_dir(dirname):

    """
    输入:

    005930_삼성전자


    返回:

    code,
    name

    """


    if "_" not in dirname:

        logger.warning(
            f"股票目录格式错误: {dirname}"
        )

        return None, None


    code, name = dirname.split(
        "_",
        1
    )


    return code, name



# ======================================================
# 扫描单个数据类型
# ======================================================

def scan_category(
    stock_dir,
    category
):

    """
    扫描:

    news

    或

    comments


    """

    result = []


    category_dir = (
        stock_dir / category
    )


    if not category_dir.exists():

        return result



    for file in category_dir.glob("*.json"):


        result.append(

            {

                "data_type": category,


                "file_path": str(file)


            }

        )


    return result



# ======================================================
# 主扫描函数
# ======================================================

def scan_outputs(
    output_dir
):

    """

    扫描:

    outputs/


    返回:

    list


    """


    output_path = Path(
        output_dir
    )


    if not output_path.exists():

        raise FileNotFoundError(

            f"输出目录不存在: {output_dir}"

        )



    records = []



    # ----------------------------------
    # 股票目录
    # ----------------------------------

    for stock_dir in output_path.iterdir():


        if not stock_dir.is_dir():

            continue



        stock_code, stock_name = parse_stock_dir(

            stock_dir.name

        )


        if not stock_code:

            continue



        # ------------------------------
        # news/comments
        # ------------------------------

        for category in [

            "news",

            "comments"

        ]:


            files = scan_category(

                stock_dir,

                category

            )



            for item in files:


                item.update(

                    {

                        "stock_code":
                            stock_code,


                        "stock_name":
                            stock_name

                    }

                )


                records.append(item)



    logger.info(

        f"扫描完成，共发现 {len(records)} 个JSON文件"

    )


    return records



# ======================================================
# 测试
# ======================================================


if __name__ == "__main__":


    logging.basicConfig(

        level=logging.INFO

    )


    import yaml



    with open(
        "config.yaml",
        "r",
        encoding="utf-8"
    ) as f:

        config = yaml.safe_load(f)



    files = scan_outputs(

        config["local"]["output_dir"]

    )


    print(
        "数量:",
        len(files)
    )


    for item in files[:10]:

        print(item)
