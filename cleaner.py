"""
cleaner.py

清理已经同步完成的原始JSON文件

原则：

上传成功后删除

避免Mac空间爆炸


"""


from pathlib import Path
import logging


logger = logging.getLogger(__name__)



# =====================================================
# 删除单个文件
# =====================================================


def remove_file(
    filepath
):


    path=Path(filepath)


    if not path.exists():

        return True



    try:

        path.unlink()


        logger.info(
            f"删除: {filepath}"
        )


        return True


    except Exception as e:


        logger.error(
            f"删除失败 {filepath}: {e}"
        )


        return False



# =====================================================
# 删除空目录
# =====================================================


def remove_empty_dirs(
    root
):


    root=Path(root)


    # 从底层向上

    for directory in sorted(
        root.rglob("*"),
        reverse=True
    ):


        if directory.is_dir():

            try:

                directory.rmdir()

                logger.info(
                    f"删除空目录:{directory}"
                )

            except OSError:

                pass




# =====================================================
# 批量清理
# =====================================================


def clean_files(

    files,

    output_dir=None

):


    """
    
    files:

    scanner.py输出:


    [
      {
        file_path:"xxx.json"
      }
    ]


    """


    success=0


    failed=0



    for item in files:


        filepath=item["file_path"]


        if remove_file(filepath):

            success+=1

        else:

            failed+=1



    if output_dir:


        remove_empty_dirs(
            output_dir
        )



    logger.info(

        f"清理完成 成功:{success},失败:{failed}"

    )



    return {

        "success":success,

        "failed":failed

    }




# =====================================================
# 测试
# =====================================================


if __name__=="__main__":


    logging.basicConfig(
        level=logging.INFO
    )


    test=[

        {

        "file_path":
        "../outputs/test.json"

        }

    ]


    print(

        clean_files(
            test
        )

    )
