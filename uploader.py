"""
uploader.py

负责把本地 warehouse 同步到远端 stocklake。

macOS/Linux 通常使用 rsync；Windows 默认没有 rsync，因此会自动回退到
OpenSSH 自带的 scp。
"""

import logging
import os
import platform
import shlex
import shutil
import subprocess
from datetime import date
from pathlib import Path

import yaml


logger = logging.getLogger(__name__)


# =====================================================
# 加载配置
# =====================================================

def load_config(
    config_path="config.yaml"
):

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as f:

        return yaml.safe_load(f)



# =====================================================
# 构造rsync命令
# =====================================================

def build_rsync_command(
    source,
    config
):


    server=config["server"]


    sync=config["sync"]


    user=server["user"]


    host=server["host"]


    remote_dir=server["data_dir"]



    ssh_port=sync.get(
        "rsync",
        {}
    ).get(
        "ssh_port",
        22
    )


    command=[

        "rsync",


        "-avz",


        "--partial",


        "--progress",


        "-e",

        f"ssh -p {ssh_port}",


        str(source)+"/",


        f"{user}@{host}:{remote_dir}/"

    ]


    return command


def get_ssh_port(config):
    return config.get("sync", {}).get("rsync", {}).get("ssh_port", 22)


def get_sync_method(config):
    method = config.get("sync", {}).get("method", "auto")

    if method != "auto":
        return method

    if platform.system().lower() == "windows":
        return "scp"

    if shutil.which("rsync"):
        return "rsync"

    return "scp"


def run_command(command):
    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    return result


def build_ssh_command(config, remote_command):
    server = config["server"]
    ssh_port = get_ssh_port(config)

    return [
        "ssh",
        "-p",
        str(ssh_port),
        f'{server["user"]}@{server["host"]}',
        remote_command,
    ]


def ensure_remote_directory(remote_dir, config):
    command = build_ssh_command(
        config,
        f"mkdir -p {shlex.quote(remote_dir)}",
    )

    result = run_command(command)

    return result.returncode == 0


def build_scp_upload_command(source, config):
    server = config["server"]
    ssh_port = get_ssh_port(config)
    remote_dir = server["data_dir"].rstrip("/")
    source_contents = f"{source}{os.sep}."

    return [
        "scp",
        "-P",
        str(ssh_port),
        "-r",
        source_contents,
        f'{server["user"]}@{server["host"]}:{remote_dir}/',
    ]


def build_scp_download_command(remote_dir, local_dir, config):
    server = config["server"]
    ssh_port = get_ssh_port(config)

    return [
        "scp",
        "-P",
        str(ssh_port),
        "-r",
        (
            f'{server["user"]}@{server["host"]}:'
            f'{remote_dir.rstrip("/")}/.'
        ),
        str(local_dir),
    ]



# =====================================================
# 上传
# =====================================================

def rsync_upload(

    source_dir,

    config_path="config.yaml"

):


    """
    上传本地 warehouse 目录。

    返回:
        True / False
    """



    config=load_config(
        config_path
    )


    source=Path(
        source_dir
    )



    if not source.exists():

        raise FileNotFoundError(

            f"上传目录不存在:{source}"

        )



    method = get_sync_method(config)

    if method == "rsync":
        command=build_rsync_command(
            source,
            config
        )
    elif method == "scp":
        remote_dir = config["server"]["data_dir"].rstrip("/")

        if not ensure_remote_directory(remote_dir, config):
            logger.error("创建远端目录失败:%s", remote_dir)
            return False

        command=build_scp_upload_command(
            source,
            config
        )
    else:
        logger.error("不支持的同步方式:%s", method)
        return False

    logger.info(
        "执行%s:\n%s",
        method,
        " ".join(command),
    )



    try:


        result=run_command(command)



        if result.returncode==0:


            logger.info(

                "%s上传成功",
                method

            )

            return True



        else:


            logger.error(

                f"{method}失败:{result.returncode}"

            )

            return False



    except Exception as e:


        logger.exception(

            f"上传异常:{e}"

        )

        return False



# =====================================================
# 上传单个文件
# =====================================================

def upload_file(

    filepath,

    config_path="config.yaml"

):


    """

    用于未来增量上传


    """



    config=load_config(
        config_path
    )


    server=config["server"]



    user=server["user"]

    host=server["host"]

    remote=server["data_dir"]



    ssh_port=config["sync"]["rsync"]["ssh_port"]



    method = get_sync_method(config)

    if method == "rsync":
        command=[
            "rsync",
            "-avz",
            "--partial",
            "-e",
            f"ssh -p {ssh_port}",
            str(filepath),
            f"{user}@{host}:{remote}/"
        ]
    elif method == "scp":
        ensure_remote_directory(remote, config)
        command=[
            "scp",
            "-P",
            str(ssh_port),
            str(filepath),
            f"{user}@{host}:{remote}/"
        ]
    else:
        logger.error("不支持的同步方式:%s", method)
        return False



    result=run_command(command)


    return result.returncode==0

def remote_directory_exists(
    remote_path: str,
    config: dict,
) -> bool:
    server = config["server"]
    ssh_port = get_ssh_port(config)

    command = [
        "ssh",
        "-p",
        str(ssh_port),
        f'{server["user"]}@{server["host"]}',
        "test",
        "-d",
        remote_path,
    ]

    result = subprocess.run(
        command,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    return result.returncode == 0


def fetch_partition_indexes(
    partitions: list[tuple[str, str, date]],
    config_path: str = "config.yaml",
) -> bool:
    """
    从服务器仅拉取本次涉及分区的ID索引。

    partitions:
        [
            ("comments", "005930", date(2026, 8, 4))
        ]
    """
    config = load_config(config_path)

    server = config["server"]
    ssh_port = get_ssh_port(config)
    method = get_sync_method(config)

    cache_root = Path(
        config["local"]["index_cache_dir"]
    )
    cache_root.mkdir(parents=True, exist_ok=True)

    for data_type, stock_code, data_date in partitions:
        relative_dir = (
            Path(data_type)
            / f"year={data_date.year:04d}"
            / f"month={data_date.month:02d}"
            / f"day={data_date.day:02d}"
            / f"stock={stock_code}"
        )

        remote_dir = (
            f'{server["data_dir"].rstrip("/")}'
            f'/_indexes/{relative_dir.as_posix()}'
        )

        local_dir = cache_root / relative_dir
        local_dir.mkdir(parents=True, exist_ok=True)

        # 第一次运行时服务器还没有该分区索引，这是正常情况。
        if not remote_directory_exists(remote_dir, config):
            continue

        if method == "rsync":
            command = [
                "rsync",
                "-az",
                "--partial",
                "-e",
                f"ssh -p {ssh_port}",
                (
                    f'{server["user"]}@{server["host"]}:'
                    f'{remote_dir.rstrip("/")}/'
                ),
                f"{local_dir}/",
            ]
        elif method == "scp":
            command = build_scp_download_command(
                remote_dir,
                local_dir,
                config,
            )
        else:
            logger.error("不支持的同步方式:%s", method)
            return False

        result = run_command(command)

        if result.returncode != 0:
            logger.error(
                "拉取历史ID索引失败：%s\n%s",
                remote_dir,
                result.stdout,
            )
            return False

    return True


# =====================================================
# 测试
# =====================================================

if __name__=="__main__":


    logging.basicConfig(

        level=logging.INFO

    )


    config=load_config()



    local_dir=config["storage"][

        "local_warehouse"

    ]


    success=rsync_upload(

        local_dir

    )


    print(

        "上传结果:",

        success

    )
