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
import tempfile
from collections import defaultdict
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

    if shutil.which("rsync"):
        return "rsync"

    if platform.system().lower() == "windows":
        return "scp"

    return "scp"


def get_index_sync_method(config):
    method = config.get("sync", {}).get("method", "auto")

    if method != "auto":
        return method

    if platform.system().lower() == "windows":
        return "scp"

    return get_sync_method(config)


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


def build_rsync_files_from_command(
    file_list_path,
    source_root,
    config,
):
    server = config["server"]
    ssh_port = get_ssh_port(config)
    source_root = Path(source_root)

    return [
        "rsync",
        "-avz",
        "--partial",
        "--relative",
        f"--files-from={file_list_path}",
        "-e",
        f"ssh -p {ssh_port}",
        f"{source_root}/",
        f'{server["user"]}@{server["host"]}:{server["data_dir"].rstrip("/")}/',
    ]


def build_scp_file_upload_command(
    file_path,
    source_root,
    config,
):
    server = config["server"]
    ssh_port = get_ssh_port(config)
    source_root = Path(source_root)
    file_path = Path(file_path)
    relative_path = file_path.resolve().relative_to(source_root.resolve())
    remote_file = (
        f'{server["data_dir"].rstrip("/")}/'
        f'{relative_path.as_posix()}'
    )

    return [
        "scp",
        "-P",
        str(ssh_port),
        str(file_path),
        f'{server["user"]}@{server["host"]}:{remote_file}',
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


def collect_parquet_upload_paths(parquet_files):
    paths = []

    for item in parquet_files:
        for key in ("local_file_path", "local_id_index_path"):
            value = item.get(key)

            if value:
                paths.append(Path(value))

    return paths


def upload_parquet_files(
    parquet_files,
    source_dir,
    config_path="config.yaml",
):
    """
    只上传本批生成的正文 Parquet 和 ID 索引 Parquet。
    """
    file_paths = collect_parquet_upload_paths(parquet_files)

    if not file_paths:
        logger.info("没有需要上传的Parquet文件")
        return True

    config = load_config(config_path)
    method = get_sync_method(config)
    source_root = Path(source_dir)

    missing_files = [
        str(path)
        for path in file_paths
        if not path.exists()
    ]

    if missing_files:
        logger.error("本批上传文件不存在：%s", missing_files)
        return False

    if method == "rsync":
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
        ) as file:
            file_list_path = file.name

            for path in file_paths:
                relative_path = path.resolve().relative_to(
                    source_root.resolve()
                )
                file.write(f"./{relative_path.as_posix()}\n")

        command = build_rsync_files_from_command(
            file_list_path,
            source_root,
            config,
        )

        logger.info(
            "执行增量rsync，本批文件=%s",
            len(file_paths),
        )

        try:
            result = subprocess.run(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
        except Exception as e:
            logger.exception("增量上传异常:%s", e)
            return False
        finally:
            try:
                Path(file_list_path).unlink()
            except OSError:
                pass

        if result.stdout:
            print(result.stdout)

        if result.returncode == 0:
            logger.info("增量rsync上传成功")
            return True

        logger.error("增量rsync失败:%s", result.returncode)
        return False

    if method == "scp":
        remote_root = config["server"]["data_dir"].rstrip("/")

        for file_path in file_paths:
            relative_path = file_path.resolve().relative_to(
                source_root.resolve()
            )
            remote_dir = (
                f"{remote_root}/"
                f"{relative_path.parent.as_posix()}"
            )

            if not ensure_remote_directory(remote_dir, config):
                logger.error("创建远端目录失败:%s", remote_dir)
                return False

            command = build_scp_file_upload_command(
                file_path,
                source_root,
                config,
            )
            result = run_command(command)

            if result.returncode != 0:
                logger.error("增量scp失败:%s", file_path)
                return False

        logger.info("增量scp上传成功，本批文件=%s", len(file_paths))
        return True

    logger.error("不支持的同步方式:%s", method)
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


def iter_index_day_partitions(partitions):
    days = defaultdict(set)

    for data_type, stock_code, data_date in partitions:
        days[(data_type, data_date)].add(stock_code)

    return sorted(days.items(), key=lambda item: (item[0][0], item[0][1]))


def build_index_day_relative_path(data_type: str, data_date: date) -> Path:
    return (
        Path(data_type)
        / f"year={data_date.year:04d}"
        / f"month={data_date.month:02d}"
        / f"day={data_date.day:02d}"
    )


def build_index_stock_relative_path(
    data_type: str,
    stock_code: str,
    data_date: date,
) -> Path:
    return (
        build_index_day_relative_path(data_type, data_date)
        / f"stock={stock_code}"
    )


def build_index_download_command(
    remote_dir: str,
    local_dir: Path,
    config: dict,
    method: str,
) -> list[str] | None:
    server = config["server"]
    ssh_port = get_ssh_port(config)

    if method == "rsync":
        return [
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

    if method == "scp":
        return build_scp_download_command(
            remote_dir,
            local_dir,
            config,
        )

    return None


def fetch_index_directory(
    remote_dir: str,
    local_dir: Path,
    config: dict,
    method: str,
    fallback_to_scp: bool = True,
) -> tuple[bool, str]:
    if not remote_directory_exists(remote_dir, config):
        return True, ""

    local_dir.mkdir(parents=True, exist_ok=True)
    command = build_index_download_command(
        remote_dir,
        local_dir,
        config,
        method,
    )

    if command is None:
        return False, f"unsupported sync method: {method}"

    result = run_command(command)
    if result.returncode == 0:
        return True, result.stdout or ""

    output = result.stdout or ""

    if method == "rsync" and fallback_to_scp:
        logger.warning(
            "rsync index fetch failed; retrying with scp: %s\n%s",
            remote_dir,
            output,
        )
        scp_command = build_index_download_command(
            remote_dir,
            local_dir,
            config,
            "scp",
        )

        if scp_command is None:
            return False, output

        scp_result = run_command(scp_command)

        if scp_result.returncode == 0:
            return True, scp_result.stdout or ""

        return False, (scp_result.stdout or output)

    return False, output


def fetch_partition_indexes(
    partitions: list[tuple[str, str, date]],
    config_path: str = "config.yaml",
) -> bool:
    """
    Fetch historical ID indexes for touched partitions only.
    """
    config = load_config(config_path)

    server = config["server"]
    method = get_index_sync_method(config)

    if method not in ("rsync", "scp"):
        logger.error("unsupported sync method:%s", method)
        return False

    cache_root = Path(
        config["local"]["index_cache_dir"]
    )
    cache_root.mkdir(parents=True, exist_ok=True)

    day_partitions = iter_index_day_partitions(partitions)
    total_days = len(day_partitions)
    use_day_fetch = method == "rsync"

    logger.info(
        "historical ID index sync: partitions=%s, day_dirs=%s, method=%s",
        len(partitions),
        total_days,
        method,
    )

    for index, ((data_type, data_date), stock_codes) in enumerate(
        day_partitions,
        start=1,
    ):
        relative_dir = build_index_day_relative_path(
            data_type,
            data_date,
        )

        remote_dir = (
            f'{server["data_dir"].rstrip("/")}'
            f'/_indexes/{relative_dir.as_posix()}'
        )

        local_dir = cache_root / relative_dir

        if index == 1 or index % 100 == 0 or index == total_days:
            logger.info(
                "index sync progress: %s/%s day dirs, current=%s, stocks=%s",
                index,
                total_days,
                relative_dir.as_posix(),
                len(stock_codes),
            )

        if use_day_fetch:
            success, output = fetch_index_directory(
                remote_dir,
                local_dir,
                config,
                method,
                fallback_to_scp=False,
            )

            if success:
                continue

            use_day_fetch = False
            logger.warning(
                "day-level index fetch failed; using stock-level fetch from now on: %s\n%s",
                remote_dir,
                output,
            )

        for stock_index, stock_code in enumerate(sorted(stock_codes), start=1):
            stock_relative_dir = build_index_stock_relative_path(
                data_type,
                stock_code,
                data_date,
            )
            stock_remote_dir = (
                f'{server["data_dir"].rstrip("/")}'
                f'/_indexes/{stock_relative_dir.as_posix()}'
            )
            stock_local_dir = cache_root / stock_relative_dir

            if (
                stock_index == 1
                or stock_index % 1000 == 0
                or stock_index == len(stock_codes)
            ):
                logger.info(
                    "stock-level index sync progress: %s/%s, current=%s",
                    stock_index,
                    len(stock_codes),
                    stock_relative_dir.as_posix(),
                )

            stock_success, stock_output = fetch_index_directory(
                stock_remote_dir,
                stock_local_dir,
                config,
                method,
                fallback_to_scp=True,
            )

            if not stock_success:
                logger.error(
                    "failed to fetch historical ID index: %s\n%s",
                    stock_remote_dir,
                    stock_output,
                )
                return False

        logger.info(
            "stock-level index fetch complete: %s, stocks=%s",
            remote_dir,
            len(stock_codes),
        )

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
