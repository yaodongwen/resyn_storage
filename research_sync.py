"""
research_sync.py

Synchronize the Naver research project warehouse and PDF files to the same
server-side stocklake used by the storage project.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import subprocess
import tempfile
import time
from pathlib import Path
from typing import Any

import yaml

from uploader import get_ssh_port, get_sync_method, rsync_upload


logger = logging.getLogger(__name__)
PENDING_DIR = Path(".research_sync_pending")


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    with open(config_path, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    logger.info("执行命令：%s", " ".join(command))

    result = subprocess.run(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    return result


def build_ssh_command(config: dict[str, Any], remote_command: str) -> list[str]:
    server = config["server"]
    return [
        "ssh",
        "-p",
        str(get_ssh_port(config)),
        f'{server["user"]}@{server["host"]}',
        remote_command,
    ]


def ensure_remote_directory(
    remote_dir: str,
    config: dict[str, Any],
) -> bool:
    command = build_ssh_command(
        config,
        f"mkdir -p {shlex.quote(remote_dir)}",
    )
    return run_command(command).returncode == 0


def sync_directory_to_remote(
    source_dir: str | Path,
    remote_dir: str,
    config: dict[str, Any],
) -> bool:
    source = Path(source_dir)

    if not source.exists():
        logger.error("目录不存在：%s", source)
        return False

    method = get_sync_method(config)
    server = config["server"]

    if not ensure_remote_directory(remote_dir, config):
        logger.error("创建远端目录失败：%s", remote_dir)
        return False

    if method == "rsync":
        command = [
            "rsync",
            "-avz",
            "--partial",
            "--progress",
            "-e",
            f"ssh -p {get_ssh_port(config)}",
            f"{source}/",
            f'{server["user"]}@{server["host"]}:{remote_dir.rstrip("/")}/',
        ]
    elif method == "scp":
        command = [
            "scp",
            "-P",
            str(get_ssh_port(config)),
            "-r",
            f"{source}{os.sep}.",
            f'{server["user"]}@{server["host"]}:{remote_dir.rstrip("/")}/',
        ]
    else:
        logger.error("不支持的同步方式：%s", method)
        return False

    return run_command(command).returncode == 0


def get_file_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def scan_stable_files(
    root: Path,
    quiet_seconds: int,
    suffixes: tuple[str, ...],
) -> list[Path]:
    now = time.time()
    files = []

    if not root.exists():
        return files

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue

        if suffixes and not path.name.endswith(suffixes):
            continue

        try:
            stat = path.stat()
        except OSError:
            continue

        if now - stat.st_mtime < quiet_seconds:
            continue

        files.append(path)

    return files


def select_batch(
    files: list[Path],
    min_files: int,
    min_bytes: int,
    max_files: int,
) -> list[Path]:
    if not files:
        return []

    selected = files[:max_files] if max_files else files
    total_bytes = sum(get_file_size(path) for path in selected)

    if len(selected) >= min_files or total_bytes >= min_bytes:
        return selected

    return []


def save_pending_batch(
    warehouse_files: list[Path],
    pdf_files: list[Path],
) -> Path:
    PENDING_DIR.mkdir(parents=True, exist_ok=True)
    manifest_path = PENDING_DIR / f"pending-{int(time.time() * 1000)}.json"

    with manifest_path.open("w", encoding="utf-8") as file:
        json.dump(
            {
                "created_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
                "warehouse_files": [str(path) for path in warehouse_files],
                "pdf_files": [str(path) for path in pdf_files],
            },
            file,
            ensure_ascii=False,
            indent=2,
        )

    logger.info("写入research恢复点：%s", manifest_path)
    return manifest_path


def remove_pending_batch(manifest_path: Path) -> None:
    try:
        manifest_path.unlink()
        logger.info("删除research恢复点：%s", manifest_path)
    except FileNotFoundError:
        pass


def delete_files(files: list[Path]) -> None:
    for path in files:
        try:
            path.unlink()
            logger.info("删除已同步文件：%s", path)
        except FileNotFoundError:
            pass


def prune_empty_dirs(root: Path) -> None:
    if not root.exists():
        return

    for directory in sorted(root.rglob("*"), reverse=True):
        if not directory.is_dir():
            continue

        try:
            directory.rmdir()
        except OSError:
            pass


def upload_file_list(
    files: list[Path],
    source_root: Path,
    remote_root: str,
    config: dict[str, Any],
) -> bool:
    if not files:
        return True

    method = get_sync_method(config)
    server = config["server"]

    if not ensure_remote_directory(remote_root, config):
        return False

    if method == "rsync":
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            delete=False,
        ) as file:
            file_list_path = file.name

            for path in files:
                relative_path = path.resolve().relative_to(source_root.resolve())
                file.write(f"./{relative_path.as_posix()}\n")

        command = [
            "rsync",
            "-avz",
            "--partial",
            "--relative",
            f"--files-from={file_list_path}",
            "-e",
            f"ssh -p {get_ssh_port(config)}",
            f"{source_root}/",
            f'{server["user"]}@{server["host"]}:{remote_root.rstrip("/")}/',
        ]

        try:
            result = run_command(command)
            return result.returncode == 0
        finally:
            try:
                Path(file_list_path).unlink()
            except OSError:
                pass

    if method == "scp":
        for path in files:
            relative_path = path.resolve().relative_to(source_root.resolve())
            remote_file = f"{remote_root.rstrip('/')}/{relative_path.as_posix()}"
            remote_dir = str(Path(remote_file).parent)

            if not ensure_remote_directory(remote_dir, config):
                return False

            command = [
                "scp",
                "-P",
                str(get_ssh_port(config)),
                str(path),
                f'{server["user"]}@{server["host"]}:{remote_file}',
            ]

            if run_command(command).returncode != 0:
                return False

        return True

    logger.error("不支持的同步方式：%s", method)
    return False


def sync_research_batch(
    config: dict[str, Any],
    warehouse_files: list[Path],
    pdf_files: list[Path],
    delete_after_upload: bool,
    manifest_path: Path | None = None,
) -> bool:
    research = config["research"]
    warehouse_root = Path(research["warehouse_dir"])
    pdf_root = Path(research["pdf_dir"])

    if manifest_path is None:
        manifest_path = save_pending_batch(warehouse_files, pdf_files)

    warehouse_ok = upload_file_list(
        warehouse_files,
        warehouse_root,
        config["server"]["data_dir"],
        config,
    )

    if not warehouse_ok:
        logger.error("research warehouse 增量上传失败")
        return False

    pdf_ok = upload_file_list(
        pdf_files,
        pdf_root,
        research["remote_pdf_dir"],
        config,
    )

    if not pdf_ok:
        logger.error("research PDF 增量上传失败")
        return False

    if delete_after_upload:
        delete_files(warehouse_files + pdf_files)
        prune_empty_dirs(warehouse_root)
        prune_empty_dirs(pdf_root)

    remove_pending_batch(manifest_path)
    return True


def recover_pending_batches(config: dict[str, Any]) -> None:
    if not PENDING_DIR.exists():
        return

    for manifest_path in sorted(PENDING_DIR.glob("pending-*.json")):
        with manifest_path.open("r", encoding="utf-8") as file:
            payload = json.load(file)

        warehouse_files = [
            Path(path)
            for path in payload.get("warehouse_files", [])
            if Path(path).exists()
        ]
        pdf_files = [
            Path(path)
            for path in payload.get("pdf_files", [])
            if Path(path).exists()
        ]

        if not warehouse_files and not pdf_files:
            remove_pending_batch(manifest_path)
            continue

        logger.info("恢复research未完成批次：%s", manifest_path)
        ok = sync_research_batch(
            config,
            warehouse_files,
            pdf_files,
            config["research"].get("delete_after_upload", False),
            manifest_path,
        )

        if not ok:
            return


def sync_research(
    config_path: str = "config.yaml",
    include_pdf: bool = True,
    warehouse_only: bool = False,
    pdf_only: bool = False,
) -> None:
    config = load_config(config_path)
    research = config["research"]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not pdf_only:
        # research warehouse contains the top-level "research/" directory.
        success = rsync_upload(
            research["warehouse_dir"],
            config_path,
        )

        if not success:
            raise RuntimeError("研究报告 warehouse 上传失败")

    if warehouse_only:
        return

    if not include_pdf or not research.get("sync_pdf", True):
        return

    if pdf_only or include_pdf:
        success = sync_directory_to_remote(
            research["pdf_dir"],
            research["remote_pdf_dir"],
            config,
        )

        if not success:
            raise RuntimeError("研究报告 PDF 上传失败")


def watch_research(config_path: str = "config.yaml") -> None:
    config = load_config(config_path)
    research = config["research"]
    watch_config = research.get("watch", {})

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    interval_seconds = int(watch_config.get("interval_seconds", 30))
    quiet_seconds = int(watch_config.get("quiet_seconds", 60))
    min_files = int(watch_config.get("min_files", 20))
    min_bytes = int(watch_config.get("min_bytes", 100 * 1024 * 1024))
    max_files = int(watch_config.get("max_files_per_batch", 2000))
    delete_after_upload = bool(research.get("delete_after_upload", False))

    warehouse_root = Path(research["warehouse_dir"])
    pdf_root = Path(research["pdf_dir"])

    logger.info(
        "===== research持续同步启动：interval=%ss quiet=%ss min_files=%s min_bytes=%s delete=%s =====",
        interval_seconds,
        quiet_seconds,
        min_files,
        min_bytes,
        delete_after_upload,
    )

    recover_pending_batches(config)

    while True:
        try:
            warehouse_files = scan_stable_files(
                warehouse_root,
                quiet_seconds,
                (".parquet", ".meta.json"),
            )
            pdf_files = scan_stable_files(
                pdf_root,
                quiet_seconds,
                (".pdf",),
            )
            all_files = warehouse_files + pdf_files
            selected = select_batch(
                all_files,
                min_files,
                min_bytes,
                max_files,
            )

            logger.info(
                "research扫描完成：warehouse=%s pdf=%s selected=%s size=%.2fMB",
                len(warehouse_files),
                len(pdf_files),
                len(selected),
                sum(get_file_size(path) for path in selected) / 1024 / 1024,
            )

            if selected:
                selected_set = set(selected)
                selected_warehouse = [
                    path for path in warehouse_files if path in selected_set
                ]
                selected_pdf = [
                    path for path in pdf_files if path in selected_set
                ]

                sync_research_batch(
                    config,
                    selected_warehouse,
                    selected_pdf,
                    delete_after_upload,
                )

            time.sleep(interval_seconds)

        except KeyboardInterrupt:
            logger.info("research持续同步停止")
            return
        except Exception:
            logger.exception("research本轮同步失败，继续下一轮")
            time.sleep(interval_seconds)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="同步 naver_research 的 Parquet warehouse 和 PDF 到服务器。"
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="配置文件路径。",
    )
    parser.add_argument(
        "--no-pdf",
        action="store_true",
        help="只同步 research warehouse，不同步 PDF。",
    )
    parser.add_argument(
        "--warehouse-only",
        action="store_true",
        help="只同步 research warehouse。",
    )
    parser.add_argument(
        "--pdf-only",
        action="store_true",
        help="只同步 PDF。",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="持续监控 research warehouse/PDF，达到阈值后增量上传。",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.watch:
        watch_research(args.config)
    else:
        sync_research(
            config_path=args.config,
            include_pdf=not args.no_pdf,
            warehouse_only=args.warehouse_only,
            pdf_only=args.pdf_only,
        )
