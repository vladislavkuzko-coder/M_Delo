# core/sync.py
from __future__ import annotations

import os
import shutil
import sqlite3

from typing import Iterable, Tuple


DEFAULT_IGNORE_NAMES = {
    "venv",
    "__pycache__",
    ".git",
    "backups",
    "internal_exchange",
}

DEFAULT_IGNORE_FILES = {
    "writer.lock",
}


SYNC_REQUEST_FILENAME = "sync_request.flag"


def get_sync_request_path(program_dir: str) -> str:
    """Path to a file used as a cheap 'sync requested' signal.

    We store it under data/internal_exchange because that folder is ignored by
    source→mirror file sync, so the mirror can safely touch it without being
    overwritten by the next push from source.
    """
    d = os.path.join(os.path.abspath(program_dir), "data", "internal_exchange")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, SYNC_REQUEST_FILENAME)


def touch_sync_request(program_dir: str) -> None:
    """Update mtime of sync request flag."""
    try:
        p = get_sync_request_path(program_dir)
        with open(p, "a", encoding="utf-8") as f:
            f.write("")
        os.utime(p, None)
    except Exception:
        # Best-effort only
        pass


def _is_newer(src: str, dst: str) -> bool:
    if not os.path.exists(dst):
        return True
    try:
        return os.path.getmtime(src) > os.path.getmtime(dst)
    except Exception:
        return True


def _copy_sqlite_atomic(src_path: str, dst_path: str) -> bool:
    """Copy SQLite DB safely using the backup API (works even when DB is open).

    Returns True if copied successfully.
    """
    try:
        os.makedirs(os.path.dirname(dst_path), exist_ok=True)
        tmp = dst_path + ".tmp"
        uri = f"file:{src_path}?mode=ro"
        src = sqlite3.connect(uri, uri=True)
        try:
            dst = sqlite3.connect(tmp)
            try:
                src.backup(dst)
                dst.commit()
            finally:
                dst.close()
        finally:
            src.close()
        os.replace(tmp, dst_path)
        try:
            shutil.copystat(src_path, dst_path)
        except Exception:
            pass
        return True
    except Exception:
        try:
            if os.path.exists(dst_path + ".tmp"):
                os.remove(dst_path + ".tmp")
        except Exception:
            pass
        return False


def sync_folders(
    source_dir: str,
    mirror_dir: str,
    ignore_names: Iterable[str] = DEFAULT_IGNORE_NAMES,
    ignore_files: Iterable[str] = DEFAULT_IGNORE_FILES,
) -> Tuple[int, int]:
    """
    Односторонняя синхронизация:
    - копируем ТОЛЬКО из source_dir → mirror_dir
    - если файл в зеркале новее — НЕ копируем обратно и НЕ трогаем source
    - если в зеркале есть лишние файлы — НЕ удаляем (чтобы случайно ничего не потерять)

    Возвращает (copied, skipped)
    """
    src = os.path.abspath(source_dir)
    dst = os.path.abspath(mirror_dir)

    if not os.path.isdir(src):
        raise FileNotFoundError(f"Source не найден: {src}")
    os.makedirs(dst, exist_ok=True)

    copied = 0
    skipped = 0

    ignore_names = set(ignore_names or [])
    ignore_files = set(ignore_files or [])

    for root, dirs, files in os.walk(src):
        rel = os.path.relpath(root, src)
        dst_root = os.path.join(dst, rel) if rel != "." else dst
        os.makedirs(dst_root, exist_ok=True)

        # не спускаемся в игнорируемые папки
        dirs[:] = [d for d in dirs if d not in ignore_names]
        for d in dirs:
            os.makedirs(os.path.join(dst_root, d), exist_ok=True)

        for fn in files:
            if fn in ignore_files:
                skipped += 1
                continue
            if fn.endswith(".pyc"):
                skipped += 1
                continue
            s_path = os.path.join(root, fn)
            d_path = os.path.join(dst_root, fn)

            # SQLite DBs: copy via backup API to avoid corruption while DB is in use
            low_fn = fn.lower()
            if low_fn.endswith((".sqlite3", ".sqlite", ".db")):
                if _is_newer(s_path, d_path):
                    if _copy_sqlite_atomic(s_path, d_path):
                        copied += 1
                        continue


            # если файл изменился / новый — копируем
            if _is_newer(s_path, d_path):
                tmp = d_path + ".tmp"
                try:
                    # пишем атомарно: сначала во временный файл, потом replace
                    shutil.copy2(s_path, tmp)
                    os.replace(tmp, d_path)
                    copied += 1
                except Exception:
                    # если упали — стараемся убрать временный файл, чтобы он не мешал
                    try:
                        if os.path.exists(tmp):
                            os.remove(tmp)
                    except Exception:
                        pass
                    skipped += 1
            else:
                skipped += 1

    return copied, skipped
