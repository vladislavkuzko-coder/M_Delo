# core/backup.py
from __future__ import annotations

import os
import shutil
from datetime import datetime
from typing import Tuple

import sqlite3


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def make_db_backup(db_path: str, backups_dir: str, keep_last: int = 30) -> Tuple[str, int]:
    """Создает резервную копию файла БД.

    Возвращает (путь_к_копии, сколько_файлов_удалено_по_лимиту)
    """
    ensure_dir(backups_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base = os.path.basename(db_path)
    dst = os.path.join(backups_dir, f"{base}.{ts}.bak")
    # Prefer SQLite online-backup API for a consistent snapshot
    # even if the DB is in use (WAL).
    try:
        # Use context managers so connections are always closed,
        # even if backup fails mid-way.
        with sqlite3.connect(db_path, timeout=5.0) as src:
            src.execute("PRAGMA busy_timeout=5000;")
            with sqlite3.connect(dst) as dst_con:
                src.backup(dst_con)
    except Exception:
        # fallback to file copy (best effort)
        shutil.copy2(db_path, dst)

    # чистим старые
    removed = 0
    try:
        items = []
        for fn in os.listdir(backups_dir):
            p = os.path.join(backups_dir, fn)
            if os.path.isfile(p) and fn.startswith(base + ".") and fn.endswith(".bak"):
                items.append((os.path.getmtime(p), p))
        items.sort(reverse=True)
        for _mt, p in items[keep_last:]:
            try:
                os.remove(p)
                removed += 1
            except Exception:
                pass
    except Exception:
        pass

    return dst, removed
