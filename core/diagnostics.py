# core/diagnostics.py
from __future__ import annotations

import os
import sqlite3
from typing import Tuple


def integrity_check(db_path: str) -> Tuple[bool, str]:
    """Запускает PRAGMA integrity_check и возвращает (ok, message)."""
    try:
        con = sqlite3.connect(db_path)
        try:
            cur = con.execute("PRAGMA integrity_check")
            row = cur.fetchone()
            msg = (row[0] if row else "") or ""
            return (msg.strip().lower() == "ok"), msg
        finally:
            con.close()
    except Exception as e:
        return False, str(e)


def get_data_dirs(db_path: str) -> dict:
    data_dir = os.path.dirname(db_path)
    return {
        "data_dir": data_dir,
        "backups_dir": os.path.join(data_dir, "backups"),
        "logs_dir": os.path.join(data_dir, "logs"),
    }
