# core/recovery.py
from __future__ import annotations

import os
import shutil
import sqlite3
from typing import Optional, Tuple


def integrity_ok(db_path: str) -> Tuple[bool, str]:
    """Return (ok, message)."""
    if not os.path.exists(db_path):
        return False, "DB file not found"
    try:
        con = sqlite3.connect(db_path, timeout=5.0)
        con.execute("PRAGMA busy_timeout=5000;")
        cur = con.cursor()
        cur.execute("PRAGMA integrity_check;")
        msg = (cur.fetchone() or ["?"])[0]
        con.close()
        ok = str(msg).strip().lower() == "ok"
        return ok, str(msg)
    except Exception as e:
        return False, f"integrity_check failed: {e}"


def find_latest_backup(db_path: str, backups_dir: str) -> Optional[str]:
    base = os.path.basename(db_path)
    if not os.path.isdir(backups_dir):
        return None
    best = None
    best_m = -1.0
    for fn in os.listdir(backups_dir):
        if not (fn.startswith(base + ".") and fn.endswith(".bak")):
            continue
        p = os.path.join(backups_dir, fn)
        try:
            mt = os.path.getmtime(p)
        except Exception:
            continue
        if mt > best_m:
            best_m = mt
            best = p
    return best


def restore_backup(db_path: str, backup_path: str) -> None:
    """Restore backup atomically (best effort).

    If SQLite WAL files exist next to the DB, they can conflict with the
    restored snapshot. We remove `-wal` / `-shm` after restore.
    """
    if not os.path.exists(backup_path):
        raise FileNotFoundError(backup_path)

    db_dir = os.path.dirname(os.path.abspath(db_path))
    os.makedirs(db_dir, exist_ok=True)
    tmp = db_path + ".restore_tmp"
    shutil.copy2(backup_path, tmp)
    os.replace(tmp, db_path)

    # Clean WAL sidecars (best effort).
    for suf in ("-wal", "-shm"):
        try:
            p = db_path + suf
            if os.path.exists(p):
                os.remove(p)
        except Exception:
            pass
