# core/internal_exchange.py
from __future__ import annotations

import os
import sqlite3
import datetime as _dt
import uuid as _uuid
from typing import Any, Dict, List, Optional, Tuple

from core.sync import touch_sync_request

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS internal_docs(
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ext_id TEXT NOT NULL UNIQUE,           -- UUID for cross-sync dedup
    reg_no INTEGER NOT NULL,               -- local sequential number in mirror
    reg_date TEXT DEFAULT '',
    recipient TEXT DEFAULT '',
    subject TEXT DEFAULT '',
    notes TEXT DEFAULT '',
    attachments TEXT DEFAULT '',           -- relative paths (within mirror program folder)
    ref_in_no TEXT DEFAULT '',
    doc_date TEXT DEFAULT '',
    doc_no TEXT DEFAULT '',
    due_date TEXT DEFAULT '',
    executor TEXT DEFAULT '',
    work_state TEXT DEFAULT 'in_work',
    done_date TEXT DEFAULT '',
    created_ts TEXT DEFAULT '',
    updated_ts TEXT DEFAULT '',
    exported INTEGER DEFAULT 0,            -- 0 not exported, 1 exported to truth
    exported_ts TEXT DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_internal_docs_exported ON internal_docs(exported, reg_no);
"""

def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_exchange_db_path(app_base_dir: str) -> str:
    d = os.path.join(app_base_dir, "data", "internal_exchange")
    os.makedirs(d, exist_ok=True)
    return os.path.join(d, "internal_docs.sqlite3")

def connect_exchange(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON;")
    return con

def init_exchange_db(db_path: str) -> None:
    con = connect_exchange(db_path)
    try:
        con.executescript(SCHEMA_SQL)
        # migrations for older exchange DBs
        cur = con.cursor()
        cur.execute("PRAGMA table_info(internal_docs);")
        cols = {r[1] for r in cur.fetchall()}
        def _ensure(col, ddl):
            if col not in cols:
                con.execute(ddl)
        _ensure('executor', "ALTER TABLE internal_docs ADD COLUMN executor TEXT DEFAULT ''")
        _ensure('work_state', "ALTER TABLE internal_docs ADD COLUMN work_state TEXT DEFAULT 'in_work'")
        _ensure('done_date', "ALTER TABLE internal_docs ADD COLUMN done_date TEXT DEFAULT ''")
        con.commit()
    finally:
        con.close()

def next_reg_no(db_path: str) -> int:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT COALESCE(MAX(reg_no), 0) + 1 AS n FROM internal_docs;")
        return int(cur.fetchone()["n"])
    finally:
        con.close()

def list_items(db_path: str, include_exported: bool = True) -> List[Dict[str, Any]]:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        if include_exported:
            cur.execute("SELECT * FROM internal_docs ORDER BY reg_no DESC;")
        else:
            cur.execute("SELECT * FROM internal_docs WHERE exported=0 ORDER BY reg_no DESC;")
        return [dict(r) for r in cur.fetchall()]
    finally:
        con.close()

def get_item(db_path: str, item_id: int) -> Optional[Dict[str, Any]]:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        cur.execute("SELECT * FROM internal_docs WHERE id=?;", (int(item_id),))
        r = cur.fetchone()
        return dict(r) if r else None
    finally:
        con.close()

def create_item(db_path: str, data: Dict[str, Any]) -> int:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        ext_id = (data.get("ext_id") or "").strip() or str(_uuid.uuid4())
        reg_no = int(data.get("reg_no") or next_reg_no(db_path))
        now = _now_iso()
        cur.execute(
            """INSERT INTO internal_docs(
                    ext_id, reg_no, reg_date, recipient, subject, notes, attachments,
                    ref_in_no, doc_date, doc_no, due_date, executor, work_state, done_date,
                    created_ts, updated_ts, exported, exported_ts
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                ext_id, reg_no,
                data.get("reg_date","") or "",
                data.get("recipient","") or "",
                data.get("subject","") or "",
                data.get("notes","") or "",
                data.get("attachments","") or "",
                data.get("ref_in_no","") or "",
                data.get("doc_date","") or "",
                data.get("doc_no","") or "",
                data.get("due_date","") or "",
                data.get("executor","") or "",
                data.get("work_state","in_work") or "in_work",
                data.get("done_date","") or "",
                now, now, 0, ""
            )
        )
        con.commit()
        new_id = int(cur.lastrowid)
        # Сигнализируем администратору (истина), что в зеркале есть изменения для подтягивания
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(db_path), "..", ".."))
            touch_sync_request(base_dir)
        except Exception:
            pass
        return new_id
    finally:
        con.close()

def update_item(db_path: str, item_id: int, data: Dict[str, Any]) -> None:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        now = _now_iso()
        cur.execute(
            """UPDATE internal_docs SET
                    reg_date=?, recipient=?, subject=?, notes=?, attachments=?,
                    ref_in_no=?, doc_date=?, doc_no=?, due_date=?, executor=?, work_state=?, done_date=?,
                    updated_ts=?
                WHERE id=?;""",
            (
                data.get("reg_date","") or "",
                data.get("recipient","") or "",
                data.get("subject","") or "",
                data.get("notes","") or "",
                data.get("attachments","") or "",
                data.get("ref_in_no","") or "",
                data.get("doc_date","") or "",
                data.get("doc_no","") or "",
                data.get("due_date","") or "",
                data.get("executor","") or "",
                data.get("work_state","in_work") or "in_work",
                data.get("done_date","") or "",
                now, int(item_id)
            )
        )
        con.commit()
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(db_path), "..", ".."))
            touch_sync_request(base_dir)
        except Exception:
            pass
    finally:
        con.close()

def delete_item(db_path: str, item_id: int) -> None:
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        cur = con.cursor()
        cur.execute("DELETE FROM internal_docs WHERE id=?;", (int(item_id),))
        con.commit()
        try:
            base_dir = os.path.abspath(os.path.join(os.path.dirname(db_path), "..", ".."))
            touch_sync_request(base_dir)
        except Exception:
            pass
    finally:
        con.close()

def list_pending_for_export(db_path: str) -> List[Dict[str, Any]]:
    return list_items(db_path, include_exported=False)

def mark_exported(db_path: str, ext_ids: List[str]) -> None:
    if not ext_ids:
        return
    init_exchange_db(db_path)
    con = connect_exchange(db_path)
    try:
        now = _now_iso()
        cur = con.cursor()
        cur.executemany(
            "UPDATE internal_docs SET exported=1, exported_ts=? WHERE ext_id=?;",
            [(now, e) for e in ext_ids]
        )
        con.commit()
    finally:
        con.close()
