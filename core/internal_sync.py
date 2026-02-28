# core/internal_sync.py
from __future__ import annotations

import os
import shutil
from typing import Any, Dict, List, Tuple

from core.db import connect, ensure_column, upsert_dictionary_value
from core.internal_exchange import (
    get_exchange_db_path, init_exchange_db, list_pending_for_export, mark_exported, connect_exchange
)

def _normalize_rel(path: str) -> str:
    p = (path or "").replace("\\", "/").strip()
    while p.startswith("./"):
        p = p[2:]
    return p.lstrip("/")


def _parse_attachment_paths(val: str) -> List[str]:
    """Parse attachments field.

    UI stores either legacy single string or JSON list.
    JSON format: [ {"path": "...", "name": "..."}, ... ]
    """
    s = (val or "").strip()
    if not s:
        return []
    if s.startswith('['):
        try:
            import json

            x = json.loads(s)
            out: List[str] = []
            if isinstance(x, list):
                for it in x:
                    if isinstance(it, dict):
                        p = str(it.get('path') or '').strip()
                        if p:
                            out.append(p)
                    else:
                        p = str(it).strip()
                        if p:
                            out.append(p)
            return out
        except Exception:
            return [s]
    return [s]


def _filter_attachments_value(original: str, keep_rel_paths: List[str]) -> str:
    """Return attachments value with only successfully copied files kept."""
    s = (original or "").strip()
    if not s:
        return ""
    keep = {_normalize_rel(p) for p in keep_rel_paths if (p or "").strip()}
    if s.startswith('['):
        try:
            import json

            x = json.loads(s)
            if not isinstance(x, list):
                return ""
            out: List[Any] = []
            for it in x:
                if isinstance(it, dict):
                    p = _normalize_rel(str(it.get('path') or '').strip())
                    if p and p in keep:
                        out.append(it)
                else:
                    p = _normalize_rel(str(it).strip())
                    if p and p in keep:
                        out.append(it)
            return json.dumps(out, ensure_ascii=False)
        except Exception:
            return keep_rel_paths[0] if keep_rel_paths else ""
    return keep_rel_paths[0] if keep_rel_paths else ""

def _copy_attachment(src_abs: str, dst_abs: str) -> None:
    os.makedirs(os.path.dirname(dst_abs), exist_ok=True)
    tmp = dst_abs + ".tmp"
    shutil.copy2(src_abs, tmp)
    os.replace(tmp, dst_abs)

def pull_internal_from_mirror(truth_db_path: str, mirror_dir: str) -> Tuple[int, int]:
    """
    Забирает внутренние документы, созданные пользователями в зеркале, в 'истину'.
    Возвращает (imported, skipped).
    """
    mirror_dir = (mirror_dir or "").strip()
    if not mirror_dir or not os.path.isdir(mirror_dir):
        return (0, 0)

    mirror_exchange_db = get_exchange_db_path(mirror_dir)
    if not os.path.exists(mirror_exchange_db):
        return (0, 0)

    init_exchange_db(mirror_exchange_db)
    pending = list_pending_for_export(mirror_exchange_db)
    if not pending:
        return (0, 0)

    # Ensure extra columns exist in truth correspondence table for dedup/origin
    con = connect(truth_db_path, read_only=False)
    try:
        ensure_column(con, "correspondence", "ext_id", "TEXT DEFAULT ''")
        ensure_column(con, "correspondence", "origin", "TEXT DEFAULT ''")
        ensure_column(con, "correspondence", "origin_reg_no", "INTEGER DEFAULT 0")
        ensure_column(con, "correspondence", "work_state", "TEXT DEFAULT 'in_work'")
        ensure_column(con, "correspondence", "done_date", "TEXT DEFAULT ''")
        con.commit()
    finally:
        con.close()

    imported = 0
    skipped = 0
    ext_ids_to_mark: List[str] = []

    con = connect(truth_db_path, read_only=False)
    try:
        cur = con.cursor()

        # Build set of ext_ids already imported
        cur.execute("SELECT ext_id FROM correspondence WHERE ext_id <> '';")
        existing = set([r[0] for r in cur.fetchall()])

        # determine next internal reg_no in truth
        cur.execute("SELECT COALESCE(MAX(reg_no), 0) FROM correspondence WHERE kind='internal';")
        next_no = int(cur.fetchone()[0] or 0) + 1

        for r in pending:
            ext_id = (r.get("ext_id") or "").strip()
            if not ext_id:
                skipped += 1
                continue
            if ext_id in existing:
                skipped += 1
                ext_ids_to_mark.append(ext_id)
                continue

            # attachments: copy files from mirror program folder to truth program folder.
            # Stored as relative paths under the program base (legacy string) or JSON list.
            att_raw = r.get("attachments", "") or ""
            att_list = _parse_attachment_paths(att_raw)
            copied_rel: List[str] = []
            if att_list:
                truth_base = os.path.abspath(os.path.join(os.path.dirname(truth_db_path), ".."))
                for p in att_list:
                    rel = _normalize_rel(p)
                    if not rel:
                        continue
                    src_abs = os.path.join(mirror_dir, rel)
                    dst_abs = os.path.join(truth_base, rel)
                    if os.path.exists(src_abs):
                        try:
                            _copy_attachment(src_abs, dst_abs)
                            copied_rel.append(rel)
                        except Exception:
                            pass
            att_val = _filter_attachments_value(att_raw, copied_rel)

            # use mirror reg_no as origin_reg_no; assign reg_no sequentially in truth to avoid collisions
            origin_reg_no = int(r.get("reg_no") or 0)

            # IMPORTANT: keep placeholders count aligned with columns.
            # We pass kind as a parameter (instead of embedding a constant in SQL)
            # to avoid "X values for Y columns" errors.
            cur.execute(
                """INSERT INTO correspondence(
                        kind, reg_no, reg_date, sender, recipient, subject, notes, attachments,
                        ref_in_no, doc_date, doc_no, due_date,
                        executor, status, in_date, in_no, out_date, out_no,
                        work_state, done_date,
                        dept_reg_no, dept_reg_date,
                        created_ts, updated_ts,
                        ext_id, origin, origin_reg_no
                    ) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                    );""",
                (
                    "internal",
                    next_no,
                    r.get("reg_date","") or "",
                    "",  # sender for internal not used
                    r.get("recipient","") or "",
                    r.get("subject","") or "",
                    r.get("notes","") or "",
                    att_val,
                    r.get("ref_in_no","") or "",
                    r.get("doc_date","") or "",
                    r.get("doc_no","") or "",
                    r.get("due_date","") or "",
                    r.get("executor","") or "",
                    "", "", "", "", "",
                    r.get("work_state","in_work") or "in_work",
                    r.get("done_date","") or "",
                    "", "",
                    r.get("created_ts","") or "",
                    r.get("updated_ts","") or "",
                    ext_id, "mirror", origin_reg_no
                )
            )

            # пополняем справочники контрагентов для автодополнения
            try:
                party = (r.get('recipient') or '').strip()
                if party:
                    upsert_dictionary_value(truth_db_path, 'lender', party)
                    upsert_dictionary_value(truth_db_path, 'borrower', party)
            except Exception:
                pass

            imported += 1
            next_no += 1
            ext_ids_to_mark.append(ext_id)

        con.commit()
    finally:
        con.close()

    # Mark exported in mirror exchange db
    try:
        mark_exported(mirror_exchange_db, ext_ids_to_mark)
    except Exception:
        pass

    return (imported, skipped)
