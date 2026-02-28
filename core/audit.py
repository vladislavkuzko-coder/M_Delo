"""core.audit

Минималистичный журнал действий (audit_log).

Модуль intentionally "best-effort": любые ошибки БД/файла журнала не должны
ломать работу приложения.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional


def _utc_ts() -> str:
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def log_event(
    db_path: str,
    username: str,
    action: str,
    entity: str,
    entity_id: Optional[int] = None,
    summary: str = "",
    payload: Optional[Dict[str, Any]] = None,
) -> None:
    """Записать событие в audit_log.

    Не выбрасывает исключения наружу.
    """
    try:
        payload_json = json.dumps(payload or {}, ensure_ascii=False)
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                """
                INSERT INTO audit_log(ts, username, action, entity, entity_id, summary, payload_json)
                VALUES(?,?,?,?,?,?,?)
                """,
                (
                    _utc_ts(),
                    username or "",
                    action or "",
                    entity or "",
                    entity_id,
                    summary or "",
                    payload_json,
                ),
            )
            conn.commit()
    except Exception:
        return


def list_events(
    db_path: str,
    limit: int = 200,
    entity: Optional[str] = None,
    entity_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    q = "SELECT id, ts, username, action, entity, entity_id, summary, payload_json FROM audit_log"
    params: List[Any] = []
    where: List[str] = []

    if entity:
        where.append("entity = ?")
        params.append(entity)
    if entity_id is not None:
        where.append("entity_id = ?")
        params.append(entity_id)
    if where:
        q += " WHERE " + " AND ".join(where)

    q += " ORDER BY id DESC LIMIT ?"
    params.append(int(limit))

    try:
        with sqlite3.connect(db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(q, params).fetchall()
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            try:
                d["payload"] = json.loads(d.get("payload_json") or "{}")
            except Exception:
                d["payload"] = {}
            out.append(d)
        return out
    except Exception:
        return []


def purge_old(db_path: str, keep_days: int = 365) -> None:
    try:
        cutoff = datetime.utcnow() - timedelta(days=int(keep_days))
        cutoff_ts = cutoff.replace(microsecond=0).isoformat() + "Z"
        with sqlite3.connect(db_path) as conn:
            conn.execute("DELETE FROM audit_log WHERE ts < ?", (cutoff_ts,))
            conn.commit()
    except Exception:
        return
