from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List, Optional

from core.db import connect


def list_reminders(db_path: str, username: str, include_done: bool = False, limit: int = 200) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    if include_done:
        cur.execute(
            """
            SELECT id, username, due_date, due_time, text, done, created_ts, done_ts
            FROM reminders
            WHERE username=?
            ORDER BY COALESCE(due_date,''), id DESC
            LIMIT ?;
            """,
            (username, int(limit)),
        )
    else:
        cur.execute(
            """
            SELECT id, username, due_date, due_time, text, done, created_ts, done_ts
            FROM reminders
            WHERE username=? AND COALESCE(done,0)=0
            ORDER BY COALESCE(due_date,''), id DESC
            LIMIT ?;
            """,
            (username, int(limit)),
        )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def add_reminder(db_path: str, username: str, text: str, due_date: str = "", due_time: str = "") -> int:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    ts = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute(
        "INSERT INTO reminders(username,due_date,due_time,text,done,created_ts,done_ts) VALUES(?,?,?,?,?,?,?);",
        (username, (due_date or "").strip(), (due_time or "").strip(), (text or "").strip(), 0, ts, ""),
    )
    rid = int(cur.lastrowid or 0)
    con.commit()
    con.close()
    return rid


def update_reminder(
    db_path: str,
    rid: int,
    text: str | None = None,
    due_date: str | None = None,
    due_time: str | None = None,
    done: Optional[bool] = None,
) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    fields = []
    args: list[Any] = []
    if text is not None:
        fields.append("text=?")
        args.append((text or "").strip())
    if due_date is not None:
        fields.append("due_date=?")
        args.append((due_date or "").strip())
    if due_time is not None:
        fields.append("due_time=?")
        args.append((due_time or "").strip())
    if done is not None:
        fields.append("done=?")
        args.append(1 if bool(done) else 0)
        if bool(done):
            fields.append("done_ts=?")
            args.append(_dt.datetime.now().strftime("%Y-%m-%d %H:%M"))
    if not fields:
        con.close()
        return
    args.append(int(rid))
    cur.execute(f"UPDATE reminders SET {', '.join(fields)} WHERE id=?;", tuple(args))
    con.commit()
    con.close()


def delete_reminder(db_path: str, rid: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM reminders WHERE id=?;", (int(rid),))
    con.commit()
    con.close()


def list_done_reminders(db_path: str, username: str, limit: int = 500) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, username, due_date, due_time, text, done, created_ts, done_ts
        FROM reminders
        WHERE username=? AND COALESCE(done,0)=1
        ORDER BY COALESCE(done_ts,'' ) DESC, id DESC
        LIMIT ?;
        """,
        (username, int(limit)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows
