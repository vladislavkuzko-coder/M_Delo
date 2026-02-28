# core/users.py
from __future__ import annotations

from typing import List, Dict, Any, Optional
from core.db import connect


def list_users(db_path: str) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT id, username, is_admin, can_edit, active
        FROM users
        ORDER BY is_admin DESC, username COLLATE NOCASE ASC;
    """)
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def create_user(db_path: str, username: str, password: str, is_admin: int = 0, active: int = 1, can_edit: int = 0) -> None:
    u = (username or "").strip()
    p = (password or "").strip()
    if not u or not p:
        raise ValueError("Логин и пароль не могут быть пустыми.")
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        "INSERT INTO users(username,password,is_admin,can_edit,active) VALUES(?,?,?,?,?);",
        (u, p, int(is_admin), int(can_edit) or (1 if int(is_admin) else 0), int(active)),
    )
    con.commit()
    con.close()


def delete_user(db_path: str, user_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM users WHERE id=?;", (int(user_id),))
    con.commit()
    con.close()


def set_user_active(db_path: str, user_id: int, active: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("UPDATE users SET active=? WHERE id=?;", (int(active), int(user_id)))
    con.commit()
    con.close()


def set_user_can_edit(db_path: str, user_id: int, can_edit: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("UPDATE users SET can_edit=? WHERE id=?;", (int(can_edit), int(user_id)))
    # админ всегда с правом редактирования
    try:
        cur.execute("UPDATE users SET can_edit=1 WHERE id=? AND is_admin=1;", (int(user_id),))
    except Exception:
        pass
    con.commit()
    con.close()


def update_user_credentials(db_path: str, user_id: int, username: str, password: str) -> None:
    u = (username or "").strip()
    p = (password or "").strip()
    if not u or not p:
        raise ValueError("Логин и пароль не могут быть пустыми.")
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("UPDATE users SET username=?, password=? WHERE id=?;", (u, p, int(user_id)))
    con.commit()
    con.close()


def get_user_id(db_path: str, username: str) -> Optional[int]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT id FROM users WHERE username=?;", ((username or "").strip(),))
    r = cur.fetchone()
    con.close()
    return int(r["id"]) if r else None
