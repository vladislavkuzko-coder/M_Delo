# core/auth.py
from __future__ import annotations

import os
import hashlib
from typing import Optional, Dict, Any, List

from core.db import connect


def _hash_password(password: str, salt: str) -> str:
    return hashlib.sha256((salt + password).encode("utf-8")).hexdigest()


def _make_salt() -> str:
    return os.urandom(16).hex()


def ensure_default_admin(db_path: str):
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT id FROM users WHERE username='admin';")
    r = cur.fetchone()
    if r:
        con.close()
        return
    salt = _make_salt()
    h = _hash_password("admin", salt)
    cur.execute("""
        INSERT INTO users(username, password_hash, is_admin, is_active)
        VALUES(?,?,1,1);
    """, ("admin", f"{salt}${h}"))
    con.commit()
    con.close()


def verify_user(db_path: str, username: str, password: str) -> Optional[Dict[str, Any]]:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT id, username, password_hash, is_admin, is_active
        FROM users WHERE username=?;
    """, (username,))
    u = cur.fetchone()
    con.close()
    if not u:
        return None
    if int(u["is_active"]) != 1:
        return None
    salt, hh = str(u["password_hash"]).split("$", 1)
    if _hash_password(password, salt) != hh:
        return None
    return {"id": int(u["id"]), "username": u["username"], "is_admin": int(u["is_admin"]) == 1}


def list_users(db_path: str) -> List[Dict[str, Any]]:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT id, username, is_admin, is_active FROM users ORDER BY username COLLATE NOCASE;")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def create_user(db_path: str, username: str, password: str, is_admin: bool = False):
    username = (username or "").strip()
    if not username:
        raise ValueError("Пустой логин.")
    salt = _make_salt()
    h = _hash_password(password or "", salt)
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO users(username, password_hash, is_admin, is_active)
        VALUES(?,?,?,1);
    """, (username, f"{salt}${h}", 1 if is_admin else 0))
    con.commit()
    con.close()


def set_user_active(db_path: str, user_id: int, active: bool):
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("UPDATE users SET is_active=? WHERE id=?;", (1 if active else 0, int(user_id)))
    con.commit()
    con.close()


def set_user_admin(db_path: str, user_id: int, is_admin: bool):
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("UPDATE users SET is_admin=? WHERE id=?;", (1 if is_admin else 0, int(user_id)))
    con.commit()
    con.close()


def change_password(db_path: str, user_id: int, new_password: str):
    salt = _make_salt()
    h = _hash_password(new_password or "", salt)
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("UPDATE users SET password_hash=? WHERE id=?;", (f"{salt}${h}", int(user_id)))
    con.commit()
    con.close()


def rename_user(db_path: str, user_id: int, new_username: str):
    new_username = (new_username or "").strip()
    if not new_username:
        raise ValueError("Пустой логин.")
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("UPDATE users SET username=? WHERE id=?;", (new_username, int(user_id)))
    con.commit()
    con.close()
