# core/org.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from core.db import connect


def list_units(db_path: str, active_only: bool = False) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    if active_only:
        cur.execute(
            """
            SELECT id, name, parent_id, code, manager_user_id, active, sort_order
            FROM org_units
            WHERE active=1
            ORDER BY sort_order ASC, name COLLATE NOCASE ASC;
            """
        )
    else:
        cur.execute(
            """
            SELECT id, name, parent_id, code, manager_user_id, active, sort_order
            FROM org_units
            ORDER BY active DESC, sort_order ASC, name COLLATE NOCASE ASC;
            """
        )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def ensure_root_unit(db_path: str) -> int:
    """Create a single root unit if none exist."""
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT id FROM org_units ORDER BY id ASC LIMIT 1;")
    r = cur.fetchone()
    if r:
        con.close()
        return int(r[0])
    cur.execute(
        "INSERT INTO org_units(name,parent_id,code,active,sort_order) VALUES(?,?,?,?,?);",
        ("Организация", None, "", 1, 0),
    )
    cur.execute("SELECT last_insert_rowid();")
    root_id = int(cur.fetchone()[0])
    con.commit()
    con.close()
    return root_id


def create_unit(db_path: str, name: str, parent_id: Optional[int] = None, code: str = "", manager_user_id: Optional[int] = None) -> int:
    name = (name or "").strip()
    if not name:
        raise ValueError("Пустое название подразделения.")
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO org_units(name,parent_id,code,manager_user_id,active,sort_order)
        VALUES(?,?,?,?,1,0);
        """,
        (name, parent_id, (code or "").strip(), manager_user_id),
    )
    uid = int(cur.lastrowid)
    con.commit()
    con.close()
    return uid


def update_unit(db_path: str, unit_id: int, *, name: str, parent_id: Optional[int], code: str, manager_user_id: Optional[int], active: bool, sort_order: int = 0) -> None:
    name = (name or "").strip()
    if not name:
        raise ValueError("Пустое название подразделения.")
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE org_units
        SET name=?, parent_id=?, code=?, manager_user_id=?, active=?, sort_order=?
        WHERE id=?;
        """,
        (name, parent_id, (code or "").strip(), manager_user_id, 1 if active else 0, int(sort_order or 0), int(unit_id)),
    )
    con.commit()
    con.close()


def delete_unit(db_path: str, unit_id: int) -> None:
    """Delete a unit. Children will be detached (parent_id -> NULL)."""
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("UPDATE org_units SET parent_id=NULL WHERE parent_id=?;", (int(unit_id),))
    cur.execute("UPDATE user_org SET unit_id=NULL WHERE unit_id=?;", (int(unit_id),))
    cur.execute("DELETE FROM org_units WHERE id=?;", (int(unit_id),))
    con.commit()
    con.close()


def list_users_with_unit(db_path: str) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT u.id, u.username, u.is_admin, u.active,
               COALESCE(o.unit_id, NULL) AS unit_id,
               COALESCE(o.title, '') AS title
        FROM users u
        LEFT JOIN user_org o ON o.user_id=u.id
        ORDER BY u.username COLLATE NOCASE ASC;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def set_user_unit(db_path: str, user_id: int, unit_id: Optional[int], title: str = "") -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT user_id FROM user_org WHERE user_id=?;", (int(user_id),))
    if cur.fetchone():
        cur.execute("UPDATE user_org SET unit_id=?, title=? WHERE user_id=?;", (unit_id, (title or "").strip(), int(user_id)))
    else:
        cur.execute("INSERT INTO user_org(user_id, unit_id, title) VALUES(?,?,?);", (int(user_id), unit_id, (title or "").strip()))
    con.commit()
    con.close()


def get_user_unit_id(db_path: str, user_id: int) -> Optional[int]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT unit_id FROM user_org WHERE user_id=?;", (int(user_id),))
    r = cur.fetchone()
    con.close()
    return int(r[0]) if r and r[0] is not None else None


def get_user_id_by_username(db_path: str, username: str) -> Optional[int]:
    u = (username or "").strip()
    if not u:
        return None
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT id FROM users WHERE username=?;", (u,))
    r = cur.fetchone()
    con.close()
    return int(r[0]) if r else None


def _children_map(units: List[Dict[str, Any]]) -> Dict[Optional[int], List[int]]:
    m: Dict[Optional[int], List[int]] = {}
    for u in units:
        pid = u.get("parent_id")
        m.setdefault(pid, []).append(int(u["id"]))
    return m


def get_descendant_unit_ids(db_path: str, root_unit_id: int, include_self: bool = True) -> List[int]:
    units = list_units(db_path, active_only=False)
    ch = _children_map(units)
    out: List[int] = []
    stack = [int(root_unit_id)]
    seen: Set[int] = set()
    while stack:
        u = stack.pop()
        if u in seen:
            continue
        seen.add(u)
        out.append(u)
        for c in ch.get(u, []):
            stack.append(c)
    if not include_self:
        out = [x for x in out if x != int(root_unit_id)]
    return out


def get_accessible_unit_ids(db_path: str, user: Dict[str, Any]) -> Optional[List[int]]:
    """Return list of unit ids a user can see.

    - Admin: None (means "no restriction")
    - User with assigned unit: the unit + all descendants.
    - User without unit: empty list (will be treated as "only own records" via created_by).
    """
    if int(user.get("is_admin", 0)) == 1:
        return None
    uid = user.get("id")
    if uid is None:
        return []
    unit_id = get_user_unit_id(db_path, int(uid))
    if unit_id is None:
        return []
    return get_descendant_unit_ids(db_path, unit_id, include_self=True)
