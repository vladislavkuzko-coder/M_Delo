from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, List, Optional

from core.db import connect, upsert_dictionary_value
from core.numbering import next_sequence_for_year


KIND_LABELS = {
    "in": "Входящие",
    "out": "Исходящие",
    "internal": "Внутренние",
}

WORK_STATE_LABELS = {
    "in_work": "В работе",
    "done": "Исполнено",
}


def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _today_iso_date() -> str:
    return _dt.date.today().strftime("%Y-%m-%d")


def next_reg_no(db_path: str, kind: str) -> int:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(reg_no), 0) AS m FROM correspondence WHERE kind=?;", (kind,))
    m = int(cur.fetchone()["m"])
    con.close()
    return m + 1


def _extract_order_number(num: str) -> int:
    """Extract a numeric part from document number for ordering.

    Examples:
      17-07.3/123-26 -> 123
      ВН-0004 -> 4

    If nothing can be extracted, returns 0.
    """
    import re

    s = (num or "").strip()
    if not s:
        return 0
    # strip trailing year suffix like -26 or -2026
    s2 = re.sub(r"-(\d{2}|\d{4})\s*$", "", s)
    m = re.findall(r"(\d+)", s2)
    if not m:
        return 0
    try:
        return int(m[-1])
    except Exception:
        return 0


def _resequence_reg_no(con, kind: str) -> None:
    """Recompute reg_no for outgoing/internal by chronological order.

    Requirement: when user adds older letters later, the "№ п/п" (reg_no)
    must be reassigned so that earlier dates get smaller numbers.
    """
    if kind not in ("out", "internal"):
        return
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, reg_date, doc_date, doc_no, dept_reg_date, dept_reg_no
        FROM correspondence
        WHERE kind=?;
        """,
        (kind,),
    )
    rows = [dict(r) for r in cur.fetchall()]

    def _date_for(r: Dict[str, Any]):
        # Prefer explicit doc_date, then dept_reg_date, then reg_date.
        d = _parse_iso_date_yyyy_mm_dd(r.get("doc_date") or "")
        if d is None:
            d = _parse_iso_date_yyyy_mm_dd(r.get("dept_reg_date") or "")
        if d is None:
            d = _parse_iso_date_yyyy_mm_dd(r.get("reg_date") or "")
        return d or _dt.date.max

    def _num_for(r: Dict[str, Any]) -> str:
        if kind == "internal":
            return (r.get("dept_reg_no") or r.get("doc_no") or "").strip()
        return (r.get("doc_no") or "").strip()

    rows.sort(key=lambda r: (_date_for(r), _extract_order_number(_num_for(r)), _num_for(r)))
    for i, r in enumerate(rows, start=1):
        cur.execute("UPDATE correspondence SET reg_no=? WHERE id=?;", (i, int(r["id"])))


def _parse_iso_date_yyyy_mm_dd(text: str) -> Optional[_dt.date]:
    t = (text or "").strip()
    if not t:
        return None
    # Accept "YYYY-MM-DD" or "DD.MM.YYYY" (from some widgets)
    try:
        if "." in t:
            d, m, y = t.split(".")
            return _dt.date(int(y), int(m), int(d))
        return _dt.date.fromisoformat(t[:10])
    except Exception:
        return None


def _dept_prefix(kind: str) -> str:
    # legacy (kept for backward compatibility if settings are absent)
    if kind == "in":
        return "17-07.3/ВХ-"
    if kind == "internal":
        return "17-07.3/ВН-"
    return ""


def _extract_seq(dept_reg_no: str, prefix: str) -> Optional[int]:
    s = (dept_reg_no or "").strip()
    if not s or not prefix or not s.startswith(prefix):
        return None
    tail = s[len(prefix):]
    digits = "".join(ch for ch in tail if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def next_dept_reg_no(db_path: str, kind: str, dept_reg_date: str = "", cfg: Optional[Dict[str, Any]] = None) -> str:
    """Next department registration number.

    Если передан cfg (из настроек нумерации), используем его.
    Иначе — legacy-логика с фиксированным префиксом.
    """
    if isinstance(cfg, dict) and cfg.get("enabled", True):
        return next_sequence_for_year(
            db_path=db_path,
            table="correspondence",
            field="dept_reg_no",
            kind_filter=("kind", kind),
            date_field="dept_reg_date",
            date_value=dept_reg_date,
            prefix=str(cfg.get("prefix", "") or ""),
            suffix=str(cfg.get("suffix", "") or ""),
            pad=int(cfg.get("pad", 0) or 0),
            reset_per_year=bool(cfg.get("reset_per_year", True)),
        )

    # legacy fallback
    prefix = _dept_prefix(kind)
    if not prefix:
        return ""
    return next_sequence_for_year(
        db_path=db_path,
        table="correspondence",
        field="dept_reg_no",
        kind_filter=("kind", kind),
        date_field="dept_reg_date",
        date_value=dept_reg_date,
        prefix=prefix,
        suffix="",
        pad=4,
        reset_per_year=True,
    )


def next_doc_no(db_path: str, kind: str, doc_date: str = "", cfg: Optional[Dict[str, Any]] = None) -> str:
    """Next document number (used for outgoing/internal "Номер")."""
    if isinstance(cfg, dict) and cfg.get("enabled", True):
        return next_sequence_for_year(
            db_path=db_path,
            table="correspondence",
            field="doc_no",
            kind_filter=("kind", kind),
            date_field="doc_date",
            date_value=doc_date,
            prefix=str(cfg.get("prefix", "") or ""),
            suffix=str(cfg.get("suffix", "") or ""),
            pad=int(cfg.get("pad", 0) or 0),
            reset_per_year=bool(cfg.get("reset_per_year", True)),
        )
    # if no cfg — do not auto-generate
    return ""


def list_items(db_path: str, kind: str, search: str = "", user: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """List correspondence items of a kind.

    Search matches numbers/parties/content.
    """
    q = (search or "").strip()
    con = connect(db_path, read_only=True)
    cur = con.cursor()

    base_select = (
        "SELECT id, kind, reg_no, reg_date, "
        "sender, recipient, subject, notes, attachments, "
        "ref_in_no, doc_date, doc_no, in_date, in_no, out_date, out_no, "
        "status, due_date, executor, dept_reg_no, dept_reg_date, "
        "work_state, done_date, owner_unit_id, created_by "
        "FROM correspondence "
    )

    where_vis = ""
    vis_params: List[Any] = []
    if user is not None:
        try:
            from core.org import get_accessible_unit_ids

            units = get_accessible_unit_ids(db_path, user)
            if units is None:
                where_vis = ""
            elif len(units) == 0:
                where_vis = " AND (created_by=? OR created_by='')"
                vis_params.append(user.get("username", "") or "")
            else:
                qs = ",".join(["?"] * len(units))
                where_vis = f" AND (owner_unit_id IN ({qs}) OR created_by=?)"
                vis_params.extend([int(x) for x in units])
                vis_params.append(user.get("username", "") or "")
        except Exception:
            pass

    if q:
        like = f"%{q.lower()}%"
        cur.execute(
            base_select
            + f"""
            WHERE kind=?{where_vis} AND (
                LOWER(CAST(reg_no AS TEXT)) LIKE ? OR
                LOWER(COALESCE(ref_in_no,'')) LIKE ? OR
                LOWER(COALESCE(doc_no,'')) LIKE ? OR
                LOWER(COALESCE(in_no,'')) LIKE ? OR
                LOWER(COALESCE(out_no,'')) LIKE ? OR
                LOWER(COALESCE(sender,'')) LIKE ? OR
                LOWER(COALESCE(recipient,'')) LIKE ? OR
                LOWER(COALESCE(subject,'')) LIKE ? OR
                LOWER(COALESCE(notes,'')) LIKE ? OR
                LOWER(COALESCE(executor,'')) LIKE ?
            )
            ORDER BY reg_no DESC;
            """,
            # 10 LIKE placeholders in the query above
            tuple([kind] + vis_params + [like] * 10),
        )
    else:
        cur.execute(base_select + f"WHERE kind=?{where_vis} ORDER BY reg_no DESC;", tuple([kind] + vis_params))

    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def get_item(db_path: str, item_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, kind, reg_no, reg_date,
               sender, recipient, subject, notes, attachments,
               ref_in_no, doc_date, doc_no, in_date, in_no, out_date, out_no,
               status, due_date, executor, dept_reg_no, dept_reg_date,
               work_state, done_date, owner_unit_id, created_by
        FROM correspondence
        WHERE id=?;
        """,
        (int(item_id),),
    )
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


# ---- links support (chains: incoming -> outgoing/internal etc.) ----

def _parse_regno_list(text: str) -> List[int]:
    nums: List[int] = []
    t = (text or "").replace(";", ",")
    for part in t.split(","):
        p = part.strip()
        if not p:
            continue
        m = ""
        for ch in p:
            if ch.isdigit():
                m += ch
            elif m:
                break
        if m:
            try:
                nums.append(int(m))
            except Exception:
                pass
    # unique preserving order
    out: List[int] = []
    seen = set()
    for n in nums:
        if n not in seen:
            seen.add(n)
            out.append(n)
    return out


def list_all(db_path: str, search: str = "") -> List[Dict[str, Any]]:
    """Search across all kinds (used by link picker)."""
    q = (search or "").strip()
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    base_select = (
        "SELECT id, kind, reg_no, reg_date, sender, recipient, subject, notes, "
        "doc_date, doc_no, ref_in_no, in_date, in_no, out_date, out_no "
        "FROM correspondence "
    )
    if q:
        like = f"%{q.lower()}%"
        cur.execute(
            base_select
            + """
            WHERE (
                LOWER(CAST(reg_no AS TEXT)) LIKE ? OR
                LOWER(COALESCE(doc_no,'')) LIKE ? OR
                LOWER(COALESCE(in_no,'')) LIKE ? OR
                LOWER(COALESCE(out_no,'')) LIKE ? OR
                LOWER(COALESCE(ref_in_no,'')) LIKE ? OR
                LOWER(COALESCE(sender,'')) LIKE ? OR
                LOWER(COALESCE(recipient,'')) LIKE ? OR
                LOWER(COALESCE(subject,'')) LIKE ? OR
                LOWER(COALESCE(notes,'')) LIKE ?
            )
            ORDER BY updated_ts DESC, id DESC;
            """,
            (like, like, like, like, like, like, like, like, like),
        )
    else:
        cur.execute(base_select + "ORDER BY updated_ts DESC, id DESC;")
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def list_links(db_path: str, item_id: int) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.id, c.kind, c.reg_no, c.reg_date, c.sender, c.recipient, c.subject, c.notes,
               c.doc_date, c.doc_no, c.ref_in_no
        FROM correspondence_links l
        JOIN correspondence c
          ON c.id = CASE WHEN l.a_id = ? THEN l.b_id ELSE l.a_id END
        WHERE l.a_id = ? OR l.b_id = ?
        ORDER BY c.kind, c.reg_no;
        """,
        (int(item_id), int(item_id), int(item_id)),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def _set_links_in_cursor(cur, item_id: int, linked_ids: List[int]) -> None:
    """Сохранить связи, используя уже открытую транзакцию/курсор.

    Это важно, чтобы не открывать второе соединение к SQLite во время сохранения карточки
    (иначе возможна блокировка и связи silently не сохранятся).
    """
    ids: List[int] = []
    seen = set()
    for x in linked_ids or []:
        try:
            xi = int(x)
        except Exception:
            continue
        if xi <= 0 or xi == int(item_id):
            continue
        if xi not in seen:
            seen.add(xi)
            ids.append(xi)

    # Таблица связей может отсутствовать в старой БД
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS correspondence_links(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            a_id INTEGER NOT NULL,
            b_id INTEGER NOT NULL,
            relation TEXT DEFAULT '',
            created_ts TEXT DEFAULT '',
            UNIQUE(a_id, b_id),
            FOREIGN KEY(a_id) REFERENCES correspondence(id) ON DELETE CASCADE,
            FOREIGN KEY(b_id) REFERENCES correspondence(id) ON DELETE CASCADE
        );
        """
    )
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_links_a ON correspondence_links(a_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_links_b ON correspondence_links(b_id);")
    except Exception:
        pass

    cur.execute("DELETE FROM correspondence_links WHERE a_id=? OR b_id=?;", (int(item_id), int(item_id)))
    ts = _now_iso()
    for other in ids:
        a, b = (int(item_id), int(other))
        if a > b:
            a, b = b, a
        cur.execute(
            "INSERT OR IGNORE INTO correspondence_links(a_id,b_id,relation,created_ts) VALUES(?,?,?,?);",
            (a, b, "", ts),
        )


def set_links(db_path: str, item_id: int, linked_ids: List[int]) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    _set_links_in_cursor(cur, int(item_id), list(linked_ids or []))
    con.commit()
    con.close()



def _resolve_incoming_ids_by_ref(db_path: str, ref_in_no: str) -> List[int]:
    reg_nos = _parse_regno_list(ref_in_no)
    if not reg_nos:
        return []
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    out: List[int] = []
    for n in reg_nos:
        cur.execute("SELECT id FROM correspondence WHERE kind='in' AND reg_no=?;", (int(n),))
        r = cur.fetchone()
        if r:
            out.append(int(r["id"]))
    con.close()
    return out


def _sync_parties_to_dicts(db_path: str, *values: str) -> None:
    """Add parties into lender/borrower dictionaries for autocomplete."""
    for v in values:
        vv = (v or "").strip()
        if vv:
            upsert_dictionary_value(db_path, "lender", vv, active=1)
            upsert_dictionary_value(db_path, "borrower", vv, active=1)


def _normalize_work_state(v: str) -> str:
    vv = (v or "").strip().lower()
    if vv in ("done", "исполнено", "исполнен", "закрыто", "закрыт"):
        return "done"
    if vv in ("in_work", "в работе", "работа"):
        return "in_work"
    return "in_work"


def create_item(
    db_path: str,
    kind: str,
    data: Dict[str, Any],
    *,
    created_by: str = "",
    owner_unit_id: Optional[int] = None,
) -> int:
    reg_no = int(data.get("reg_no") or 0) or next_reg_no(db_path, kind)
    ts = _now_iso()

    sender = (data.get("sender") or "").strip()
    recipient = (data.get("recipient") or "").strip()
    _sync_parties_to_dicts(db_path, sender, recipient)

    # Only incoming has a work state in UI; others are always "in_work".
    work_state = _normalize_work_state(data.get("work_state") or "in_work") if kind == 'in' else 'in_work'
    done_date = (data.get("done_date") or "").strip()
    if kind == 'in' and work_state == "done" and not done_date:
        done_date = _today_iso_date()
    if kind != 'in':
        done_date = ""

    # Auto department registration numbers (incoming + internal)
    dept_reg_date = (data.get("dept_reg_date") or "").strip()
    if kind in ("in", "internal"):
        if not dept_reg_date:
            dept_reg_date = _today_iso_date()
        dept_reg_no = (data.get("dept_reg_no") or "").strip()
        if not dept_reg_no:
            dept_reg_no = next_dept_reg_no(db_path, kind, dept_reg_date)
        data = dict(data)
        data["dept_reg_no"] = dept_reg_no
        data["dept_reg_date"] = dept_reg_date

    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO correspondence(
            kind, reg_no, reg_date,
            sender, recipient,
            subject, notes, attachments,
            ref_in_no, doc_date, doc_no, in_date, in_no, out_date, out_no,
            status, due_date, executor, dept_reg_no, dept_reg_date,
            work_state, done_date,
            created_ts, updated_ts,
            owner_unit_id, created_by
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        (
            kind,
            reg_no,
            (data.get("reg_date") or "").strip(),
            sender,
            recipient,
            (data.get("subject") or "").strip(),
            (data.get("notes") or "").strip(),
            (data.get("attachments") or "").strip(),
            (data.get("ref_in_no") or "").strip(),
            (data.get("doc_date") or "").strip(),
            (data.get("doc_no") or "").strip(),
            (data.get("in_date") or "").strip(),
            (data.get("in_no") or "").strip(),
            (data.get("out_date") or "").strip(),
            (data.get("out_no") or "").strip(),
            (data.get("status") or "").strip(),
            (data.get("due_date") or "").strip(),
            (data.get("executor") or "").strip(),
            (data.get("dept_reg_no") or "").strip(),
            (data.get("dept_reg_date") or "").strip(),
            work_state,
            done_date,
            ts,
            ts,
            owner_unit_id,
            (created_by or "").strip(),
        ),
    )
    new_id = int(cur.lastrowid)

    # links
    linked_ids = list(data.get("linked_ids") or [])
    if linked_ids:
        try:
            _set_links_in_cursor(cur, int(new_id), linked_ids)
        except Exception:
            pass

    # Ensure chronological numbering for outgoing/internal.
    try:
        _resequence_reg_no(con, kind)
    except Exception:
        pass

    con.commit()
    con.close()
    return new_id


def update_item(db_path: str, item_id: int, data: Dict[str, Any]) -> None:
    ts = _now_iso()

    # Determine kind safely
    kind = (data.get("kind") or "").strip()
    if not kind:
        old = get_item(db_path, int(item_id))
        kind = (old.get("kind") if old else "") or ""

    sender = (data.get("sender") or "").strip()
    recipient = (data.get("recipient") or "").strip()
    _sync_parties_to_dicts(db_path, sender, recipient)

    work_state = _normalize_work_state(data.get("work_state") or "in_work") if kind == 'in' else 'in_work'
    done_date = (data.get("done_date") or "").strip()
    if kind == 'in' and work_state == "done" and not done_date:
        done_date = _today_iso_date()
    if kind != 'in':
        done_date = ""

    # Auto department registration numbers (incoming + internal)
    if kind in ("in", "internal"):
        dept_reg_date = (data.get("dept_reg_date") or "").strip() or _today_iso_date()
        dept_reg_no = (data.get("dept_reg_no") or "").strip()
        if not dept_reg_no:
            dept_reg_no = next_dept_reg_no(db_path, kind, dept_reg_date)
        data = dict(data)
        data["dept_reg_no"] = dept_reg_no
        data["dept_reg_date"] = dept_reg_date

    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE correspondence
        SET reg_no=?, reg_date=?,
            sender=?, recipient=?,
            subject=?, notes=?, attachments=?,
            ref_in_no=?, doc_date=?, doc_no=?, in_date=?, in_no=?, out_date=?, out_no=?,
            status=?, due_date=?, executor=?, dept_reg_no=?, dept_reg_date=?,
            work_state=?, done_date=?,
            updated_ts=?
        WHERE id=?;
        """,
        (
            int(data.get("reg_no") or 0),
            (data.get("reg_date") or "").strip(),
            sender,
            recipient,
            (data.get("subject") or "").strip(),
            (data.get("notes") or "").strip(),
            (data.get("attachments") or "").strip(),
            (data.get("ref_in_no") or "").strip(),
            (data.get("doc_date") or "").strip(),
            (data.get("doc_no") or "").strip(),
            (data.get("in_date") or "").strip(),
            (data.get("in_no") or "").strip(),
            (data.get("out_date") or "").strip(),
            (data.get("out_no") or "").strip(),
            (data.get("status") or "").strip(),
            (data.get("due_date") or "").strip(),
            (data.get("executor") or "").strip(),
            (data.get("dept_reg_no") or "").strip(),
            (data.get("dept_reg_date") or "").strip(),
            work_state,
            done_date,
            ts,
            int(item_id),
        ),
    )

    # links
    linked_ids = list(data.get("linked_ids") or [])
    try:
        _set_links_in_cursor(cur, int(item_id), linked_ids)
    except Exception:
        pass

    con.commit()
    try:
        _resequence_reg_no(con, kind)
        con.commit()
    except Exception:
        pass
    con.close()


def delete_item(db_path: str, item_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    kind = ""
    try:
        cur.execute("SELECT kind FROM correspondence WHERE id=?;", (int(item_id),))
        r = cur.fetchone()
        kind = (r["kind"] if r else "") or ""
    except Exception:
        kind = ""
    cur.execute("DELETE FROM correspondence WHERE id=?;", (int(item_id),))
    try:
        _resequence_reg_no(con, kind)
    except Exception:
        pass
    con.commit()
    con.close()


# ---- analytics helpers (dashboard / control) ----

def count_incoming_overdue(db_path: str, executor: str | None = None) -> int:
    """Count incoming letters in work that are overdue by due_date."""
    today = _today_iso_date()
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    ex = (executor or "").strip()
    if ex:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM correspondence
            WHERE kind='in'
              AND COALESCE(work_state,'in_work')!='done'
              AND COALESCE(due_date,'')!=''
              AND due_date < ?
              AND TRIM(COALESCE(executor,'')) = TRIM(?);
            """,
            (today, ex),
        )
    else:
        cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM correspondence
        WHERE kind='in'
          AND COALESCE(work_state,'in_work')!='done'
          AND COALESCE(due_date,'')!=''
          AND due_date < ?;
        """,
        (today,),
        )
    c = int(cur.fetchone()["c"])
    con.close()
    return c


def count_incoming_due_soon(db_path: str, days: int = 7, executor: str | None = None) -> int:
    """Count incoming letters in work with due_date within N days (including today)."""
    days = int(days or 0)
    if days <= 0:
        return 0
    today = _dt.date.today()
    end = (today + _dt.timedelta(days=days)).strftime("%Y-%m-%d")
    start = today.strftime("%Y-%m-%d")
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    ex = (executor or "").strip()
    if ex:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM correspondence
            WHERE kind='in'
              AND COALESCE(work_state,'in_work')!='done'
              AND COALESCE(due_date,'')!=''
              AND due_date >= ?
              AND due_date <= ?
              AND TRIM(COALESCE(executor,'')) = TRIM(?);
            """,
            (start, end, ex),
        )
    else:
        cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM correspondence
        WHERE kind='in'
          AND COALESCE(work_state,'in_work')!='done'
          AND COALESCE(due_date,'')!=''
          AND due_date >= ?
          AND due_date <= ?;
        """,
        (start, end),
        )
    c = int(cur.fetchone()["c"])
    con.close()
    return c


def count_incoming_in_work(db_path: str, executor: str | None = None) -> int:
    """Count incoming letters that are not done."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    ex = (executor or "").strip()
    if ex:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM correspondence
            WHERE kind='in'
              AND COALESCE(work_state,'in_work')!='done'
              AND TRIM(COALESCE(executor,'')) = TRIM(?);
            """,
            (ex,),
        )
    else:
        cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM correspondence
            WHERE kind='in'
              AND COALESCE(work_state,'in_work')!='done';
            """
        )
    c = int(cur.fetchone()["c"])
    con.close()
    return c
