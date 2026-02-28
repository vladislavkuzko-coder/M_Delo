from __future__ import annotations

import os
import shutil
import datetime as _dt
from typing import Any, Dict, List, Optional

from core.attachments import ensure_local_copy_to_dir, resolve_attachment_path

from core.db import connect, create_object


def _now_iso() -> str:
    return _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def init_treasury(db_path: str) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS treasury_assets(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            inv_no TEXT DEFAULT '',
            name TEXT DEFAULT '',
            address TEXT DEFAULT '',
            cadastral TEXT DEFAULT '',
            area REAL,
            status TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            -- fields similar to `objects` (for convenience in treasury)
            object_type TEXT DEFAULT '',
            municipality TEXT DEFAULT '',
            settlement_type TEXT DEFAULT '',
            settlement TEXT DEFAULT '',
            street_type TEXT DEFAULT '',
            street TEXT DEFAULT '',
            house TEXT DEFAULT '',
            latitude REAL,
            longitude REAL,
            additional_info TEXT DEFAULT '',
            owner_unit_id INTEGER,
            created_by TEXT DEFAULT '',
            created_ts TEXT DEFAULT '',
            updated_ts TEXT DEFAULT ''
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_assets_inv ON treasury_assets(inv_no);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_assets_owner ON treasury_assets(owner_unit_id);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS treasury_docs(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            doc_type TEXT DEFAULT '',
            title TEXT DEFAULT '',
            file_path TEXT DEFAULT '',
            uploaded_ts TEXT DEFAULT '',
            FOREIGN KEY(asset_id) REFERENCES treasury_assets(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_docs_asset ON treasury_docs(asset_id);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS treasury_actions(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            action_date TEXT DEFAULT '',
            action_text TEXT DEFAULT '',
            performed_by TEXT DEFAULT '',
            created_ts TEXT DEFAULT '',
            FOREIGN KEY(asset_id) REFERENCES treasury_assets(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_actions_asset ON treasury_actions(asset_id);")

    # ---- PDF plan layers / annotations (per page) ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS treasury_plan_layers(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asset_id INTEGER NOT NULL,
            doc_id INTEGER NOT NULL,
            page_no INTEGER NOT NULL,
            data_json TEXT DEFAULT '',
            updated_ts TEXT DEFAULT '',
            UNIQUE(doc_id, page_no),
            FOREIGN KEY(asset_id) REFERENCES treasury_assets(id) ON DELETE CASCADE,
            FOREIGN KEY(doc_id) REFERENCES treasury_docs(id) ON DELETE CASCADE
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_plan_asset ON treasury_plan_layers(asset_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_treasury_plan_doc ON treasury_plan_layers(doc_id);")

    # lightweight migrations for existing dbs
    def _ensure_col(table: str, col: str, col_def: str) -> None:
        cur.execute(f"PRAGMA table_info({table});")
        cols = {r[1] for r in cur.fetchall()}
        if col not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")

    _ensure_col("treasury_assets", "owner_unit_id", "owner_unit_id INTEGER")
    _ensure_col("treasury_assets", "created_by", "created_by TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "created_ts", "created_ts TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "updated_ts", "updated_ts TEXT DEFAULT ''")

    # object-like fields (may be absent in older DBs)
    _ensure_col("treasury_assets", "object_type", "object_type TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "municipality", "municipality TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "settlement_type", "settlement_type TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "settlement", "settlement TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "street_type", "street_type TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "street", "street TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "house", "house TEXT DEFAULT ''")
    _ensure_col("treasury_assets", "latitude", "latitude REAL")
    _ensure_col("treasury_assets", "longitude", "longitude REAL")
    _ensure_col("treasury_assets", "additional_info", "additional_info TEXT DEFAULT ''")

    _ensure_col("treasury_plan_layers", "data_json", "data_json TEXT DEFAULT ''")
    _ensure_col("treasury_plan_layers", "updated_ts", "updated_ts TEXT DEFAULT ''")

    # treasury_actions -> turn into "events" (keep backward compatibility)
    _ensure_col("treasury_actions", "seq_no", "seq_no INTEGER DEFAULT 0")
    _ensure_col("treasury_actions", "event_type", "event_type TEXT DEFAULT ''")
    _ensure_col("treasury_actions", "planned_date", "planned_date TEXT DEFAULT ''")
    _ensure_col("treasury_actions", "fact_date", "fact_date TEXT DEFAULT ''")
    _ensure_col("treasury_actions", "event_no", "event_no TEXT DEFAULT ''")
    _ensure_col("treasury_actions", "extra", "extra TEXT DEFAULT ''")
    _ensure_col("treasury_actions", "corr_item_id", "corr_item_id INTEGER DEFAULT 0")

    con.commit()
    con.close()

# --- self-healing schema guard (for existing DBs where init_db was not run) ---
def ensure_treasury_schema(db_path: str) -> None:
    """Make sure treasury tables exist.

    This is intentionally cheap and safe to call from any treasury function.
    """
    try:
        con = connect(db_path, read_only=False)
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='treasury_assets';")
        ok = cur.fetchone() is not None
        con.close()
        if not ok:
            init_treasury(db_path)
    except Exception:
        # Don't crash UI on first run; the real query will raise a clearer error if DB is truly broken
        return



def get_plan_layer(db_path: str, doc_id: int, page_no: int) -> str:
    ensure_treasury_schema(db_path)
    """Return stored JSON for a PDF plan page (empty string if not found)."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT COALESCE(data_json,'') AS j
        FROM treasury_plan_layers
        WHERE doc_id=? AND page_no=?;
        """,
        (int(doc_id), int(page_no)),
    )
    r = cur.fetchone()
    con.close()
    return (r[0] if r else "") or ""


def save_plan_layer(db_path: str, *, asset_id: int, doc_id: int, page_no: int, data_json: str) -> None:
    ensure_treasury_schema(db_path)
    """Upsert stored JSON for a PDF plan page."""
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO treasury_plan_layers(asset_id, doc_id, page_no, data_json, updated_ts)
        VALUES(?,?,?,?,?)
        ON CONFLICT(doc_id, page_no) DO UPDATE SET
            data_json=excluded.data_json,
            updated_ts=excluded.updated_ts;
        """,
        (int(asset_id), int(doc_id), int(page_no), (data_json or ""), ts),
    )
    con.commit()
    con.close()


def list_assets(db_path: str, *, user: Optional[Dict[str, Any]] = None, search: str = "") -> List[Dict[str, Any]]:
    ensure_treasury_schema(db_path)
    q = (search or "").strip()
    con = connect(db_path, read_only=True)
    cur = con.cursor()

    where = []
    params: List[Any] = []

    if q:
        where.append("(COALESCE(inv_no,'') LIKE ? OR COALESCE(name,'') LIKE ? OR COALESCE(address,'') LIKE ? OR COALESCE(cadastral,'') LIKE ?)")
        like = f"%{q}%"
        params.extend([like, like, like, like])

    if user is not None:
        try:
            from core.org import get_accessible_unit_ids

            units = get_accessible_unit_ids(db_path, user)
            if units is None:
                pass
            elif len(units) == 0:
                where.append("(created_by=? OR created_by='')")
                params.append(user.get("username", "") or "")
            else:
                qs = ",".join(["?"] * len(units))
                where.append(f"(owner_unit_id IN ({qs}) OR created_by=?)")
                params.extend([int(x) for x in units])
                params.append(user.get("username", "") or "")
        except Exception:
            pass

    wsql = "WHERE " + " AND ".join(where) if where else ""
    cur.execute(
        f"""
        SELECT id, inv_no, name, address, cadastral, area, status, notes,
               owner_unit_id, created_by, created_ts, updated_ts
        FROM treasury_assets
        {wsql}
        ORDER BY id DESC;
        """,
        tuple(params),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def get_asset(db_path: str, asset_id: int) -> Optional[Dict[str, Any]]:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT * FROM treasury_assets WHERE id=?;", (int(asset_id),))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def create_asset(db_path: str, data: Dict[str, Any], *, created_by: str = "", owner_unit_id: Optional[int] = None) -> int:
    ensure_treasury_schema(db_path)
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO treasury_assets(
            inv_no,name,address,cadastral,area,status,notes,
            object_type, municipality, settlement_type, settlement,
            street_type, street, house, latitude, longitude, additional_info,
            owner_unit_id,created_by,created_ts,updated_ts
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        (
            (data.get("inv_no") or "").strip(),
            (data.get("name") or "").strip(),
            (data.get("address") or "").strip(),
            (data.get("cadastral") or "").strip(),
            data.get("area", None),
            (data.get("status") or "").strip(),
            (data.get("notes") or "").strip(),
            (data.get("object_type") or "").strip(),
            (data.get("municipality") or "").strip(),
            (data.get("settlement_type") or "").strip(),
            (data.get("settlement") or "").strip(),
            (data.get("street_type") or "").strip(),
            (data.get("street") or "").strip(),
            (data.get("house") or "").strip(),
            data.get("latitude", None),
            data.get("longitude", None),
            (data.get("additional_info") or "").strip(),
            owner_unit_id,
            (created_by or "").strip(),
            ts,
            ts,
        ),
    )
    aid = int(cur.lastrowid)
    con.commit()
    con.close()
    return aid


def update_asset(db_path: str, asset_id: int, data: Dict[str, Any]) -> None:
    ensure_treasury_schema(db_path)
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE treasury_assets
        SET inv_no=?, name=?, address=?, cadastral=?, area=?, status=?, notes=?,
            object_type=?, municipality=?, settlement_type=?, settlement=?,
            street_type=?, street=?, house=?, latitude=?, longitude=?, additional_info=?,
            updated_ts=?
        WHERE id=?;
        """,
        (
            (data.get("inv_no") or "").strip(),
            (data.get("name") or "").strip(),
            (data.get("address") or "").strip(),
            (data.get("cadastral") or "").strip(),
            data.get("area", None),
            (data.get("status") or "").strip(),
            (data.get("notes") or "").strip(),
            (data.get("object_type") or "").strip(),
            (data.get("municipality") or "").strip(),
            (data.get("settlement_type") or "").strip(),
            (data.get("settlement") or "").strip(),
            (data.get("street_type") or "").strip(),
            (data.get("street") or "").strip(),
            (data.get("house") or "").strip(),
            data.get("latitude", None),
            data.get("longitude", None),
            (data.get("additional_info") or "").strip(),
            ts,
            int(asset_id),
        ),
    )
    con.commit()
    con.close()


def delete_asset(db_path: str, asset_id: int) -> None:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM treasury_assets WHERE id=?;", (int(asset_id),))
    con.commit()
    con.close()


def list_docs(db_path: str, asset_id: int) -> List[Dict[str, Any]]:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, asset_id, doc_type, title, file_path, uploaded_ts
        FROM treasury_docs
        WHERE asset_id=?
        ORDER BY id DESC;
        """,
        (int(asset_id),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def add_doc(db_path: str, asset_id: int, *, doc_type: str, title: str, source_path: str, dest_dir: str) -> int:
    ensure_treasury_schema(db_path)
    # Все добавляемые пользователем файлы должны храниться внутри папки программы.
    # Храним *относительный* путь к корню программы, как и в других разделах.
    # dest_dir параметр оставлен для обратной совместимости (старые вызовы),
    # но фактически мы складываем документы под data/attachments/treasury/...
    # Используем ASCII-имя файла (числовое), чтобы избежать проблем QtPdf
    # на Windows при кириллице в имени файла.
    import uuid as _uuid
    import time as _time

    src_base = os.path.basename((source_path or "").replace('\\', '/'))
    _stem, ext = os.path.splitext(src_base)
    ext = ext or ""
    safe_name = f"{int(asset_id):06d}_{int(_time.time()*1000)}_{_uuid.uuid4().hex}{ext}"

    rel = ensure_local_copy_to_dir(
        db_path=db_path,
        rel_dir=f"treasury/asset_{int(asset_id):06d}",
        src_path=source_path,
        filename=safe_name,
    )
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO treasury_docs(asset_id, doc_type, title, file_path, uploaded_ts)
        VALUES(?,?,?,?,?);
        """,
        (int(asset_id), (doc_type or "").strip(), (title or "").strip(), rel, ts),
    )
    did = int(cur.lastrowid)
    con.commit()
    con.close()
    return did


def delete_doc(db_path: str, doc_id: int) -> None:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT file_path FROM treasury_docs WHERE id=?;", (int(doc_id),))
    r = cur.fetchone()
    cur.execute("DELETE FROM treasury_docs WHERE id=?;", (int(doc_id),))
    con.commit()
    con.close()
    if r and r[0]:
        try:
            p = resolve_attachment_path(db_path, str(r[0]))
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass


def list_actions(db_path: str, asset_id: int) -> List[Dict[str, Any]]:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    # Backward compatible: if DB still uses old columns, extra fields will just be empty.
    cur.execute(
        """
        SELECT id, asset_id,
               COALESCE(event_type,'') AS event_type,
               COALESCE(planned_date,'') AS planned_date,
               COALESCE(fact_date,'') AS fact_date,
               COALESCE(event_no,'') AS event_no,
               COALESCE(extra,'') AS extra,
               COALESCE(corr_item_id,0) AS corr_item_id,
               COALESCE(seq_no,0) AS seq_no,
               action_date, action_text, performed_by, created_ts
        FROM treasury_actions
        WHERE asset_id=?
        ORDER BY CASE WHEN COALESCE(seq_no,0)=0 THEN 999999 ELSE seq_no END ASC, id ASC;
        """,
        (int(asset_id),),
    )
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        # Old records: map action_date/action_text to fact_date/extra if new fields empty.
        if not (d.get('planned_date') or '').strip() and (d.get('action_date') or '').strip():
            # treat old action_date as fact date
            d['fact_date'] = (d.get('action_date') or '').strip()
        if not (d.get('extra') or '').strip() and (d.get('action_text') or '').strip():
            d['extra'] = (d.get('action_text') or '').strip()
        if not (d.get('event_type') or '').strip():
            d['event_type'] = 'Отметка'
        rows.append(d)
    con.close()
    return rows


def add_action(db_path: str, asset_id: int, *, action_date: str, action_text: str, performed_by: str) -> int:
    ensure_treasury_schema(db_path)
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO treasury_actions(asset_id, action_date, action_text, performed_by, created_ts)
        VALUES(?,?,?,?,?);
        """,
        (int(asset_id), (action_date or "").strip(), (action_text or "").strip(), (performed_by or "").strip(), ts),
    )
    aid = int(cur.lastrowid)
    con.commit()
    con.close()
    return aid


# ---- new "events" API (preferred by UI) ----
def add_event(
    db_path: str,
    asset_id: int,
    *,
    event_type: str,
    planned_date: str = "",
    fact_date: str = "",
    event_no: str = "",
    extra: str = "",
    corr_item_id: int = 0,
    performed_by: str = "",
) -> int:
    ensure_treasury_schema(db_path)
    ts = _now_iso()
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(seq_no),0)+1 AS n FROM treasury_actions WHERE asset_id=?;", (int(asset_id),))
    n = int((cur.fetchone() or {}).get('n') or 1)
    cur.execute(
        """
        INSERT INTO treasury_actions(
            asset_id, seq_no, event_type, planned_date, fact_date, event_no, extra, corr_item_id,
            performed_by, created_ts,
            action_date, action_text
        )
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        (
            int(asset_id),
            n,
            (event_type or '').strip(),
            (planned_date or '').strip(),
            (fact_date or '').strip(),
            (event_no or '').strip(),
            (extra or '').strip(),
            int(corr_item_id or 0),
            (performed_by or '').strip(),
            ts,
            # legacy fields
            (fact_date or planned_date or '').strip(),
            (extra or '').strip(),
        ),
    )
    eid = int(cur.lastrowid)
    con.commit()
    con.close()
    return eid


def update_event(
    db_path: str,
    event_id: int,
    *,
    event_type: str,
    planned_date: str = "",
    fact_date: str = "",
    event_no: str = "",
    extra: str = "",
    corr_item_id: int = 0,
) -> None:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE treasury_actions
        SET event_type=?, planned_date=?, fact_date=?, event_no=?, extra=?, corr_item_id=?,
            action_date=?, action_text=?
        WHERE id=?;
        """,
        (
            (event_type or '').strip(),
            (planned_date or '').strip(),
            (fact_date or '').strip(),
            (event_no or '').strip(),
            (extra or '').strip(),
            int(corr_item_id or 0),
            (fact_date or planned_date or '').strip(),
            (extra or '').strip(),
            int(event_id),
        ),
    )
    con.commit()
    con.close()


def delete_action(db_path: str, action_id: int) -> None:
    ensure_treasury_schema(db_path)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM treasury_actions WHERE id=?;", (int(action_id),))
    con.commit()
    con.close()


def ensure_object_for_asset(db_path: str, asset_id: int, *, created_by: str = "") -> int:
    """Создаёт (или находит) объект в таблице `objects`, связанный с объектом из реестра имущества.

    Нужно для сценария: "договор по моему имуществу".
    Чужие объекты по договорам остаются только в `objects` и в реестр не попадают.
    """
    aid = int(asset_id)
    if aid <= 0:
        raise ValueError("asset_id must be > 0")

    con = connect(db_path, read_only=False)
    cur = con.cursor()
    # 1) если уже есть связанный object
    try:
        cur.execute("SELECT id FROM objects WHERE treasury_asset_id=? LIMIT 1;", (aid,))
        r = cur.fetchone()
        if r and int(r[0]) > 0:
            con.close()
            return int(r[0])
    except Exception:
        pass

    a = get_asset(db_path, aid) or {}
    # minimal mapping: храним удобочитаемо в additional_info
    add_info = ""
    try:
        inv = (a.get("inv_no") or "").strip()
        nm = (a.get("name") or "").strip()
        addr = (a.get("address") or "").strip()
        cad = (a.get("cadastral") or "").strip()
        parts = []
        if inv:
            parts.append(f"Инв.№ {inv}")
        if nm:
            parts.append(nm)
        if addr:
            parts.append(addr)
        if cad:
            parts.append(f"Кадастр: {cad}")
        add_info = " | ".join(parts)
    except Exception:
        add_info = ""

    data = {
        "object_type": "Имущество (реестр)",
        "municipality": "",
        "settlement_type": "",
        "settlement": "",
        "street_type": "",
        "street": (a.get("address") or "")[:200],
        "house": "",
        "latitude": None,
        "longitude": None,
        "area": a.get("area", None),
        "cadastral": a.get("cadastral", "") or "",
        "additional_info": add_info,
        "treasury_asset_id": aid,
    }

    # create_object не знает про treasury_asset_id — вставим отдельно
    oid = create_object(db_path, data, created_by=created_by)
    try:
        cur.execute("UPDATE objects SET treasury_asset_id=? WHERE id=?;", (aid, int(oid)))
        con.commit()
    except Exception:
        pass
    con.close()
    return int(oid)
