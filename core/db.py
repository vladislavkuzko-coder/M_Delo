# core/db.py
from __future__ import annotations

import os
import sqlite3
from typing import Any, Dict, List, Optional, Tuple


def connect(db_path: str, read_only: bool = False) -> sqlite3.Connection:
    """Open SQLite connection with safer defaults.

    - RW connections use WAL + NORMAL sync to reduce corruption risk
      and improve resilience to power loss.
    - busy_timeout makes concurrent access smoother.

    Notes (Windows):
    SQLite URI paths should use forward slashes. We normalize the path for
    read-only connections to avoid `unable to open database file` on Windows.
    """
    if read_only:
        abs_path = os.path.abspath(db_path).replace('\\', '/')
        uri = f"file:{abs_path}?mode=ro"
        con = sqlite3.connect(uri, uri=True, timeout=5.0)
    else:
        con = sqlite3.connect(db_path, timeout=5.0)

    con.row_factory = sqlite3.Row

    try:
        con.execute("PRAGMA foreign_keys=ON;")
        con.execute("PRAGMA busy_timeout=5000;")
        if not read_only:
            con.execute("PRAGMA journal_mode=WAL;")
            con.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass

    return con


def init_db(db_path: str) -> None:
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("PRAGMA foreign_keys=ON;")

    # users
    # NOTE: Simple (username/password) auth.
    # can_edit позволяет дать права редактирования нескольким пользователям,
    # не делая их администраторами.
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password TEXT NOT NULL,
        is_admin INTEGER NOT NULL DEFAULT 0,
        can_edit INTEGER NOT NULL DEFAULT 0,
        active INTEGER NOT NULL DEFAULT 1
    );
    """)
    # default admin
    cur.execute("SELECT COUNT(*) AS c FROM users;")
    if int(cur.fetchone()["c"]) == 0:
        cur.execute("INSERT INTO users(username,password,is_admin,active) VALUES('admin','admin',1,1);")

    # migrations: older DBs may not have can_edit
    try:
        cur.execute("PRAGMA table_info(users);")
        cols = {r[1] for r in cur.fetchall()}
        if "can_edit" not in cols:
            cur.execute("ALTER TABLE users ADD COLUMN can_edit INTEGER NOT NULL DEFAULT 0;")
        # админы всегда могут редактировать
        cur.execute("UPDATE users SET can_edit=1 WHERE is_admin=1;")
    except Exception:
        pass

    # dictionaries
    cur.execute("""
    CREATE TABLE IF NOT EXISTS dictionary(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        dict_type TEXT NOT NULL,
        value TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1,
        sort_order INTEGER NOT NULL DEFAULT 0,
        UNIQUE(dict_type, value)
    );
    """)

    # migrations: older DBs may not have sort_order
    try:
        cur.execute("PRAGMA table_info(dictionary);")
        cols = {r[1] for r in cur.fetchall()}
        if "sort_order" not in cols:
            cur.execute("ALTER TABLE dictionary ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    # contracts
    cur.execute("""
    CREATE TABLE IF NOT EXISTS contracts(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        status TEXT DEFAULT '',
        lender TEXT DEFAULT '',
        borrower TEXT DEFAULT '',
        contract_number TEXT DEFAULT '',
        start_date TEXT DEFAULT '',
        end_date TEXT DEFAULT '',
        executor TEXT DEFAULT '',
        role TEXT DEFAULT '',
        notes TEXT DEFAULT '',
        owner_unit_id INTEGER,
        created_by TEXT DEFAULT '',
        row_version INTEGER NOT NULL DEFAULT 0
    );
    """)

    # ---- org structure (unlimited levels) ----
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS org_units(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            parent_id INTEGER,
            code TEXT DEFAULT '',
            manager_user_id INTEGER,
            active INTEGER NOT NULL DEFAULT 1,
            sort_order INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY(parent_id) REFERENCES org_units(id) ON DELETE SET NULL,
            FOREIGN KEY(manager_user_id) REFERENCES users(id) ON DELETE SET NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_org_units_parent ON org_units(parent_id);")
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_org_units_name_parent ON org_units(parent_id, name);")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_org(
            user_id INTEGER PRIMARY KEY,
            unit_id INTEGER,
            title TEXT DEFAULT '',
            FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
            FOREIGN KEY(unit_id) REFERENCES org_units(id) ON DELETE SET NULL
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_user_org_unit ON user_org(unit_id);")

    # Ensure at least one root unit exists (for convenient setup)
    try:
        cur.execute("SELECT id FROM org_units ORDER BY id ASC LIMIT 1;")
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO org_units(name,parent_id,code,manager_user_id,active,sort_order) VALUES(?,?,?,?,?,?);",
                ("Организация", None, "", None, 1, 0),
            )
    except Exception:
        pass

    # notes / reminders (for "Личный кабинет")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS reminders(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT NOT NULL,
            due_date TEXT DEFAULT '',
            due_time TEXT DEFAULT '',
            text TEXT DEFAULT '',
            done INTEGER NOT NULL DEFAULT 0,
            created_ts TEXT DEFAULT '',
            done_ts TEXT DEFAULT ''
        );
        """
    )

    # objects
    cur.execute("""
    CREATE TABLE IF NOT EXISTS objects(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        object_type TEXT DEFAULT '',
        municipality TEXT DEFAULT '',
        settlement_type TEXT DEFAULT '',
        settlement TEXT DEFAULT '',
        street_type TEXT DEFAULT '',
        street TEXT DEFAULT '',
        house TEXT DEFAULT '',
        latitude REAL,
        longitude REAL,
        area REAL,
        cadastral TEXT DEFAULT '',
        additional_info TEXT DEFAULT '',
        owner_unit_id INTEGER,
        created_by TEXT DEFAULT '',
        treasury_asset_id INTEGER,
        row_version INTEGER NOT NULL DEFAULT 0
    );
    """)

    # ---- lightweight migrations ----
    # Older DBs may not have latitude/longitude.
    try:
        cur.execute("PRAGMA table_info(objects);")
        cols = {r[1] for r in cur.fetchall()}
        if "latitude" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN latitude REAL;")
        if "longitude" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN longitude REAL;")
        if "owner_unit_id" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN owner_unit_id INTEGER;")
        if "created_by" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN created_by TEXT DEFAULT ''; ")
        if "treasury_asset_id" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN treasury_asset_id INTEGER;")
        if "row_version" not in cols:
            cur.execute("ALTER TABLE objects ADD COLUMN row_version INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    # contracts: older DBs may not have org columns
    try:
        cur.execute("PRAGMA table_info(contracts);")
        cols = {r[1] for r in cur.fetchall()}
        if "owner_unit_id" not in cols:
            cur.execute("ALTER TABLE contracts ADD COLUMN owner_unit_id INTEGER;")
        if "created_by" not in cols:
            cur.execute("ALTER TABLE contracts ADD COLUMN created_by TEXT DEFAULT ''; ")
        if "row_version" not in cols:
            cur.execute("ALTER TABLE contracts ADD COLUMN row_version INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    # reminders: older DBs may not have due_time/done_ts
    try:
        cur.execute("PRAGMA table_info(reminders);")
        cols = {r[1] for r in cur.fetchall()}
        if "due_time" not in cols:
            cur.execute("ALTER TABLE reminders ADD COLUMN due_time TEXT DEFAULT ''; ")
        if "done_ts" not in cols:
            cur.execute("ALTER TABLE reminders ADD COLUMN done_ts TEXT DEFAULT ''; ")
    except Exception:
        pass

    # link contract_objects
    cur.execute("""
    CREATE TABLE IF NOT EXISTS contract_objects(
        contract_id INTEGER NOT NULL,
        object_id INTEGER NOT NULL,
        PRIMARY KEY(contract_id, object_id),
        FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE,
        FOREIGN KEY(object_id) REFERENCES objects(id) ON DELETE CASCADE
    );
    """)

    # stages (этапы)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS stages(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_id INTEGER NOT NULL,
        seq_no INTEGER NOT NULL,
        name TEXT NOT NULL,
        info TEXT DEFAULT '',
        stage_date TEXT DEFAULT '',
        stage_no TEXT DEFAULT '',
        extra TEXT DEFAULT '',
        corr_item_id INTEGER DEFAULT 0,
        FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE
    );
    """)

    # migrations: older DBs may not have extra columns
    try:
        cur.execute("PRAGMA table_info(stages);")
        cols = {r[1] for r in cur.fetchall()}
        if "stage_date" not in cols:
            cur.execute("ALTER TABLE stages ADD COLUMN stage_date TEXT DEFAULT '';")
        if "stage_no" not in cols:
            cur.execute("ALTER TABLE stages ADD COLUMN stage_no TEXT DEFAULT '';")
        if "extra" not in cols:
            cur.execute("ALTER TABLE stages ADD COLUMN extra TEXT DEFAULT '';")
        if "corr_item_id" not in cols:
            cur.execute("ALTER TABLE stages ADD COLUMN corr_item_id INTEGER DEFAULT 0;")
    except Exception:
        pass

    # inspections (осмотры/проверки) — одна таблица: план + факт
    cur.execute("""
    CREATE TABLE IF NOT EXISTS inspections(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        contract_id INTEGER NOT NULL,
        object_id INTEGER,
        planned_date TEXT DEFAULT '',
        inspection_date TEXT DEFAULT '',
        result TEXT DEFAULT '',
        act_path TEXT DEFAULT '',
        photos_dir TEXT DEFAULT '',
        next_date TEXT DEFAULT '',
        row_version INTEGER NOT NULL DEFAULT 0,
        FOREIGN KEY(contract_id) REFERENCES contracts(id) ON DELETE CASCADE,
        FOREIGN KEY(object_id) REFERENCES objects(id) ON DELETE SET NULL
    );
    """)

    # migrations: older DBs may not have row_version
    try:
        cur.execute("PRAGMA table_info(inspections);")
        cols = {r[1] for r in cur.fetchall()}
        if "row_version" not in cols:
            cur.execute("ALTER TABLE inspections ADD COLUMN row_version INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass

    # audit log (журнал действий)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            username TEXT NOT NULL,
            action TEXT NOT NULL,
            entity TEXT NOT NULL,
            entity_id INTEGER,
            summary TEXT DEFAULT '',
            payload_json TEXT DEFAULT ''
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_audit_entity ON audit_log(entity, entity_id);")

    # correspondence registry (реестр корреспонденции)
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS correspondence(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kind TEXT NOT NULL,                 -- 'in' | 'out' | 'internal'
            reg_no INTEGER NOT NULL,            -- № п/п по виду (вх/исх/вн)
            reg_date TEXT DEFAULT '',           -- универсальная дата (по умолчанию)
            sender TEXT DEFAULT '',             -- отправитель (для входящих)
            recipient TEXT DEFAULT '',          -- получатель (для исходящих/внутренних)
            subject TEXT DEFAULT '',            -- содержание/тема
            notes TEXT DEFAULT '',              -- примечания
            attachments TEXT DEFAULT '',        -- пути к вложениям (строка)

            -- дополнительные поля под логику Excel-реестра
            ref_in_no TEXT DEFAULT '',          -- № п/п входящего (для исх/вн)
            doc_date TEXT DEFAULT '',           -- дата документа
            doc_no TEXT DEFAULT '',             -- номер документа
            in_date TEXT DEFAULT '',            -- дата входящая (для вх)
            in_no TEXT DEFAULT '',              -- номер входящий (для вх)
            out_date TEXT DEFAULT '',           -- дата исходящая (для вх)
            out_no TEXT DEFAULT '',             -- номер исходящий (для вх)
            status TEXT DEFAULT '',             -- статус (для вх)
            due_date TEXT DEFAULT '',           -- срок/срок ответа
            executor TEXT DEFAULT '',           -- исполнитель
            dept_reg_no TEXT DEFAULT '',        -- регистрационный номер внутри отдела
            dept_reg_date TEXT DEFAULT '',      -- дата регистрации внутри отдела
            created_ts TEXT DEFAULT '',
            updated_ts TEXT DEFAULT '',
            row_version INTEGER NOT NULL DEFAULT 0
        );
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_kind_no ON correspondence(kind, reg_no);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_kind_date ON correspondence(kind, reg_date);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_kind_docno ON correspondence(kind, doc_no);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_kind_sender ON correspondence(kind, sender);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_kind_recipient ON correspondence(kind, recipient);")

    # migrations: older DBs may not have row_version
    try:
        cur.execute("PRAGMA table_info(correspondence);")
        cols = {r[1] for r in cur.fetchall()}
        if "row_version" not in cols:
            cur.execute("ALTER TABLE correspondence ADD COLUMN row_version INTEGER NOT NULL DEFAULT 0;")
    except Exception:
        pass


# links between correspondence items (for tracking chains: incoming -> outgoing/internal etc.)
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
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_links_a ON correspondence_links(a_id);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_corr_links_b ON correspondence_links(b_id);")

    # ---- migrations for existing DBs: add missing columns (if DB was created with older schema) ----
    def _ensure_col(table: str, col: str, col_def: str) -> None:
        cur.execute(f"PRAGMA table_info({table});")
        cols = {r[1] for r in cur.fetchall()}
        if col not in cols:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col_def};")

    _ensure_col("correspondence", "ref_in_no", "ref_in_no TEXT DEFAULT ''")
    _ensure_col("correspondence", "doc_date", "doc_date TEXT DEFAULT ''")
    _ensure_col("correspondence", "doc_no", "doc_no TEXT DEFAULT ''")
    _ensure_col("correspondence", "in_date", "in_date TEXT DEFAULT ''")
    _ensure_col("correspondence", "in_no", "in_no TEXT DEFAULT ''")
    _ensure_col("correspondence", "out_date", "out_date TEXT DEFAULT ''")
    _ensure_col("correspondence", "out_no", "out_no TEXT DEFAULT ''")
    _ensure_col("correspondence", "status", "status TEXT DEFAULT ''")
    _ensure_col("correspondence", "due_date", "due_date TEXT DEFAULT ''")
    _ensure_col("correspondence", "executor", "executor TEXT DEFAULT ''")
    _ensure_col("correspondence", "dept_reg_no", "dept_reg_no TEXT DEFAULT ''")
    _ensure_col("correspondence", "dept_reg_date", "dept_reg_date TEXT DEFAULT ''")
    _ensure_col("correspondence", "work_state", "work_state TEXT DEFAULT 'in_work'")
    _ensure_col("correspondence", "done_date", "done_date TEXT DEFAULT ''")

    # correspondence visibility (отдел/иерархия)
    _ensure_col("correspondence", "owner_unit_id", "owner_unit_id INTEGER")
    _ensure_col("correspondence", "created_by", "created_by TEXT DEFAULT ''")

    # treasury registry (имущество казны)
    try:
        from core.treasury import init_treasury

        init_treasury(db_path)
    except Exception:
        # keep DB init resilient
        pass

    con.commit()
    con.close()


# ---- auth ----


class ConcurrencyError(RuntimeError):
    """Raised when an optimistic-lock update fails (record was changed by someone else)."""

def get_user_by_credentials(db_path: str, username: str, password: str) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT id, username, is_admin, can_edit, active
        FROM users
        WHERE username=? AND password=? AND active=1;
    """, ((username or "").strip(), (password or "").strip()))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


# ---- dictionaries ----
def list_dictionary_items(db_path: str, dict_type: str) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT id, value, active, sort_order
        FROM dictionary
        WHERE dict_type=?
        ORDER BY sort_order ASC, value COLLATE NOCASE ASC;
    """, (dict_type,))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


# Backward-compatible helper (used by UI code)
def get_dictionary_values(db_path: str, dict_type: str, active_only: bool = True) -> List[str]:
    """Return dictionary values for a given type.

    Some UI modules expect this name/signature.
    - active_only=True  -> only active=1
    - active_only=False -> all records
    """
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    if active_only:
        cur.execute(
            """
            SELECT value
            FROM dictionary
            WHERE dict_type=? AND active=1
            ORDER BY sort_order ASC, value COLLATE NOCASE ASC;
            """,
            (dict_type,),
        )
    else:
        cur.execute(
            """
            SELECT value
            FROM dictionary
            WHERE dict_type=?
            ORDER BY sort_order ASC, value COLLATE NOCASE ASC;
            """,
            (dict_type,),
        )
    values = [r["value"] for r in cur.fetchall() if (r["value"] or "").strip()]
    con.close()
    return values


def get_counterparty_values(db_path: str, active_only: bool = True) -> List[str]:
    """Return unique counterparty names for autocomplete.

    We keep existing dictionaries ("lender"/"borrower") intact so that
    "Показать договоры" по ним не ломался. For convenience, this returns the
    UNION of values from both types.
    """
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    if active_only:
        cur.execute(
            """
            SELECT DISTINCT value
            FROM dictionary
            WHERE dict_type IN ('lender','borrower') AND active=1
            ORDER BY value COLLATE NOCASE ASC;
            """
        )
    else:
        cur.execute(
            """
            SELECT DISTINCT value
            FROM dictionary
            WHERE dict_type IN ('lender','borrower')
            ORDER BY value COLLATE NOCASE ASC;
            """
        )
    values = [r["value"] for r in cur.fetchall() if (r["value"] or "").strip()]
    con.close()
    return values


def get_correspondence_party_values(db_path: str) -> List[str]:
    """Return distinct sender/recipient values from correspondence table (for autocomplete)."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    vals: set[str] = set()
    try:
        cur.execute("SELECT DISTINCT COALESCE(sender,'') AS v FROM correspondence WHERE COALESCE(sender,'')!='';")
        vals.update({(r[0] if not isinstance(r, dict) else r.get('v') or '').strip() for r in cur.fetchall()})
        cur.execute("SELECT DISTINCT COALESCE(recipient,'') AS v FROM correspondence WHERE COALESCE(recipient,'')!='';")
        vals.update({(r[0] if not isinstance(r, dict) else r.get('v') or '').strip() for r in cur.fetchall()})
    except Exception:
        pass
    con.close()
    return sorted({v for v in vals if v})


def upsert_dictionary_value(db_path: str, dict_type: str, value: str, active: int = 1) -> None:
    v = (value or "").strip()
    if not v:
        return
    def _do(cur, dtyp: str) -> None:
        # Если записи ещё нет — выставляем sort_order в конец списка.
        cur.execute("SELECT COALESCE(MAX(sort_order),0)+1 AS n FROM dictionary WHERE dict_type=?;", (dtyp,))
        n = int(cur.fetchone()["n"])
        cur.execute(
            """
            INSERT INTO dictionary(dict_type,value,active,sort_order)
            VALUES(?,?,?,?)
            ON CONFLICT(dict_type,value) DO UPDATE SET active=excluded.active;
            """,
            (dtyp, v, int(active), n),
        )

    con = connect(db_path, read_only=False)
    cur = con.cursor()

    # Единый список организаций: ссудодатели/ссудополучатели должны совпадать.
    # Поэтому добавление в один справочник автоматически добавляет в другой.
    if dict_type in ("lender", "borrower"):
        _do(cur, "lender")
        _do(cur, "borrower")
    else:
        _do(cur, dict_type)

    con.commit()
    con.close()


def set_dictionary_active(db_path: str, item_id: int, active: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("UPDATE dictionary SET active=? WHERE id=?;", (int(active), int(item_id)))
    con.commit()
    con.close()


def delete_dictionary_item(db_path: str, item_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM dictionary WHERE id=?;", (int(item_id),))
    con.commit()
    con.close()


def swap_dictionary_order(db_path: str, id1: int, id2: int) -> None:
    """Swap sort_order between two dictionary rows."""
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT id, sort_order FROM dictionary WHERE id IN (?,?);", (int(id1), int(id2)))
    rows = {int(r["id"]): int(r["sort_order"]) for r in cur.fetchall()}
    if int(id1) not in rows or int(id2) not in rows:
        con.close()
        return
    o1, o2 = rows[int(id1)], rows[int(id2)]
    cur.execute("UPDATE dictionary SET sort_order=? WHERE id=?;", (o2, int(id1)))
    cur.execute("UPDATE dictionary SET sort_order=? WHERE id=?;", (o1, int(id2)))
    con.commit()
    con.close()


def rename_dictionary_item(db_path: str, item_id: int, new_value: str) -> None:
    """Rename dictionary row by id.

    We keep sort_order and active as-is. If the new value already exists for the
    same dict_type, we merge: existing row is activated, current row is removed.
    """
    v = (new_value or "").strip()
    if not v:
        return
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT id, dict_type, active, sort_order FROM dictionary WHERE id=?;", (int(item_id),))
    row = cur.fetchone()
    if not row:
        con.close()
        return
    dict_type = row["dict_type"]
    active = int(row["active"])
    sort_order = int(row["sort_order"])

    # If another row with same dict_type/value exists — merge.
    cur.execute(
        "SELECT id, active, sort_order FROM dictionary WHERE dict_type=? AND value=? AND id<>?;",
        (dict_type, v, int(item_id)),
    )
    other = cur.fetchone()
    if other:
        other_id = int(other["id"])
        other_active = int(other["active"])
        # prefer earlier sort_order (smaller) to keep list stable
        other_sort = int(other["sort_order"])
        cur.execute(
            "UPDATE dictionary SET active=?, sort_order=? WHERE id=?;",
            (max(active, other_active), min(sort_order, other_sort), other_id),
        )
        cur.execute("DELETE FROM dictionary WHERE id=?;", (int(item_id),))
        con.commit()
        con.close()
        return

    cur.execute("UPDATE dictionary SET value=? WHERE id=?;", (v, int(item_id)))
    con.commit()
    con.close()


# ---- contracts CRUD ----
def list_contracts(db_path: str, user: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """List contracts with optional org-based visibility."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()

    where = ""
    params: List[Any] = []
    if user is not None:
        try:
            from core.org import get_accessible_unit_ids

            units = get_accessible_unit_ids(db_path, user)
            if units is None:
                where = ""
            elif len(units) == 0:
                # If org-structure is not configured for the user (no unit assigned),
                # do NOT hide all contracts. In practice this caused "empty" lists
                # for regular users while many legacy records have created_by=''.
                # Contracts are considered shared by default.
                where = ""
            else:
                qs = ",".join(["?"] * len(units))
                where = f"WHERE (owner_unit_id IN ({qs}) OR created_by=?)"
                params.extend([int(x) for x in units])
                params.append(user.get("username", "") or "")
        except Exception:
            pass

    cur.execute(
        f"""
        SELECT id,status,lender,borrower,contract_number,start_date,end_date,executor,role,notes,
               owner_unit_id, created_by
        FROM contracts
        {where}
        ORDER BY id ASC;
        """,
        tuple(params),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def get_contract(db_path: str, contract_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT id,status,lender,borrower,contract_number,start_date,end_date,executor,role,notes,
               owner_unit_id, created_by, row_version
        FROM contracts WHERE id=?;
    """, (int(contract_id),))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def create_contract(db_path: str, *, created_by: str = "", owner_unit_id: Optional[int] = None) -> int:
    # создаём только при "Сохранить" (чтобы не было дыр)
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO contracts(status,lender,borrower,contract_number,start_date,end_date,executor,role,notes,owner_unit_id,created_by)
        VALUES('ЧЕРНОВИК','','','','','','','','',?,?);
        """,
        (owner_unit_id, (created_by or "").strip()),
    )
    cur.execute("SELECT last_insert_rowid();")
    cid = int(cur.fetchone()[0])
    con.commit()
    con.close()
    return cid


def update_contract(db_path: str, contract_id: int, data: Dict[str, Any]) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    expected_ver = data.get("_row_version", None)
    if expected_ver is None:
        # backward compatibility
        cur.execute(
            """
            UPDATE contracts
            SET status=?, lender=?, borrower=?, contract_number=?, start_date=?, end_date=?, executor=?, role=?, notes=?
            WHERE id=?;
            """,
            (
                data.get("status", "") or "",
                data.get("lender", "") or "",
                data.get("borrower", "") or "",
                data.get("contract_number", "") or "",
                data.get("start_date", "") or "",
                data.get("end_date", "") or "",
                data.get("executor", "") or "",
                data.get("role", "") or "",
                data.get("notes", "") or "",
                int(contract_id),
            ),
        )
    else:
        cur.execute(
            """
            UPDATE contracts
            SET status=?, lender=?, borrower=?, contract_number=?, start_date=?, end_date=?, executor=?, role=?, notes=?,
                row_version=row_version+1
            WHERE id=? AND row_version=?;
            """,
            (
                data.get("status", "") or "",
                data.get("lender", "") or "",
                data.get("borrower", "") or "",
                data.get("contract_number", "") or "",
                data.get("start_date", "") or "",
                data.get("end_date", "") or "",
                data.get("executor", "") or "",
                data.get("role", "") or "",
                data.get("notes", "") or "",
                int(contract_id),
                int(expected_ver),
            ),
        )
        if cur.rowcount == 0:
            con.rollback()
            con.close()
            raise ConcurrencyError(
                "Карточка договора была изменена другим пользователем. Обновите карточку и попробуйте снова."
            )
    con.commit()
    con.close()


def delete_contract(db_path: str, contract_id: int) -> None:
    # ВАЖНО: без дыр "назад" — это невозможно в SQLite autoincrement,
    # но ты просил «без дыр»: мы обеспечиваем это тем, что ID создаётся только при сохранении.
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM contracts WHERE id=?;", (int(contract_id),))
    con.commit()
    con.close()


def list_contracts_by_party(db_path: str, field: str, value: str) -> List[Dict[str, Any]]:
    # field: lender/borrower/executor
    if field not in ("lender", "borrower", "executor"):
        return []
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(f"""
        SELECT id,status,lender,borrower,contract_number,start_date,end_date,executor
        FROM contracts
        WHERE {field}=? 
        ORDER BY id ASC;
    """, ((value or "").strip(),))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def list_contracts_by_object(db_path: str, object_id: int | None) -> List[Dict[str, Any]]:
    if not object_id:
        return []
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.id,c.status,c.lender,c.borrower,c.contract_number,c.start_date,c.end_date,c.executor
        FROM contracts c
        JOIN contract_objects co ON co.contract_id=c.id
        WHERE co.object_id=?
        ORDER BY c.id ASC;
        """,
        (int(object_id),),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


# ---- helpers for map / analytics ----
def list_object_ids_for_contract(db_path: str, contract_id: int) -> List[int]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT object_id FROM contract_objects WHERE contract_id=? ORDER BY object_id ASC;", (int(contract_id),))
    ids = [int(r[0]) for r in cur.fetchall()]
    con.close()
    return ids


def list_object_names_for_contract(db_path: str, contract_id: int) -> List[str]:
    """Список адресов объектов для договора.

    Используется в осмотрах/диалогах выбора объекта.
    Всегда включает пункт "Все объекты договора".
    """
    names: List[str] = ["Все объекты договора"]
    ids = list_object_ids_for_contract(db_path, int(contract_id))
    if not ids:
        return names

    con = connect(db_path, read_only=True)
    cur = con.cursor()
    q = ",".join(["?"] * len(ids))
    cur.execute(f"SELECT * FROM objects WHERE id IN ({q}) ORDER BY id ASC;", tuple(ids))
    for row in cur.fetchall():
        o = dict(row)
        addr = format_address_row(o)
        names.append(addr if addr else f"Объект #{o.get('id')}")
    con.close()
    return names


def list_object_ids_for_contract_statuses(db_path: str, statuses: List[str]) -> List[int]:
    sts = [s for s in (statuses or []) if (s or '').strip()]
    if not sts:
        return []
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    q = ",".join(["?"] * len(sts))
    cur.execute(
        f"""
        SELECT DISTINCT co.object_id
        FROM contract_objects co
        JOIN contracts c ON c.id=co.contract_id
        WHERE c.status IN ({q})
        ORDER BY co.object_id ASC;
        """,
        tuple(sts),
    )
    ids = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
    con.close()
    return ids


def list_contract_ids_for_object(db_path: str, object_id: int) -> List[int]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.id
        FROM contracts c
        JOIN contract_objects co ON co.contract_id=c.id
        WHERE co.object_id=?
        ORDER BY c.id DESC;
        """,
        (int(object_id),),
    )
    ids = [int(r[0]) for r in cur.fetchall()]
    con.close()
    return ids


# ---- objects ----
def list_objects(db_path: str, user: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    # Показываем только объекты, которые привязаны хотя бы к одному договору.
    # Это соответствует логике реестра: "непривязанные" объекты считаем неактуальными.
    where_extra = ""
    params: List[Any] = []
    if user is not None:
        try:
            from core.org import get_accessible_unit_ids

            units = get_accessible_unit_ids(db_path, user)
            if units is None:
                where_extra = ""
            elif len(units) == 0:
                where_extra = " AND (o.created_by=? OR o.created_by='')"
                params.append(user.get("username", "") or "")
            else:
                qs = ",".join(["?"] * len(units))
                where_extra = f" AND (o.owner_unit_id IN ({qs}) OR o.created_by=?)"
                params.extend([int(x) for x in units])
                params.append(user.get("username", "") or "")
        except Exception:
            pass

    cur.execute(
        f"""
        SELECT o.*
        FROM objects o
        WHERE EXISTS (SELECT 1 FROM contract_objects co WHERE co.object_id = o.id)
        {where_extra}
        ORDER BY o.id DESC;
        """,
        tuple(params),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def list_objects_any(db_path: str, user: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """List all objects (including those not linked to any contract)."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    where = ""
    params: List[Any] = []
    if user is not None:
        try:
            from core.org import get_accessible_unit_ids

            units = get_accessible_unit_ids(db_path, user)
            if units is None:
                where = ""
            elif len(units) == 0:
                where = "WHERE (o.created_by=? OR o.created_by='')"
                params.append(user.get("username", "") or "")
            else:
                qs = ",".join(["?"] * len(units))
                where = f"WHERE (o.owner_unit_id IN ({qs}) OR o.created_by=?)"
                params.extend([int(x) for x in units])
                params.append(user.get("username", "") or "")
        except Exception:
            pass

    cur.execute(
        f"""
        SELECT o.*
        FROM objects o
        {where}
        ORDER BY o.id DESC;
        """,
        tuple(params),
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows



def get_object(db_path: str, object_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT * FROM objects WHERE id=?;", (int(object_id),))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def create_object(db_path: str, data: Dict[str, Any], *, created_by: str = "", owner_unit_id: Optional[int] = None) -> int:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO objects(object_type, municipality, settlement_type, settlement, street_type, street, house, latitude, longitude, area, cadastral, additional_info, owner_unit_id, created_by)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?);
    """, (
        data.get("object_type","") or "",
        data.get("municipality","") or "",
        data.get("settlement_type","") or "",
        data.get("settlement","") or "",
        data.get("street_type","") or "",
        data.get("street","") or "",
        data.get("house","") or "",
        data.get("latitude", None),
        data.get("longitude", None),
        data.get("area", None),
        data.get("cadastral","") or "",
        data.get("additional_info","") or "",
        owner_unit_id,
        (created_by or "").strip(),
    ))
    oid = int(cur.lastrowid)
    con.commit()
    con.close()
    return oid


def update_object(db_path: str, object_id: int, data: Dict[str, Any]) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    expected_ver = data.get("_row_version", None)
    if expected_ver is None:
        cur.execute(
            """
            UPDATE objects
            SET object_type=?, municipality=?, settlement_type=?, settlement=?, street_type=?, street=?, house=?,
                latitude=?, longitude=?, area=?, cadastral=?, additional_info=?
            WHERE id=?;
            """,
            (
                data.get("object_type", "") or "",
                data.get("municipality", "") or "",
                data.get("settlement_type", "") or "",
                data.get("settlement", "") or "",
                data.get("street_type", "") or "",
                data.get("street", "") or "",
                data.get("house", "") or "",
                data.get("latitude", None),
                data.get("longitude", None),
                data.get("area", None),
                data.get("cadastral", "") or "",
                data.get("additional_info", "") or "",
                int(object_id),
            ),
        )
    else:
        cur.execute(
            """
            UPDATE objects
            SET object_type=?, municipality=?, settlement_type=?, settlement=?, street_type=?, street=?, house=?,
                latitude=?, longitude=?, area=?, cadastral=?, additional_info=?,
                row_version=row_version+1
            WHERE id=? AND row_version=?;
            """,
            (
                data.get("object_type", "") or "",
                data.get("municipality", "") or "",
                data.get("settlement_type", "") or "",
                data.get("settlement", "") or "",
                data.get("street_type", "") or "",
                data.get("street", "") or "",
                data.get("house", "") or "",
                data.get("latitude", None),
                data.get("longitude", None),
                data.get("area", None),
                data.get("cadastral", "") or "",
                data.get("additional_info", "") or "",
                int(object_id),
                int(expected_ver),
            ),
        )
        if cur.rowcount == 0:
            con.rollback()
            con.close()
            raise ConcurrencyError(
                "Карточка объекта была изменена другим пользователем. Обновите карточку и попробуйте снова."
            )
    con.commit()
    con.close()


def format_address_row(o: Dict[str, Any]) -> str:
    """Human-friendly address for tables.

    Требование:
      - отображать как: "городской округ Донецк, город Донецк, улица Артема, 5"
    """
    parts: List[str] = []

    mun = (o.get("municipality") or "").strip()
    st_type = (o.get("settlement_type") or "").strip()
    sett = (o.get("settlement") or "").strip()
    street_type = (o.get("street_type") or "").strip()
    street = (o.get("street") or "").strip()
    house = (o.get("house") or "").strip()

    if mun:
        parts.append(mun)

    if st_type and sett:
        parts.append(f"{st_type} {sett}")
    elif st_type:
        parts.append(st_type)
    elif sett:
        parts.append(sett)

    if street_type and street:
        parts.append(f"{street_type} {street}")
    elif street_type:
        parts.append(street_type)
    elif street:
        parts.append(street)

    if house:
        parts.append(house)

    return ", ".join([p for p in parts if p])


# ---- contract ↔ objects ----
def list_contract_object_ids(db_path: str, contract_id: int) -> List[int]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT object_id FROM contract_objects WHERE contract_id=? ORDER BY object_id;", (int(contract_id),))
    ids = [int(r["object_id"]) for r in cur.fetchall()]
    con.close()
    return ids


def add_object_to_contract(db_path: str, contract_id: int, object_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO contract_objects(contract_id, object_id) VALUES(?,?);
    """, (int(contract_id), int(object_id)))
    con.commit()
    con.close()


def remove_object_from_contract(db_path: str, contract_id: int, object_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM contract_objects WHERE contract_id=? AND object_id=?;", (int(contract_id), int(object_id)))
    con.commit()
    con.close()


# ---- stages ----
def list_stages(db_path: str, contract_id: int) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT id, seq_no, name, info, stage_date, stage_no, extra, corr_item_id
        FROM stages
        WHERE contract_id=?
        ORDER BY seq_no ASC;
    """, (int(contract_id),))
    rows = [dict(r) for r in cur.fetchall()]
    con.close()
    return rows


def add_stage(db_path: str, contract_id: int, name: str, info: str = "", stage_date: str = "", stage_no: str = "", extra: str = "", corr_item_id: int = 0) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("SELECT COALESCE(MAX(seq_no),0)+1 AS n FROM stages WHERE contract_id=?;", (int(contract_id),))
    n = int(cur.fetchone()["n"])
    # backward compatibility: if old UI passes "info" only, store it in extra as well.
    if not extra and info:
        extra = info
    cur.execute("""
        INSERT INTO stages(contract_id, seq_no, name, info, stage_date, stage_no, extra, corr_item_id)
        VALUES(?,?,?,?,?,?,?,?);
    """, (
        int(contract_id),
        n,
        (name or "").strip(),
        (info or "").strip(),
        (stage_date or "").strip(),
        (stage_no or "").strip(),
        (extra or "").strip(),
        int(corr_item_id or 0),
    ))
    con.commit()
    con.close()


def get_stage(db_path: str, stage_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, contract_id, seq_no, name, info, stage_date, stage_no, extra, corr_item_id
        FROM stages
        WHERE id=?;
        """,
        (int(stage_id),),
    )
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_stage(db_path: str, stage_id: int, *, name: str, stage_date: str = "", stage_no: str = "", extra: str = "", corr_item_id: int = 0) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute(
        """
        UPDATE stages
        SET name=?, stage_date=?, stage_no=?, extra=?, corr_item_id=?
        WHERE id=?;
        """,
        (
            (name or "").strip(),
            (stage_date or "").strip(),
            (stage_no or "").strip(),
            (extra or "").strip(),
            int(corr_item_id or 0),
            int(stage_id),
        ),
    )
    con.commit()
    con.close()


def delete_stage(db_path: str, stage_id: int) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("DELETE FROM stages WHERE id=?;", (int(stage_id),))
    con.commit()
    con.close()


# ---- inspections ----
def list_inspections_for_contract(db_path: str, contract_id: int) -> List[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT i.*, o.object_type, o.municipality, o.settlement_type, o.settlement, o.street_type, o.street, o.house
        FROM inspections i
        LEFT JOIN objects o ON o.id=i.object_id
        WHERE i.contract_id=?
        ORDER BY
          CASE WHEN (i.inspection_date IS NULL OR i.inspection_date='') THEN 0 ELSE 1 END,
          COALESCE(i.planned_date,'') DESC,
          i.id DESC;
    """, (int(contract_id),))
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        # красивое имя объекта
        if d.get("object_id"):
            addr = format_address_row(d)
            d["object_name"] = f'{d.get("object_type","")}: {addr}'.strip(": ")
        else:
            d["object_name"] = "По договору (все объекты)"
        rows.append(d)
    con.close()
    return rows


def create_planned_inspection(db_path: str, contract_id: int, planned_date: str, object_id: Optional[int]) -> int:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO inspections(contract_id, object_id, planned_date, inspection_date, result, act_path, photos_dir, next_date)
        VALUES(?,?,?,?,?,?,?,?);
    """, (
        int(contract_id),
        int(object_id) if object_id else None,
        planned_date or "",
        "",
        "",
        "",
        "",
        ""
    ))
    iid = int(cur.lastrowid)
    con.commit()
    con.close()
    return iid


def get_inspection(db_path: str, inspection_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("SELECT * FROM inspections WHERE id=?;", (int(inspection_id),))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def update_inspection(db_path: str, inspection_id: int, data: Dict[str, Any]) -> None:
    con = connect(db_path, read_only=False)
    cur = con.cursor()
    expected_ver = data.get("_row_version", None)
    if expected_ver is None:
        cur.execute(
            """
            UPDATE inspections
            SET object_id=?, planned_date=?, inspection_date=?, result=?, act_path=?, photos_dir=?, next_date=?
            WHERE id=?;
            """,
            (
                int(data["object_id"]) if data.get("object_id") else None,
                data.get("planned_date", "") or "",
                data.get("inspection_date", "") or "",
                data.get("result", "") or "",
                data.get("act_path", "") or "",
                data.get("photos_dir", "") or "",
                data.get("next_date", "") or "",
                int(inspection_id),
            ),
        )
    else:
        cur.execute(
            """
            UPDATE inspections
            SET object_id=?, planned_date=?, inspection_date=?, result=?, act_path=?, photos_dir=?, next_date=?,
                row_version=row_version+1
            WHERE id=? AND row_version=?;
            """,
            (
                int(data["object_id"]) if data.get("object_id") else None,
                data.get("planned_date", "") or "",
                data.get("inspection_date", "") or "",
                data.get("result", "") or "",
                data.get("act_path", "") or "",
                data.get("photos_dir", "") or "",
                data.get("next_date", "") or "",
                int(inspection_id),
                int(expected_ver),
            ),
        )
        if cur.rowcount == 0:
            con.rollback()
            con.close()
            raise ConcurrencyError(
                "Карточка осмотра была изменена другим пользователем. Обновите карточку и попробуйте снова."
            )
    con.commit()
    con.close()


def finalize_inspection_and_maybe_plan_next(db_path: str, inspection_id: int, data: Dict[str, Any]) -> Optional[int]:
    """
    Сохраняет осмотр (как факт), а если next_date задана — автоматически создаёт следующий план
    (с тем же contract_id и тем же object_id)
    """
    ins = get_inspection(db_path, inspection_id)
    if not ins:
        return None

    update_inspection(db_path, inspection_id, data)

    nd = (data.get("next_date") or "").strip()
    if not nd:
        return None

    # создать следующий план
    return create_planned_inspection(
        db_path,
        int(ins["contract_id"]),
        nd,
        ins.get("object_id")
    )


def ensure_column(con, table: str, column: str, ddl: str) -> bool:
    """Ensures column exists; returns True if added."""
    try:
        cur = con.cursor()
        cur.execute(f"PRAGMA table_info({table});")
        cols = [r[1] for r in cur.fetchall()]
        if column in cols:
            return False
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl};")
        return True
    except Exception:
        return False
