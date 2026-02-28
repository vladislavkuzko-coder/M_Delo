# core/control.py
from __future__ import annotations

from typing import List, Dict, Any
from datetime import datetime, timedelta

from core.db import connect, format_address_row


def list_contracts_stalled(db_path: str, statuses: List[str], days_stalled: int) -> List[Dict[str, Any]]:
    """Контроль: договоры, по которым нет движения (по дате последнего этапа) N дней и более."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT c.id,c.status,c.lender,c.borrower,c.contract_number,c.start_date,c.end_date,c.executor,
               MAX(s.stage_date) AS last_stage_date
        FROM contracts c
        LEFT JOIN stages s ON s.contract_id=c.id
        GROUP BY c.id
        ORDER BY c.id DESC;
        """
    )
    all_rows = [dict(r) for r in cur.fetchall()]
    con.close()

    if statuses:
        all_rows = [r for r in all_rows if (r.get("status") or "") in statuses]

    from datetime import datetime
    today = datetime.now().date()

    def parse_ru(d: str):
        try:
            d = (d or "").strip()
            if len(d) != 10:
                return None
            return datetime.strptime(d, "%d.%m.%Y").date()
        except Exception:
            return None

    out: List[Dict[str, Any]] = []
    for r in all_rows:
        ld = parse_ru(r.get("last_stage_date") or "")
        if not ld:
            continue
        if (today - ld).days >= int(days_stalled):
            out.append(r)
    return out


def list_contracts_expiring(db_path: str, statuses: List[str], days_ahead: int) -> List[Dict[str, Any]]:
    """
    Контроль: договоры, у которых end_date не пустая и <= today + days_ahead и статус входит в statuses.
    end_date хранится как "ДД.ММ.ГГГГ"
    """
    con = connect(db_path, read_only=True)
    cur = con.cursor()

    cur.execute("""
        SELECT id,status,lender,borrower,contract_number,start_date,end_date,executor
        FROM contracts
        ORDER BY id DESC;
    """)
    all_rows = [dict(r) for r in cur.fetchall()]
    con.close()

    if not statuses:
        statuses = ["ДЕЙСТВУЮЩИЙ"]

    today = datetime.now().date()
    lim = today + timedelta(days=int(days_ahead))

    def parse_ru(d: str):
        try:
            d = (d or "").strip()
            if len(d) != 10:
                return None
            return datetime.strptime(d, "%d.%m.%Y").date()
        except Exception:
            return None

    out = []
    for r in all_rows:
        if (r.get("status") or "") not in statuses:
            continue
        ed = parse_ru(r.get("end_date") or "")
        if not ed:
            continue
        if ed <= lim:
            out.append(r)
    return out


def list_planned_inspections(db_path: str) -> List[Dict[str, Any]]:
    """
    Для вкладки "Контроль" -> "План осмотров":
    берём те inspections, у которых inspection_date пустая (то есть это план)
    показываем: contract_id, next_date/planned_date, object_name, id
    """
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute("""
        SELECT i.id, i.contract_id, i.object_id, i.planned_date, i.next_date,
               o.object_type, o.municipality, o.settlement_type, o.settlement, o.street_type, o.street, o.house
        FROM inspections i
        LEFT JOIN objects o ON o.id=i.object_id
        WHERE (i.inspection_date IS NULL OR i.inspection_date='')
        ORDER BY COALESCE(i.next_date,i.planned_date,'') ASC, i.id ASC;
    """)
    rows = []
    for r in cur.fetchall():
        d = dict(r)
        if d.get("object_id"):
            addr = format_address_row(d)
            d["object_name"] = f'{d.get("object_type","")}: {addr}'.strip(": ")
        else:
            d["object_name"] = "По договору (все объекты)"
        rows.append(d)
    con.close()
    return rows
