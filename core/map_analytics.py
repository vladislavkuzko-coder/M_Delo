# core/map_analytics.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Dict, List, Tuple

from core.db import connect, list_object_ids_for_contract


def _parse_ru_date(s: str):
    try:
        s = (s or "").strip()
        if len(s) != 10:
            return None
        return datetime.strptime(s, "%d.%m.%Y").date()
    except Exception:
        return None


def list_object_ids_with_planned_inspections(db_path: str, days_ahead: int) -> List[int]:
    """Return object IDs that have a planned inspection within N days.

    Rules:
    - planned inspections are rows where inspection_date is empty
    - date is COALESCE(next_date, planned_date)
    - if object_id is NULL -> means "all objects of contract" (include all linked objects)
    """
    today = datetime.now().date()
    lim = today + timedelta(days=int(days_ahead))

    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, contract_id, object_id, planned_date, next_date, inspection_date
        FROM inspections
        WHERE (inspection_date IS NULL OR inspection_date='')
        ORDER BY id DESC;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    out: set[int] = set()
    for r in rows:
        ds = (r.get("next_date") or r.get("planned_date") or "").strip()
        d = _parse_ru_date(ds)
        if not d:
            continue
        if not (today <= d <= lim):
            continue
        oid = r.get("object_id")
        if oid is not None:
            try:
                out.add(int(oid))
            except Exception:
                pass
        else:
            # all objects of the contract
            try:
                cid = int(r.get("contract_id"))
            except Exception:
                continue
            for x in list_object_ids_for_contract(db_path, cid):
                out.add(int(x))
    return sorted(out)


def nearest_planned_date_by_object(db_path: str) -> Dict[int, str]:
    """Return mapping object_id -> nearest planned date (DD.MM.YYYY) for unfinished inspections."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT contract_id, object_id, planned_date, next_date, inspection_date
        FROM inspections
        WHERE (inspection_date IS NULL OR inspection_date='')
        ORDER BY id DESC;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    best: Dict[int, Tuple[datetime.date, str]] = {}
    for r in rows:
        ds = (r.get("next_date") or r.get("planned_date") or "").strip()
        d = _parse_ru_date(ds)
        if not d:
            continue
        oid = r.get("object_id")
        if oid is not None:
            try:
                oid_i = int(oid)
            except Exception:
                continue
            cur_best = best.get(oid_i)
            if (cur_best is None) or (d < cur_best[0]):
                best[oid_i] = (d, ds)
        else:
            # applies to all objects of the contract
            try:
                cid = int(r.get("contract_id"))
            except Exception:
                continue
            for oid_i in list_object_ids_for_contract(db_path, cid):
                cur_best = best.get(oid_i)
                if (cur_best is None) or (d < cur_best[0]):
                    best[oid_i] = (d, ds)

    return {k: v[1] for k, v in best.items()}


def list_object_ids_with_overdue_inspections(db_path: str) -> List[int]:
    """Return object IDs that have an unfinished inspection with planned date < today."""
    today = datetime.now().date()

    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id, contract_id, object_id, planned_date, next_date, inspection_date
        FROM inspections
        WHERE (inspection_date IS NULL OR inspection_date='')
        ORDER BY id DESC;
        """
    )
    rows = [dict(r) for r in cur.fetchall()]
    con.close()

    out: set[int] = set()
    for r in rows:
        ds = (r.get("next_date") or r.get("planned_date") or "").strip()
        d = _parse_ru_date(ds)
        if not d:
            continue
        if not (d < today):
            continue
        oid = r.get("object_id")
        if oid is not None:
            try:
                out.add(int(oid))
            except Exception:
                pass
        else:
            try:
                cid = int(r.get("contract_id"))
            except Exception:
                continue
            for x in list_object_ids_for_contract(db_path, cid):
                out.add(int(x))
    return sorted(out)


def list_object_ids_without_coords(db_path: str) -> List[int]:
    """Objects that have no latitude/longitude."""
    con = connect(db_path, read_only=True)
    cur = con.cursor()
    cur.execute(
        """
        SELECT id FROM objects
        WHERE latitude IS NULL OR longitude IS NULL
        ORDER BY id DESC;
        """
    )
    ids = [int(r[0]) for r in cur.fetchall()]
    con.close()
    return ids
