# core/inspections.py
from __future__ import annotations

from typing import Optional, Dict, Any, List
from core.db import connect, list_object_names_for_contract


def create_planned_inspection(db_path: str, contract_id: int, planned_date: str = "", object_name: str = "") -> int:
    """
    Создаёт "пустышку" запланированного осмотра.
    planned_date (inspection_date) можно оставить пустым.
    object_name: '' или 'Все объекты договора' или конкретный адрес.
    """
    obj = (object_name or "").strip() or "Все объекты договора"
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("""
        INSERT INTO inspections(contract_id, inspection_date, object_name, result, next_date, act_path, photos_path, is_planned)
        VALUES(?,?,?,?,?,?,?,1);
    """, (int(contract_id), (planned_date or "").strip(), obj, "", "", "", ""))
    con.commit()
    iid = int(cur.lastrowid)
    con.close()
    return iid


def get_inspection(db_path: str, inspection_id: int) -> Optional[Dict[str, Any]]:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT * FROM inspections WHERE id=?;", (int(inspection_id),))
    r = cur.fetchone()
    con.close()
    return dict(r) if r else None


def list_inspections_for_contract(db_path: str, contract_id: int) -> List[Dict[str, Any]]:
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("""
        SELECT id, inspection_date, object_name, is_planned, act_path, photos_path, next_date
        FROM inspections
        WHERE contract_id=?
        ORDER BY id DESC;
    """, (int(contract_id),))
    rows = [dict(x) for x in cur.fetchall()]
    con.close()
    return rows


def update_inspection(
    db_path: str,
    inspection_id: int,
    inspection_date: str,
    object_name: str,
    result: str,
    next_date: str,
    act_path: str,
    photos_path: str,
    is_planned: int
):
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("""
        UPDATE inspections SET
            inspection_date=?,
            object_name=?,
            result=?,
            next_date=?,
            act_path=?,
            photos_path=?,
            is_planned=?
        WHERE id=?;
    """, (
        (inspection_date or "").strip(),
        (object_name or "").strip() or "Все объекты договора",
        (result or "").strip(),
        (next_date or "").strip(),
        (act_path or "").strip(),
        (photos_path or "").strip(),
        int(is_planned),
        int(inspection_id)
    ))
    con.commit()
    con.close()


def schedule_next_from_current(db_path: str, contract_id: int, current_inspection_id: int) -> Optional[int]:
    """
    Если у текущего осмотра заполнено next_date — создаём новый planned осмотр.
    """
    cur = get_inspection(db_path, current_inspection_id)
    if not cur:
        return None
    nd = (cur.get("next_date") or "").strip()
    if not nd:
        return None
    obj = (cur.get("object_name") or "").strip() or "Все объекты договора"
    return create_planned_inspection(db_path, contract_id=int(contract_id), planned_date=nd, object_name=obj)


def object_choices_for_contract(db_path: str, contract_id: int) -> List[str]:
    """
    Список для выбора объекта в осмотре:
    'Все объекты договора' + адреса объектов.
    """
    return list_object_names_for_contract(db_path, int(contract_id))
