from __future__ import annotations

import datetime as _dt
from typing import Any, Dict, Optional, Tuple

from core.db import connect


def _parse_date(text: str) -> Optional[_dt.date]:
    t = (text or "").strip()
    if not t:
        return None
    try:
        if "." in t:
            d, m, y = t.split(".")
            return _dt.date(int(y), int(m), int(d))
        return _dt.date.fromisoformat(t[:10])
    except Exception:
        return None


def _resolve_suffix(suffix: str, year: int) -> str:
    s = suffix or ""
    return (
        s.replace("{yy}", f"{year % 100:02d}")
        .replace("{yyyy}", f"{year:04d}")
    )


def extract_seq(number: str, prefix: str, suffix_resolved: str) -> Optional[int]:
    """Extract sequence part from number based on prefix + resolved suffix.

    We take digits from the middle part.
    """
    n = (number or "").strip()
    p = (prefix or "").strip()
    sfx = (suffix_resolved or "").strip()
    if p and not n.startswith(p):
        return None
    if sfx and not n.endswith(sfx):
        return None
    mid = n[len(p):]
    if sfx:
        mid = mid[: -len(sfx)]
    digits = "".join(ch for ch in mid if ch.isdigit())
    if not digits:
        return None
    try:
        return int(digits)
    except Exception:
        return None


def format_number(prefix: str, seq: int, suffix: str, year: int, pad: int = 0) -> str:
    sfx = _resolve_suffix(suffix, year)
    if pad and pad > 0:
        mid = f"{int(seq):0{int(pad)}d}"
    else:
        mid = str(int(seq))
    return f"{(prefix or '')}{mid}{sfx}"


def next_sequence_for_year(
    db_path: str,
    table: str,
    field: str,
    kind_filter: Tuple[str, str] | None,
    date_field: str,
    date_value: str,
    prefix: str,
    suffix: str,
    pad: int = 0,
    reset_per_year: bool = True,
) -> str:
    """Find next free sequence number and format it.

    - If reset_per_year=True: search within year boundaries by date_field
    - otherwise: search all rows.
    """
    d = _parse_date(date_value) or _dt.date.today()
    y = d.year
    sfx_res = _resolve_suffix(suffix, y)

    con = connect(db_path, read_only=True)
    cur = con.cursor()

    where = []
    args: list[Any] = []

    if kind_filter:
        where.append(f"{kind_filter[0]}=?")
        args.append(kind_filter[1])

    if reset_per_year:
        y0 = f"{y:04d}-01-01"
        y1 = f"{y:04d}-12-31"
        where.append("COALESCE(%s,'')!=''" % date_field)
        where.append("substr(%s,1,10) BETWEEN ? AND ?" % date_field)
        args.extend([y0, y1])

    if prefix:
        where.append(f"{field} LIKE ?")
        args.append(f"{prefix}%")
    if sfx_res:
        where.append(f"{field} LIKE ?")
        args.append(f"%{sfx_res}")

    w = (" WHERE " + " AND ".join(where)) if where else ""
    cur.execute(f"SELECT {field} AS v, {date_field} AS d FROM {table}{w};", tuple(args))
    used = set()
    for r in cur.fetchall():
        v = (r["v"] or "").strip()
        n = extract_seq(v, prefix, sfx_res)
        if n and n > 0:
            used.add(int(n))
    con.close()

    # После импорта/ручного ввода часто уже есть сквозная нумерация
    # (например 150..180). В таком случае нужно продолжать с максимального
    # значения, а не искать первую "дыру".
    if used:
        seq = max(used) + 1
    else:
        seq = 1

    return format_number(prefix, seq, suffix, y, pad=pad)


def get_cfg(settings: Dict[str, Any], key: str) -> Dict[str, Any]:
    cfg = (settings or {}).get("numbering", {}).get(key, {})
    if not isinstance(cfg, dict):
        return {}
    out = {
        "enabled": bool(cfg.get("enabled", True)),
        "prefix": str(cfg.get("prefix", "") or ""),
        "suffix": str(cfg.get("suffix", "") or ""),
        "pad": int(cfg.get("pad", 0) or 0),
        "reset_per_year": bool(cfg.get("reset_per_year", True)),
    }
    return out
