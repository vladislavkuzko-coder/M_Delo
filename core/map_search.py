from __future__ import annotations

"""Offline map search over registry objects.

This is the first step towards "Yandex-like" search: user types an address/name
and gets suggestions that can be centered on the map.

At this stage we search only *your registry data* (objects + treasury assets)
that already contains coordinates. This works fully offline and is fast.

Next iterations can extend this to OSM-address search by building a dedicated
geocoder index from the PBF/tiles.
"""

from typing import Any

from core.db import connect


def _norm(s: str) -> str:
    return (s or "").strip()


def _tokens(q: str) -> list[str]:
    q = _norm(q).lower()
    # split by whitespace and punctuation-ish separators
    out: list[str] = []
    buf = ""
    for ch in q:
        if ch.isalnum() or ch in ("#", "/", "-"):
            buf += ch
        else:
            if buf:
                out.append(buf)
                buf = ""
    if buf:
        out.append(buf)
    # unique, stable
    seen = set()
    uniq: list[str] = []
    for t in out:
        if t and t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def _mk_addr(*parts: str) -> str:
    return ", ".join([p for p in [_norm(x) for x in parts] if p])


def search_map(db_path: str, query: str, *, limit: int = 30) -> list[dict[str, Any]]:
    q = _norm(query)
    if not q:
        return []

    ql = q.lower()
    toks = _tokens(q)
    like = f"%{ql}%"
    out: list[dict[str, Any]] = []

    con = connect(db_path, read_only=True)
    cur = con.cursor()

    # --- Objects (contracts-linked objects table) ---
    try:
        cur.execute(
            """
            SELECT id, settlement, street, house, municipality, object_type, cadastral,
                   additional_info, latitude, longitude
            FROM objects
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
              AND (
                LOWER(COALESCE(settlement,'')) LIKE ? OR
                LOWER(COALESCE(street,'')) LIKE ? OR
                LOWER(COALESCE(house,'')) LIKE ? OR
                LOWER(COALESCE(cadastral,'')) LIKE ? OR
                LOWER(COALESCE(additional_info,'')) LIKE ?
              )
            ORDER BY id DESC
            LIMIT ?;
            """,
            (like, like, like, like, like, int(limit)),
        )
        for r in cur.fetchall():
            d = dict(r)
            title = _mk_addr(d.get("settlement"), d.get("street"), d.get("house"))
            if not title:
                title = f"Объект #{d.get('id')}"
            subtitle = _mk_addr(d.get("municipality"), d.get("object_type"), d.get("cadastral"))
            out.append(
                {
                    "kind": "object",
                    "id": int(d.get("id")),
                    "title": title,
                    "subtitle": subtitle,
                    "lat": float(d.get("latitude")),
                    "lon": float(d.get("longitude")),
                }
            )
    except Exception:
        pass

    # --- Treasury assets ---
    try:
        cur.execute(
            """
            SELECT id, name, inv_no, settlement, street, house, municipality,
                   cadastral, additional_info, latitude, longitude
            FROM treasury_assets
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
              AND (
                LOWER(COALESCE(name,'')) LIKE ? OR
                LOWER(COALESCE(inv_no,'')) LIKE ? OR
                LOWER(COALESCE(settlement,'')) LIKE ? OR
                LOWER(COALESCE(street,'')) LIKE ? OR
                LOWER(COALESCE(house,'')) LIKE ? OR
                LOWER(COALESCE(cadastral,'')) LIKE ? OR
                LOWER(COALESCE(additional_info,'')) LIKE ?
              )
            ORDER BY id DESC
            LIMIT ?;
            """,
            (like, like, like, like, like, like, like, int(limit)),
        )
        for r in cur.fetchall():
            d = dict(r)
            title = _mk_addr(d.get("name"), d.get("inv_no")) or f"Имущество #{d.get('id')}"
            addr = _mk_addr(d.get("settlement"), d.get("street"), d.get("house"))
            subtitle = _mk_addr(addr, d.get("cadastral"), d.get("municipality"))
            out.append(
                {
                    "kind": "asset",
                    "id": int(d.get("id")),
                    "title": title,
                    "subtitle": subtitle,
                    "lat": float(d.get("latitude")),
                    "lon": float(d.get("longitude")),
                }
            )
    except Exception:
        pass
    finally:
        con.close()

    # Prefer assets/objects with more relevant match.
    # Still simple/fast, but closer to what users expect from "Yandex-like" search.
    def score(it: dict[str, Any]) -> int:
        t = (it.get("title") or "").lower()
        s = (it.get("subtitle") or "").lower()
        blob = f"{t} {s}"

        sc = 0
        # Exact/prefix matches
        if t == ql:
            sc += 100
        if t.startswith(ql):
            sc += 40
        if ql in t:
            sc += 20
        if ql in s:
            sc += 6

        # Token coverage (multi-word queries)
        if toks:
            hit = sum(1 for tok in toks if tok and tok in blob)
            sc += hit * 8
            if hit == len(toks):
                sc += 10

        # Small preference: objects first (usually more useful on map)
        if (it.get("kind") or "") == "object":
            sc += 1

        # bigger score = better; sort descending
        return -sc

    out.sort(key=score)
    return out[: int(limit)]
