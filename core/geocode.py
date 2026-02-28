# core/geocode.py
from __future__ import annotations

import json
import re
import time
import urllib.parse
import urllib.request
import ssl
from typing import Optional, Tuple

from core.db import connect


def _ssl_context() -> ssl.SSLContext:
    """Use certifi CA bundle when available (Windows-friendly)."""
    try:
        import certifi  # type: ignore

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _norm_address(addr: str) -> str:
    a = (addr or "").strip()
    a = re.sub(r"\s+", " ", a)
    return a


def ensure_geocode_cache(db_path: str):
    con = connect(db_path)
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS geocode_cache (
            address TEXT PRIMARY KEY,
            lat REAL NOT NULL,
            lon REAL NOT NULL,
            ts TEXT NOT NULL
        )"""
    )
    con.commit()
    con.close()


def get_cached(db_path: str, address: str) -> Optional[Tuple[float, float]]:
    ensure_geocode_cache(db_path)
    con = connect(db_path)
    cur = con.cursor()
    cur.execute("SELECT lat, lon FROM geocode_cache WHERE address = ?", (_norm_address(address),))
    row = cur.fetchone()
    con.close()
    if not row:
        return None
    try:
        return float(row[0]), float(row[1])
    except Exception:
        return None


def put_cached(db_path: str, address: str, lat: float, lon: float):
    ensure_geocode_cache(db_path)
    con = connect(db_path)
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO geocode_cache(address, lat, lon, ts) VALUES(?,?,?,datetime('now'))",
        (_norm_address(address), float(lat), float(lon)),
    )
    con.commit()
    con.close()


def geocode_nominatim(address: str, countrycodes: str = "", accept_language: str = "ru") -> Optional[Tuple[float, float]]:
    """Онлайн геокодинг через Nominatim (OpenStreetMap).
    Требует интернет. Возвращает (lat, lon) или None.
    """
    addr = _norm_address(address)
    if not addr:
        return None

    params = {
        "q": addr,
        "format": "jsonv2",
        "limit": "1",
    }
    if countrycodes:
        params["countrycodes"] = countrycodes
    if accept_language:
        params["accept-language"] = accept_language

    url = "https://nominatim.openstreetmap.org/search?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "RegistryApp/1.0 (offline registry tool)",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=8, context=_ssl_context()) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        js = json.loads(data)
        if not js:
            return None
        lat = float(js[0]["lat"])
        lon = float(js[0]["lon"])
        return lat, lon
    except Exception:
        return None


def geocode_address(db_path: str, address: str) -> Optional[Tuple[float, float]]:
    # 1) cache
    cached = get_cached(db_path, address)
    if cached:
        return cached
    # 2) online
    res = geocode_nominatim(address, countrycodes="", accept_language="ru")
    if res:
        put_cached(db_path, address, res[0], res[1])
    return res
