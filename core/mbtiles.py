"""MBTiles helpers.

We use MBTiles (SQLite) with raster PNG/JPG tiles.
Schema follows the common MBTiles 1.3 layout:
 - tiles(zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)
 - metadata(name TEXT, value TEXT)

tile_row is stored in TMS (flipped Y). For XYZ requests we convert:
  tms_y = (2**z - 1 - y)
"""

from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from typing import Optional, Iterable, List, Union


@dataclass(frozen=True)
class BBox:
    min_lat: float
    min_lon: float
    max_lat: float
    max_lon: float


def ensure_mbtiles(path: str, *, name: str = "offline", fmt: str = "png") -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    con = sqlite3.connect(path)
    try:
        cur = con.cursor()
        cur.execute(
            "CREATE TABLE IF NOT EXISTS tiles (zoom_level INTEGER, tile_column INTEGER, tile_row INTEGER, tile_data BLOB)"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS metadata (name TEXT PRIMARY KEY, value TEXT)"
        )
        cur.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS tile_index ON tiles (zoom_level, tile_column, tile_row)"
        )
        cur.execute("INSERT OR REPLACE INTO metadata(name,value) VALUES('name', ?)", (name,))
        cur.execute("INSERT OR REPLACE INTO metadata(name,value) VALUES('format', ?)", (fmt,))
        con.commit()
    finally:
        con.close()


def mbtiles_has_tiles(path: str) -> bool:
    if not path or not os.path.exists(path):
        return False
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT 1 FROM tiles LIMIT 1")
        return cur.fetchone() is not None
    except Exception:
        return False
    finally:
        try:
            con.close()
        except Exception:
            pass


def read_tile(path: str, z: int, x: int, y_xyz: int) -> Optional[bytes]:
    """Return tile bytes for XYZ y (not TMS)."""
    if not path or not os.path.exists(path):
        return None
    n = 1 << int(z)
    if x < 0 or x >= n or y_xyz < 0 or y_xyz >= n:
        return None
    y_tms = (n - 1) - int(y_xyz)
    con = sqlite3.connect(path)
    try:
        cur = con.cursor()
        cur.execute(
            "SELECT tile_data FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
            (int(z), int(x), int(y_tms)),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        con.close()


def expand_mbtiles_paths(path_or_dir: str) -> List[str]:
    """Return a list of mbtiles file paths.

    Accepts:
      - a file path (*.mbtiles)
      - a directory path (all *.mbtiles inside, sorted)
      - a semicolon-separated list of file paths
    """
    p = (path_or_dir or "").strip()
    if not p:
        return []
    if ";" in p:
        out: List[str] = []
        for part in [x.strip() for x in p.split(";") if x.strip()]:
            out.extend(expand_mbtiles_paths(part))
        # de-dup preserving order
        seen = set()
        uniq = []
        for x in out:
            if x not in seen:
                uniq.append(x)
                seen.add(x)
        return uniq
    if os.path.isdir(p):
        files = [
            os.path.join(p, f)
            for f in os.listdir(p)
            if f.lower().endswith(".mbtiles") or f.lower().endswith(".sqlite")
        ]
        files.sort()
        return files
    return [p] if os.path.exists(p) else []


def read_tile_any(path_or_dir: str, z: int, x: int, y_xyz: int) -> Optional[bytes]:
    """Read tile from the first MBTiles file that contains it.

    This supports split coverage (many mbtiles files for one region).
    """
    for p in expand_mbtiles_paths(path_or_dir):
        data = read_tile(p, z, x, y_xyz)
        if data:
            return data
    return None


def read_metadata(path: str) -> dict:
    """Read MBTiles metadata table into a dict."""
    if not path or not os.path.exists(path):
        return {}
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT name, value FROM metadata")
        out = {str(k): ("" if v is None else str(v)) for (k, v) in cur.fetchall()}
        return out
    except Exception:
        return {}
    finally:
        try:
            con.close()
        except Exception:
            pass


def first_usable_mbtiles(path_or_dir: str) -> Optional[str]:
    """Return the first existing mbtiles/sqlite path from a file/dir/list."""
    for p in expand_mbtiles_paths(path_or_dir):
        if os.path.exists(p):
            return p
    return None


def mbtiles_format(path: str) -> str:
    """Return metadata.format (png/jpg), default 'png'."""
    if not path or not os.path.exists(path):
        return "png"
    try:
        con = sqlite3.connect(path)
        cur = con.cursor()
        cur.execute("SELECT value FROM metadata WHERE name='format'")
        row = cur.fetchone()
        return (row[0] if row else "png") or "png"
    except Exception:
        return "png"
    finally:
        try:
            con.close()
        except Exception:
            pass
