from __future__ import annotations

import os
import sqlite3
import time
import urllib.request
from dataclasses import dataclass
from typing import Callable, Optional, Tuple, List

from core.mbtiles import BBox, ensure_mbtiles
from core.tile_math import latlon_to_tile_xy, tile_xy_bounds


ProgressCb = Callable[[int, int, int, int], None]


@dataclass
class DownloadSpec:
    mbtiles_path: str
    url_template: str  # e.g. https://.../{z}/{x}/{y}.png
    bbox: BBox
    zoom_min: int
    zoom_max: int
    user_agent: str = "RegistryApp OfflineTiles/1.0"
    delay_s: float = 0.2  # polite throttling by default
    timeout_s: float = 15.0
    # Optional polygon mask (GeoJSON). List of rings (outer rings only) in lon/lat.
    # If provided, only tiles intersecting the mask are downloaded.
    mask_polygons: Optional[List[List[Tuple[float, float]]]] = None


def _point_in_poly(x: float, y: float, poly: List[Tuple[float, float]]) -> bool:
    """Ray casting point-in-polygon. poly is list of (lon,lat)."""
    inside = False
    n = len(poly)
    if n < 3:
        return False
    x0, y0 = poly[0]
    for i in range(1, n + 1):
        x1, y1 = poly[i % n]
        if (y0 > y) != (y1 > y):
            xinters = (x1 - x0) * (y - y0) / (y1 - y0 + 1e-18) + x0
            if x < xinters:
                inside = not inside
        x0, y0 = x1, y1
    return inside


def _tile_hits_mask(z: int, x: int, y: int, polygons: List[List[Tuple[float, float]]]) -> bool:
    """Return True if tile likely intersects any polygon (fast conservative test)."""
    min_lon, min_lat, max_lon, max_lat = tile_xy_bounds(z, x, y)
    cx = (min_lon + max_lon) / 2.0
    cy = (min_lat + max_lat) / 2.0
    pts = [
        (min_lon, min_lat),
        (min_lon, max_lat),
        (max_lon, min_lat),
        (max_lon, max_lat),
        (cx, cy),
    ]
    for poly in polygons:
        for px, py in pts:
            if _point_in_poly(px, py, poly):
                return True
    return False


def estimate_tiles(bbox: BBox, zoom_min: int, zoom_max: int) -> int:
    total = 0
    for z in range(int(zoom_min), int(zoom_max) + 1):
        x0, y0 = latlon_to_tile_xy(bbox.max_lat, bbox.min_lon, z)
        x1, y1 = latlon_to_tile_xy(bbox.min_lat, bbox.max_lon, z)
        if x1 < x0:
            x0, x1 = x1, x0
        if y1 < y0:
            y0, y1 = y1, y0
        total += (x1 - x0 + 1) * (y1 - y0 + 1)
    return int(total)


def estimate_tiles_masked(bbox: BBox, zoom_min: int, zoom_max: int, polygons: List[List[Tuple[float, float]]]) -> int:
    """Estimate tile count with polygon mask. Conservative: counts tiles whose center/corners are inside."""
    total = 0
    for z in range(int(zoom_min), int(zoom_max) + 1):
        x0, y0 = latlon_to_tile_xy(bbox.max_lat, bbox.min_lon, z)
        x1, y1 = latlon_to_tile_xy(bbox.min_lat, bbox.max_lon, z)
        if x1 < x0:
            x0, x1 = x1, x0
        if y1 < y0:
            y0, y1 = y1, y0
        n = 1 << int(z)
        for x in range(int(x0), int(x1) + 1):
            for y in range(int(y0), int(y1) + 1):
                if x < 0 or x >= n or y < 0 or y >= n:
                    continue
                if _tile_hits_mask(int(z), int(x), int(y), polygons):
                    total += 1
    return int(total)


def download_to_mbtiles(spec: DownloadSpec, progress: Optional[ProgressCb] = None, stop_flag: Optional[Callable[[], bool]] = None) -> Tuple[int, int]:
    """Download tiles into MBTiles. Returns (downloaded, skipped_existing)."""
    ensure_mbtiles(spec.mbtiles_path, name="donetsk_offline")

    con = sqlite3.connect(spec.mbtiles_path)
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    cur = con.cursor()

    opener = urllib.request.build_opener()
    opener.addheaders = [("User-Agent", spec.user_agent)]

    downloaded = 0
    skipped = 0
    failed = 0
    done = 0

    # IMPORTANT:
    # For a GeoJSON mask, an exact "total" requires scanning every tile and testing intersection,
    # which can take minutes for zoom>=14 on a big region and makes the UI look frozen.
    # We therefore use the bbox total as a fast upper bound for progress reporting.
    total = estimate_tiles(spec.bbox, spec.zoom_min, spec.zoom_max)
    last_err: Optional[Exception] = None

    try:
        for z in range(int(spec.zoom_min), int(spec.zoom_max) + 1):
            x0, y0 = latlon_to_tile_xy(spec.bbox.max_lat, spec.bbox.min_lon, z)
            x1, y1 = latlon_to_tile_xy(spec.bbox.min_lat, spec.bbox.max_lon, z)
            if x1 < x0:
                x0, x1 = x1, x0
            if y1 < y0:
                y0, y1 = y1, y0

            n = 1 << int(z)

            for x in range(int(x0), int(x1) + 1):
                if stop_flag and stop_flag():
                    con.commit()
                    return downloaded, skipped

                for y in range(int(y0), int(y1) + 1):
                    if stop_flag and stop_flag():
                        con.commit()
                        return downloaded, skipped

                    # clamp to world
                    if x < 0 or x >= n or y < 0 or y >= n:
                        done += 1
                        continue

                    # polygon mask
                    if spec.mask_polygons and not _tile_hits_mask(int(z), int(x), int(y), spec.mask_polygons):
                        done += 1
                        if progress:
                            progress(done, total, downloaded, skipped)
                        continue

                    y_tms = (n - 1) - int(y)

                    cur.execute(
                        "SELECT 1 FROM tiles WHERE zoom_level=? AND tile_column=? AND tile_row=?",
                        (int(z), int(x), int(y_tms)),
                    )
                    if cur.fetchone() is not None:
                        skipped += 1
                        done += 1
                        if progress:
                            progress(done, total, downloaded, skipped)
                        continue

                    url = spec.url_template.format(z=z, x=x, y=y)
                    try:
                        with opener.open(url, timeout=float(spec.timeout_s)) as resp:
                            data = resp.read()
                        if data:
                            cur.execute(
                                "INSERT OR REPLACE INTO tiles(zoom_level,tile_column,tile_row,tile_data) VALUES(?,?,?,?)",
                                (int(z), int(x), int(y_tms), sqlite3.Binary(data)),
                            )
                            downloaded += 1
                    except Exception as e:
                        # Network / HTTP errors. Count them to surface a helpful message if nothing downloads.
                        failed += 1
                        last_err = e

                        # If everything fails early, stop and show a clear error instead of silently doing nothing.
                        if downloaded == 0 and failed >= 50:
                            raise RuntimeError(
                                "Не удалось скачать ни одного тайла. "
                                "Похоже, источник тайлов недоступен/блокирует массовую загрузку или нет доступа в интернет. "
                                f"Последняя ошибка: {type(last_err).__name__}: {last_err}"
                            )

                    done += 1
                    if done % 200 == 0:
                        con.commit()

                    if progress:
                        progress(done, total, downloaded, skipped)

                    if spec.delay_s and spec.delay_s > 0:
                        time.sleep(float(spec.delay_s))

        con.commit()
        return downloaded, skipped
    finally:
        try:
            con.close()
        except Exception:
            pass
