from __future__ import annotations

import math
from typing import Tuple


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def latlon_to_pixel_xy(lat: float, lon: float, z: int, tile_size: int = 256) -> Tuple[float, float]:
    """WebMercator lat/lon -> global pixel coordinates at zoom z."""
    lat = clamp(lat, -85.05112878, 85.05112878)
    lon = ((lon + 180.0) % 360.0) - 180.0
    n = 2.0 ** z
    x = (lon + 180.0) / 360.0 * n * tile_size
    lat_rad = math.radians(lat)
    y = (1.0 - math.log(math.tan(lat_rad) + 1.0 / math.cos(lat_rad)) / math.pi) / 2.0
    y = y * n * tile_size
    return x, y


def pixel_xy_to_latlon(px: float, py: float, z: int, tile_size: int = 256) -> Tuple[float, float]:
    n = 2.0 ** z
    lon = px / (n * tile_size) * 360.0 - 180.0
    y = 1.0 - 2.0 * (py / (n * tile_size))
    lat = math.degrees(math.atan(math.sinh(y * math.pi)))
    return lat, lon


def latlon_to_tile_xy(lat: float, lon: float, z: int) -> Tuple[int, int]:
    px, py = latlon_to_pixel_xy(lat, lon, z)
    return int(px // 256), int(py // 256)


def tile_xy_bounds(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    """Return tile bounds (min_lon, min_lat, max_lon, max_lat) in WGS84 for slippy tile x/y."""
    # top-left pixel
    px0 = float(x * 256)
    py0 = float(y * 256)
    # bottom-right pixel
    px1 = float((x + 1) * 256)
    py1 = float((y + 1) * 256)
    lat0, lon0 = pixel_xy_to_latlon(px0, py0, z)
    lat1, lon1 = pixel_xy_to_latlon(px1, py1, z)
    min_lon = min(lon0, lon1)
    max_lon = max(lon0, lon1)
    min_lat = min(lat0, lat1)
    max_lat = max(lat0, lat1)
    return min_lon, min_lat, max_lon, max_lat
