from __future__ import annotations

import json
from dataclasses import dataclass
from typing import List, Tuple


Ring = List[Tuple[float, float]]  # (lon,lat)


@dataclass
class GeoMask:
    polygons: List[Ring]
    min_lon: float
    min_lat: float
    max_lon: float
    max_lat: float


def _update_bounds(bounds, pts):
    min_lon, min_lat, max_lon, max_lat = bounds
    for lon, lat in pts:
        if lon < min_lon:
            min_lon = lon
        if lon > max_lon:
            max_lon = lon
        if lat < min_lat:
            min_lat = lat
        if lat > max_lat:
            max_lat = lat
    return min_lon, min_lat, max_lon, max_lat


def load_geojson_mask(path: str) -> GeoMask:
    """Load GeoJSON file and return outer rings (Polygon/MultiPolygon)."""
    with open(path, "r", encoding="utf-8") as f:
        gj = json.load(f)

    polygons: List[Ring] = []
    bounds = (1e9, 1e9, -1e9, -1e9)

    def handle_geom(geom):
        nonlocal bounds
        if not geom:
            return
        gtype = geom.get("type")
        coords = geom.get("coordinates")
        if gtype == "Polygon" and coords:
            ring = coords[0]
            poly = [(float(lon), float(lat)) for lon, lat in ring]
            polygons.append(poly)
            bounds = _update_bounds(bounds, poly)
        elif gtype == "MultiPolygon" and coords:
            for poly_coords in coords:
                if not poly_coords:
                    continue
                ring = poly_coords[0]
                poly = [(float(lon), float(lat)) for lon, lat in ring]
                polygons.append(poly)
                bounds = _update_bounds(bounds, poly)

    if gj.get("type") == "FeatureCollection":
        for feat in gj.get("features", []) or []:
            handle_geom((feat or {}).get("geometry"))
    elif gj.get("type") == "Feature":
        handle_geom(gj.get("geometry"))
    else:
        handle_geom(gj)

    if not polygons:
        raise ValueError("GeoJSON не содержит Polygon/MultiPolygon")

    min_lon, min_lat, max_lon, max_lat = bounds
    return GeoMask(polygons=polygons, min_lon=min_lon, min_lat=min_lat, max_lon=max_lon, max_lat=max_lat)
