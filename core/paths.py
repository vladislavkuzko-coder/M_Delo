# core/paths.py
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class Paths:
    base_dir: str
    data_dir: str
    materials_dir: str
    db_path: str
    settings_path: str


def _is_frozen() -> bool:
    return getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS")


def _default_base_dir() -> str:
    if _is_frozen():
        return os.path.dirname(sys.executable)

    here = os.path.abspath(os.path.dirname(__file__))   # .../core
    base = os.path.abspath(os.path.join(here, ".."))    # корень проекта
    return base


def get_paths(base_dir: str | None = None) -> Paths:
    base = os.path.abspath(base_dir) if base_dir else _default_base_dir()

    data_dir = os.path.join(base, "data")
    materials_dir = os.path.join(base, "materials")
    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(materials_dir, exist_ok=True)

    db_path = os.path.join(data_dir, "registry.sqlite3")
    settings_path = os.path.join(data_dir, "settings.json")

    return Paths(
        base_dir=base,
        data_dir=data_dir,
        materials_dir=materials_dir,
        db_path=db_path,
        settings_path=settings_path,
    )


def resolve_paths(base_dir: str | None = None) -> Tuple[str, str, str, str]:
    """
    Совместимость со старым main.py:
    возвращает (base_dir, db_path, materials_dir, settings_path)
    """
    p = get_paths(base_dir)
    return p.base_dir, p.db_path, p.materials_dir, p.settings_path
