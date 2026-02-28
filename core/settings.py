# core/settings.py
from __future__ import annotations

import json
import os
from typing import Any, Dict


DEFAULT_SETTINGS: Dict[str, Any] = {
    # По умолчанию — светлая тема (первый запуск)
    "theme": {"name": "light"},
    # Название приложения (фиксированное)
    "app": {"title": "Меридиан.Дело", "short_title": "М-Дело"},
    "map": {
        "mode": "online",
        # stored relative to materials_dir
        "pbf": "",
        "shp_layers": [],
        "mbtiles": "",
        "pmtiles": "",
    },
    "sync": {
        "enabled": False,
        "source_dir": "",
        "mirror_dir": "",
        "period_minutes": 0
    },
    "control_filters": {"status": ["ДЕЙСТВУЮЩИЙ"], "days_ahead": 45},
    "tables": {},
    # последние открытые сущности для быстрого перехода
    # элементы: {"kind": "contract"|"object", "id": int, "label": str}
    "recent": [],
    # дата последнего автобэкапа БД в формате YYYY-MM-DD
    "last_db_backup_date": "",
    # настройки нумерации (можно менять в Настройках)
    "numbering": {
        # Входящие: Рег№ внутри
        "in_dept": {
            "enabled": True,
            "prefix": "17.07-3/",
            "suffix": "-{yy}",
            "pad": 0,
            "reset_per_year": True,
        },
        # Исходящие: Номер
        "out_doc": {
            "enabled": True,
            "prefix": "17.07-3/",
            "suffix": "-{yy}",
            "pad": 0,
            "reset_per_year": True,
        },
        # Внутренние: Номер
        "internal_doc": {
            "enabled": True,
            "prefix": "17.07-3/",
            "suffix": "-{yy}",
            "pad": 0,
            "reset_per_year": True,
        },
    },
    # Привязка учетной записи пользователя к Исполнителю (для "Личного кабинета")
    "user_executor_map": {},
}


def load_settings(path: str) -> Dict[str, Any]:
    # Some dialogs may be opened without a settings_path (e.g. standalone widgets).
    # In this case we fall back to defaults and avoid touching the filesystem.
    if not (path or "").strip():
        return DEFAULT_SETTINGS.copy()

    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        save_settings(path, DEFAULT_SETTINGS.copy())
        return DEFAULT_SETTINGS.copy()

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except Exception:
        data = {}

    # merge defaults
    merged = DEFAULT_SETTINGS.copy()
    merged.update(data)

    merged.setdefault("sync", {})
    for k, v in DEFAULT_SETTINGS["sync"].items():
        merged["sync"].setdefault(k, v)

    merged.setdefault("numbering", {})
    for k, v in DEFAULT_SETTINGS.get("numbering", {}).items():
        merged["numbering"].setdefault(k, v)

    merged.setdefault("user_executor_map", {})

    merged.setdefault("theme", DEFAULT_SETTINGS["theme"])
    merged.setdefault("map", DEFAULT_SETTINGS["map"])
    for k, v in DEFAULT_SETTINGS.get("map", {}).items():
        if isinstance(merged.get("map"), dict):
            merged["map"].setdefault(k, v)
    merged.setdefault("control_filters", DEFAULT_SETTINGS["control_filters"])
    merged.setdefault("tables", {})
    merged.setdefault("recent", [])
    merged.setdefault("last_db_backup_date", "")
    return merged


def _deep_merge(base: Any, patch: Any) -> Any:
    """Deep-merge dictionaries; patch overrides base."""
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out and isinstance(out.get(k), dict) and isinstance(v, dict):
                out[k] = _deep_merge(out.get(k), v)
            else:
                out[k] = v
        return out
    return patch


def save_settings(path: str, data: Dict[str, Any]) -> None:
    """Save settings, preserving keys that callers didn't touch.

    Multiple parts of the UI may keep their own in-memory copy of settings.
    If they write it back later, it can accidentally wipe newer values
    (e.g. user_executor_map). To avoid this, we reload current settings
    from disk and deep-merge the patch before writing.
    """
    if not (path or "").strip():
        return

    os.makedirs(os.path.dirname(path), exist_ok=True)

    current: Dict[str, Any] = {}
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                current = json.load(f) or {}
    except Exception:
        current = {}

    merged = _deep_merge(current, data or {})

    with open(path, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

