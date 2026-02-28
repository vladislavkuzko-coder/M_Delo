from __future__ import annotations

import os
import shutil
import time
import uuid
from typing import Optional


def _base_dir_from_db(db_path: str) -> str:
    # db_path: <base>/data/registry.sqlite3
    return os.path.abspath(os.path.join(os.path.dirname(db_path), ".."))


def attachments_root(db_path: str) -> str:
    base = _base_dir_from_db(db_path)
    root = os.path.join(base, "data", "attachments")
    os.makedirs(root, exist_ok=True)
    return root


def _safe_name(name: str) -> str:
    name = (name or "").strip().replace("\\", "_").replace("/", "_")
    name = name.replace(":", "_").replace("*", "_").replace("?", "_")
    name = name.replace('"', "_").replace("<", "_").replace(">", "_").replace("|", "_")
    return name or "file"


def ensure_local_copy(
    *,
    db_path: str,
    kind: str,
    reg_no: int,
    src_path: str,
    prefer_ext: Optional[str] = None,
) -> str:
    """Copy an arbitrary file into the program folder and return a *relative* path.

    - Keeps files under data/attachments/...
    - If src_path is already inside attachments folder, returns relative path.
    - For name collisions, appends _2/_3 ...
    """

    p = (src_path or "").strip().strip('"')
    if not p:
        return ""
    p = os.path.abspath(p)

    root = attachments_root(db_path)
    base = _base_dir_from_db(db_path)

    try:
        if os.path.commonpath([p, root]) == root:
            rel = os.path.relpath(p, base)
            return rel.replace("\\", "/")
    except Exception:
        pass

    sub = os.path.join(root, "correspondence", kind)
    os.makedirs(sub, exist_ok=True)

    src_name = _safe_name(os.path.basename(p))
    stem, ext = os.path.splitext(src_name)
    if prefer_ext and not ext:
        ext = prefer_ext
    stem = f"{reg_no:06d}_{stem}" if reg_no else stem
    cand = os.path.join(sub, f"{stem}{ext}")
    i = 2
    while os.path.exists(cand):
        cand = os.path.join(sub, f"{stem}_{i}{ext}")
        i += 1

    shutil.copy2(p, cand)
    rel = os.path.relpath(cand, base)
    return rel.replace("\\", "/")


def ensure_local_copy_numeric(
    *,
    db_path: str,
    kind: str,
    reg_no: int,
    src_path: str,
) -> str:
    """Copy a file into the program folder with an ASCII-only numeric name.

    На Windows QtPdf иногда не открывает файлы с кириллицей в имени/пути.
    Поэтому для вложений корреспонденции сохраняем как:
    <reg_no>_<timestamp>_<uuid>.<ext>
    """

    p = (src_path or "").strip().strip('"')
    if not p:
        return ""
    p = os.path.abspath(p)

    root = attachments_root(db_path)
    base = _base_dir_from_db(db_path)

    # already inside attachments
    try:
        if os.path.commonpath([p, root]) == root:
            rel = os.path.relpath(p, base)
            return rel.replace('\\', '/')
    except Exception:
        pass

    sub = os.path.join(root, "correspondence", kind)
    os.makedirs(sub, exist_ok=True)

    _, ext = os.path.splitext(os.path.basename(p))
    ext = ext or ""

    ts = int(time.time() * 1000)
    uid = uuid.uuid4().hex
    prefix = f"{reg_no:06d}" if reg_no else "000000"
    stem = f"{prefix}_{ts}_{uid}"
    cand = os.path.join(sub, f"{stem}{ext}")
    i = 2
    while os.path.exists(cand):
        cand = os.path.join(sub, f"{stem}_{i}{ext}")
        i += 1

    shutil.copy2(p, cand)
    rel = os.path.relpath(cand, base)
    return rel.replace('\\', '/')


def resolve_attachment_path(db_path: str, stored_path: str) -> str:
    """Resolve stored (relative or absolute) path to absolute local path.

    Исторически в базе могли храниться пути разного формата:
    - абсолютный путь (C:\\...)
    - относительный к корню программы: data/attachments/...
    - относительный к корню вложений: inspections/..., correspondence/...

    Функция старается корректно обработать все варианты.
    """
    sp = (stored_path or "").strip().strip('"')
    if not sp:
        return ""
    if os.path.isabs(sp):
        return sp

    base = _base_dir_from_db(db_path)

    # Нормальный путь относительно корня программы
    cand = os.path.abspath(os.path.join(base, sp))
    if os.path.exists(cand):
        return cand

    # Старый формат: путь относительно data/attachments
    root = attachments_root(db_path)
    sp2 = sp.replace('\\', '/').lstrip('/')
    cand2 = os.path.abspath(os.path.join(root, sp2))
    if os.path.exists(cand2):
        return cand2

    # Fallback: считаем, что это относительный к корню программы
    return cand


def ensure_local_copy_to_dir(
    *,
    db_path: str,
    rel_dir: str,
    src_path: str,
    filename: Optional[str] = None,
) -> str:
    """Copy a file into an arbitrary subfolder under data/attachments and return relative path.

    rel_dir: e.g. "inspections/000123" (relative to data/attachments)
    filename: optional fixed file name; if not provided uses source basename with collision handling.
    """
    p = (src_path or "").strip().strip('"')
    if not p:
        return ""
    p = os.path.abspath(p)

    root = attachments_root(db_path)
    base = _base_dir_from_db(db_path)

    # already inside attachments
    try:
        if os.path.commonpath([p, root]) == root:
            rel = os.path.relpath(p, base)
            return rel.replace('\\', '/')
    except Exception:
        pass

    rel_dir = (rel_dir or "").strip().replace('\\', '/').strip('/')
    sub = os.path.join(root, rel_dir)
    os.makedirs(sub, exist_ok=True)

    if filename:
        fname = _safe_name(filename)
    else:
        fname = _safe_name(os.path.basename(p))

    stem, ext = os.path.splitext(fname)
    cand = os.path.join(sub, f"{stem}{ext}")
    i = 2
    while os.path.exists(cand):
        cand = os.path.join(sub, f"{stem}_{i}{ext}")
        i += 1

    shutil.copy2(p, cand)
    rel = os.path.relpath(cand, base)
    return rel.replace('\\', '/')


def ensure_dir_under_attachments(db_path: str, rel_dir: str) -> str:
    """Ensure directory under data/attachments exists and return absolute path."""
    root = attachments_root(db_path)
    rel_dir = (rel_dir or "").strip().replace('\\', '/').strip('/')
    abs_dir = os.path.join(root, rel_dir)
    os.makedirs(abs_dir, exist_ok=True)
    return abs_dir
