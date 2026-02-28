# core/locking.py
from __future__ import annotations

import os
import json
import time
import socket
from dataclasses import dataclass
from typing import Optional


@dataclass
class LockHandle:
    path: str

    def release(self):
        try:
            os.remove(self.path)
        except Exception:
            pass


def _lock_meta() -> dict:
    return {
        "host": socket.gethostname(),
        "pid": os.getpid(),
        "time": int(time.time()),
    }


def acquire_writer_lock(lock_path: str, stale_seconds: int = 12 * 60 * 60) -> Optional[LockHandle]:
    """
    Пытаемся создать lock-файл (эксклюзивно).
    Если он уже есть — проверяем "протухание" и при необходимости удаляем.
    Возвращаем LockHandle либо None.
    """
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)

    # если lock уже есть — проверим, не протух ли
    if os.path.exists(lock_path):
        try:
            with open(lock_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            ts = int(meta.get("time", 0))
            if ts and (time.time() - ts) > stale_seconds:
                # протух — удаляем
                os.remove(lock_path)
        except Exception:
            # если файл битый — тоже удаляем (чтобы не блокировал навсегда)
            try:
                os.remove(lock_path)
            except Exception:
                pass

    # эксклюзивное создание
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        try:
            meta = _lock_meta()
            os.write(fd, json.dumps(meta, ensure_ascii=False).encode("utf-8"))
        finally:
            os.close(fd)
        return LockHandle(lock_path)
    except FileExistsError:
        return None
    except Exception:
        return None


def describe_lock(lock_path: str) -> str:
    if not os.path.exists(lock_path):
        return "lock отсутствует"
    try:
        with open(lock_path, "r", encoding="utf-8") as f:
            meta = json.load(f)
        return f"занято: host={meta.get('host')} pid={meta.get('pid')} time={meta.get('time')}"
    except Exception:
        return "занято (не удалось прочитать lock)"
