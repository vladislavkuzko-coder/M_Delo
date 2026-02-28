# core/logging_util.py
from __future__ import annotations

import logging
import os
from logging.handlers import RotatingFileHandler


def get_log_path(data_dir: str) -> str:
    """Return absolute path to the main application log file.

    Ensures data/logs directory exists.
    """
    logs_dir = os.path.join(data_dir, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    return os.path.join(logs_dir, "app.log")


def setup_logging(data_dir: str) -> logging.Logger:
    """Configure rotating log file under data/logs.

    Returns a named logger used across the app.
    """
    try:
        log_path = get_log_path(data_dir)

        logger = logging.getLogger("registry_app")
        logger.setLevel(logging.INFO)

        # avoid duplicate handlers if called twice
        if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
            h = RotatingFileHandler(
                log_path, maxBytes=1_000_000, backupCount=5, encoding="utf-8"
            )
            fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            h.setFormatter(fmt)
            logger.addHandler(h)

        # also log warnings+ to console for dev runs
        if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
            sh = logging.StreamHandler()
            sh.setLevel(logging.WARNING)
            sh.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
            logger.addHandler(sh)

        return logger
    except Exception:
        # fallback to basic config
        logging.basicConfig(level=logging.INFO)
        return logging.getLogger("registry_app")
