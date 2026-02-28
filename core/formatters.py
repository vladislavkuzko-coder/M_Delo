# core/formatters.py
from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation


def normalize_area(text: str) -> str:
    """
    Ввод: '203,2' -> '203,20'
    '1 203,2' -> '1 203,20'
    '1203.2' -> '1 203,20'
    Всегда 2 знака после запятой, разделение тысяч пробелом.
    """
    s = (text or "").strip()
    if not s:
        return ""

    s = s.replace(" ", "").replace("\u00A0", "")
    s = s.replace(",", ".")
    # оставить цифры и точку
    s = re.sub(r"[^0-9.]", "", s)
    if not s:
        return ""

    try:
        d = Decimal(s)
    except InvalidOperation:
        return ""

    q = d.quantize(Decimal("0.01"))
    # форматируем с тысячами
    sign = "-" if q < 0 else ""
    q = abs(q)
    int_part = int(q)
    frac_part = int((q - int_part) * 100)

    int_str = f"{int_part:,}".replace(",", " ")
    return f"{sign}{int_str},{frac_part:02d}"


def parse_area_to_float(text: str):
    s = (text or "").strip()
    if not s:
        return None
    s = s.replace(" ", "").replace("\u00A0", "")
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def normalize_date_ddmmyyyy(text: str) -> str:
    """
    Не строгий парсер: если '21122025' -> '21.12.2025'
    Если уже '21.12.2025' — оставляет.
    Иначе возвращает исходное (без падения).
    """
    t = (text or "").strip()
    if not t:
        return ""
    if re.fullmatch(r"\d{2}\.\d{2}\.\d{4}", t):
        return t
    if re.fullmatch(r"\d{8}", t):
        return f"{t[0:2]}.{t[2:4]}.{t[4:8]}"
    return t
