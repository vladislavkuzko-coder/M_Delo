# app/main.py
from __future__ import annotations

import os
import sys
from datetime import datetime
import logging

from PySide6.QtCore import QTimer, qInstallMessageHandler
from PySide6.QtWidgets import QApplication, QMessageBox

from core.db import init_db, get_user_by_credentials
from core.paths import resolve_paths
from core.settings import load_settings, save_settings
# writer-lock больше не используется: редактирование допускается нескольким пользователям,
# конфликты решаются оптимистической блокировкой на уровне карточек.
from core.backup import make_db_backup
from core.recovery import integrity_ok, find_latest_backup, restore_backup
from core.logging_util import setup_logging
from core.audit import purge_old

from ui.login_dialog import LoginDialog
from ui.main_window import MainWindow
from ui.theme import apply_theme


def main():
    # --- QWebEngine stutter / DirectComposition ---
    # На некоторых Windows-системах Chromium/QWebEngine пишет:
    #   direct_composition_support.cc ... 0x80004002
    # и при этом наблюдаются подёргивания при зуме/перетаскивании карты.
    # Безопасный дефолт — отключить DirectComposition, если пользователь
    # сам не переопределил флаги.
    os.environ.setdefault("QTWEBENGINE_CHROMIUM_FLAGS", "--disable-direct-composition")

    # Некоторые компоненты карты (QML/OpenStreetMap) при отсутствии сети пишут в консоль
    # "...osm_map.qml: Network error". Это не критично (есть оффлайн-режимы), но путает.
    # Фильтруем только эти сообщения, остальные оставляем.
    def _qt_msg_handler(_mode, _ctx, msg: str):
        try:
            if "osm_map.qml" in msg and "Network error" in msg:
                return
        except Exception:
            pass
        sys.stderr.write(msg + "\n")

    qInstallMessageHandler(_qt_msg_handler)

    app = QApplication(sys.argv)

    base_dir, db_path, materials_dir, settings_path = resolve_paths()
    data_dir = os.path.dirname(db_path)
    logger = setup_logging(data_dir)
    init_db(db_path)

    # ---- поддержка журнала действий ----
    try:
        purge_old(db_path, keep_days=365)
    except Exception:
        logger.exception("Audit purge failed")

    # ---- проверка целостности БД + предложение восстановить из резервной копии ----
    try:
        ok, msg = integrity_ok(db_path)
        if not ok:
            backups_dir = os.path.join(os.path.dirname(db_path), "backups")
            latest = find_latest_backup(db_path, backups_dir)
            if latest:
                mb = QMessageBox(None)
                mb.setIcon(QMessageBox.Warning)
                mb.setWindowTitle("Восстановление")
                mb.setText(
                    "Обнаружена проблема с базой данных (integrity_check не OK).\n\n"
                    f"Ответ SQLite: {msg}\n\n"
                    "Можно восстановить последнюю резервную копию."
                )
                b_restore = mb.addButton("Восстановить", QMessageBox.AcceptRole)
                mb.addButton("Продолжить", QMessageBox.RejectRole)
                mb.setDefaultButton(b_restore)
                mb.exec()
                if mb.clickedButton() == b_restore:
                    restore_backup(db_path, latest)
            # если backup нет — просто продолжаем (init_db уже создал структуру)
    except Exception:
        logger.exception("Integrity check failed")

    # ---- авто-резервная копия БД (1 раз в день) ----
    try:
        s0 = load_settings(settings_path)
        today = datetime.now().strftime("%Y-%m-%d")
        if (s0.get("last_db_backup_date") or "") != today:
            backups_dir = os.path.join(os.path.dirname(db_path), "backups")
            make_db_backup(db_path, backups_dir, keep_last=30)
            s0["last_db_backup_date"] = today
            save_settings(settings_path, s0)
    except Exception:
        logger.exception("Auto backup failed")

    # если синхронизация включена, а исходная папка не задана — считаем исходником текущую программу
    try:
        s_init = load_settings(settings_path)
        s_init.setdefault("sync", {})
        if not (s_init["sync"].get("source_dir") or "").strip():
            s_init["sync"]["source_dir"] = base_dir
            save_settings(settings_path, s_init)
    except Exception:
        logger.exception("Init sync defaults failed")

    # ---- окно входа: при ошибке логина/пароля НЕ выходим, даём повторить ----
    login = LoginDialog(None, settings_path=settings_path)
    user = None
    while True:
        if login.exec() != LoginDialog.Accepted:
            return
        username, password = login.get_credentials()
        user = get_user_by_credentials(db_path, username, password)
        if user:
            break
        QMessageBox.warning(None, "Вход", "Неверный логин/пароль или учетная запись отключена.")
        try:
            login.ed_pass.clear()
            login.ed_pass.setFocus()
        except Exception:
            pass

    # Права редактирования:
    # - администратор всегда может редактировать
    # - также можно выдать право can_edit обычным пользователям
    can_edit = (int(user.get("is_admin", 0)) == 1) or (int(user.get("can_edit", 0)) == 1)

    read_only_mode = True

    # ---- защита "зеркала": редактирование только из source_dir (обычно флешка) ----
    def _edit_allowed_here() -> bool:
        try:
            s = load_settings(settings_path)
            cfg = s.get("sync", {})
            src_dir = os.path.abspath((cfg.get("source_dir") or "").strip() or base_dir)
            allow_anywhere = bool(cfg.get("allow_admin_edit_anywhere", False))
            if allow_anywhere:
                return True
            return os.path.abspath(base_dir) == src_dir
        except Exception:
            return True

    if can_edit and _edit_allowed_here():
        read_only_mode = False
    else:
        read_only_mode = True
        if can_edit and (not _edit_allowed_here()):
            QMessageBox.information(
                None,
                "Режим просмотра",
                "Этот запуск выполнен не из 'источника' (обычно флешка).\n"
                "Для безопасности зеркало открыто только для просмотра.\n\n"
                "Если нужно разрешить редактирование из любого места — включи настройку\n"
                "Настройки → Синхронизация → 'Разрешить админу редактировать не из источника'."
            )

    settings = load_settings(settings_path)
    apply_theme(app, settings)

    def apply_theme_cb():
        s = load_settings(settings_path)
        apply_theme(app, s)

    win = MainWindow(
        db_path=db_path,
        materials_dir=materials_dir,
        settings_path=settings_path,
        user={
            "username": user["username"],
            "is_admin": bool(user.get("is_admin", 0)),
            "can_edit": bool(can_edit) and (not read_only_mode),
        },
        settings=settings,
        apply_theme_cb=apply_theme_cb
    )
    win.show()

    # ---- автосинхронизация по таймеру (только если включено) ----
    timer = QTimer()
    timer.setSingleShot(False)

    def tick_sync():
        # синхронизируем только если включено и параметры заданы
        try:
            win.sync_now(silent=True)
        except Exception:
            logger.exception("Auto sync tick failed")

    def apply_timer_from_settings():
        s = load_settings(settings_path)
        cfg = s.get("sync", {})
        enabled = bool(cfg.get("enabled", False))
        minutes = int(cfg.get("period_minutes", 0) or 0)
        if enabled and minutes > 0:
            timer.start(minutes * 60 * 1000)
        else:
            timer.stop()

    apply_timer_from_settings()
    timer.timeout.connect(tick_sync)

    # обновлять таймер при возврате фокуса (если ты поменял настройки)
    win.windowActivated = apply_timer_from_settings  # type: ignore

    rc = app.exec()

    # ---- синхронизация при закрытии ----
    try:
        win.sync_now(silent=True)
    except Exception:
        logger.exception("Final sync on exit failed")

    # writer_lock был удалён (теперь редактировать могут несколько пользователей).
    # Оставляем безопасный выход без обращения к несуществующей переменной.

    sys.exit(rc)


if __name__ == "__main__":
    main()
