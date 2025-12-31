import asyncio
from typing import Any

from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, MessageHandler, filters

from .config import ADMIN_BOT_TOKEN, ADMIN_BOT_ID, ADMIN_BOT_NAME, BOT_REFRESH_INTERVAL_S
from .handlers import start, set_token, open_settings_cmd, handle_buttons, handle_text, _tap_all
from .admin import admin_add_bot, admin_list_bots, admin_list_users, admin_bot_info, admin_botinfo_callback
from db import init_db, add_bot_instance, list_bot_instances


def _ensure_admin_bot():
    if not ADMIN_BOT_TOKEN:
        print("⚠️ ADMIN_BOT_TOKEN missing; admin bot will not start.")
        return
    bot_id = ADMIN_BOT_ID or "admin-bot"
    bot_name = ADMIN_BOT_NAME or "Admin Bot"
    add_bot_instance(bot_id, ADMIN_BOT_TOKEN, bot_name, role="admin", default_timezone="UTC")


def _build_application(bot_row: dict):
    app = ApplicationBuilder().token(bot_row["bot_token"]).build()
    app.bot_data["bot_id"] = bot_row["bot_id"]
    app.bot_data["role"] = bot_row.get("role") or "user"

    if app.bot_data["role"] == "admin":
        app.add_handler(CommandHandler("start", start))
        app.add_handler(CommandHandler("addbot", admin_add_bot))
        app.add_handler(CommandHandler("listbots", admin_list_bots))
        app.add_handler(CommandHandler("botinfo", admin_bot_info))
        app.add_handler(CommandHandler("bot", admin_bot_info))
        app.add_handler(CommandHandler("listusers", admin_list_users))
        app.add_handler(CallbackQueryHandler(admin_botinfo_callback, pattern=r"^admin_botinfo:"))
        return app

    app.add_handler(MessageHandler(filters.ALL, _tap_all), group=-1)
    app.add_handler(CallbackQueryHandler(_tap_all), group=-1)
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("token", set_token))
    app.add_handler(CommandHandler("settings", open_settings_cmd))
    app.add_handler(CallbackQueryHandler(handle_buttons))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
    return app


async def _start_application(app):
    await app.initialize()
    await app.start()
    await app.updater.start_polling()


async def _run_manager():
    init_db()
    _ensure_admin_bot()

    apps: dict[str, Any] = {}

    async def _start_bot_row(row: dict):
        bot_id = row["bot_id"]
        if bot_id in apps:
            return
        app = _build_application(row)
        await _start_application(app)
        apps[bot_id] = app
        print(f"✅ Bot started: {bot_id} (role={app.bot_data.get('role')})")

    for row in list_bot_instances():
        await _start_bot_row(row)

    if not apps:
        print("⚠️ No bots registered yet. Add admin bot via ADMIN_BOT_TOKEN or use /addbot after startup.")

    while True:
        await asyncio.sleep(BOT_REFRESH_INTERVAL_S)
        for row in list_bot_instances():
            if row["bot_id"] not in apps:
                await _start_bot_row(row)


def run():
    asyncio.run(_run_manager())
