import logging
import os
from datetime import datetime

from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message

from bot.database import Database
from bot.imap_client import IMAPClient

logger = logging.getLogger(__name__)

# Get admin IDs from env
ADMIN_IDS = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]


def is_admin(user_id: int) -> bool:
    """Check if user is admin."""
    return user_id in ADMIN_IDS


def register_handlers(dp: Dispatcher, db: Database):
    """Register all bot handlers."""

    @dp.message(Command("start"))
    async def cmd_start(message: Message):
        """Handle /start command."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied. This bot is private.")
            return

        await message.answer(
            "👋 <b>YooKassa Tax Bot</b>\n\n"
            "Автоматическая обработка реестров для НПД.\n\n"
            "Команды:\n"
            "/run — проверить почту сейчас\n"
            "/status — статистика обработки"
        )

    @dp.message(Command("status"))
    async def cmd_status(message: Message):
        """Handle /status command."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        stats = await db.get_stats()
        
        last_check = stats.get("last_check")
        
        # Проверяем, что last_check это строка, а не None
        if last_check and isinstance(last_check, str):
            try:
                last_check = datetime.fromisoformat(last_check).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                last_check = "Never"
        else:
            last_check = "Never"

        await message.answer(
            f"📊 <b>Статус бота</b>\n\n"
            f"🕐 Последняя проверка: {last_check}\n"
            f"📧 Писем обработано: {stats.get('emails_processed', 0)}\n"
            f"📁 Реестров обработано: {stats.get('files_processed', 0)}"
        )

    @dp.message(Command("run"))
    async def cmd_run(message: Message):
        """Handle /run command - manual trigger."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        status_msg = await message.answer("🔄 Проверяю почту...")

        try:
            client = IMAPClient(db)
            results = await client.check_and_process()

            if not results:
                await status_msg.edit_text("✅ Новых реестров не найдено.")
                return

            # Send results
            for result in results:
                await send_tax_report(message, result)

            await status_msg.edit_text(f"✅ Обработано реестров: {len(results)}")

        except Exception as e:
            logger.error(f"Error in manual run: {e}", exc_info=True)
            await status_msg.edit_text(f"❌ Ошибка: {str(e)}")


async def send_tax_report(message: Message, result: dict):
    """Send tax report to admin."""
    date = result["date"]
    total = result["total_amount"]
    count = result["payments_count"]
    commission = result["commission"]
    description = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

    # Format message
    text = (
        f"📊 <b>Реестр от {date}</b>\n\n"
        f"💰 Доход: <b>{total:.2f} RUB</b>\n"
        f"📦 Платежей: {count}\n"
        f"💸 Комиссия: {commission:.2f} RUB (справочно)\n\n"
        f"<b>Для «Мой налог»:</b>\n"
        f"<code>{date} — {total:.2f} RUB — {description}</code>"
    )

    # Send message with files
    await message.answer(text)
    
    # Send tax_ready file
    if result.get("tax_file"):
        from aiogram.types import FSInputFile
        await message.answer_document(
            document=FSInputFile(result["tax_file"]),
            caption="📄 Итоговая запись для НПД"
        )

    # Send payments details
    if result.get("payments_file"):
        from aiogram.types import FSInputFile
        await message.answer_document(
            document=FSInputFile(result["payments_file"]),
            caption="📋 Детализация платежей"
        )