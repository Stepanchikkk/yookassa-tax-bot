import logging
import os
from datetime import datetime
from pathlib import Path

from aiogram import Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder

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
            "👋 <b>YooKassa Tax Bot для НПД</b>\n\n"
            "Автоматическая обработка реестров для «Мой налог».\n\n"
            "<b>Команды:</b>\n"
            "/run — проверить почту сейчас\n"
            "/status — статистика обработки\n"
            "/stats — доходы и статистика НПД\n"
            "/history — история реестров\n"
            "/settings — настройки бота"
        )

    @dp.message(Command("status"))
    async def cmd_status(message: Message):
        """Handle /status command."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        stats = await db.get_stats()
        
        last_check = stats.get("last_check")
        
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
                await send_tax_report(message, result, db)

            await status_msg.edit_text(f"✅ Обработано реестров: {len(results)}")

        except Exception as e:
            logger.error(f"Error in manual run: {e}", exc_info=True)
            await status_msg.edit_text(f"❌ Ошибка: {str(e)}")

    @dp.message(Command("stats"))
    async def cmd_stats(message: Message):
        """Handle /stats command - НПД statistics."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        now = datetime.now()
        current_year = now.year
        current_month = now.month

        # Current month stats
        month_stats = await db.get_monthly_stats(current_year, current_month)
        
        # Current year stats
        year_stats = await db.get_yearly_stats(current_year)
        
        # All time stats
        all_time_stats = await db.get_all_time_stats()

        # Month name
        month_names = {
            1: "январь", 2: "февраль", 3: "март", 4: "апрель",
            5: "май", 6: "июнь", 7: "июль", 8: "август",
            9: "сентябрь", 10: "октябрь", 11: "ноябрь", 12: "декабрь"
        }
        month_name = month_names.get(current_month, str(current_month))

        # Days in month
        import calendar
        days_in_month = calendar.monthrange(current_year, current_month)[1]

        # НПД limit check
        npd_limit = float(os.getenv("NPD_YEARLY_LIMIT", "2400000"))
        year_income = year_stats["total_income"]
        limit_percent = (year_income / npd_limit * 100) if npd_limit > 0 else 0
        
        limit_emoji = "🟢"
        if limit_percent >= 90:
            limit_emoji = "🔴"
        elif limit_percent >= 75:
            limit_emoji = "🟡"

        text = (
            f"📊 <b>Статистика доходов НПД</b>\n\n"
            f"<b>За текущий месяц ({month_name} {current_year}):</b>\n"
            f"💰 Доход: <b>{month_stats['total_income']:,.2f} RUB</b>\n"
            f"💸 Комиссия: {month_stats['total_commission']:,.2f} RUB\n"
            f"📦 Платежей: {month_stats['total_payments']}\n"
            f"📅 Дней с доходом: {month_stats['days_with_income']}/{days_in_month}\n\n"
            f"<b>За {current_year} год:</b>\n"
            f"💰 Доход: <b>{year_income:,.2f} RUB</b>\n"
            f"💸 Комиссия: {year_stats['total_commission']:,.2f} RUB\n"
            f"📦 Платежей: {year_stats['total_payments']}\n\n"
            f"<b>Лимит НПД {current_year}:</b>\n"
            f"{limit_emoji} {year_income:,.2f} / {npd_limit:,.0f} RUB ({limit_percent:.1f}%)\n"
        )

        if limit_percent >= 90:
            text += f"\n⚠️ <b>Внимание!</b> Вы приближаетесь к годовому лимиту НПД!"

        text += (
            f"\n\n<b>За всё время:</b>\n"
            f"💰 Доход: {all_time_stats['total_income']:,.2f} RUB\n"
            f"📦 Платежей: {all_time_stats['total_payments']}\n"
            f"📁 Реестров: {all_time_stats['registries_count']}"
        )

        await message.answer(text)

    @dp.message(Command("history"))
    async def cmd_history(message: Message):
        """Handle /history command."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        history = await db.get_history(limit=15)

        if not history:
            await message.answer("📋 История пуста. Реестры ещё не обрабатывались.")
            return

        text = "📋 <b>История реестров</b>\n\n"

        for reg in history:
            date = reg["date"]
            amount = reg["total_amount"]
            count = reg["payments_count"]
            
            emoji = "✅" if amount > 0 else "⚪"
            
            if amount > 0:
                text += f"{emoji} {date} — <b>{amount:,.2f} RUB</b> ({count} шт.)\n"
            else:
                text += f"{emoji} {date} — пусто\n"

        await message.answer(text)

    @dp.message(Command("settings"))
    async def cmd_settings(message: Message):
        """Handle /settings command."""
        if not is_admin(message.from_user.id):
            await message.answer("⛔ Access denied.")
            return

        # Get current settings
        notify_empty = await db.get_setting("notify_empty_registries")
        if notify_empty is None:
            notify_empty = os.getenv("NOTIFY_EMPTY_REGISTRIES", "true")
        
        tax_desc = await db.get_setting("tax_description")
        if tax_desc is None:
            tax_desc = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

        notify_status = "✅ Вкл" if notify_empty.lower() == "true" else "❌ Выкл"

        text = (
            f"⚙️ <b>Настройки бота</b>\n\n"
            f"📢 Уведомления о пустых реестрах: {notify_status}\n"
            f"📝 Описание для налоговой:\n<code>{tax_desc}</code>"
        )

        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="📢 Уведомления",
                callback_data="settings_toggle_notify"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="📝 Изменить описание",
                callback_data="settings_change_desc"
            )
        )

        await message.answer(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "settings_toggle_notify")
    async def toggle_notify(callback: CallbackQuery):
        """Toggle empty registries notifications."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        current = await db.get_setting("notify_empty_registries")
        if current is None:
            current = os.getenv("NOTIFY_EMPTY_REGISTRIES", "true")

        new_value = "false" if current.lower() == "true" else "true"
        await db.set_setting("notify_empty_registries", new_value)

        notify_status = "✅ Вкл" if new_value == "true" else "❌ Выкл"
        
        tax_desc = await db.get_setting("tax_description")
        if tax_desc is None:
            tax_desc = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

        text = (
            f"⚙️ <b>Настройки бота</b>\n\n"
            f"📢 Уведомления о пустых реестрах: {notify_status}\n"
            f"📝 Описание для налоговой:\n<code>{tax_desc}</code>"
        )

        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="📢 Уведомления",
                callback_data="settings_toggle_notify"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="📝 Изменить описание",
                callback_data="settings_change_desc"
            )
        )

        await callback.message.edit_text(text, reply_markup=builder.as_markup())
        await callback.answer("✅ Настройка изменена")

    @dp.callback_query(F.data == "settings_change_desc")
    async def change_description(callback: CallbackQuery):
        """Prompt to change tax description."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()
        await callback.message.answer(
            "📝 Отправьте новое описание для налоговой.\n\n"
            "Например: <code>Доступ к IT-сервису (подписка)</code>\n\n"
            "Или /cancel для отмены."
        )
        # Note: для полноценной реализации нужен FSM, упростим через простую команду

    # Callback handlers for tax reports
    @dp.callback_query(F.data.startswith("registry_details_"))
    async def show_registry_details(callback: CallbackQuery):
        """Show detailed payments list."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        date = callback.data.replace("registry_details_", "")
        
        registry = await db.get_registry(date)
        
        if not registry:
            await callback.answer("❌ Реестр не найден", show_alert=True)
            return

        payments = registry.get("payments", [])
        
        if not payments:
            await callback.answer("📋 Платежей нет", show_alert=True)
            return

        text = f"📋 <b>Детализация платежей ({len(payments)} шт.)</b>\n\n"

        for i, p in enumerate(payments[:50], 1):  # Limit to 50 to avoid message length limit
            amount = p["amount"]
            time = p.get("payment_time", "").split()[0] if p.get("payment_time") else "?"
            desc = p.get("description", "")[:30]
            
            text += f"{i}️⃣ {time} — <b>{amount:.2f} RUB</b>"
            if desc:
                text += f" ({desc})"
            text += "\n"

        if len(payments) > 50:
            text += f"\n<i>... и ещё {len(payments) - 50} платежей</i>"

        await callback.message.answer(text)
        await callback.answer()

    @dp.callback_query(F.data.startswith("registry_csv_"))
    async def send_registry_csv(callback: CallbackQuery):
        """Send CSV files for registry."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        date = callback.data.replace("registry_csv_", "")
        
        registry = await db.get_registry(date)
        
        if not registry:
            await callback.answer("❌ Реестр не найден", show_alert=True)
            return

        tax_file = registry.get("tax_file")
        payments_file = registry.get("payments_file")

        if not tax_file or not Path(tax_file).exists():
            await callback.answer("❌ Файлы не найдены", show_alert=True)
            return

        # Send files
        await callback.message.answer_document(
            FSInputFile(tax_file),
            caption="📄 Итоговая запись для НПД"
        )

        if payments_file and Path(payments_file).exists():
            await callback.message.answer_document(
                FSInputFile(payments_file),
                caption="📋 Детализация платежей"
            )

        await callback.answer("✅ Файлы отправлены")

    @dp.callback_query(F.data.startswith("delete_message"))
    async def delete_message(callback: CallbackQuery):
        """Delete message."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        try:
            await callback.message.delete()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            await callback.answer("❌ Не удалось удалить")


async def send_tax_report(message: Message, result: dict, db: Database):
    """Send tax report to admin."""
    date = result["date"]
    total = result["total_amount"]
    count = result["payments_count"]
    commission = result["commission"]
    
    # Get tax description from settings or env
    description = await db.get_setting("tax_description")
    if description is None:
        description = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

    # Check if should notify about empty registries
    if count == 0:
        notify_empty = await db.get_setting("notify_empty_registries")
        if notify_empty is None:
            notify_empty = os.getenv("NOTIFY_EMPTY_REGISTRIES", "true")
        
        if notify_empty.lower() != "true":
            return  # Don't send notification for empty registry

    # Save to database
    await db.save_registry(result)

    # Format message
    text = (
        f"📊 <b>Реестр от {date}</b>\n\n"
        f"💰 Доход: <b>{total:,.2f} RUB</b>\n"
        f"📦 Платежей: {count}\n"
        f"💸 Комиссия: {commission:,.2f} RUB (справочно)\n\n"
        f"<b>Для «Мой налог»:</b>\n"
        f"<code>{date} — {total:.2f} RUB — {description}</code>"
    )

    # Build keyboard
    builder = InlineKeyboardBuilder()
    
    if count > 0:
        builder.row(
            InlineKeyboardButton(
                text="📊 Показать детали",
                callback_data=f"registry_details_{date}"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="📄 Скачать CSV",
                callback_data=f"registry_csv_{date}"
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="🗑 Удалить",
            callback_data="delete_message"
        )
    )

    await message.answer(text, reply_markup=builder.as_markup())