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

        # Delete user command
        try:
            await message.delete()
        except:
            pass

        await show_main_menu(message, db)

    # Delete any text messages from user (not requested by bot)
    @dp.message(F.text)
    async def handle_text(message: Message):
        """Auto-delete user text messages."""
        if not is_admin(message.from_user.id):
            return

        try:
            await message.delete()
        except:
            pass

    # Callback handlers
    @dp.callback_query(F.data == "main_menu")
    async def callback_main_menu(callback: CallbackQuery):
        """Show main menu."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()
        await show_main_menu(callback.message, db, edit=True)

    @dp.callback_query(F.data == "check_mail")
    async def callback_check_mail(callback: CallbackQuery):
        """Manual mail check."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer("🔄 Проверяю почту...")

        try:
            client = IMAPClient(db)
            results = await client.check_and_process()

            if not results:
                # Show notification about empty check
                builder = InlineKeyboardBuilder()
                builder.row(InlineKeyboardButton(text="🗑 Закрыть", callback_data="delete_message"))
                
                await callback.message.answer(
                    "✅ Проверка завершена. Новых реестров не найдено.",
                    reply_markup=builder.as_markup()
                )
                return

            # Send results
            for result in results:
                await send_tax_report(callback.message, result, db)

        except Exception as e:
            logger.error(f"Error in callback check: {e}", exc_info=True)
            
            builder = InlineKeyboardBuilder()
            builder.row(InlineKeyboardButton(text="🗑 Закрыть", callback_data="delete_message"))
            
            await callback.message.answer(
                f"❌ Ошибка при проверке почты:\n\n<code>{str(e)}</code>",
                reply_markup=builder.as_markup()
            )

    @dp.callback_query(F.data == "show_status")
    async def callback_status(callback: CallbackQuery):
        """Show status."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()

        stats = await db.get_stats()
        
        last_check = stats.get("last_check")
        if last_check and isinstance(last_check, str):
            try:
                last_check = datetime.fromisoformat(last_check).strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                last_check = "Никогда"
        else:
            last_check = "Никогда"

        text = (
            f"📊 <b>Статус бота</b>\n\n"
            f"🕐 Последняя проверка: {last_check}\n"
            f"📧 Писем обработано: {stats.get('emails_processed', 0)}\n"
            f"📁 Реестров обработано: {stats.get('files_processed', 0)}"
        )

        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="◀️ Назад в меню", callback_data="main_menu"))

        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "show_stats")
    async def callback_stats(callback: CallbackQuery):
        """Show NPD statistics."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()

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
            f"✅ Внесено в налоговую: {month_stats['confirmed_income']:,.2f} RUB\n"
            f"⚠️ Ждёт подтверждения: {month_stats['pending_income']:,.2f} RUB\n"
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
            f"📁 Реестров с доходом: {all_time_stats['registries_count']}"
        )

        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="◀️ Назад в меню", callback_data="main_menu"))

        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "show_history")
    async def callback_history(callback: CallbackQuery):
        """Show history."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()

        history = await db.get_history(limit=15)

        if not history:
            text = "📋 История пуста. Реестры ещё не обрабатывались."
        else:
            text = "📋 <b>История реестров</b>\n\n"

            for reg in history:
                date = reg["date"]
                amount = reg["total_amount"]
                count = reg["payments_count"]
                status = reg["status"]
                
                if status == "confirmed":
                    emoji = "✅"
                else:
                    emoji = "🟡"
                
                if amount > 0:
                    status_text = " (ждёт)" if status == "pending" else ""
                    text += f"{emoji} {date} — <b>{amount:,.2f} RUB</b> ({count} шт.){status_text}\n"
                else:
                    text += f"⚪ {date} — пусто\n"

        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="◀️ Назад в меню", callback_data="main_menu"))

        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "show_pending")
    async def callback_pending(callback: CallbackQuery):
        """Show pending registries."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()

        pending = await db.get_pending_registries()

        if not pending:
            text = "✅ Все реестры подтверждены!"
        else:
            total_pending = sum(r["total_amount"] for r in pending)
            
            text = f"⚠️ <b>Неподтверждённые реестры ({len(pending)})</b>\n\n"

            for reg in pending:
                date = reg["date"]
                amount = reg["total_amount"]
                count = reg["payments_count"]
                
                text += f"🟡 {date} — <b>{amount:,.2f} RUB</b> ({count} шт.)\n"

            text += f"\n<b>Всего ждёт внесения: {total_pending:,.2f} RUB</b>"

        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="◀️ Назад в меню", callback_data="main_menu"))

        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "show_settings")
    async def callback_settings(callback: CallbackQuery):
        """Show settings."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        await callback.answer()

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
            f"📢 Уведомления о пустых реестрах: {notify_status}\n\n"
            f"📝 Описание для налоговой:\n<code>{tax_desc}</code>\n\n"
            f"<i>Чтобы изменить описание, измените TAX_DESCRIPTION в .env и перезапустите бота</i>"
        )

        builder = InlineKeyboardBuilder()
        builder.row(
            InlineKeyboardButton(
                text="📢 Переключить уведомления",
                callback_data="settings_toggle_notify"
            )
        )
        builder.row(
            InlineKeyboardButton(
                text="◀️ Назад в меню",
                callback_data="main_menu"
            )
        )

        await callback.message.edit_text(text, reply_markup=builder.as_markup())

    @dp.callback_query(F.data == "settings_toggle_notify")
    async def callback_toggle_notify(callback: CallbackQuery):
        """Toggle empty registries notifications."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        current = await db.get_setting("notify_empty_registries")
        if current is None:
            current = os.getenv("NOTIFY_EMPTY_REGISTRIES", "true")

        new_value = "false" if current.lower() == "true" else "true"
        await db.set_setting("notify_empty_registries", new_value)

        await callback.answer("✅ Настройка изменена")
        
        # Refresh settings view
        await callback_settings(callback)

    # Callback handlers for tax reports
    @dp.callback_query(F.data.startswith("registry_details_"))
    async def callback_registry_details(callback: CallbackQuery):
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
        
        # Show even if empty
        if not payments:
            text = (
                f"📋 <b>Детализация за {date}</b>\n\n"
                f"⚪ Платежей не найдено.\n\n"
                f"<i>Реестр пустой — доход 0.00 RUB</i>"
            )
        else:
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

        builder = InlineKeyboardBuilder()
        builder.row(InlineKeyboardButton(text="🗑 Закрыть", callback_data="delete_message"))

        await callback.message.answer(text, reply_markup=builder.as_markup())
        await callback.answer()

    @dp.callback_query(F.data.startswith("registry_csv_"))
    async def callback_registry_csv(callback: CallbackQuery):
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

    @dp.callback_query(F.data.startswith("confirm_registry_"))
    async def callback_confirm_registry(callback: CallbackQuery):
        """Confirm registry as added to tax."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        date = callback.data.replace("confirm_registry_", "")
        
        await db.confirm_registry(date)
        
        await callback.answer("✅ Отмечено как внесено в налоговую")
        
        # Delete message
        try:
            await callback.message.delete()
        except:
            pass

    @dp.callback_query(F.data == "delete_message")
    async def callback_delete_message(callback: CallbackQuery):
        """Delete message."""
        if not is_admin(callback.from_user.id):
            await callback.answer("⛔ Access denied.", show_alert=True)
            return

        try:
            await callback.message.delete()
            await callback.answer()
        except Exception as e:
            logger.error(f"Error deleting message: {e}")
            await callback.answer("❌ Не удалось закрыть")


async def show_main_menu(message: Message, db: Database, edit: bool = False):
    """Show main menu with inline buttons."""
    # Get pending count
    pending = await db.get_pending_registries()
    pending_count = len(pending)
    
    text = "👋 <b>YooKassa Tax Bot для НПД</b>\n\nВыберите действие:"

    builder = InlineKeyboardBuilder()
    
    builder.row(
        InlineKeyboardButton(text="🔍 Проверить почту", callback_data="check_mail"),
        InlineKeyboardButton(text="📊 Статус", callback_data="show_status")
    )
    builder.row(
        InlineKeyboardButton(text="📈 Статистика НПД", callback_data="show_stats"),
        InlineKeyboardButton(text="📋 История", callback_data="show_history")
    )
    
    if pending_count > 0:
        builder.row(
            InlineKeyboardButton(
                text=f"⚠️ Неподтверждённые ({pending_count})",
                callback_data="show_pending"
            )
        )
    
    builder.row(
        InlineKeyboardButton(text="⚙️ Настройки", callback_data="show_settings")
    )

    if edit:
        await message.edit_text(text, reply_markup=builder.as_markup())
    else:
        await message.answer(text, reply_markup=builder.as_markup())


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
        # Has payments - show confirm button
        builder.row(
            InlineKeyboardButton(
                text="✅ Добавлено в налоговую",
                callback_data=f"confirm_registry_{date}"
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="📊 Показать детали",
            callback_data=f"registry_details_{date}"
        )
    )
    
    if count > 0:
        builder.row(
            InlineKeyboardButton(
                text="📄 Скачать CSV",
                callback_data=f"registry_csv_{date}"
            )
        )
    
    builder.row(
        InlineKeyboardButton(
            text="🗑 Закрыть",
            callback_data="delete_message"
        )
    )

    await message.answer(text, reply_markup=builder.as_markup())