import asyncio
import logging
import os
from datetime import datetime, time
from zoneinfo import ZoneInfo

from aiogram import Bot
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from pathlib import Path

from bot.database import Database
from bot.imap_client import IMAPClient

logger = logging.getLogger(__name__)


class Scheduler:
    """Daily email check scheduler."""

    def __init__(self, bot: Bot, db: Database):
        self.bot = bot
        self.db = db
        self.timezone = os.getenv("TIMEZONE", "UTC")
        self.daily_hour = int(os.getenv("DAILY_HOUR", "10"))
        self.daily_minute = int(os.getenv("DAILY_MINUTE", "0"))
        self.admin_ids = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]

    async def run(self):
        """Run scheduler loop."""
        logger.info(
            f"Scheduler started: daily check at {self.daily_hour:02d}:{self.daily_minute:02d} {self.timezone}"
        )

        while True:
            try:
                # Calculate next run time
                next_run = self._get_next_run_time()
                now = datetime.now(ZoneInfo(self.timezone))
                
                sleep_seconds = (next_run - now).total_seconds()

                if sleep_seconds > 0:
                    logger.info(f"Next check at {next_run.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                    await asyncio.sleep(sleep_seconds)

                # Run check
                logger.info("Running scheduled email check...")
                await self._run_check()

            except asyncio.CancelledError:
                logger.info("Scheduler cancelled")
                break
            except Exception as e:
                logger.error(f"Error in scheduler: {e}", exc_info=True)
                # Sleep 1 hour on error
                await asyncio.sleep(3600)

    def _get_next_run_time(self) -> datetime:
        """Calculate next run time."""
        tz = ZoneInfo(self.timezone)
        now = datetime.now(tz)
        
        # Target time today
        target_time = time(hour=self.daily_hour, minute=self.daily_minute)
        target = datetime.combine(now.date(), target_time, tzinfo=tz)

        # If already passed today, schedule for tomorrow
        if target <= now:
            from datetime import timedelta
            target = target + timedelta(days=1)

        return target

    async def _run_check(self):
        """Run email check and send results to admins."""
        try:
            client = IMAPClient(self.db)
            results = await client.check_and_process()

            if not results:
                logger.info("No new registries found")
                return

            # Send results to all admins
            for admin_id in self.admin_ids:
                try:
                    for result in results:
                        await self._send_report(admin_id, result)
                except Exception as e:
                    logger.error(f"Error sending to admin {admin_id}: {e}")

            logger.info(f"Sent {len(results)} reports to admins")

        except Exception as e:
            logger.error(f"Error in scheduled check: {e}", exc_info=True)

    async def _send_report(self, admin_id: int, result: dict):
        """Send tax report to admin with inline buttons."""
        date = result["date"]
        total = result["total_amount"]
        count = result["payments_count"]
        commission = result["commission"]
        
        # Get tax description from settings or env
        description = await self.db.get_setting("tax_description")
        if description is None:
            description = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

        # Check if should notify about empty registries
        if count == 0:
            notify_empty = await self.db.get_setting("notify_empty_registries")
            if notify_empty is None:
                notify_empty = os.getenv("NOTIFY_EMPTY_REGISTRIES", "true")
            
            if notify_empty.lower() != "true":
                return  # Don't send notification for empty registry

        # Save to database
        await self.db.save_registry(result)

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

        await self.bot.send_message(
            admin_id,
            text,
            reply_markup=builder.as_markup(),
            parse_mode="HTML"
        )