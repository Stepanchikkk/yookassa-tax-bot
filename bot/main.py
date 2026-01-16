import asyncio
import logging
import os
import sys
from pathlib import Path

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.types import BotCommand

from bot.database import Database
from bot.handlers import register_handlers
from bot.scheduler import Scheduler

# Configure logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point."""
    # Validate environment
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN not set in .env")
        sys.exit(1)

    admin_ids = os.getenv("ADMIN_IDS", "")
    if not admin_ids:
        logger.error("ADMIN_IDS not set in .env")
        sys.exit(1)

    # Ensure data directory exists
    data_dir = Path("/app/data")
    data_dir.mkdir(exist_ok=True)

    # Initialize database
    db = Database(data_dir / "bot.db")
    await db.init()

    # Initialize bot and dispatcher
    bot = Bot(
        token=token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )
    dp = Dispatcher()

    # Set bot commands menu (автоматически)
    await bot.set_my_commands([
        BotCommand(command="run", description="Проверить почту сейчас"),
        BotCommand(command="status", description="Статистика обработки"),
        BotCommand(command="stats", description="Доходы и статистика НПД"),
        BotCommand(command="history", description="История реестров"),
        BotCommand(command="settings", description="Настройки бота"),
    ])
    logger.info("Bot commands menu set")

    # Register handlers
    register_handlers(dp, db)

    # Initialize scheduler
    scheduler = Scheduler(bot, db)

    # Start scheduler in background
    scheduler_task = asyncio.create_task(scheduler.run())

    logger.info("Bot started successfully")

    try:
        # Start polling
        await dp.start_polling(bot)
    finally:
        # Cleanup
        scheduler_task.cancel()
        await db.close()
        await bot.session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")