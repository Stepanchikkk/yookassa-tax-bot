import asyncio
import email
import hashlib
import imaplib
import logging
import os
from datetime import datetime, timedelta
from email.header import decode_header
from pathlib import Path
from typing import List, Dict, Optional

import aiofiles

from bot.csv_parser import parse_yookassa_csv
from bot.database import Database

logger = logging.getLogger(__name__)


class IMAPClient:
    """IMAP client for fetching emails with CSV attachments."""

    def __init__(self, db: Database):
        self.db = db
        self.host = os.getenv("IMAP_HOST")
        self.port = int(os.getenv("IMAP_PORT", "993"))
        self.user = os.getenv("IMAP_USER")
        self.password = os.getenv("IMAP_PASSWORD")
        self.from_filter = os.getenv("EMAIL_FROM_FILTER", "")
        self.subject_filter = os.getenv("EMAIL_SUBJECT_FILTER", "")
        self.days_to_check = int(os.getenv("DAYS_TO_CHECK", "7"))
        self.allowed_ext = os.getenv("ALLOWED_EXTENSIONS", ".csv").split(",")

        # Temp directory for downloads
        self.temp_dir = Path("/app/data/temp")
        self.temp_dir.mkdir(exist_ok=True)

    async def check_and_process(self) -> List[Dict]:
        """Check mailbox and process new CSV files."""
        logger.info("Starting email check...")

        try:
            # Connect to IMAP (sync, but wrapped in executor)
            loop = asyncio.get_event_loop()
            mail = await loop.run_in_executor(None, self._connect)

            # Search emails
            messages = await loop.run_in_executor(None, self._search_messages, mail)
            logger.info(f"Found {len(messages)} messages to check")

            results = []

            for msg_id in messages:
                try:
                    # Fetch message
                    msg_data = await loop.run_in_executor(
                        None, self._fetch_message, mail, msg_id
                    )

                    if not msg_data:
                        continue

                    message_id = msg_data["message_id"]
                    attachments = msg_data["attachments"]

                    # Process attachments
                    for attachment in attachments:
                        filename = attachment["filename"]
                        content = attachment["content"]

                        # Calculate hash
                        file_hash = hashlib.sha256(content).hexdigest()

                        # Check if already processed
                        if await self.db.is_processed(message_id, filename, file_hash):
                            logger.info(f"Skipping already processed: {filename}")
                            continue

                        # Parse CSV
                        result = await self._process_csv(content, filename)

                        if result:
                            # Mark as processed
                            await self.db.mark_processed(message_id, filename, file_hash)
                            results.append(result)

                except Exception as e:
                    logger.error(f"Error processing message {msg_id}: {e}")
                    continue

            # Close connection
            await loop.run_in_executor(None, mail.logout)

            # Update stats
            await self.db.update_stats(len(messages), len(results))

            logger.info(f"Email check complete. Processed {len(results)} new files.")
            return results

        except Exception as e:
            logger.error(f"Error in email check: {e}", exc_info=True)
            return []

    def _connect(self) -> imaplib.IMAP4_SSL:
        """Connect to IMAP server (sync)."""
        mail = imaplib.IMAP4_SSL(self.host, self.port, timeout=30)
        mail.login(self.user, self.password)
        mail.select("INBOX")
        return mail

    def _search_messages(self, mail: imaplib.IMAP4_SSL) -> List[str]:
        """Search for messages matching filters (sync)."""
        # Build search criteria
        since_date = (datetime.now() - timedelta(days=self.days_to_check)).strftime("%d-%b-%Y")
        
        criteria = f'SINCE {since_date}'
        
        if self.from_filter:
            criteria += f' FROM "{self.from_filter}"'
        
        if self.subject_filter:
            criteria += f' SUBJECT "{self.subject_filter}"'

        logger.info(f"IMAP search criteria: {criteria}")

        status, messages = mail.search(None, criteria)
        
        if status != "OK":
            logger.warning("No messages found")
            return []

        message_ids = messages[0].split()
        return [msg_id.decode() for msg_id in message_ids]

    def _fetch_message(self, mail: imaplib.IMAP4_SSL, msg_id: str) -> Optional[Dict]:
        """Fetch and parse message (sync)."""
        status, msg_data = mail.fetch(msg_id, "(RFC822)")
        
        if status != "OK":
            return None

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)

        # Get message ID
        message_id = msg.get("Message-ID", f"unknown-{msg_id}")

        # Extract attachments
        attachments = []

        for part in msg.walk():
            if part.get_content_maintype() == "multipart":
                continue

            if part.get("Content-Disposition") is None:
                continue

            filename = part.get_filename()
            
            if not filename:
                continue

            # Decode filename
            decoded = decode_header(filename)
            if decoded[0][1]:
                filename = decoded[0][0].decode(decoded[0][1])
            else:
                filename = decoded[0][0]

            # Check extension
            if not any(filename.lower().endswith(ext) for ext in self.allowed_ext):
                continue

            # Get content
            content = part.get_payload(decode=True)

            attachments.append({
                "filename": filename,
                "content": content
            })

        return {
            "message_id": message_id,
            "attachments": attachments
        }

    async def _process_csv(self, content: bytes, filename: str) -> Optional[Dict]:
        """Process CSV file."""
        try:
            # Decode content
            text = content.decode("utf-8-sig")  # YooKassa uses UTF-8 with BOM

            # Parse CSV
            result = parse_yookassa_csv(text)

            if not result:
                logger.warning(f"No data parsed from {filename}")
                return None

            # Generate output files
            date = result["date"]
            
            # Tax-ready CSV
            tax_file = await self._create_tax_csv(result)
            
            # Payments details CSV
            payments_file = await self._create_payments_csv(result)

            return {
                **result,
                "tax_file": tax_file,
                "payments_file": payments_file
            }

        except Exception as e:
            logger.error(f"Error processing CSV {filename}: {e}", exc_info=True)
            return None

    async def _create_tax_csv(self, data: Dict) -> str:
        """Create tax-ready CSV file."""
        date = data["date"]
        total = data["total_amount"]
        count = data["payments_count"]
        description = os.getenv("TAX_DESCRIPTION", "Доступ к IT-сервису")

        filename = self.temp_dir / f"tax_ready_{date}.csv"

        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write("date,total_rub,payments_count,description\n")
            await f.write(f"{date},{total:.2f},{count},{description}\n")

        return str(filename)

    async def _create_payments_csv(self, data: Dict) -> str:
        """Create payments details CSV file."""
        date = data["date"]
        payments = data["payments"]

        filename = self.temp_dir / f"payments_{date}.csv"

        async with aiofiles.open(filename, "w", encoding="utf-8") as f:
            await f.write("payment_id,time,amount,description,type\n")
            
            for p in payments:
                await f.write(
                    f"{p['payment_id']},{p['payment_time']},"
                    f"{p['amount']},{p['description']},{p['payment_type']}\n"
                )

        return str(filename)