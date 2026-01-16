import csv
import logging
from io import StringIO
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


def parse_yookassa_csv(content: str) -> Optional[Dict]:
    """
    Parse YooKassa CSV registry.
    
    Format:
    - First 2 lines: metadata (registry info, date)
    - Then: header row
    - Then: payment rows
    - Last lines: summary (starts with "Сумма принятых платежей")
    
    Returns dict with:
    - date: YYYY-MM-DD
    - total_amount: float (sum of "Сумма платежа")
    - commission: float (sum of commission)
    - payments_count: int
    - payments: list of dicts
    """
    try:
        lines = content.strip().split("\n")
        
        if len(lines) < 4:
            logger.warning("CSV too short")
            return None

        # Extract date from first 5 lines: "Дата платежей: 2026-01-15"
        date_str = "unknown"
        for i, line in enumerate(lines[:5]):
            if "Дата платежей:" in line or "Дата платежей" in line:
                # Split by : and take second part
                parts = line.split(":", 1)
                if len(parts) > 1:
                    date_str = parts[1].strip()
                    logger.info(f"Found date in line {i}: {date_str}")
                    break
        
        if date_str == "unknown":
            logger.warning(f"Date not found in CSV. Line 1 was: {lines[1][:100]}")

        # Find header row (contains "Идентификатор платежа")
        header_idx = None
        for i, line in enumerate(lines):
            if "Идентификатор платежа" in line:
                header_idx = i
                break

        if header_idx is None:
            logger.warning("Header not found in CSV")
            return None

        # Parse CSV starting from header
        csv_content = "\n".join(lines[header_idx:])
        reader = csv.DictReader(StringIO(csv_content), delimiter=";")

        payments = []
        total_amount = 0.0
        commission = 0.0

        for row in reader:
            # Stop at summary lines
            first_col = list(row.values())[0] if row else ""
            
            if "Сумма принятых платежей" in first_col or "Число платежей" in first_col:
                break

            # Skip empty rows
            if not row.get("Идентификатор платежа"):
                continue

            try:
                # Extract fields
                payment_id = row.get("Идентификатор платежа", "").strip()
                amount_str = row.get("Сумма платежа", "0").strip()
                commission_str = row.get("Сумма комиссии без НДС", "0").strip()
                payment_time = row.get("Время платежа", "").strip()
                description = row.get("Описание", "").strip()
                payment_type = row.get("Тип платежа", "").strip()

                # Parse amounts (replace comma with dot for Russian format)
                amount = float(amount_str.replace(",", ".").replace(" ", ""))
                comm = float(commission_str.replace(",", ".").replace(" ", ""))

                total_amount += amount
                commission += comm

                payments.append({
                    "payment_id": payment_id,
                    "amount": amount,
                    "currency": row.get("Валюта платежа", "RUB").strip(),
                    "payment_time": payment_time,
                    "description": description,
                    "payment_type": payment_type
                })

            except (ValueError, KeyError) as e:
                logger.warning(f"Error parsing row: {e}")
                continue

        # ВАЖНО: даже если платежей 0, возвращаем результат (чтобы админ знал о пустом реестре)
        logger.info(f"Parsed CSV: date={date_str}, payments={len(payments)}, total={total_amount:.2f} RUB")

        return {
            "date": date_str,
            "total_amount": total_amount,
            "commission": commission,
            "payments_count": len(payments),
            "payments": payments
        }

    except Exception as e:
        logger.error(f"Error parsing CSV: {e}", exc_info=True)
        return None