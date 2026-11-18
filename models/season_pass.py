from dataclasses import dataclass
from datetime import datetime, date
from decimal import Decimal
from typing import Any

@dataclass
class SeasonPass:
    txid: str
    rfid: str
    purchase_time: datetime
    price_usd: Decimal
    expiration_time: date
    days_used: int

    name: str
    address: dict[str, Any]
    phone: str
    email: str
    emergency_contact: dict[str, Any]