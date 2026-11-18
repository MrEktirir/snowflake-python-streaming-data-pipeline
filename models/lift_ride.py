from dataclasses import dataclass
from datetime import datetime

@dataclass
class LiftRide:
    txid: str
    rfid: str
    resort: str
    lift: str
    ride_time: datetime
    activation_day_count: int
    