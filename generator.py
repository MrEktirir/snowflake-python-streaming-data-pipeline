import time

import uuid
import random

from datetime import datetime, date, timedelta, timezone
from decimal import Decimal
from dataclasses import asdict

from dotenv import load_dotenv
from faker import Faker

from models.resort_ticket import ResortTicket
from models.season_pass import SeasonPass
from models.lift_ride import LiftRide
from streamer import SnowflakeStreamingSink


load_dotenv()
fake = Faker()


def generate_resort_ticket() -> ResortTicket:
    now = datetime.now(timezone.utc)

    txid = str(uuid.uuid4())
    rfid = str(uuid.uuid4())

    resort = random.choice([
        "Aspen",
        "Vail",
        "Breckenridge"
    ])

    days = random.choice([1, 3, 5, 7])

    daily_price = Decimal("85.00")
    price_usd = daily_price * days

    expiration_time = (now + timedelta(days=days)).date()
    days_used = 0

    name = fake.name()
    phone = fake.phone_number()
    email = fake.email()

    address = {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state(),
        "zip_code": fake.zipcode()
    }

    emergency_contact = {
        "name": fake.name(),
        "phone": fake.phone_number()
    }

    return ResortTicket(
        txid=txid,
        rfid=rfid,
        resort=resort,
        purchase_time=now,
        price_usd=price_usd,
        expiration_time=expiration_time,
        days=days,
        days_used=days_used,
        name=name,
        address=address,
        phone=phone,
        email=email,
        emergency_contact=emergency_contact
    )


def generate_season_pass() -> SeasonPass:
    now = datetime.now(timezone.utc)

    txid = str(uuid.uuid4())
    rfid = str(uuid.uuid4())

    price_usd = Decimal("899.00")
    expiration_time = (now + timedelta(days=180)).date()
    days_used = 0

    name = fake.name()
    phone = fake.phone_number()
    email = fake.email()

    address = {
        "street": fake.street_address(),
        "city": fake.city(),
        "state": fake.state(),
        "zip_code": fake.zipcode()
    }

    emergency_contact = {
        "name": fake.name(),
        "phone": fake.phone_number()
    }

    return SeasonPass(
        txid=txid,
        rfid=rfid,
        purchase_time=now,
        price_usd=price_usd,
        expiration_time=expiration_time,
        days_used=days_used,
        name=name,
        address=address,
        phone=phone,
        email=email,
        emergency_contact=emergency_contact
    )


def generate_lift_ride(
    rfid: str,
    activation_day_count: int
) -> LiftRide:

    txid = str(uuid.uuid4())

    resort = random.choice([
        "Aspen",
        "Vail",
        "Breckenridge"
    ])

    lift = random.choice([
        "Lift A",
        "Lift B",
        "Lift C"
    ])

    ride_time = datetime.now(timezone.utc)

    return LiftRide(
        txid=txid,
        rfid=rfid,
        resort=resort,
        lift=lift,
        ride_time=ride_time,
        activation_day_count=activation_day_count
    )


def to_snowflake_row(record):
    row = {}

    for key, value in asdict(record).items():
        column_name = key.upper()

        if isinstance(value, datetime):
            value = value.isoformat()

        elif isinstance(value, date):
            value = value.isoformat()

        elif isinstance(value, Decimal):
            value = float(value)

        row[column_name] = value

    return row


def generate_batch():
    resort_ticket = generate_resort_ticket()
    season_pass = generate_season_pass()

    lift_ride = generate_lift_ride(
        rfid=resort_ticket.rfid,
        activation_day_count=1
    )

    return {
        "resort_tickets": [
            to_snowflake_row(resort_ticket)
        ],
        "season_passes": [
            to_snowflake_row(season_pass)
        ],
        "lift_rides": [
            to_snowflake_row(lift_ride)
        ]
    }


def main():
    sink = SnowflakeStreamingSink()

    try:
        while True:
            batches = generate_batch()
            sink.append_batches(batches)

            print("3 records sent to Snowflake")

            time.sleep(5)

    except KeyboardInterrupt:
        print("Streaming stopped by user.")

    finally:
        sink.close()


if __name__ == "__main__":
    main()
