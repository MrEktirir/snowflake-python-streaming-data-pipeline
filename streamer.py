
import os
import time

from dotenv import load_dotenv
from snowflake.ingest.streaming import (
    StreamingIngestClient,
    StreamingIngestError,
)

load_dotenv()

BACKPRESSURE_TIMEOUT_SECONDS = 30.0


def account_uri_parts():
    uri = os.environ["SNOWFLAKE_ACCOUNT_URI"].strip()

    if "://" not in uri:
        uri = "https://" + uri

    uri = uri.rstrip("/")
    host = uri.split("://", 1)[1].split("/", 1)[0]
    account = host.removesuffix(".snowflakecomputing.com")

    return uri, account


def create_connection_properties():
    account_url, account = account_uri_parts()

    properties = {
        "account": account,
        "user": os.environ["SNOWFLAKE_USER"],
        "private_key": os.environ["PRIVATE_KEY"],
        "url": account_url,
    }

    role = os.getenv("SNOWFLAKE_ROLE")

    if role:
        properties["role"] = role

    return properties


def get_streaming_targets():
    database = os.environ["DATABASE_NAME"]
    schema = os.environ["SCHEMA_NAME"]
    client_name = os.environ["CLIENT_NAME"]

    pipe_names = {
        "resort_tickets": os.environ["RESORT_TICKET_PIPE_NAME"],
        "season_passes": os.environ["SEASON_PASS_PIPE_NAME"],
        "lift_rides": os.environ["LIFT_RIDE_PIPE_NAME"],
    }

    return database, schema, client_name, pipe_names


class SnowflakeStreamingSink:

    def __init__(self):
        self.properties = create_connection_properties()

        (
            self.database,
            self.schema,
            self.client_name,
            self.pipe_names,
        ) = get_streaming_targets()

        self._clients = {}
        self._channels = {}
        self._append_seq = 0

        for stream_name, pipe_name in self.pipe_names.items():
            client = StreamingIngestClient(
                client_name=f"{self.client_name}-{stream_name}",
                db_name=self.database,
                schema_name=self.schema,
                pipe_name=pipe_name,
                properties=self.properties,
            )

            self._clients[stream_name] = client
            self._channels[stream_name] = client.get_elastic_channel()

    def append_batches(self, batches):
        pending = []

        for stream_name, rows in batches.items():
            if rows:
                self._append_seq += 1

                token = f"{stream_name}-{self._append_seq}"
                channel = self._channels[stream_name]

                deadline = (
                    time.monotonic()
                    + BACKPRESSURE_TIMEOUT_SECONDS
                )

                while True:
                    try:
                        future = channel.append_rows_with_wait(
                            rows,
                            token
                        )

                        pending.append(
                            (stream_name, len(rows), future)
                        )
                        break

                    except StreamingIngestError as exc:
                        if (
                            exc.http_status_code != 429
                            or time.monotonic() >= deadline
                        ):
                            pending.append(
                                (stream_name, len(rows), exc)
                            )
                            break

                        time.sleep(0.1)

                    except Exception as exc:
                        pending.append(
                            (stream_name, len(rows), exc)
                        )
                        break

        failures = []

        for stream_name, row_count, future in pending:
            try:
                if isinstance(future, BaseException):
                    raise future

                future.result()

            except Exception as exc:
                failures.append((stream_name, exc))

        if failures:
            names = ", ".join(
                stream_name for stream_name, _ in failures
            )

            raise RuntimeError(
                f"Snowflake failed to acknowledge: {names}"
            ) from failures[0][1]

    def close(self):
        for client in self._clients.values():
            client.close()
