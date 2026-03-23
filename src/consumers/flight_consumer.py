import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from kafka import KafkaConsumer
from kafka.errors import KafkaError

from logger import get_logger
from models.flight import flight_deserializer

import os
from dotenv import load_dotenv

load_dotenv()

SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = os.getenv("TOPIC_FLIGHTS", "flight-feeds")
GROUP_ID = "flights"

log = get_logger(__name__)


def build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[SERVER],
        auto_offset_reset="earliest",
        group_id=GROUP_ID,
        value_deserializer=flight_deserializer,
    )


def run():
    log.info(f"connecting to topic: {TOPIC_NAME}")

    consumer = build_consumer()
    count = 0

    try:
        for message in consumer:
            flight = message.value

            if flight is None:
                log.warning(f"skipping malformed message at offset {message.offset}")
                continue

            count += 1
            log.info(
                f"[{count}] {flight.callsign or 'N/A'} | {flight.origin_country} | lat={flight.latitude} lon={flight.longitude} alt={flight.baro_altitude}m"
            )

    except KafkaError as e:
        log.error(f"kafka error: {e}")
    except KeyboardInterrupt:
        log.info("stopped by user")
    finally:
        consumer.close()
        log.info(f"consumer closed — total messages processed: {count}")


if __name__ == "__main__":
    run()
