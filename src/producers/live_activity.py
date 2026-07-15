import json
from datetime import datetime, timedelta

from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVER


def publish_live_activity(
    employee_id: int,
    sport_type: str,
    distance_m: int | None,
    duration_s: int,
    comment: str | None,
) -> dict:
    """
    Publie une activité unique dans Redpanda.

    Cette fonction est utilisée pendant la démonstration live :
    une activité est créée, publiée dans Kafka/Redpanda,
    puis consommée et chargée dans PostgreSQL.
    """
    start_date = datetime.now()
    end_date = start_date + timedelta(seconds=duration_s)

    activity = {
        "activity_id": int(datetime.now().timestamp() * 1000),
        "employee_id": employee_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "sport_type": sport_type,
        "distance_m": distance_m,
        "duration_s": duration_s,
        "comment": comment,
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
    )

    producer.send("sport-activities", value=activity)
    producer.flush()
    producer.close()

    return activity