import json
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

from config.settings import KAFKA_BOOTSTRAP_SERVER


TOPIC_NAME = "sport-activities"


def publish_live_activity(
    employee_id: int,
    sport_type: str,
    distance_m: int | None,
    duration_s: int,
    comment: str | None,
) -> dict:
    """
    Publie une activité sportive unique dans Redpanda.

    Cette fonction est utilisée pour la démonstration en temps réel :
    une seule activité est créée, publiée dans Redpanda, puis consommée
    et enregistrée dans PostgreSQL.
    """
    start_date = datetime.now(timezone.utc)
    end_date = start_date + timedelta(seconds=duration_s)

    activity = {
        "activity_id": uuid.uuid4().int & ((1 << 63) - 1),
        "employee_id": employee_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "sport_type": sport_type.strip(),
        "distance_m": distance_m,
        "duration_s": duration_s,
        "comment": comment.strip() if comment else None,
    }

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        key_serializer=lambda value: str(value).encode("utf-8"),
        acks="all",
        retries=3,
    )

    try:
        future = producer.send(
            TOPIC_NAME,
            key=activity["activity_id"],
            value=activity,
        )

        metadata = future.get(timeout=10)

        print(
            "Activité publiée : "
            f"topic={metadata.topic}, "
            f"partition={metadata.partition}, "
            f"offset={metadata.offset}, "
            f"activity_id={activity['activity_id']}"
        )

        return activity

    except KafkaError as error:
        raise RuntimeError(
            f"Échec de la publication dans Redpanda : {error}"
        ) from error

    finally:
        producer.flush()
        producer.close()