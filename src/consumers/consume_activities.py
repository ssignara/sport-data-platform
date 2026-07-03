import json

import pandas as pd
from kafka import KafkaConsumer

from src.loaders.database import engine
from src.notifications.slack import (
    build_slack_message,
    send_slack_message,
)

from config.settings import KAFKA_BOOTSTRAP_SERVER


def main() -> None:
    """
    Consomme les activités publiées dans Redpanda,
    les enregistre dans PostgreSQL puis envoie
    une notification Slack.
    """

    consumer = KafkaConsumer(
        "sport-activities",
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="sport-activities-consumer",
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=10000,
    )

    activities = []

    for message in consumer:
        activities.append(message.value)

    if not activities:
        print("Aucune activité à consommer.")
        return

    df = pd.DataFrame(activities)

    # Chargement dans PostgreSQL
    df.to_sql(
        name="activities",
        schema="bronze",
        con=engine,
        if_exists="append",
        index=False,
    )

    print(f"{len(df)} activités insérées dans PostgreSQL.")

    # Envoi des notifications Slack (limité à 10)
    print("\nSimulation des notifications Slack :\n")

    for activity in activities[:10]:
        message = build_slack_message(activity)
        send_slack_message(message)


if __name__ == "__main__":
    main()