from datetime import datetime, timedelta
import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer


fake = Faker("fr_FR")

SPORTS = ["Course à pied", "Vélo", "Randonnée", "Natation", "Tennis", "Escalade"]


def load_employees() -> pd.DataFrame:
    project_root = Path(__file__).resolve().parents[2]
    file_path = project_root / "data" / "raw" / "Données RH.xlsx"
    return pd.read_excel(file_path)


def generate_activity(activity_id: int, employee_id: int) -> dict:
    sport = random.choice(SPORTS)
    start_date = datetime.now() - timedelta(days=random.randint(0, 365))

    duration_s = random.randint(1200, 14400)

    distance_m = None
    if sport not in ["Escalade", "Tennis"]:
        distance_m = random.randint(1000, 60000)

    return {
        "activity_id": activity_id,
        "employee_id": int(employee_id),
        "start_date": start_date.isoformat(),
        "end_date": (start_date + timedelta(seconds=duration_s)).isoformat(),
        "sport_type": sport,
        "distance_m": distance_m,
        "duration_s": duration_s,
        "comment": fake.sentence(nb_words=8),
    }


def main() -> None:
    employees = load_employees()

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )

    activity_id = 1

    for _, employee in employees.iterrows():
        employee_id = employee["ID salarié"]
        number_of_activities = random.randint(5, 40)

        for _ in range(number_of_activities):
            activity = generate_activity(activity_id, employee_id)
            producer.send("sport-activities", value=activity)
            activity_id += 1

    producer.flush()
    producer.close()

    print(f"{activity_id - 1} activités publiées dans Redpanda.")


if __name__ == "__main__":
    main()