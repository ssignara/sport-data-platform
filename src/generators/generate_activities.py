from datetime import datetime, timedelta
import json
import random
from pathlib import Path

import pandas as pd
from faker import Faker
from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVER


fake = Faker("fr_FR")

DEFAULT_SPORTS = [
    "Course à pied",
    "Vélo",
    "Randonnée",
    "Natation",
    "Tennis",
    "Escalade",
]

SPORT_MAPPING = {
    "Runing": "Course à pied",
    "Running": "Course à pied",
    "Randonnée": "Randonnée",
    "Natation": "Natation",
    "Tennis": "Tennis",
    "Escalade": "Escalade",
    "Football": "Football",
    "Basketball": "Basketball",
    "Rugby": "Rugby",
    "Judo": "Judo",
    "Boxe": "Boxe",
    "Badminton": "Badminton",
    "Tennis de table": "Tennis de table",
    "Équitation": "Équitation",
    "Voile": "Voile",
    "Triathlon": "Triathlon",
}


SPORT_DISTANCE_RANGES = {
    "Course à pied": (3000, 25000),
    "Vélo": (5000, 80000),
    "Randonnée": (3000, 30000),
    "Natation": (500, 5000),
    "Tennis": (None, None),
    "Escalade": (None, None),
    "Football": (None, None),
    "Basketball": (None, None),
    "Rugby": (None, None),
    "Judo": (None, None),
    "Boxe": (None, None),
    "Badminton": (None, None),
    "Tennis de table": (None, None),
    "Équitation": (None, None),
    "Voile": (3000, 25000),
    "Triathlon": (10000, 60000),
}


def load_source_files() -> tuple[pd.DataFrame, pd.DataFrame]:
    project_root = Path(__file__).resolve().parents[2]

    rh_path = project_root / "data" / "raw" / "Données RH.xlsx"
    sports_path = project_root / "data" / "raw" / "Données Sportives.xlsx"

    employees = pd.read_excel(rh_path)
    sports = pd.read_excel(sports_path)

    return employees, sports


def build_employee_sport_mapping(sports: pd.DataFrame) -> dict[int, str]:
    sports = sports.rename(
        columns={
            "ID salarié": "employee_id",
            "Pratique d'un sport": "declared_sport",
        }
    )

    sports = sports.dropna(subset=["employee_id"])

    mapping = {}

    for _, row in sports.iterrows():
        employee_id = int(row["employee_id"])
        declared_sport = row.get("declared_sport")

        if pd.isna(declared_sport) or declared_sport == "":
            continue

        normalized_sport = SPORT_MAPPING.get(str(declared_sport).strip())

        if normalized_sport:
            mapping[employee_id] = normalized_sport

    return mapping


def choose_sport(employee_id: int, employee_sports: dict[int, str]) -> str:
    declared_sport = employee_sports.get(employee_id)

    if declared_sport:
        # 80 % des activités suivent le sport déclaré.
        # 20 % permettent de simuler d'autres pratiques occasionnelles.
        if random.random() < 0.8:
            return declared_sport

    return random.choice(DEFAULT_SPORTS)


def generate_duration_from_distance(sport: str, distance_m: int | None) -> int:
    if distance_m is None:
        return random.randint(1800, 10800)

    average_speed_kmh = {
        "Course à pied": random.uniform(7, 13),
        "Vélo": random.uniform(14, 30),
        "Randonnée": random.uniform(3, 6),
        "Natation": random.uniform(2, 4),
        "Voile": random.uniform(5, 15),
        "Triathlon": random.uniform(10, 25),
    }.get(sport, random.uniform(4, 12))

    distance_km = distance_m / 1000
    duration_hours = distance_km / average_speed_kmh
    duration_seconds = int(duration_hours * 3600)

    return max(duration_seconds, 600)


def generate_activity(
    activity_id: int,
    employee_id: int,
    employee_sports: dict[int, str],
) -> dict:
    sport = choose_sport(employee_id, employee_sports)
    start_date = datetime.now() - timedelta(days=random.randint(0, 365))

    min_distance, max_distance = SPORT_DISTANCE_RANGES.get(sport, (None, None))

    distance_m = None
    if min_distance is not None and max_distance is not None:
        distance_m = random.randint(min_distance, max_distance)

    duration_s = generate_duration_from_distance(sport, distance_m)

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
    employees, sports = load_source_files()
    employee_sports = build_employee_sport_mapping(sports)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
    )

    activity_id = int(datetime.now().timestamp() * 1000)
    total_generated = 0

    for _, employee in employees.iterrows():
        employee_id = int(employee["ID salarié"])

        has_declared_sport = employee_id in employee_sports

        if has_declared_sport:
            number_of_activities = random.randint(15, 55)
        else:
            number_of_activities = random.randint(2, 18)

        for _ in range(number_of_activities):
            activity = generate_activity(activity_id, employee_id, employee_sports)
            producer.send("sport-activities", value=activity)

            activity_id += 1
            total_generated += 1

    producer.flush()
    producer.close()

    print(f"{total_generated} activités publiées dans Redpanda.")


if __name__ == "__main__":
    main()