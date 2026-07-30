from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
import random
from zoneinfo import ZoneInfo

from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVER
from src.generators.generate_activities import (
    build_employee_sport_mapping,
    generate_distance_and_duration,
    load_source_files,
)


TOPIC_NAME = "sport-activities"
PARIS_TIMEZONE = ZoneInfo("Europe/Paris")

# Environ 12 % des salariés sportifs réalisent une activité
# lors d'une journée donnée.
DAILY_ACTIVITY_PROBABILITY = 0.12


def build_deterministic_activity_id(
    employee_id: int,
    activity_date: str,
) -> int:
    """
    Construit un identifiant stable à partir du salarié et de la date.

    Un même salarié ne peut ainsi avoir qu'une activité quotidienne
    générée automatiquement pour une même journée.
    """

    raw_value = f"daily-{employee_id}-{activity_date}"

    digest = hashlib.blake2b(
        raw_value.encode("utf-8"),
        digest_size=8,
    ).digest()

    # PostgreSQL BIGINT est signé : on conserve 63 bits.
    return int.from_bytes(
        digest,
        byteorder="big",
        signed=False,
    ) & ((1 << 63) - 1)


def generate_daily_start_date(
    current_datetime: datetime,
) -> datetime:
    """
    Génère une heure d'activité comprise entre 6 h et 21 h
    pour la journée courante.
    """

    hour = random.randint(6, 20)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    return current_datetime.replace(
        hour=hour,
        minute=minute,
        second=second,
        microsecond=0,
    )


def generate_daily_activity(
    employee_id: int,
    sport: str,
    current_datetime: datetime,
) -> dict:
    """
    Génère une activité du jour cohérente avec le sport déclaré.
    """

    activity_date = current_datetime.date().isoformat()

    activity_id = build_deterministic_activity_id(
        employee_id=employee_id,
        activity_date=activity_date,
    )

    start_date = generate_daily_start_date(
        current_datetime=current_datetime,
    )

    distance_m, duration_s = generate_distance_and_duration(
        sport=sport,
    )

    end_date = start_date + timedelta(
        seconds=duration_s,
    )

    return {
        "activity_id": activity_id,
        "employee_id": employee_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "sport_type": sport,
        "distance_m": distance_m,
        "duration_s": duration_s,
        "comment": "Activité quotidienne générée automatiquement.",
    }


def main() -> dict[str, int | str]:
    """
    Génère uniquement les nouvelles activités de la journée.

    Contrairement au générateur historique, cette fonction :
    - ne recrée pas les douze derniers mois ;
    - ne concerne que les salariés sportifs ;
    - génère au maximum une activité par salarié et par jour ;
    - utilise un identifiant déterministe pour faciliter l'idempotence.
    """

    employees, sports = load_source_files()

    employee_sports = build_employee_sport_mapping(
        sports=sports,
    )

    current_datetime = datetime.now(
        PARIS_TIMEZONE,
    )

    activity_date = current_datetime.date().isoformat()

    # La graine dépend de la date.
    # Une relance le même jour sélectionne les mêmes salariés.
    random.seed(activity_date)

    selected_employees: list[tuple[int, str]] = []

    for _, employee in employees.iterrows():
        employee_id = int(employee["ID salarié"])

        declared_sport = employee_sports.get(
            employee_id,
        )

        if declared_sport is None:
            continue

        if random.random() <= DAILY_ACTIVITY_PROBABILITY:
            selected_employees.append(
                (
                    employee_id,
                    declared_sport,
                )
            )

    # On garantit au moins une activité quotidienne
    # pour rendre le workflow démontrable.
    if not selected_employees and employee_sports:
        employee_id = random.choice(
            list(employee_sports.keys())
        )

        selected_employees.append(
            (
                employee_id,
                employee_sports[employee_id],
            )
        )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        key_serializer=lambda value: str(
            value
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    generated_count = 0

    try:
        for employee_id, sport in selected_employees:
            activity = generate_daily_activity(
                employee_id=employee_id,
                sport=sport,
                current_datetime=current_datetime,
            )

            producer.send(
                TOPIC_NAME,
                key=activity["activity_id"],
                value=activity,
            )

            generated_count += 1

        producer.flush()

    finally:
        producer.close()

    result = {
        "status": "generated",
        "generation_mode": "daily",
        "activity_date": activity_date,
        "generated_activities": generated_count,
        "eligible_sport_employees": len(employee_sports),
    }

    print(
        f"{generated_count} activités quotidiennes publiées "
        f"pour le {activity_date}."
    )

    return result


if __name__ == "__main__":
    main()