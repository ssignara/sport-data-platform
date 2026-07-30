from __future__ import annotations

from datetime import datetime, timedelta
import json
import random
from pathlib import Path
from typing import Any

import pandas as pd
from faker import Faker
from kafka import KafkaProducer

from config.settings import KAFKA_BOOTSTRAP_SERVER


fake = Faker("fr_FR")

# Graine fixe pour obtenir un jeu de démonstration reproductible.
# Les mêmes fichiers sources produiront les mêmes profils sportifs.
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
Faker.seed(RANDOM_SEED)


# ============================================================
# NORMALISATION DES SPORTS
# ============================================================

SPORT_MAPPING = {
    "Runing": "Course à pied",
    "Running": "Course à pied",
    "Course à pied": "Course à pied",
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


# ============================================================
# PROFILS DE PRATIQUE
# ============================================================
# Le seuil métier des jours bien-être est fixé à 15 activités.
#
# Occasionnel :
#   reste sous le seuil.
#
# Régulier :
#   peut se situer juste autour du seuil.
#
# Très actif :
#   dépasse clairement le seuil.
#
# Les probabilités permettent d'obtenir une population variée
# et des KPI plus intéressants pour les équipes RH.

ACTIVITY_PROFILES = {
    "occasionnel": {
        "minimum": 4,
        "maximum": 12,
        "weight": 0.40,
    },
    "regulier": {
        "minimum": 13,
        "maximum": 24,
        "weight": 0.40,
    },
    "tres_actif": {
        "minimum": 25,
        "maximum": 45,
        "weight": 0.20,
    },
}


# ============================================================
# CARACTÉRISTIQUES PAR SPORT
# ============================================================
# distance_min_m / distance_max_m :
#   distance réaliste en mètres.
#
# Pour les sports dont la distance n'est pas pertinente,
# les valeurs restent à None et seule une durée est générée.

SPORT_RULES = {
    "Course à pied": {
        "distance_min_m": 3000,
        "distance_max_m": 15000,
        "speed_min_kmh": 7,
        "speed_max_kmh": 13,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Vélo": {
        "distance_min_m": 10000,
        "distance_max_m": 60000,
        "speed_min_kmh": 15,
        "speed_max_kmh": 30,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Randonnée": {
        "distance_min_m": 5000,
        "distance_max_m": 22000,
        "speed_min_kmh": 3,
        "speed_max_kmh": 6,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Natation": {
        "distance_min_m": 500,
        "distance_max_m": 3000,
        "speed_min_kmh": 1.5,
        "speed_max_kmh": 3.5,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Triathlon": {
        "distance_min_m": 15000,
        "distance_max_m": 60000,
        "speed_min_kmh": 10,
        "speed_max_kmh": 22,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Voile": {
        "distance_min_m": 5000,
        "distance_max_m": 30000,
        "speed_min_kmh": 5,
        "speed_max_kmh": 15,
        "duration_min_s": None,
        "duration_max_s": None,
    },
    "Tennis": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 3600,
        "duration_max_s": 7200,
    },
    "Escalade": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 3600,
        "duration_max_s": 10800,
    },
    "Football": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 3600,
        "duration_max_s": 7200,
    },
    "Basketball": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 3600,
        "duration_max_s": 7200,
    },
    "Rugby": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 4800,
        "duration_max_s": 7200,
    },
    "Judo": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 2700,
        "duration_max_s": 5400,
    },
    "Boxe": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 2700,
        "duration_max_s": 5400,
    },
    "Badminton": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 2700,
        "duration_max_s": 7200,
    },
    "Tennis de table": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 1800,
        "duration_max_s": 5400,
    },
    "Équitation": {
        "distance_min_m": None,
        "distance_max_m": None,
        "speed_min_kmh": None,
        "speed_max_kmh": None,
        "duration_min_s": 3600,
        "duration_max_s": 10800,
    },
}


def load_source_files() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Charge les fichiers RH et sportifs utilisés pour la simulation.
    """

    project_root = Path(__file__).resolve().parents[2]

    rh_path = project_root / "data" / "raw" / "Données RH.xlsx"
    sports_path = project_root / "data" / "raw" / "Données Sportives.xlsx"

    if not rh_path.exists():
        raise FileNotFoundError(
            f"Le fichier RH est introuvable : {rh_path}"
        )

    if not sports_path.exists():
        raise FileNotFoundError(
            f"Le fichier sportif est introuvable : {sports_path}"
        )

    employees = pd.read_excel(rh_path)
    sports = pd.read_excel(sports_path)

    required_employee_columns = {"ID salarié"}
    required_sport_columns = {
        "ID salarié",
        "Pratique d'un sport",
    }

    missing_employee_columns = (
        required_employee_columns - set(employees.columns)
    )

    missing_sport_columns = (
        required_sport_columns - set(sports.columns)
    )

    if missing_employee_columns:
        raise ValueError(
            "Colonnes manquantes dans le fichier RH : "
            f"{sorted(missing_employee_columns)}"
        )

    if missing_sport_columns:
        raise ValueError(
            "Colonnes manquantes dans le fichier sportif : "
            f"{sorted(missing_sport_columns)}"
        )

    return employees, sports


def build_employee_sport_mapping(
    sports: pd.DataFrame,
) -> dict[int, str]:
    """
    Associe chaque salarié à son sport déclaré.

    Les salariés sans pratique sportive déclarée ne sont pas ajoutés
    au dictionnaire et ne recevront donc aucune activité simulée.
    """

    renamed_sports = sports.rename(
        columns={
            "ID salarié": "employee_id",
            "Pratique d'un sport": "declared_sport",
        }
    ).copy()

    renamed_sports = renamed_sports.dropna(
        subset=["employee_id"]
    )

    mapping: dict[int, str] = {}

    for _, row in renamed_sports.iterrows():
        employee_id = int(row["employee_id"])
        declared_sport = row.get("declared_sport")

        if pd.isna(declared_sport):
            continue

        raw_sport = str(declared_sport).strip()

        if not raw_sport:
            continue

        normalized_sport = SPORT_MAPPING.get(raw_sport)

        if normalized_sport is None:
            raise ValueError(
                "Sport non reconnu dans le fichier source : "
                f"{raw_sport}"
            )

        mapping[employee_id] = normalized_sport

    return mapping


def choose_activity_profile() -> str:
    """
    Attribue un profil de pratique à un salarié sportif.
    """

    profile_names = list(ACTIVITY_PROFILES.keys())

    profile_weights = [
        ACTIVITY_PROFILES[name]["weight"]
        for name in profile_names
    ]

    return random.choices(
        population=profile_names,
        weights=profile_weights,
        k=1,
    )[0]


def choose_number_of_activities(profile_name: str) -> int:
    """
    Détermine le nombre annuel d'activités du salarié.
    """

    profile = ACTIVITY_PROFILES[profile_name]

    return random.randint(
        int(profile["minimum"]),
        int(profile["maximum"]),
    )


def generate_distance_and_duration(
    sport: str,
) -> tuple[int | None, int]:
    """
    Génère une distance et une durée cohérentes avec le sport.

    Pour les sports de distance, la durée est calculée à partir
    d'une vitesse réaliste.

    Pour les autres sports, seule une durée réaliste est générée.
    """

    rules = SPORT_RULES.get(sport)

    if rules is None:
        raise ValueError(
            f"Aucune règle de génération définie pour le sport : {sport}"
        )

    distance_min_m = rules["distance_min_m"]
    distance_max_m = rules["distance_max_m"]

    if (
        distance_min_m is not None
        and distance_max_m is not None
    ):
        distance_m = random.randint(
            int(distance_min_m),
            int(distance_max_m),
        )

        speed_kmh = random.uniform(
            float(rules["speed_min_kmh"]),
            float(rules["speed_max_kmh"]),
        )

        distance_km = distance_m / 1000
        duration_hours = distance_km / speed_kmh
        duration_s = max(
            int(duration_hours * 3600),
            600,
        )

        return distance_m, duration_s

    duration_s = random.randint(
        int(rules["duration_min_s"]),
        int(rules["duration_max_s"]),
    )

    return None, duration_s


def generate_activity_dates(
    number_of_activities: int,
) -> list[datetime]:
    """
    Répartit les activités sur les douze derniers mois.

    Les dates sont générées sans créer plusieurs activités exactement
    au même moment pour un même salarié.
    """

    end_period = datetime.now().replace(
        microsecond=0
    )

    start_period = end_period - timedelta(days=365)

    total_period_seconds = int(
        (end_period - start_period).total_seconds()
    )

    selected_offsets = random.sample(
        range(total_period_seconds),
        k=number_of_activities,
    )

    activity_dates = [
        start_period + timedelta(seconds=offset)
        for offset in selected_offsets
    ]

    return sorted(activity_dates)


def generate_activity(
    activity_id: int,
    employee_id: int,
    sport: str,
    start_date: datetime,
    profile_name: str,
) -> dict[str, Any]:
    """
    Génère une activité sportive cohérente.
    """

    distance_m, duration_s = generate_distance_and_duration(
        sport
    )

    end_date = start_date + timedelta(
        seconds=duration_s
    )

    return {
        "activity_id": activity_id,
        "employee_id": employee_id,
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "sport_type": sport,
        "distance_m": distance_m,
        "duration_s": duration_s,
        "comment": (
            f"Activité {sport.lower()} - profil {profile_name}."
        ),
    }


def main() -> None:
    """
    Génère un historique sportif annuel cohérent puis publie
    les événements dans Redpanda.

    Règles principales :
    - seuls les salariés ayant un sport déclaré reçoivent des activités ;
    - chaque salarié conserve le sport présent dans le fichier source ;
    - le nombre d'activités dépend d'un profil de pratique ;
    - les distances et durées dépendent du sport.
    """

    employees, sports = load_source_files()

    employee_sports = build_employee_sport_mapping(
        sports
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        value_serializer=lambda value: json.dumps(
            value,
            ensure_ascii=False,
        ).encode("utf-8"),
        acks="all",
        retries=5,
    )

    # L'identifiant contient le timestamp du lancement.
    # Les activités d'une même génération restent ensuite séquentielles.
    activity_id = int(
        datetime.now().timestamp() * 1000
    )

    total_generated = 0
    generated_employees = 0
    profile_counts = {
        "occasionnel": 0,
        "regulier": 0,
        "tres_actif": 0,
    }

    for _, employee in employees.iterrows():
        employee_id = int(employee["ID salarié"])

        declared_sport = employee_sports.get(
            employee_id
        )

        # Aucun sport déclaré :
        # aucune activité n'est artificiellement générée.
        if declared_sport is None:
            continue

        profile_name = choose_activity_profile()

        number_of_activities = choose_number_of_activities(
            profile_name
        )

        activity_dates = generate_activity_dates(
            number_of_activities
        )

        profile_counts[profile_name] += 1
        generated_employees += 1

        for start_date in activity_dates:
            activity = generate_activity(
                activity_id=activity_id,
                employee_id=employee_id,
                sport=declared_sport,
                start_date=start_date,
                profile_name=profile_name,
            )

            producer.send(
                "sport-activities",
                value=activity,
            )

            activity_id += 1
            total_generated += 1

    producer.flush()
    producer.close()

    print(
        f"{total_generated} activités publiées dans Redpanda."
    )

    print(
        f"{generated_employees} salariés sportifs sur "
        f"{len(employees)} salariés."
    )

    print(
        "Répartition des profils : "
        f"{profile_counts}"
    )


if __name__ == "__main__":
    main()