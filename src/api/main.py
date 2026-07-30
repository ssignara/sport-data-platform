import os
import subprocess
from pathlib import Path

import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.consumers.consume_activities import main as consume_activities
from src.generators.generate_activities import main as generate_activities
from src.producers.live_activity import publish_live_activity

from src.generators.generate_daily_activities import (
    main as generate_daily_activities,
)

app = FastAPI(
    title="Sport Data Platform API",
    description="API utilisée par Kestra pour orchestrer le pipeline sportif.",
    version="1.3.0",
)


class LiveActivityRequest(BaseModel):
    employee_id: int = Field(..., gt=0)
    sport_type: str = Field(..., min_length=2)
    distance_m: int | None = Field(default=None, ge=0)
    duration_s: int = Field(..., gt=0)
    comment: str | None = None


def get_database_connection():
    """
    Ouvre une connexion vers PostgreSQL à partir des variables
    d'environnement définies dans docker-compose.yml.
    """
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "sport_data"),
        user=os.getenv("DB_USER", "sport_user"),
        password=os.getenv("DB_PASSWORD", "sport_password"),
    )


def employee_exists(employee_id: int) -> bool:
    """
    Vérifie que le salarié existe dans la table bronze.employees.
    """
    connection = get_database_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT 1
                FROM bronze.employees
                WHERE employee_id = %s
                LIMIT 1
                """,
                (employee_id,),
            )

            return cursor.fetchone() is not None

    finally:
        connection.close()


def execute_sql_file(sql_file_path: Path) -> None:
    """
    Exécute l'intégralité d'un fichier SQL dans PostgreSQL.

    Toutes les instructions sont exécutées dans une transaction :
    - si tout fonctionne, la transaction est validée ;
    - si une instruction échoue, toutes les modifications sont annulées.
    """
    if not sql_file_path.exists():
        raise FileNotFoundError(
            f"Le fichier SQL est introuvable : {sql_file_path}"
        )

    sql_content = sql_file_path.read_text(encoding="utf-8")

    if not sql_content.strip():
        raise ValueError(
            f"Le fichier SQL est vide : {sql_file_path}"
        )

    connection = get_database_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_content)

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


@app.get("/health")
def health_check() -> dict[str, str]:
    """Vérifie que l'API est disponible."""
    return {"status": "ok"}


@app.post("/generate")
def generate() -> dict[str, str]:
    """
    Génère l'historique des activités et le publie dans Redpanda.

    Cet endpoint est destiné au workflow batch.
    Il ne doit pas être utilisé après la création d'une activité
    unique via /live-activity.
    """
    try:
        generate_activities()

        return {
            "status": "generated",
            "message": "Les activités ont été générées et publiées.",
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la génération : {error}",
        ) from error


@app.post("/generate-daily")
def generate_daily() -> dict:
    """
    Génère uniquement les nouvelles activités de la journée.

    Cet endpoint est destiné au déclencheur quotidien Kestra.
    Il ne recrée pas l'historique annuel.
    """
    try:
        result = generate_daily_activities()

        return {
            "status": "generated",
            "message": (
                "Les activités quotidiennes ont été générées "
                "et publiées dans Redpanda."
            ),
            **result,
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=(
                "Échec de la génération quotidienne : "
                f"{error}"
            ),
        ) from error


@app.post("/live-activity")
def create_live_activity(payload: LiveActivityRequest) -> dict:
    """
    Publie une seule activité dans Redpanda.

    Le salarié est vérifié dans PostgreSQL avant publication afin
    d'éviter la création d'une activité orpheline.
    """
    try:
        if not employee_exists(payload.employee_id):
            raise HTTPException(
                status_code=422,
                detail=(
                    f"Le salarié {payload.employee_id} n'existe pas "
                    "dans la table bronze.employees."
                ),
            )

        activity = publish_live_activity(
            employee_id=payload.employee_id,
            sport_type=payload.sport_type,
            distance_m=payload.distance_m,
            duration_s=payload.duration_s,
            comment=payload.comment,
        )

        return {
            "status": "published",
            "message": "Une activité a été publiée dans Redpanda.",
            "activity": activity,
        }

    except HTTPException:
        raise

    except psycopg2.Error as error:
        raise HTTPException(
            status_code=500,
            detail=(
                "Impossible de vérifier le salarié dans PostgreSQL : "
                f"{error}"
            ),
        ) from error

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la publication : {error}",
        ) from error


@app.post("/consume")
def consume() -> dict[str, str]:
    """
    Consomme les activités disponibles dans Redpanda
    et les charge dans PostgreSQL.
    """
    try:
        consume_activities()

        return {
            "status": "consumed",
            "message": "Les activités disponibles ont été consommées.",
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la consommation : {error}",
        ) from error


@app.post("/transform")
def transform() -> dict[str, str]:
    """
    Exécute les transformations SQL après le chargement des données Bronze.

    Le script crée ou met à jour les vues du schéma analytics afin que
    Metabase dispose de données déjà nettoyées, converties, agrégées et
    enrichies par les règles métier.
    """
    sql_file_path = Path("/app/sql/03_create_reporting_views.sql")

    try:
        execute_sql_file(sql_file_path)

        return {
            "status": "transformed",
            "message": (
                "Les données ont été transformées et les vues analytics "
                "ont été créées ou mises à jour."
            ),
        }

    except FileNotFoundError as error:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "transform_file_not_found",
                "message": str(error),
            },
        ) from error

    except psycopg2.Error as error:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "transform_database_failed",
                "message": (
                    "La transformation SQL a échoué dans PostgreSQL : "
                    f"{error}"
                ),
            },
        ) from error

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "transform_failed",
                "message": f"Échec de la transformation : {error}",
            },
        ) from error


@app.post("/quality")
def quality_check() -> dict[str, str]:
    """Exécute les contrôles de qualité Soda."""
    command = [
        "soda",
        "scan",
        "-d",
        "sport_data",
        "-c",
        "/app/soda/configuration.yml",
        "/app/soda/checks.yml",
    ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

    except subprocess.TimeoutExpired as error:
        raise HTTPException(
            status_code=504,
            detail={
                "status": "quality_timeout",
                "message": "Le contrôle qualité a dépassé 120 secondes.",
            },
        ) from error

    except OSError as error:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "quality_execution_failed",
                "message": f"Impossible d'exécuter Soda : {error}",
            },
        ) from error

    output = "\n".join(
        part.strip()
        for part in (result.stdout, result.stderr)
        if part.strip()
    )

    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "status": "quality_failed",
                "return_code": result.returncode,
                "output": output,
            },
        )

    return {
        "status": "quality_passed",
        "message": "Tous les contrôles qualité sont passés.",
        "output": output,
    }