import os
import subprocess

import psycopg2
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.consumers.consume_activities import main as consume_activities
from src.generators.generate_activities import main as generate_activities
from src.producers.live_activity import publish_live_activity


app = FastAPI(
    title="Sport Data Platform API",
    description="API utilisée par Kestra pour orchestrer le pipeline sportif.",
    version="1.2.0",
)


class LiveActivityRequest(BaseModel):
    employee_id: int = Field(..., gt=0)
    sport_type: str = Field(..., min_length=2)
    distance_m: int | None = Field(default=None, ge=0)
    duration_s: int = Field(..., gt=0)
    comment: str | None = None


def employee_exists(employee_id: int) -> bool:
    """
    Vérifie que le salarié existe dans la table bronze.employees.

    La connexion utilise les variables d'environnement définies
    dans docker-compose.yml.
    """
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "sport_data"),
        user=os.getenv("DB_USER", "sport_user"),
        password=os.getenv("DB_PASSWORD", "sport_password"),
    )

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