import subprocess

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from src.consumers.consume_activities import main as consume_activities
from src.generators.generate_activities import main as generate_activities
from src.producers.live_activity import publish_live_activity


app = FastAPI(
    title="Sport Data Platform API",
    description="API utilisée par Kestra pour orchestrer le pipeline sportif.",
    version="1.1.0",
)


class LiveActivityRequest(BaseModel):
    employee_id: int = Field(..., gt=0)
    sport_type: str = Field(..., min_length=2)
    distance_m: int | None = Field(default=None, ge=0)
    duration_s: int = Field(..., gt=0)
    comment: str | None = None


@app.get("/health")
def health_check() -> dict[str, str]:
    """Vérifie que l'API est disponible."""
    return {"status": "ok"}


@app.post("/generate")
def generate() -> dict[str, str]:
    """Génère l'historique des activités et le publie dans Redpanda."""
    try:
        generate_activities()
        return {"status": "generated"}
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la génération : {error}",
        ) from error


@app.post("/live-activity")
def create_live_activity(payload: LiveActivityRequest) -> dict:
    """Publie une seule activité dans Redpanda pour la démonstration."""
    try:
        activity = publish_live_activity(
            employee_id=payload.employee_id,
            sport_type=payload.sport_type,
            distance_m=payload.distance_m,
            duration_s=payload.duration_s,
            comment=payload.comment,
        )

        return {
            "status": "published",
            "activity": activity,
        }

    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la publication : {error}",
        ) from error


@app.post("/consume")
def consume() -> dict[str, str]:
    """Consomme les activités et les charge dans PostgreSQL."""
    try:
        consume_activities()
        return {"status": "consumed"}
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

    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
    )

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
        "output": output,
    }