import subprocess

from fastapi import FastAPI, HTTPException

from src.consumers.consume_activities import main as consume_activities
from src.generators.generate_activities import main as generate_activities


app = FastAPI(
    title="Sport Data Platform API",
    description="API permettant à Kestra d'orchestrer le pipeline sportif.",
    version="1.0.0",
)


@app.get("/health")
def health_check() -> dict[str, str]:
    """Vérifie que l'API est disponible."""
    return {"status": "ok"}


@app.post("/generate")
def generate() -> dict[str, str]:
    """Génère et publie les activités dans Redpanda."""
    try:
        generate_activities()
        return {"status": "generated"}
    except Exception as error:
        raise HTTPException(
            status_code=500,
            detail=f"Échec de la génération : {error}",
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
    """Exécute les contrôles de qualité Soda sur PostgreSQL."""
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