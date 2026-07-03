import os
import requests
from dotenv import load_dotenv

load_dotenv()


def build_slack_message(activity: dict) -> str:
    """Construit un message Slack à partir d'une activité sportive."""
    distance_km = activity.get("distance_m")

    if distance_km is not None:
        distance_km = round(distance_km / 1000, 1)
        distance_text = f"{distance_km} km"
    else:
        distance_text = "une belle séance"

    duration_min = round(activity["duration_s"] / 60)

    return (
        f"Bravo salarié {activity['employee_id']} ! "
        f"Tu viens de terminer une activité : {activity['sport_type']} "
        f"sur {distance_text} en {duration_min} min 🔥🏅"
    )


def send_slack_message(message: str) -> None:
    """Envoie un message vers Slack via webhook."""
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url or webhook_url == "colle_ici_ton_webhook_slack":
        print("Webhook Slack non configuré. Message simulé :")
        print(message)
        return

    response = requests.post(webhook_url, json={"text": message}, timeout=10)
    response.raise_for_status()