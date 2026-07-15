import os

import requests


def build_slack_message(activity: dict) -> str:
    """Construit le message Slack associé à une activité sportive."""

    employee_name = activity.get("employee_name")

    if employee_name:
        participant = employee_name
    else:
        participant = f"salarié {activity['employee_id']}"

    duration_minutes = round(activity["duration_s"] / 60)
    distance_m = activity.get("distance_m")

    if distance_m is not None:
        distance_text = f"{distance_m / 1000:.1f} km"

        return (
            f"Bravo {participant} ! "
            f"Tu viens de terminer une activité de "
            f"{activity['sport_type']} sur {distance_text} "
            f"en {duration_minutes} min ! 🔥🏅"
        )

    return (
        f"Bravo {participant} ! "
        f"Tu viens de terminer une activité de "
        f"{activity['sport_type']} en {duration_minutes} min ! 🔥🏅"
    )


def send_slack_message(message: str) -> None:
    """
    Envoie un message dans Slack.

    Si le webhook n'est pas configuré, le message est affiché
    dans les logs afin de conserver un mode de démonstration local.
    """

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")

    if not webhook_url:
        print("Webhook Slack non configuré. Message simulé :")
        print(message)
        return

    response = requests.post(
        webhook_url,
        json={"text": message},
        timeout=10,
    )

    response.raise_for_status()

    print("Message Slack envoyé avec succès.")