from dotenv import load_dotenv
import os

load_dotenv()

# PostgreSQL
POSTGRES_HOST = os.getenv("DB_HOST", "localhost")
POSTGRES_PORT = os.getenv("DB_PORT", "5432")
POSTGRES_DATABASE = os.getenv("DB_NAME", "sport_data")
POSTGRES_USER = os.getenv("DB_USER", "sport_user")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD", "sport_password")

# Redpanda / Kafka
KAFKA_BOOTSTRAP_SERVER = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")