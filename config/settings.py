from dotenv import load_dotenv
import os

load_dotenv()

# PostgreSQL
POSTGRES_HOST = os.getenv("DB_HOST")
POSTGRES_PORT = os.getenv("DB_PORT")
POSTGRES_DATABASE = os.getenv("DB_NAME")
POSTGRES_USER = os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD")

# Redpanda / Kafka
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

# Slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")