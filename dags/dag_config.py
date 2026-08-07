"""
Airflow DAG configuration.
"""
from datetime import datetime, timedelta

# DAG Metadata
DAG_ID = "crypto_etl_pipeline"

DAG_DESCRIPTION = (
    "ETL pipeline for extracting cryptocurrency market data, "
    "transforming it with Apache Spark, and loading it into S3 "
    "and Amazon Redshift."
)

OWNER = "Diwakar K"

TAGS = [
    "crypto",
    "etl",
    "spark",
    "aws",
    "redshift",
    "production",
]


# Scheduling
# Every day at 09:00 UTC
SCHEDULE = "0 9 * * *"

START_DATE = datetime(2026, 8, 7)

CATCHUP = False

# Retry Configuration

RETRIES = 3

RETRY_DELAY = timedelta(minutes=5)

# Execution Configuration
DAG_RUN_TIMEOUT = timedelta(hours=1)

MAX_ACTIVE_RUNS = 1

MAX_ACTIVE_TASKS = 4

# Airflow Default Arguments
DEFAULT_ARGS = {
    "owner": OWNER,
    "depends_on_past": False,
    "retries": RETRIES,
    "retry_delay": RETRY_DELAY,
}