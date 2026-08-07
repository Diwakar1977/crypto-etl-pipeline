"""
Unit tests for Airflow DAG configuration.
"""
from datetime import datetime, timedelta

from dags.dag_config import (
    DAG_ID,
    DAG_DESCRIPTION,
    OWNER,
    TAGS,
    SCHEDULE,
    START_DATE,
    CATCHUP,
    RETRIES,
    RETRY_DELAY,
    DAG_RUN_TIMEOUT,
    MAX_ACTIVE_RUNS,
    MAX_ACTIVE_TASKS,
    DEFAULT_ARGS,
)

class TestDAGConfig:
    """Test Airflow DAG configuration values."""

    def test_dag_id(self):
        assert DAG_ID == "crypto_etl_pipeline"
        assert isinstance(DAG_ID, str)

    def test_dag_description(self):
        assert isinstance(DAG_DESCRIPTION, str)
        assert DAG_DESCRIPTION
        assert "cryptocurrency" in DAG_DESCRIPTION.lower()
        assert "spark" in DAG_DESCRIPTION.lower()
        assert "s3" in DAG_DESCRIPTION.lower()
        assert "redshift" in DAG_DESCRIPTION.lower()

    def test_owner(self):
        assert OWNER == "Diwakar K"
        assert isinstance(OWNER, str)

    def test_tags(self):
        assert isinstance(TAGS, list)

        expected_tags = {
            "crypto",
            "etl",
            "spark",
            "aws",
            "redshift",
            "production",
        }

        assert expected_tags.issubset(set(TAGS))

    def test_schedule(self):
        assert SCHEDULE == "0 9 * * *"
        assert isinstance(SCHEDULE, str)

    def test_start_date(self):
        assert isinstance(START_DATE, datetime)
        assert START_DATE == datetime(2026, 8, 7)

    def test_catchup(self):
        assert CATCHUP is False
        assert isinstance(CATCHUP, bool)

    def test_retry_configuration(self):
        assert RETRIES == 3
        assert isinstance(RETRIES, int)
        assert RETRIES >= 0

        assert isinstance(RETRY_DELAY, timedelta)
        assert RETRY_DELAY == timedelta(minutes=5)

    def test_execution_configuration(self):
        assert isinstance(DAG_RUN_TIMEOUT, timedelta)
        assert DAG_RUN_TIMEOUT == timedelta(hours=1)

        assert MAX_ACTIVE_RUNS == 1
        assert isinstance(MAX_ACTIVE_RUNS, int)
        assert MAX_ACTIVE_RUNS > 0

        assert MAX_ACTIVE_TASKS == 4
        assert isinstance(MAX_ACTIVE_TASKS, int)
        assert MAX_ACTIVE_TASKS > 0

    def test_default_args(self):
        assert isinstance(DEFAULT_ARGS, dict)

        required_keys = {
            "owner",
            "depends_on_past",
            "retries",
            "retry_delay",
        }

        assert required_keys.issubset(DEFAULT_ARGS.keys())

    def test_default_args_values(self):
        assert DEFAULT_ARGS["owner"] == OWNER
        assert DEFAULT_ARGS["depends_on_past"] is False
        assert DEFAULT_ARGS["retries"] == RETRIES
        assert DEFAULT_ARGS["retry_delay"] == RETRY_DELAY

    def test_retry_values_match_default_args(self):
        assert DEFAULT_ARGS["retries"] == RETRIES
        assert DEFAULT_ARGS["retry_delay"] == RETRY_DELAY

