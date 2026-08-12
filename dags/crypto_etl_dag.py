"""
Airflow DAG for Crypto ETL Pipeline.

Pipeline:

CoinGecko API
    ↓
Extract
    ↓
S3 RAW
    ↓
Transform with Spark
    ↓
S3 PROCESSED
    ↓
Amazon Redshift
    ↓
Notification
"""

from datetime import timedelta
import logging

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.task.trigger_rule import TriggerRule
from config.config import Config

from dags.dag_config import (
    DAG_ID,
    DAG_DESCRIPTION,
    DEFAULT_ARGS,
    SCHEDULE,
    START_DATE,
    CATCHUP,
    DAG_RUN_TIMEOUT,
    MAX_ACTIVE_RUNS,
    MAX_ACTIVE_TASKS,
)

from dags.dag_callbacks import pipeline_notification

from src.extract.extract_job import ExtractJob
from src.transform.transform_job import TransformJob
from src.load.load_job import LoadJob

from src.utils.spark_session import SparkSessionManager

logger = logging.getLogger(__name__)


# EXTRACT
def extract_data(**context):
    """
    Extract cryptocurrency data from CoinGecko
    and upload raw data to S3.
    """

    logger.info("Starting Extract task.")

    ti = context["ti"]

    result = ExtractJob().run()

    raw_s3_path = (
        f"{Config.RAW_PATH}"
        f"{result['s3_key'].replace('raw_data/', '')}"
    )

    logger.info(
        "Raw S3 Path: %s",
        raw_s3_path,
    )

    # XCom: raw S3 path
    ti.xcom_push(
        key="raw_s3_path",
        value=raw_s3_path,
    )

    # XCom: extracted records
    ti.xcom_push(
        key="extracted",
        value=result["record_count"],
    )

    # XCom: execution duration
    ti.xcom_push(
        key="extract_duration",
        value=result["execution_time"],
    )

    logger.info(
        "Extract task completed successfully."
    )

    return raw_s3_path

# TRANSFORM
def transform_data(**context):
    """
    Read raw data from S3,
    transform using Spark,
    and write processed Parquet data to S3.
    """

    logger.info("Starting Transform task.")

    ti = context["ti"]

    # Get raw path from Extract
    raw_s3_path = ti.xcom_pull(
        task_ids="extract_task",
        key="raw_s3_path",
    )

    if not raw_s3_path:
        raise ValueError("Raw S3 path was not found in XCom.")

    logger.info("Raw S3 Path received: %s", raw_s3_path,)

    spark = None

    try:

        spark = SparkSessionManager.get_session()

        logger.info("Spark session created for Transform task.")

        result = TransformJob(
            spark=spark,
            schema_file=Config.SCHEMA_FILE,
            input_path=raw_s3_path,
            output_path=Config.PROCESSED_PATH,
        ).run()

        # XCom: processed S3 path
        ti.xcom_push(
            key="s3_processed_path",
            value=result["s3_processed_path"],
        )

        # XCom: transformed records
        ti.xcom_push(
            key="transformed",
            value=result["processed_rows"],
        )

        # XCom: rejected records
        ti.xcom_push(
            key="rejected",
            value=result["rejected_rows"],
        )

        # XCom: execution duration
        ti.xcom_push(
            key="transform_duration",
            value=result["execution_time"],
        )

        logger.info("Transform task completed successfully.")

        return result["s3_processed_path"]

    finally:

        if spark is not None:

            SparkSessionManager.stop_session()

            logger.info("Spark session stopped after Transform task.")

# LOAD
def load_data(**context):
    """
    Load processed Parquet data from S3
    into Amazon Redshift.
    """

    logger.info("Starting Load task.")

    ti = context["ti"]

    # Get processed path from Transform
    s3_processed_path = ti.xcom_pull(
        task_ids="transform_task",
        key="s3_processed_path",
    )

    if not s3_processed_path:
        raise ValueError("Processed S3 path was not found in XCom.")

    logger.info("Processed S3 Path received: %s", s3_processed_path,)

    spark = None

    try:

        spark = SparkSessionManager.get_session()

        logger.info("Spark session created for Load task.")

        # Read Parquet schema
        df = spark.read.parquet(s3_processed_path)

        schema = df.schema

        logger.info("Processed schema loaded successfully.")

        # Load into Redshift
        result = LoadJob(
            s3_processed_path=s3_processed_path,
            schema=schema,
        ).run()

        # XCom: loaded records
        ti.xcom_push(
            key="loaded",
            value=result["loaded_rows"],
        )

        # XCom: execution duration
        ti.xcom_push(
            key="load_duration",
            value=result["execution_time"],
        )

        # XCom: Redshift table
        ti.xcom_push(
            key="redshift_table",
            value=result["redshift_table"],
        )

        logger.info("Load task completed successfully.")

        return result["loaded_rows"]

    finally:

        if spark is not None:

            SparkSessionManager.stop_session()

            logger.info("Spark session stopped after Load task.")

# DAG
with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    default_args=DEFAULT_ARGS,
    schedule=SCHEDULE,
    start_date=START_DATE,
    catchup=CATCHUP,
    dagrun_timeout=DAG_RUN_TIMEOUT,
    max_active_runs=MAX_ACTIVE_RUNS,
    max_active_tasks=MAX_ACTIVE_TASKS,
    tags=[
        "crypto",
        "etl",
        "spark",
        "aws",
        "redshift",
        "production",
    ],
) as dag:

    # Extract Task
    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        execution_timeout=timedelta(minutes=15),
    )

    # Transform Task
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        execution_timeout=timedelta(minutes=40),
    )

    # Load Task
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        execution_timeout=timedelta(minutes=20),
    )

    # Notification Task
    notification_task = PythonOperator(
        task_id="notification_task",
        python_callable=pipeline_notification,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # Pipeline Dependency
    extract_task >> transform_task >> load_task >> notification_task