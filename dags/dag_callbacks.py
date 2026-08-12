"""
Airflow DAG notification callbacks.

Provides final SNS notification for the
Crypto ETL pipeline.
"""

from typing import Any

from config.config import Config
from src.notifications.sns_notifier import SNSNotifier
from src.utils.email_template import EmailTemplate
from src.utils.logger import Logger


logger = Logger.get_logger("dag_callbacks", "dag_callbacks.log",)

# SNS Notifier
sns_notifier = SNSNotifier()

# SNS Helper
def _send_sns(
    subject: str,
    message: str,
) -> None:
    """
    Publish a notification through Amazon SNS.
    """

    try:
        sns_notifier.publish(
            subject=subject,
            message=message,
        )

        logger.info("SNS notification published successfully.")

    except Exception as e:
        logger.exception("Failed to publish SNS notification: %s", e,)

# Final Pipeline Notification
def pipeline_notification(
    **context: Any,
) -> None:
    """
    Send ONE SNS notification after the ETL pipeline.

    Pipeline:

        Extract
            ↓
        Transform
            ↓
        Load
            ↓
        Notification

    Notification runs for both SUCCESS and FAILURE.

    Airflow 3 compatible:
    - No provide_session
    - No SQLAlchemy ORM
    - No direct metadata database access
    """

    dag_run = context.get("dag_run")

    if dag_run is None:
        logger.error(
            "dag_run was not available in Airflow context."
        )
        return

    logger.info(
        "Preparing final pipeline notification | "
        "DAG=%s | Run=%s",
        dag_run.dag_id,
        dag_run.run_id,
    )

    # Get task instances from Airflow context
    task_instances = context.get("task_instances")

    task_states = {}

    if task_instances:
        task_states = {
            ti.task_id: ti.state
            for ti in task_instances
        }

    logger.info(
        "Task states for run %s: %s",
        dag_run.run_id,
        task_states,
    )

    # Find failed ETL tasks
    failed_tasks = []

    for task_id, state in task_states.items():

        # Do not consider notification task itself
        if task_id == "notification_task":
            continue

        if state == "failed":
            failed_tasks.append(task_id)

    # SUCCESS
    if not failed_tasks:

        logger.info("Crypto ETL pipeline completed successfully.")

        subject, message = EmailTemplate.sns_success(
            job_name=Config.PIPELINE_NAME,
        )

        _send_sns(
            subject=subject,
            message=message,
        )

        return

    # FAILURE
    failed_stage = failed_tasks[0]

    error_message = (
        f"Task '{failed_stage}' failed. "
        "Check Airflow task logs for the full error."
    )

    logger.error(
        "Crypto ETL pipeline failed | "
        "DAG=%s | Stage=%s | Run=%s | Error=%s",
        dag_run.dag_id,
        failed_stage,
        dag_run.run_id,
        error_message,
    )

    # Create failure notification
    subject, message = EmailTemplate.sns_failure(
        job_name=Config.PIPELINE_NAME,
        stage=failed_stage,
        error=error_message,
    )

    # Send ONE SNS notification
    _send_sns(
        subject=subject,
        message=message,
    )