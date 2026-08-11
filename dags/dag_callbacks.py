"""
Airflow DAG callbacks.

Provides reusable SNS-based callbacks for
pipeline-level success and failure notifications.
"""

from typing import Any

from airflow.utils.state import TaskInstanceState

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


# Pipeline Success Callback
def pipeline_success_callback(
    context: dict[str, Any],
) -> None:
    """
    Send ONE SNS notification when the entire DAG succeeds.
    """

    dag = context["dag"]

    logger.info(
        "Pipeline succeeded | DAG=%s | Run=%s",
        dag.dag_id,
        context.get("run_id"),
    )

    subject, message = EmailTemplate.sns_success(
        job_name=Config.PIPELINE_NAME,
    )

    _send_sns(
        subject=subject,
        message=message,
    )

# Pipeline Failure Callback
def pipeline_failure_callback(
    context: dict[str, Any],
) -> None:
    """
    Send ONE SNS notification when the entire DAG fails.

    Notification contains:

    - Pipeline name
    - Status
    - Failed stage
    - Actual error when available
    - Run date
    """

    dag = context["dag"]
    dag_run = context["dag_run"]

    # Find failed task
    failed_tasks = dag_run.get_task_instances(
        state=TaskInstanceState.FAILED
    )

    if failed_tasks:
        failed_task = failed_tasks[0]
        stage = failed_task.task_id

    else:
        stage = "unknown"

        logger.error("DAG failed but no failed task was found.")

    # Get error
    exception = context.get("exception")

    if exception is not None:
        error_message = str(exception)

    else:
        error_message = (
            f"Task '{stage}' failed. "
            "Check Airflow task logs for the full error."
        )

    # Log failure
    logger.error(
        "Pipeline failed | "
        "DAG=%s | Stage=%s | Run=%s | Error=%s",
        dag.dag_id,
        stage,
        context.get("run_id"),
        error_message,
    )

    # Create notification
    subject, message = EmailTemplate.sns_failure(
        job_name=Config.PIPELINE_NAME,
        stage=stage,
        error=error_message,
    )

    # ONE SNS notification
    _send_sns(
        subject=subject,
        message=message,
    )