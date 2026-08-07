"""
Airflow DAG callbacks.

Provides reusable SNS-based callbacks for
successful and failed Airflow tasks.
"""
from typing import Any

from config.config import Config
from src.notifications.sns_notifier import SNSNotifier
from src.utils.email_template import EmailTemplate
from src.utils.logger import Logger

logger = Logger.get_logger("dag_callbacks", "dag_callbacks.log",)

# Reuse SNS notifier instance
sns_notifier = SNSNotifier()

def _send_sns(
    subject: str,
    message: str,
) -> None:
    """
    Publish a notification through Amazon SNS.

    Args:
        subject: SNS notification subject.
        message: SNS notification message.
    """

    try:
        sns_notifier.publish(
            subject=subject,
            message=message,
        )

        logger.info("SNS notification published successfully.")

    except Exception as e:
        logger.exception("Failed to publish SNS notification: %s", e,)

def task_success_callback(
    context: dict[str, Any],
) -> None:
    """
    Airflow callback executed when a task succeeds.

    Args:
        context: Airflow task context.
    """

    task = context["task"]
    ti = context["ti"]
    dag = context["dag"]

    logger.info(
        "Task succeeded | "
        "DAG=%s | Task=%s | Run=%s | Try=%s",
        dag.dag_id,
        task.task_id,
        context.get("run_id"),
        ti.try_number,
    )

    subject, message = EmailTemplate.sns_success(
        job_name=Config.PIPELINE_NAME,
    )

    _send_sns(
        subject=subject,
        message=message,
    )

def task_failure_callback(
    context: dict[str, Any],
) -> None:
    """
    Airflow callback executed when a task fails.

    Args:
        context: Airflow task context.
    """

    task = context["task"]
    ti = context["ti"]
    dag = context["dag"]

    exception = context.get("exception")

    error_message = (
        str(exception)
        if exception
        else "Unknown error"
    )

    logger.error(
        "Task failed | "
        "DAG=%s | Task=%s | Run=%s | Try=%s | Error=%s",
        dag.dag_id,
        task.task_id,
        context.get("run_id"),
        ti.try_number,
        error_message,
    )

    subject, message = EmailTemplate.sns_failure(
        job_name=Config.PIPELINE_NAME,
        stage=task.task_id,
        error=error_message,
    )

    _send_sns(
        subject=subject,
        message=message,
    )
