"""
Unit tests for Airflow DAG notification callbacks.
"""

from unittest.mock import ANY, MagicMock, patch

from airflow.utils.state import TaskInstanceState

from dags.dag_callbacks import (
    _send_sns,
    pipeline_notification,
)


class TestSendSNS:
    """Tests for _send_sns()."""

    @patch("dags.dag_callbacks.logger")
    @patch("dags.dag_callbacks.sns_notifier")
    def test_send_sns_success(
        self,
        mock_sns_notifier,
        mock_logger,
    ):
        """SNS notification should be published successfully."""

        _send_sns(
            subject="Test Subject",
            message="Test Message",
        )

        mock_sns_notifier.publish.assert_called_once_with(
            subject="Test Subject",
            message="Test Message",
        )

        mock_logger.info.assert_called_once_with(
            "SNS notification published successfully."
        )

    @patch("dags.dag_callbacks.logger")
    @patch("dags.dag_callbacks.sns_notifier")
    def test_send_sns_failure(
        self,
        mock_sns_notifier,
        mock_logger,
    ):
        """SNS exception should be logged and not raised."""

        mock_sns_notifier.publish.side_effect = Exception(
            "SNS error"
        )

        _send_sns(
            subject="Test Subject",
            message="Test Message",
        )

        mock_sns_notifier.publish.assert_called_once_with(
            subject="Test Subject",
            message="Test Message",
        )

        mock_logger.exception.assert_called_once()


class TestPipelineNotification:
    """Tests for pipeline_notification()."""

    @patch("dags.dag_callbacks.logger")
    def test_missing_dag_run(
        self,
        mock_logger,
    ):
        """Callback should return when dag_run is missing."""

        pipeline_notification()

        mock_logger.error.assert_called_once_with(
            "dag_run was not available in Airflow context."
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_success")
    @patch("dags.dag_callbacks.logger")
    def test_success_pipeline(
        self,
        mock_logger,
        mock_sns_success,
        mock_send_sns,
    ):
        """Successful pipeline should send success notification."""

        dag_run = MagicMock()
        dag_run.dag_id = "crypto_etl_pipeline"
        dag_run.run_id = "test_run_001"

        task_extract = MagicMock()
        task_extract.task_id = "extract_task"
        task_extract.state = TaskInstanceState.SUCCESS

        task_transform = MagicMock()
        task_transform.task_id = "transform_task"
        task_transform.state = TaskInstanceState.SUCCESS

        task_load = MagicMock()
        task_load.task_id = "load_task"
        task_load.state = TaskInstanceState.SUCCESS

        task_notification = MagicMock()
        task_notification.task_id = "notification_task"
        task_notification.state = TaskInstanceState.SUCCESS

        mock_sns_success.return_value = (
            "Success Subject",
            "Success Message",
        )

        pipeline_notification(
            dag_run=dag_run,
            task_instances=[
                task_extract,
                task_transform,
                task_load,
                task_notification,
            ],
        )

        mock_sns_success.assert_called_once_with(
            job_name=ANY,
        )

        mock_send_sns.assert_called_once_with(
            subject="Success Subject",
            message="Success Message",
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    @patch("dags.dag_callbacks.logger")
    def test_failed_pipeline(
        self,
        mock_logger,
        mock_sns_failure,
        mock_send_sns,
    ):
        """Failed pipeline should send failure notification."""

        dag_run = MagicMock()
        dag_run.dag_id = "crypto_etl_pipeline"
        dag_run.run_id = "test_run_002"

        task_extract = MagicMock()
        task_extract.task_id = "extract_task"
        task_extract.state = TaskInstanceState.SUCCESS

        task_transform = MagicMock()
        task_transform.task_id = "transform_task"
        task_transform.state = TaskInstanceState.FAILED

        task_load = MagicMock()
        task_load.task_id = "load_task"
        task_load.state = TaskInstanceState.SUCCESS

        mock_sns_failure.return_value = (
            "Failure Subject",
            "Failure Message",
        )

        pipeline_notification(
            dag_run=dag_run,
            task_instances=[
                task_extract,
                task_transform,
                task_load,
            ],
        )

        mock_sns_failure.assert_called_once_with(
            job_name=ANY,
            stage="transform_task",
            error=(
                "Task 'transform_task' failed. "
                "Check Airflow task logs for the full error."
            ),
        )

        mock_send_sns.assert_called_once_with(
            subject="Failure Subject",
            message="Failure Message",
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_success")
    def test_notification_task_is_ignored(
        self,
        mock_sns_success,
        mock_send_sns,
    ):
        """
        notification_task should not be treated as a failed
        ETL task.
        """

        dag_run = MagicMock()
        dag_run.dag_id = "crypto_etl_pipeline"
        dag_run.run_id = "test_run_003"

        task_extract = MagicMock()
        task_extract.task_id = "extract_task"
        task_extract.state = TaskInstanceState.SUCCESS

        task_notification = MagicMock()
        task_notification.task_id = "notification_task"
        task_notification.state = TaskInstanceState.FAILED

        mock_sns_success.return_value = (
            "Success Subject",
            "Success Message",
        )

        pipeline_notification(
            dag_run=dag_run,
            task_instances=[
                task_extract,
                task_notification,
            ],
        )

        mock_sns_success.assert_called_once_with(
            job_name=ANY,
        )

        mock_send_sns.assert_called_once_with(
            subject="Success Subject",
            message="Success Message",
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_success")
    def test_no_task_instances(
        self,
        mock_sns_success,
        mock_send_sns,
    ):
        """No task instances should result in success notification."""

        dag_run = MagicMock()
        dag_run.dag_id = "crypto_etl_pipeline"
        dag_run.run_id = "test_run_004"

        mock_sns_success.return_value = (
            "Success Subject",
            "Success Message",
        )

        pipeline_notification(
            dag_run=dag_run,
            task_instances=[],
        )

        mock_sns_success.assert_called_once_with(
            job_name=ANY,
        )

        mock_send_sns.assert_called_once_with(
            subject="Success Subject",
            message="Success Message",
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_first_failed_task_is_used(
        self,
        mock_sns_failure,
        mock_send_sns,
    ):
        """
        First failed ETL task should be used in the
        failure notification.
        """

        dag_run = MagicMock()
        dag_run.dag_id = "crypto_etl_pipeline"
        dag_run.run_id = "test_run_005"

        task_transform = MagicMock()
        task_transform.task_id = "transform_task"
        task_transform.state = TaskInstanceState.FAILED

        task_load = MagicMock()
        task_load.task_id = "load_task"
        task_load.state = TaskInstanceState.FAILED

        mock_sns_failure.return_value = (
            "Failure Subject",
            "Failure Message",
        )

        pipeline_notification(
            dag_run=dag_run,
            task_instances=[
                task_transform,
                task_load,
            ],
        )

        mock_sns_failure.assert_called_once_with(
            job_name=ANY,
            stage="transform_task",
            error=(
                "Task 'transform_task' failed. "
                "Check Airflow task logs for the full error."
            ),
        )

        mock_send_sns.assert_called_once_with(
            subject="Failure Subject",
            message="Failure Message",
        )