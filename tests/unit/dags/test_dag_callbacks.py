"""
Unit tests for Airflow DAG callbacks.
"""
from unittest.mock import MagicMock, patch

from dags.dag_callbacks import (
    _send_sns,
    task_success_callback,
    task_failure_callback,
)

class TestSendSNS:
    """Test SNS notification helper."""

    @patch("dags.dag_callbacks.sns_notifier")
    def test_send_sns_success(self, mock_sns):
        """SNS message should be published successfully."""

        mock_sns.publish.return_value = "message-id-123"

        _send_sns(
            subject="Test Subject",
            message="Test Message",
        )

        mock_sns.publish.assert_called_once_with(
            subject="Test Subject",
            message="Test Message",
        )

    @patch("dags.dag_callbacks.sns_notifier")
    def test_send_sns_failure(self, mock_sns):
        """SNS failure should be handled without raising."""

        mock_sns.publish.side_effect = Exception(
            "SNS publish failed"
        )

        _send_sns(
            subject="Test Subject",
            message="Test Message",
        )

        mock_sns.publish.assert_called_once_with(
            subject="Test Subject",
            message="Test Message",
        )

class TestTaskSuccessCallback:
    """Test successful task callback."""

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_success")
    def test_task_success_callback(
        self,
        mock_template,
        mock_send_sns,
    ):
        """Success callback should generate and send SNS notification."""

        mock_template.return_value = (
            "SUCCESS | Crypto ETL Pipeline",
            "Pipeline completed successfully.",
        )

        context = {
            "task": MagicMock(task_id="extract_data"),
            "ti": MagicMock(try_number=1),
            "dag": MagicMock(dag_id="crypto_etl_pipeline"),
            "run_id": "test_run_001",
        }

        task_success_callback(context)

        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
        )

        mock_send_sns.assert_called_once_with(
            subject="SUCCESS | Crypto ETL Pipeline",
            message="Pipeline completed successfully.",
        )

class TestTaskFailureCallback:
    """Test failed task callback."""

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_task_failure_callback(
        self,
        mock_template,
        mock_send_sns,
    ):
        """Failure callback should generate and send SNS notification."""

        mock_template.return_value = (
            "FAILED | Crypto ETL Pipeline",
            "Pipeline failed.",
        )

        exception = ValueError(
            "Test transformation error"
        )

        context = {
            "task": MagicMock(task_id="transform_data"),
            "ti": MagicMock(try_number=2),
            "dag": MagicMock(dag_id="crypto_etl_pipeline"),
            "run_id": "test_run_002",
            "exception": exception,
        }

        task_failure_callback(context)

        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
            stage="transform_data",
            error="Test transformation error",
        )

        mock_send_sns.assert_called_once_with(
            subject="FAILED | Crypto ETL Pipeline",
            message="Pipeline failed.",
        )

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_task_failure_callback_without_exception(
        self,
        mock_template,
        mock_send_sns,
    ):
        """Failure callback should handle missing exception."""

        mock_template.return_value = (
            "FAILED | Crypto ETL Pipeline",
            "Unknown error.",
        )

        context = {
            "task": MagicMock(task_id="load_data"),
            "ti": MagicMock(try_number=1),
            "dag": MagicMock(dag_id="crypto_etl_pipeline"),
            "run_id": "test_run_003",
            "exception": None,
        }

        task_failure_callback(context)

        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
            stage="load_data",
            error="Unknown error",
        )

        mock_send_sns.assert_called_once_with(
            subject="FAILED | Crypto ETL Pipeline",
            message="Unknown error.",
        )
