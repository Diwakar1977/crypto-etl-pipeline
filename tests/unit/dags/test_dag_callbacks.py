"""
Unit tests for Airflow DAG callbacks.
"""

from unittest.mock import MagicMock, patch

from dags.dag_callbacks import (
    _send_sns,
    pipeline_success_callback,
    pipeline_failure_callback,
)

# Test SNS Helper
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
        """SNS failure should not raise an exception."""

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


# Pipeline Success
class TestPipelineSuccessCallback:
    """Test pipeline-level success callback."""

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_success")
    def test_pipeline_success_callback(
        self,
        mock_template,
        mock_send_sns,
    ):
        """Entire DAG success sends ONE notification."""

        mock_template.return_value = (
            "SUCCESS | Crypto ETL Pipeline",
            "Pipeline completed successfully.",
        )

        dag = MagicMock()
        dag.dag_id = "crypto_etl_pipeline"

        context = {
            "dag": dag,
            "run_id": "test_run_001",
        }

        pipeline_success_callback(context)

        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
        )

        mock_send_sns.assert_called_once_with(
            subject="SUCCESS | Crypto ETL Pipeline",
            message="Pipeline completed successfully.",
        )


# Pipeline Failure
class TestPipelineFailureCallback:
    """Test pipeline-level failure callback."""

    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_pipeline_failure_callback_with_exception(
        self,
        mock_template,
        mock_send_sns,
    ):
        """
        Failed DAG should identify the failed task
        and include the actual exception.
        """

        mock_template.return_value = (
            "FAILED | Crypto ETL Pipeline",
            "Pipeline failed.",
        )

        # Failed task
        failed_task = MagicMock()
        failed_task.task_id = "transform_data"

        # DAG Run
        dag_run = MagicMock()

        dag_run.get_task_instances.return_value = [
            failed_task
        ]

        # DAG
        dag = MagicMock()
        dag.dag_id = "crypto_etl_pipeline"

        # Actual exception
        exception = ValueError(
            "Test transformation error"
        )

        context = {
            "dag": dag,
            "dag_run": dag_run,
            "run_id": "test_run_002",
            "exception": exception,
        }

        pipeline_failure_callback(context)

        # Verify stage + error
        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
            stage="transform_data",
            error="Test transformation error",
        )

        # ONE SNS notification
        mock_send_sns.assert_called_once_with(
            subject="FAILED | Crypto ETL Pipeline",
            message="Pipeline failed.",
        )


    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_pipeline_failure_callback_without_exception(
        self,
        mock_template,
        mock_send_sns,
    ):
        """
        Failed task should still identify the stage
        when exception is unavailable.
        """

        mock_template.return_value = (
            "FAILED | Crypto ETL Pipeline",
            "Pipeline failed.",
        )

        # Failed task
        failed_task = MagicMock()
        failed_task.task_id = "load_data"

        # DAG Run
        dag_run = MagicMock()

        dag_run.get_task_instances.return_value = [
            failed_task
        ]

        # DAG
        dag = MagicMock()
        dag.dag_id = "crypto_etl_pipeline"

        context = {
            "dag": dag,
            "dag_run": dag_run,
            "run_id": "test_run_003",
            "exception": None,
        }

        pipeline_failure_callback(context)

        expected_error = (
            "Task 'load_data' failed. "
            "Check Airflow task logs for the full error."
        )

        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
            stage="load_data",
            error=expected_error,
        )

        mock_send_sns.assert_called_once_with(
            subject="FAILED | Crypto ETL Pipeline",
            message="Pipeline failed.",
        )


    @patch("dags.dag_callbacks._send_sns")
    @patch("dags.dag_callbacks.EmailTemplate.sns_failure")
    def test_pipeline_failure_callback_no_failed_task(
        self,
        mock_template,
        mock_send_sns,
    ):
        """
        Handle DAG failure when no failed task
        is available.
        """

        mock_template.return_value = (
            "FAILED | Crypto ETL Pipeline",
            "Pipeline failed.",
        )

        # DAG Run with NO failed tasks
        dag_run = MagicMock()

        dag_run.get_task_instances.return_value = []

        # DAG
        dag = MagicMock()
        dag.dag_id = "crypto_etl_pipeline"

        # Exception
        exception = RuntimeError(
            "Unknown pipeline error"
        )

        context = {
            "dag": dag,
            "dag_run": dag_run,
            "run_id": "test_run_004",
            "exception": exception,
        }

        pipeline_failure_callback(context)

        # Stage should be unknown
        mock_template.assert_called_once_with(
            job_name="Crypto ETL Pipeline",
            stage="unknown",
            error="Unknown pipeline error",
        )

        # ONE SNS notification
        mock_send_sns.assert_called_once_with(
            subject="FAILED | Crypto ETL Pipeline",
            message="Pipeline failed.",
        )