import pytest
from dags import dag_callbacks

# Fake Task Instance
class FakeTaskInstance:
    def __init__(self, task_id, state):
        self.task_id = task_id
        self.state = state

# Fake DAG Run
class FakeDagRun:

    def __init__(self, task_instances):
        self.dag_id = "crypto_etl_pipeline"
        self.run_id = "test_run_001"
        self._task_instances = task_instances

    def get_task_instances(self):
        return self._task_instances

# Test 1: Complete Pipeline SUCCESS
def test_pipeline_notification_success(monkeypatch):

    task_instances = [
        FakeTaskInstance(
            "extract_task",
            "success",
        ),
        FakeTaskInstance(
            "transform_task",
            "success",
        ),
        FakeTaskInstance(
            "load_task",
            "success",
        ),
        FakeTaskInstance(
            "notification_task",
            "running",
        ),
    ]

    dag_run = FakeDagRun(task_instances)

    sent_notification = {}

    def mock_sns_success(job_name):

        return (
            "Crypto ETL Pipeline - SUCCESS",
            "Crypto ETL Pipeline completed successfully.",
        )

    def mock_send_sns(subject, message):

        sent_notification["subject"] = subject
        sent_notification["message"] = message

    monkeypatch.setattr(
        dag_callbacks.EmailTemplate,
        "sns_success",
        mock_sns_success,
    )

    monkeypatch.setattr(
        dag_callbacks,
        "_send_sns",
        mock_send_sns,
    )

    context = {"dag_run": dag_run,}

    dag_callbacks.pipeline_notification(**context)

    assert (sent_notification["subject"] == "Crypto ETL Pipeline - SUCCESS")
    assert ("completed successfully" in sent_notification["message"])

# Test 2: Transform FAILURE
def test_pipeline_notification_transform_failure(
    monkeypatch,
):

    task_instances = [
        FakeTaskInstance(
            "extract_task",
            "success",
        ),
        FakeTaskInstance(
            "transform_task",
            "failed",
        ),
        FakeTaskInstance(
            "load_task",
            "upstream_failed",
        ),
        FakeTaskInstance(
            "notification_task",
            "running",
        ),
    ]

    dag_run = FakeDagRun(task_instances)

    sent_notification = {}

    def mock_sns_failure(
        job_name,
        stage,
        error,
    ):

        return (
            "Crypto ETL Pipeline - FAILED",
            f"Pipeline failed at {stage}: {error}",
        )

    def mock_send_sns(subject, message):

        sent_notification["subject"] = subject
        sent_notification["message"] = message

    monkeypatch.setattr(
        dag_callbacks.EmailTemplate,
        "sns_failure",
        mock_sns_failure,
    )

    monkeypatch.setattr(
        dag_callbacks,
        "_send_sns",
        mock_send_sns,
    )

    context = {"dag_run": dag_run,}

    dag_callbacks.pipeline_notification(
        **context
    )

    assert (sent_notification["subject"] == "Crypto ETL Pipeline - FAILED")
    assert ("transform_task" in sent_notification["message"])

# Test 3: Load FAILURE
def test_pipeline_notification_load_failure(
    monkeypatch,
):

    task_instances = [
        FakeTaskInstance(
            "extract_task",
            "success",
        ),
        FakeTaskInstance(
            "transform_task",
            "success",
        ),
        FakeTaskInstance(
            "load_task",
            "failed",
        ),
        FakeTaskInstance(
            "notification_task",
            "running",
        ),
    ]

    dag_run = FakeDagRun(task_instances)

    sent_notification = {}

    def mock_sns_failure(
        job_name,
        stage,
        error,
    ):

        return (
            "Crypto ETL Pipeline - FAILED",
            f"Pipeline failed at {stage}: {error}",
        )

    def mock_send_sns(subject, message):

        sent_notification["subject"] = subject
        sent_notification["message"] = message

    monkeypatch.setattr(
        dag_callbacks.EmailTemplate,
        "sns_failure",
        mock_sns_failure,
    )

    monkeypatch.setattr(
        dag_callbacks,
        "_send_sns",
        mock_send_sns,
    )

    context = {"dag_run": dag_run,}

    dag_callbacks.pipeline_notification(**context)

    assert (sent_notification["subject"] == "Crypto ETL Pipeline - FAILED")
    assert ("load_task" in sent_notification["message"])

# Test 4: SNS Failure Should Not Crash Callback
def test_send_sns_failure(monkeypatch):

    def mock_publish(
        subject,
        message,
    ):

        raise Exception("SNS connection failed")
    
    monkeypatch.setattr(
        dag_callbacks.sns_notifier,
        "publish",
        mock_publish,
    )

    # Should not raise exception
    dag_callbacks._send_sns(
        subject="Test Subject",
        message="Test Message",
    )