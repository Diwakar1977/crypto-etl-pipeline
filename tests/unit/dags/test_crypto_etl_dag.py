"""
Unit tests for Crypto ETL Airflow DAG.

Tests:
- DAG configuration
- Tasks
- Dependencies
- DAG-level success callback
- DAG-level failure callback
- Execution timeouts
- No individual task notifications
"""

from datetime import timedelta

from dags.crypto_etl_dag import (
    dag,
    extract_data,
    transform_data,
    load_data,
)

from dags.dag_callbacks import (
    pipeline_success_callback,
    pipeline_failure_callback,
)

class TestCryptoETLDAG:
    """Test Crypto ETL DAG configuration and structure."""

    # DAG CONFIGURATION
    def test_dag_id(self):
        """Test DAG ID."""

        assert dag.dag_id == "crypto_etl_pipeline"

    def test_dag_description(self):
        """Test DAG description."""

        description = dag.description.lower()

        assert "cryptocurrency" in description
        assert "spark" in description
        assert "redshift" in description

    def test_dag_schedule(self):
        """Test DAG has a schedule."""

        assert dag.schedule is not None

    # DAG TASKS
    def test_dag_tasks(self):
        """Test all expected tasks exist."""

        task_ids = {
            task.task_id
            for task in dag.tasks
        }

        expected_tasks = {
            "extract_data",
            "transform_data",
            "load_data",
        }

        assert task_ids == expected_tasks

    def test_task_count(self):
        """Test DAG contains exactly three tasks."""

        assert len(dag.tasks) == 3

    # EXTRACT TASK
    def test_extract_task(self):
        """Test Extract task configuration."""

        task = dag.get_task("extract_data")

        assert task.python_callable == extract_data
        assert task.execution_timeout == timedelta(minutes=15)

    # TRANSFORM TASK
    def test_transform_task(self):
        """Test Transform task configuration."""

        task = dag.get_task("transform_data")

        assert task.python_callable == transform_data
        assert task.execution_timeout == timedelta(minutes=40)

    # LOAD TASK
    def test_load_task(self):
        """Test Load task configuration."""

        task = dag.get_task("load_data")

        assert task.python_callable == load_data
        assert task.execution_timeout == timedelta(minutes=20)

    # DAG SUCCESS CALLBACK

    def test_dag_success_callback(self):
        """
        Entire DAG should have ONE success callback.
        """

        assert dag.on_success_callback is not None
        assert (dag.on_success_callback == pipeline_success_callback)

    # DAG FAILURE CALLBACK
    def test_dag_failure_callback(self):
        """
        Entire DAG should have ONE failure callback.
        """

        assert dag.on_failure_callback is not None

        assert (dag.on_failure_callback == pipeline_failure_callback)

    # NO INDIVIDUAL TASK CALLBACKS
    def test_extract_has_no_individual_callbacks(self):
        """
        Extract should NOT send its own notification.
        """

        task = dag.get_task("extract_data")

        assert not task.on_success_callback
        assert not task.on_failure_callback

    def test_transform_has_no_individual_callbacks(self):
        """
        Transform should NOT send its own notification.
        """

        task = dag.get_task("transform_data")

        assert not task.on_success_callback
        assert not task.on_failure_callback

    def test_load_has_no_individual_callbacks(self):
        """
        Load should NOT send its own notification.
        """

        task = dag.get_task("load_data")

        assert not task.on_success_callback
        assert not task.on_failure_callback

    # DEPENDENCIES
    def test_task_dependencies(self):
        """
        Test:

        Extract
            ↓
        Transform
            ↓
        Load
        """

        extract_task = dag.get_task("extract_data")
        transform_task = dag.get_task("transform_data")
        load_task = dag.get_task("load_data")

        assert (extract_task.downstream_task_ids == {"transform_data"})
        assert (transform_task.upstream_task_ids == {"extract_data"})
        assert (transform_task.downstream_task_ids == {"load_data"})
        assert (load_task.upstream_task_ids == {"transform_data"})

    # TASK ORDER
    def test_task_order(self):
        """Test complete task execution order."""

        task_ids = [
            task.task_id
            for task in dag.topological_sort()
        ]

        assert task_ids == [
            "extract_data",
            "transform_data",
            "load_data",
        ]

    # FIRST TASK
    def test_extract_has_no_upstream(self):
        """Extract should be the first task."""

        task = dag.get_task("extract_data")

        assert task.upstream_task_ids == set()

    # FINAL TASK
    def test_load_has_no_downstream(self):
        """Load should be the final task."""

        task = dag.get_task("load_data")

        assert task.downstream_task_ids == set()