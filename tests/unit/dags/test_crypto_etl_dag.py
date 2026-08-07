"""
Unit tests for Crypto ETL Airflow DAG.

Tests DAG structure, tasks, dependencies,
callbacks, and execution timeouts.
"""

from datetime import timedelta

from dags.crypto_etl_dag import (
    dag,
    extract_data,
    transform_data,
    load_data,
)

class TestCryptoETLDAG:
    """Test Crypto ETL DAG configuration and structure."""

    def test_dag_id(self):
        """Test DAG ID."""

        assert dag.dag_id == "crypto_etl_pipeline"

    def test_dag_description(self):
        """Test DAG description."""

        assert "cryptocurrency" in dag.description.lower()
        assert "spark" in dag.description.lower()
        assert "redshift" in dag.description.lower()

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

    def test_extract_task(self):
        """Test Extract task configuration."""

        task = dag.get_task("extract_data")

        assert task.python_callable == extract_data
        assert task.execution_timeout == timedelta(
            minutes=15
        )

    def test_transform_task(self):
        """Test Transform task configuration."""

        task = dag.get_task("transform_data")

        assert task.python_callable == transform_data
        assert task.execution_timeout == timedelta(
            minutes=40
        )

    def test_load_task(self):
        """Test Load task configuration."""

        task = dag.get_task("load_data")

        assert task.python_callable == load_data
        assert task.execution_timeout == timedelta(
            minutes=20
        )

    def test_task_dependencies(self):
        """Test Extract → Transform → Load dependency."""

        extract_task = dag.get_task("extract_data")
        transform_task = dag.get_task("transform_data")
        load_task = dag.get_task("load_data")

        assert extract_task.downstream_task_ids == {"transform_data"}
        assert transform_task.upstream_task_ids == {"extract_data"}
        assert transform_task.downstream_task_ids == {"load_data"}
        assert load_task.upstream_task_ids == {"transform_data"}

    def test_extract_has_callbacks(self):
        """Test Extract task callbacks."""

        task = dag.get_task("extract_data")

        assert task.on_success_callback is not None
        assert task.on_failure_callback is not None

    def test_transform_has_callbacks(self):
        """Test Transform task callbacks."""

        task = dag.get_task("transform_data")

        assert task.on_success_callback is not None
        assert task.on_failure_callback is not None

    def test_load_has_callbacks(self):
        """Test Load task callbacks."""

        task = dag.get_task("load_data")

        assert task.on_success_callback is not None
        assert task.on_failure_callback is not None

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

    def test_extract_has_no_upstream(self):
        """Extract should be the first task."""

        task = dag.get_task("extract_data")

        assert task.upstream_task_ids == set()

    def test_load_has_no_downstream(self):
        """Load should be the final task."""

        task = dag.get_task("load_data")

        assert task.downstream_task_ids == set()

