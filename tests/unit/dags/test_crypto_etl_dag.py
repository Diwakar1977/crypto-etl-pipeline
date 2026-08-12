import pytest

# Import DAG
from dags.crypto_etl_dag import (
    dag,
    extract_data,
    transform_data,
    load_data,
)

# Test 1: DAG exists
def test_dag_exists():

    assert dag is not None

# Test 2: DAG ID
def test_dag_id():

    assert dag.dag_id == "crypto_etl_pipeline"


# Test 3: Required tasks exist
def test_required_tasks_exist():

    task_ids = {
        task.task_id
        for task in dag.tasks
    }

    assert "extract_task" in task_ids
    assert "transform_task" in task_ids
    assert "load_task" in task_ids
    assert "notification_task" in task_ids

# Test 4: Exact number of tasks
def test_task_count():

    assert len(dag.tasks) == 4

# Test 5: Task dependency
def test_task_dependencies():

    extract_task = dag.get_task("extract_task")
    transform_task = dag.get_task("transform_task")
    load_task = dag.get_task("load_task")
    notification_task = dag.get_task("notification_task")

    assert transform_task in (extract_task.downstream_list)
    assert load_task in (transform_task.downstream_list)
    assert notification_task in (load_task.downstream_list)

# Test 6: Exact dependency chain
def test_exact_pipeline_order():

    extract_task = dag.get_task("extract_task")
    transform_task = dag.get_task("transform_task")
    load_task = dag.get_task("load_task")
    notification_task = dag.get_task("notification_task")

    assert extract_task.upstream_list == []
    assert extract_task.downstream_list == [transform_task]
    assert transform_task.upstream_list == [extract_task]
    assert transform_task.downstream_list == [load_task]
    assert load_task.upstream_list == [transform_task]
    assert load_task.downstream_list == [notification_task]
    assert notification_task.upstream_list == [load_task]
    assert notification_task.downstream_list == []

# Test 7: Notification trigger rule
def test_notification_trigger_rule():

    notification_task = dag.get_task("notification_task")

    assert (notification_task.trigger_rule == "all_done")

# Test 8: Extract task
def test_extract_data(monkeypatch):

    class FakeExtractJob:

        def run(self):

            return {
                "s3_key": (
                    "raw_data/"
                    "year=2026/"
                    "month=08/"
                    "day=12/"
                    "crypto.json"
                ),
                "record_count": 100,
                "execution_time": 5.2,
            }

    monkeypatch.setattr(
        "dags.crypto_etl_dag.ExtractJob",
        FakeExtractJob,
    )

    pushed_values = {}

    class FakeTI:

        def xcom_push(self, key, value):

            pushed_values[key] = value

    context = {"ti": FakeTI()}

    result = extract_data(**context)

    assert result is not None
    assert ("raw_s3_path" in pushed_values)
    assert (pushed_values["extracted"] == 100)
    assert (pushed_values["extract_duration"] == 5.2)

# Test 9: Transform task
def test_transform_data(monkeypatch):

    class FakeSpark:

        pass

    class FakeSparkSessionManager:

        @staticmethod
        def get_session():

            return FakeSpark()

        @staticmethod
        def stop_session():

            pass

    class FakeTransformJob:

        def __init__(
            self,
            spark,
            schema_file,
            input_path,
            output_path,
        ):

            self.spark = spark
            self.schema_file = schema_file
            self.input_path = input_path
            self.output_path = output_path

        def run(self):

            return {
                "s3_processed_path": (
                    "s3://test-bucket/"
                    "processed/crypto/"
                ),
                "processed_rows": 90,
                "rejected_rows": 10,
                "execution_time": 8.5,
            }

    monkeypatch.setattr(
        "dags.crypto_etl_dag.SparkSessionManager",
        FakeSparkSessionManager,
    )

    monkeypatch.setattr(
        "dags.crypto_etl_dag.TransformJob",
        FakeTransformJob,
    )

    pushed_values = {}

    class FakeTI:

        def xcom_pull(
            self,
            task_ids,
            key,
        ):

            assert task_ids == "extract_task"
            assert key == "raw_s3_path"

            return (
                "s3://test-bucket/"
                "raw/crypto.json"
            )

        def xcom_push(
            self,
            key,
            value,
        ):

            pushed_values[key] = value

    context = {
        "ti": FakeTI()
    }

    result = transform_data(
        **context
    )

    assert result == (
        "s3://test-bucket/"
        "processed/crypto/"
    )

    assert (pushed_values["transformed"] == 90)
    assert (pushed_values["rejected"] == 10)
    assert (pushed_values["transform_duration"] == 8.5)

# Test 10: Transform missing XCom
def test_transform_data_missing_raw_path():

    class FakeTI:

        def xcom_pull(
            self,
            task_ids,
            key,
        ):

            return None

    context = {"ti": FakeTI()}

    with pytest.raises(
        ValueError,
        match="Raw S3 path was not found",
    ):

        transform_data(**context)

# Test 11: Load task
def test_load_data(monkeypatch):

    class FakeSchema:
        pass

    class FakeDataFrame:

        @property
        def schema(self):

            return FakeSchema()

    class FakeSpark:

        def read_parquet(
            self,
            path,
        ):

            return FakeDataFrame()

    class FakeRead:

        def parquet(
            self,
            path,
        ):

            return FakeDataFrame()

    class FakeSpark:

        read = FakeRead()

    class FakeSparkSessionManager:

        @staticmethod
        def get_session():

            return FakeSpark()

        @staticmethod
        def stop_session():

            pass

    class FakeLoadJob:

        def __init__(
            self,
            s3_processed_path,
            schema,
        ):

            self.s3_processed_path = (
                s3_processed_path
            )

            self.schema = schema

        def run(self):

            return {
                "loaded_rows": 90,
                "execution_time": 12.3,
                "redshift_table": (
                    "crypto_market_data"
                ),
            }

    monkeypatch.setattr(
        "dags.crypto_etl_dag.SparkSessionManager",
        FakeSparkSessionManager,
    )

    monkeypatch.setattr(
        "dags.crypto_etl_dag.LoadJob",
        FakeLoadJob,
    )

    pushed_values = {}

    class FakeTI:

        def xcom_pull(
            self,
            task_ids,
            key,
        ):

            assert (task_ids == "transform_task")
            assert (key == "s3_processed_path")

            return (
                "s3://test-bucket/"
                "processed/crypto/"
            )

        def xcom_push(
            self,
            key,
            value,
        ):

            pushed_values[key] = value

    context = {"ti": FakeTI()}

    result = load_data(**context)
    assert result == 90

    assert (pushed_values["loaded"] == 90)
    assert (pushed_values["load_duration"] == 12.3)
    assert (pushed_values["redshift_table"] == "crypto_market_data")

# Test 12: Load missing XCom
def test_load_data_missing_processed_path():

    class FakeTI:

        def xcom_pull(
            self,
            task_ids,
            key,
        ):

            return None

    context = {"ti": FakeTI()}

    with pytest.raises(
        ValueError,
        match="Processed S3 path was not found",
    ):

        load_data(**context)