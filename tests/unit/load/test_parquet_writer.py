import pytest

from src.load.parquet_writer import ParquetWriter
from src.schemas.schema_manager import SchemaManager
from src.utils.spark_session import SparkSessionManager

DATA_FILE = "tests/fixtures/sample_crypto.ndjson"
SCHEMA_FILE = "tests/fixtures/crypto_schema.json"

def load_dataframe():
    spark = SparkSessionManager.get_session()

    schema = SchemaManager(
        SCHEMA_FILE
    ).build_schema()

    return (
        spark.read
        .schema(schema)
        .json(DATA_FILE)
    )

def test_write_parquet(tmp_path):
    """Test writing parquet successfully."""

    df = load_dataframe()

    output_path = tmp_path / "parquet_output"

    writer = ParquetWriter(str(output_path))

    result = writer.write(df)

    assert result == str(output_path)
    assert output_path.exists()

def test_write_parquet_partition(tmp_path):
    """Test parquet partition writing."""

    df = load_dataframe()

    output_path = tmp_path / "partition_output"

    writer = ParquetWriter(str(output_path))

    result = writer.write(
        df,
        partition_columns=["symbol"],
    )

    assert result == str(output_path)
    assert output_path.exists()

def test_invalid_write_mode(tmp_path):
    """Invalid write mode should raise ValueError."""

    df = load_dataframe()

    writer = ParquetWriter(str(tmp_path))

    with pytest.raises(ValueError):
        writer.write(
            df,
            mode="invalid_mode",
        )

def teardown_module(module):
    SparkSessionManager.stop_session()