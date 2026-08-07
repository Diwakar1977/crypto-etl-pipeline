from src.utils.spark_session import SparkSessionManager
from src.schemas.schema_manager import SchemaManager


SCHEMA_FILE = "tests/fixtures/crypto_schema.json"
DATA_FILE = "tests/fixtures/sample_crypto.ndjson"


def test_spark_read_json():
    """Test reading JSON with inferred schema."""

    spark = SparkSessionManager.get_session()

    try:
        schema = SchemaManager(SCHEMA_FILE).build_schema()

        df = (
            spark.read
            .schema(schema)
            .json(DATA_FILE)
        )

        assert df is not None
        assert df.count() > 0
        assert len(df.columns) > 0

        assert "id" in df.columns
        assert "symbol" in df.columns
        assert "current_price" in df.columns

    finally:
        SparkSessionManager.stop_session()