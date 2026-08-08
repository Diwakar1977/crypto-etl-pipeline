from src.utils.spark_session import SparkSessionManager
from src.schemas.schema_manager import SchemaManager
from src.transform.validator import DataValidator

SCHEMA_FILE = "tests/fixtures/crypto_schema.json"
DATA_FILE = "tests/fixtures/sample_crypto.ndjson"

def test_data_validation():

    spark = SparkSessionManager.get_session()

    try:
        schema = SchemaManager(SCHEMA_FILE).build_schema()

        df = (
            spark.read
            .schema(schema)
            .json(DATA_FILE)
        )

        # Validate
        DataValidator.validate(
            df=df,
            required_columns=df.columns,
            expected_schema=schema,
            null_threshold=50.0
        )

        # Assertions
        assert df.count() > 0
        assert len(df.columns) > 0
        assert df.schema == schema

        print(f"Rows    : {df.count()}")
        print(f"Columns : {len(df.columns)}")

        df.printSchema()
        df.show(5, truncate=False)

    finally:
        SparkSessionManager.stop_session()
        