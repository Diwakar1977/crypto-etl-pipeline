from src.schemas.schema_manager import SchemaManager
from src.transform.normalizer import Normalizer
from src.utils.spark_session import SparkSessionManager

SCHEMA_FILE = "tests/fixtures/crypto_schema.json"
DATA_FILE = "tests/fixtures/sample_crypto.ndjson"

def test_normalizer_integration():
    spark = SparkSessionManager.get_session()

    try:
        schema = SchemaManager(SCHEMA_FILE).build_schema()

        df = (
            spark.read
            .schema(schema)
            .json(DATA_FILE)
        )

        normalized_df, duplicate_count = Normalizer.normalize(
            df=df,
            expected_schema=schema,
            duplicate_subset=["id"]
        )

        # Assertions
        assert normalized_df is not None
        assert duplicate_count >= 0
        assert normalized_df.count() > 0
        assert len(normalized_df.columns) == len(schema.fields)

        print(f"Rows              : {normalized_df.count()}")
        print(f"Duplicate Removed : {duplicate_count}")

        normalized_df.printSchema()
        normalized_df.show(truncate=False)

    finally:
        SparkSessionManager.stop_session()