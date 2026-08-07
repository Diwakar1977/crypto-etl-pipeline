from src.schemas.schema_infer import SchemaInfer
from src.schemas.schema_manager import SchemaManager
from src.load.redshift_schema_mapper import RedShiftSchemaMapper
import json

DATA_FILE = "tests/fixtures/sample_crypto.ndjson"
SCHEMA_FILE = "tests/fixtures/crypto_schema.json"

def test_generate_create_table_from_sample_data():
    """Generate Redshift SQL using sample crypto data."""

    with open(DATA_FILE, "r", encoding="utf-8") as file:
        records = [json.loads(line) for line in file]

    SchemaInfer().infer(
        records,
        SCHEMA_FILE
    )

    spark_schema = SchemaManager(
        SCHEMA_FILE
    ).build_schema()

    mapper = RedShiftSchemaMapper(
        "crypto_market"
    )

    sql = mapper.generate_create_table(
        spark_schema
    )

    assert "CREATE TABLE IF NOT EXISTS crypto_market" in sql
    assert '"id"' in sql
    assert '"symbol"' in sql
    assert '"current_price"' in sql
    assert '"market_cap"' in sql