from pyspark.sql.types import (
    TimestampType,
    StringType,
)

from src.schemas.schema_manager import SchemaManager
from src.transform.crypto_transform import CryptoTransformer
from src.utils.spark_session import SparkSessionManager


DATA_FILE = "tests/fixtures/sample_crypto.ndjson"
SCHEMA_FILE = "tests/fixtures/crypto_schema.json"


def load_dataframe():
    """Load sample crypto fixture."""

    spark = SparkSessionManager.get_session()

    schema = SchemaManager(
        SCHEMA_FILE
    ).build_schema()

    return (
        spark.read
        .schema(schema)
        .json(DATA_FILE)
    )

def test_drop_columns():
    df = load_dataframe()

    df = CryptoTransformer.drop_columns(
        df,
        ["roi"],
    )

    assert "roi" not in df.columns

def test_round_numeric_columns():
    df = load_dataframe()

    df = CryptoTransformer.round_numeric_columns(
        df,
        ["current_price"],
    )

    assert "current_price" in df.columns

def test_add_ingest_timestamp():
    df = load_dataframe()

    df = CryptoTransformer.add_ingest_timestamp(df)

    assert "ingest_timestamp" in df.columns

    assert isinstance(
        df.schema["ingest_timestamp"].dataType,
        TimestampType,
    )

def test_add_days_since_ath():
    df = load_dataframe()

    df = CryptoTransformer.add_days_since_ath(df)

    assert "days_since_ath" in df.columns

def test_add_days_since_atl():
    df = load_dataframe()

    df = CryptoTransformer.add_days_since_atl(df)

    assert "days_since_atl" in df.columns

def test_add_daily_volatility():
    df = load_dataframe()

    df = CryptoTransformer.add_daily_volatility(df)

    assert "daily_volatility_percentage" in df.columns

def test_add_distance_from_ath():
    df = load_dataframe()

    df = CryptoTransformer.add_distance_from_ath(df)

    assert "distance_from_ath" in df.columns

def test_add_distance_from_atl():
    df = load_dataframe()

    df = CryptoTransformer.add_distance_from_atl(df)

    assert "distance_from_atl" in df.columns

def test_add_volume_market_cap_ratio():
    df = load_dataframe()

    df = CryptoTransformer.add_volume_market_cap_ratio(df)

    assert "volume_market_cap_ratio" in df.columns

def test_add_supply_utilization():
    df = load_dataframe()

    df = CryptoTransformer.add_supply_utilization(df)

    assert "supply_utilization_pct" in df.columns

def test_add_price_direction():
    df = load_dataframe()

    df = CryptoTransformer.add_price_direction(df)

    assert "price_direction" in df.columns
    assert isinstance(
        df.schema["price_direction"].dataType,
        StringType,
    )

def test_transform():
    df = load_dataframe()

    df = CryptoTransformer.transform(df)

    expected_columns = [
        "ingest_timestamp",
        "days_since_ath",
        "days_since_atl",
        "daily_volatility_percentage",
        "distance_from_ath",
        "distance_from_atl",
        "volume_market_cap_ratio",
        "supply_utilization_pct",
        "price_direction",
    ]

    for column in expected_columns:
        assert column in df.columns

def teardown_module(module):
    SparkSessionManager.stop_session()