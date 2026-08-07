from unittest.mock import MagicMock, patch
from pyspark.sql.types import StructType
from src.load.load_job import LoadJob

@patch("src.load.load_job.RedshiftLoader")
def test_load_job(mock_loader_class):
    """Test complete LoadJob pipeline."""

    # Mock RedshiftLoader
    mock_loader = MagicMock()

    mock_loader.validate_load.return_value = 100

    mock_loader.schema = "public"
    mock_loader.table = "crypto_market"

    mock_loader_class.return_value = mock_loader

    # Sample processed schema
    schema = StructType([])

    job = LoadJob(
        s3_processed_path="s3://crypto-etl-dev/processed_data/crypto/",
        schema=schema
    )

    result = job.run()

    # Verify Redshift connection
    mock_loader.connect.assert_called_once()

    # Verify table creation with schema
    mock_loader.create_table.assert_called_once_with(
        schema
    )

    # Verify COPY
    mock_loader.copy_from_s3.assert_called_once_with(
        "s3://crypto-etl-dev/processed_data/crypto/"
    )

    # Verify validation
    mock_loader.validate_load.assert_called_once()

    # Verify close
    mock_loader.close.assert_called_once()

    # Verify output
    assert result["loaded_rows"] == 100
    assert (
        result["redshift_table"]
        == "public.crypto_market"
    )
    assert (
        result["s3_processed_path"]
        == "s3://crypto-etl-dev/processed_data/crypto/"
    )
    assert "execution_time" in result