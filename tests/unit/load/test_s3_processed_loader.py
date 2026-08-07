from unittest.mock import MagicMock, patch
import pytest

from src.load.s3_processed_loader import S3ProcessedLoader

@patch("src.load.s3_processed_loader.boto3.client")
def test_validate_local_path_success(mock_boto_client, tmp_path):
    """Test valid local parquet directory."""

    mock_boto_client.return_value = MagicMock()

    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    loader = S3ProcessedLoader()

    result = loader.validate_local_path(str(parquet_dir))

    assert result == parquet_dir


@patch("src.load.s3_processed_loader.boto3.client")
def test_validate_local_path_not_found(mock_boto_client):
    """Test missing directory."""

    mock_boto_client.return_value = MagicMock()

    loader = S3ProcessedLoader()

    with pytest.raises(FileNotFoundError):
        loader.validate_local_path("missing_directory")


@patch("src.load.s3_processed_loader.PathBuilder.processed_path")
@patch("src.load.s3_processed_loader.boto3.client")
def test_upload_directory_success(
    mock_boto_client,
    mock_processed_path,
    tmp_path,
):
    """Test successful upload."""

    mock_processed_path.return_value = (
        "processed/crypto/year=2026/month=08/day=04/"
    )

    mock_client = MagicMock()
    mock_boto_client.return_value = mock_client

    # Create temporary parquet directory
    parquet_dir = tmp_path / "parquet"
    parquet_dir.mkdir()

    # Dummy parquet files
    (parquet_dir / "part-00000.parquet").write_text("dummy")
    (parquet_dir / "_SUCCESS").write_text("")
    (parquet_dir / "part-00000.parquet.crc").write_text("crc")

    loader = S3ProcessedLoader()

    loader.bucket = "test-bucket"

    result = loader.upload_directory(
        local_path=str(parquet_dir),
        dataset_name="crypto",
    )

    assert result == (
        "s3://test-bucket/"
        "processed/crypto/year=2026/month=08/day=04/"
    )

    # Only parquet + _SUCCESS uploaded (.crc skipped)
    assert mock_client.upload_file.call_count == 2


@patch("src.load.s3_processed_loader.boto3.client")
def test_upload_directory_invalid_path(mock_boto_client):
    """Test invalid directory."""

    mock_boto_client.return_value = MagicMock()

    loader = S3ProcessedLoader()

    with pytest.raises(FileNotFoundError):
        loader.upload_directory(
            local_path="missing_directory",
            dataset_name="crypto",
        )