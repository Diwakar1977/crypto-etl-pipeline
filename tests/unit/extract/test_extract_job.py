from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.extract.extract_job import ExtractJob

SAMPLE_RECORDS = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "current_price": 110000
    }
]

def test_validate_records_success():
    """Test valid records."""

    job = ExtractJob()

    job.validate_records(SAMPLE_RECORDS)

def test_validate_records_none():
    """Test None records."""

    job = ExtractJob()

    with pytest.raises(ValueError, match="Extractor returned None."):
        job.validate_records(None)

def test_validate_records_empty():
    """Test empty records."""

    job = ExtractJob()

    with pytest.raises(ValueError, match="No records extracted."):
        job.validate_records([])

def test_validate_records_invalid_type():
    """Test invalid record type."""

    job = ExtractJob()

    with pytest.raises(TypeError):
        job.validate_records("invalid")

def test_validate_records_invalid_record():
    """Test non-dictionary record."""

    job = ExtractJob()

    with pytest.raises(TypeError):
        job.validate_records(["bitcoin"])

def test_save_local(tmp_path):
    """Test saving NDJSON locally."""

    output_file = tmp_path / "sample.ndjson"

    job = ExtractJob(output_path=str(output_file))

    job.save_local(SAMPLE_RECORDS)

    assert output_file.exists()

    content = output_file.read_text(encoding="utf-8")

    assert "bitcoin" in content

@patch("src.extract.extract_job.Config.RAW_DATASET_NAME", "crypto")
@patch("src.extract.extract_job.S3RawLoader.upload")
def test_save_s3(mock_upload):
    """Test upload to S3."""

    mock_upload.return_value = "raw_data/crypto/sample.ndjson"

    job = ExtractJob()

    s3_key = job.save_s3(SAMPLE_RECORDS)

    assert s3_key == "raw_data/crypto/sample.ndjson"

    mock_upload.assert_called_once()

@patch("src.extract.extract_job.ExtractJob.save_s3")
@patch("src.extract.extract_job.CoinGeckoExtractor.extract")
def test_run(mock_extract, mock_save_s3):
    """Test complete extract job."""

    mock_extract.return_value = SAMPLE_RECORDS
    mock_save_s3.return_value = "raw_data/crypto/sample.ndjson"

    job = ExtractJob()

    result = job.run()

    assert result["record_count"] == 1
    assert result["s3_key"] == "raw_data/crypto/sample.ndjson"
    assert "execution_time" in result

    mock_extract.assert_called_once()
    mock_save_s3.assert_called_once()