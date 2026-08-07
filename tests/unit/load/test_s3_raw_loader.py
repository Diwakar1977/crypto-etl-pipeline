from unittest.mock import patch

from src.load.s3_raw_loader import S3RawLoader

def test_upload_success():
    """Test successful upload to S3."""

    records = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "current_price": 110000
        }
    ]

    loader = S3RawLoader()

    with patch.object(loader.client, "put_object") as mock_put:
        loader.upload(records, dataset_name="crypto")

        mock_put.assert_called_once()