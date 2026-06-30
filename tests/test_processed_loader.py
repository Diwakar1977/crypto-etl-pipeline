from unittest.mock import MagicMock

from src.loads.s3_processed_loader import S3ProcessedLoader

loader = S3ProcessedLoader()

mock_df = MagicMock()

mock_df.write.mode.return_value = mock_df.write

path = loader.load_parquet(mock_df)

mock_df.write.mode.assert_called_once_with("overwrite")
mock_df.write.parquet.assert_called_once()

print("Processed Loader Test Passed")