from datetime import datetime, timezone

from config.config import AWS_BUCKET
from src.utils.logger import get_logger,log_banner

logger = get_logger(__name__)

class S3ProcessedLoader:
    """
    Save transformed spark dataframe
    into processed layer.
    """

    def __init__(self):
        self.bucket = AWS_BUCKET

    def load_parquet(self, df, dataset_name="crypto_market"):
        try:
            if df is None:
                raise ValueError("Dataframe is None.")
            
            now = datetime.now(timezone.utc)

            s3_path = (
                f"s3://{self.bucket}/processed_data/"
                f"{dataset_name}/"
                f"year={now:%Y}/"
                f"month={now:%m}/"
                f"day={now:%d}/"
            )

            log_banner(logger, "PROCESSED S3 RAW TO PARQUET")
            logger.info(f"Writing processed parquet to {s3_path}.")

            df.write.mode("overwrite").parquet(s3_path)

            logger.info("Processed parquet saved successfully.")
            
            return s3_path
        
        except Exception:
            logger.exception("Failed to save processed parquet.")
            raise