from datetime import datetime, timezone
import json
import boto3

from config.config import AWS_BUCKET, AWS_REGION
from src.utils.logger import get_logger,log_banner

logger = get_logger(__name__)

class S3RawLoader:
    """Upload raw API response to S3 raw layer."""
    def __init__(self):
        self.bucket = AWS_BUCKET
        self.s3 = boto3.client(
            "s3",
            region_name = AWS_REGION
        )

    def load_json(self, data, source_name="coingecko"):
        try:
            if not data:
                raise ValueError("Input data is empty.")
            
            now = datetime.now(timezone.utc)

            s3_key = (
                f"raw_data/{source_name}/"
                f"year={now:%Y}/"
                f"month={now:%m}/"
                f"day={now:%d}/"
                f"ingest_{now:%Y%m%d_%H%M%S}.json"
            )

            log_banner(logger,"UPLOAD S3 RAW DATA.")
            logger.info("Uploading raw file to s3.")

            self.s3.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType="application/json"
            )

            s3_path = f"s3://{self.bucket}/{s3_key}"

            logger.info(f"Raw upload successful: {s3_path}")

            return s3_path
        
        except:
            logger.exception("Raw upload failed.")
            raise
