import json
import boto3 

from config.config import Config
from botocore.exceptions import (ClientError, BotoCoreError)
from src.utils.path_builder import PathBuilder

from src.utils.logger import Logger

logger = Logger.get_logger("s3_raw_loader", "s3_raw_loader.log")

class S3RawLoader:
    """Upload raw response to s3 Raw layer"""

    def __init__(self, bucket: str = Config.S3_BUCKET, region: str = Config.AWS_REGION):
        """Initialize s3 client."""

        try:
            self.bucket = bucket
            self.region = region

            self.client = boto3.client(
                "s3",
                region_name=self.region
            )

            logger.info("S3Rawloader initialized successfully.")

        except Exception as e:
            logger.exception("Failed to initialize s3 client:", e)
            raise

    def validate_records(self, records: list):
        """Validate extracted records."""

        try:
            logger.info("Validating extracted records.")
            if records is None:
                raise ValueError("Records cannot be None.")
            
            if not isinstance(records, list):
                raise TypeError("Records must be a list.")
            
            if len(records) == 0:
                raise ValueError("Records list is empty.")
            
            logger.info("Total Records: %d", len(records))
            logger.info("Record validation completed.")

        except Exception as e:
            logger.exception("Record validation failed:", e)
            raise
    
    def upload(self, records: list, dataset_name: str = Config.RAW_DATASET_NAME):
        """
        Upload raw records to s3 as NDJSON.
        Returns:
        -------
        Uploaded s3 object key.
        """

        try:
            Logger.log_banner(logger, "RAW S3 UPLOAD STARTED")

            self.validate_records(records)
            
            s3_key = PathBuilder.raw_path(dataset_name)

            logger.info("Generated s3 key: %s", s3_key)
            logger.info("Converting records to NDJSON format.")

            ndjson_data = "\n".join(
                json.dumps(
                    record,
                    default=str
                )
                for record in records
            )

            logger.info("Uploading raw data to s3.")

            self.client.put_object(
                Bucket=self.bucket,
                Key=s3_key,
                Body=ndjson_data.encode("utf-8"),
                ContentType="application/x-ndjson"
            )

            logger.info("Upload completed successfully.")
            logger.info("Bucket: %s", self.bucket)
            logger.info("S3_key: %s", s3_key)
            logger.info("Uploaded records: %d", len(records))
            Logger.log_banner(logger, "RAW S3 UPLOAD COMPLETED")

            return s3_key
        
        except (ClientError, BotoCoreError) as e:
            logger.exception("AWS S3 upload failed:", e)
            raise

        except Exception as e:
            logger.exception("Unexpected upload error:", e)
            raise



