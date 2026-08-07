from pathlib import Path

import boto3

from config.config import Config
from src.utils.path_builder import PathBuilder
from src.utils.logger import Logger

logger = Logger.get_logger("s3_processed_loader", "s3_processed_loader.log")


class S3ProcessedLoader:
    """Upload processed parquet files to S3 processed layer."""

    def __init__(self):
        """Initialize S3 client."""

        self.bucket = Config.S3_BUCKET
        self.region = Config.AWS_REGION

        self.s3_client = boto3.client(
            "s3",
            region_name=self.region
        )

    def validate_local_path(self, local_path: str):
        """Validate local parquet directory."""

        try:
            Logger.log_banner(logger, "VALIDATE LOCAL PARQUET PATH")

            path = Path(local_path)

            if not path.exists():
                raise FileNotFoundError(
                    f"Directory not found: {path}"
                )

            if not path.is_dir():
                raise NotADirectoryError(
                    f"Expected directory: {path}"
                )

            logger.info("Local parquet directory validated: %s", path)

            return path

        except Exception as e:
            logger.exception(f"Local path validation failed: {e}")
            raise

    def upload_directory(
        self,
        local_path: str,
        dataset_name: str
    ):
        """Upload processed parquet files to S3."""

        try:
            Logger.log_banner(logger, "UPLOAD PARQUET TO S3")

            local_directory = self.validate_local_path(
                local_path
            )

            s3_prefix = PathBuilder.processed_path(
                dataset_name
            )

            uploaded_files = 0

            for file in local_directory.rglob("*"):

                if not file.is_file():
                    continue

                # Skip Spark CRC files
                if file.suffix == ".crc":
                    logger.info(
                        "Skipping CRC file: %s",
                        file.name
                    )
                    continue

                relative_path = file.relative_to(
                    local_directory
                )

                s3_key = (
                    f"{s3_prefix}"
                    f"{relative_path.as_posix()}"
                )

                logger.info(
                    "Uploading %s -> s3://%s/%s",
                    file,
                    self.bucket,
                    s3_key
                )

                self.s3_client.upload_file(
                    Filename=str(file),
                    Bucket=self.bucket,
                    Key=s3_key
                )

                uploaded_files += 1

            logger.info("Total uploaded files: %d", uploaded_files)

            logger.info("Processed data uploaded successfully.")

            return (f"s3://{self.bucket}/{s3_prefix}")

        except Exception as e:
            logger.exception(f"Failed to upload processed data: {e}")
            raise