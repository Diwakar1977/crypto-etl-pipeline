import json
from pathlib import Path
from typing import List, Dict, Any
from time import perf_counter
from typing import Optional

from config.config import Config
from src.extract.crypto_extractor import CoinGeckoExtractor
from src.load.s3_raw_loader import S3RawLoader
from src.utils.logger import Logger

logger = Logger.get_logger("extract_job", "extract_job.log")

class ExtractJob:
    """Execute extract pipeline."""

    def __init__(self, output_path: Optional[str] = None):
        """Initialize extract job."""

        self.output_path = output_path
        self.extractor = CoinGeckoExtractor()
        self.s3_loader = S3RawLoader()

    def validate_records(self, records: List[Dict[str, Any]]):
        """Validate extracted records."""

        try:
            logger.info("Validating extracted records.")

            if records is None:
                raise ValueError("Extractor returned None.")

            if not isinstance(records, list):
                raise TypeError("Extractor output must be a list.")

            if len(records) == 0:
                raise ValueError("No records extracted.")

            for index, record in enumerate(records):
                if not isinstance(record, dict):
                    raise TypeError(
                        f"Record {index} is not a JSON object."
                    )

            logger.info(
                "Validation completed successfully. Total records: %d",
                len(records)
            )

        except Exception as e:
            logger.exception(f"Validation failed: {e}")
            raise

    def save_local(self, records: List[Dict[str, Any]]):
        """Save extracted records locally as NDJSON."""

        try:
            Logger.log_banner(logger, "SAVE RAW DATA LOCALLY")

            path = Path(self.output_path)
            path.parent.mkdir(parents=True, exist_ok=True)

            logger.info("Saving raw data: %s", path)

            with path.open("w", encoding="utf-8") as file:
                for record in records:
                    file.write(json.dumps(record))
                    file.write("\n")

            logger.info("Local NDJSON saved successfully.")
            logger.info("Saved records: %d", len(records))

        except Exception as e:
            logger.exception(f"Local save failed: {e}")
            raise

    def save_s3(self, records: List[Dict[str, Any]]):
        """Upload raw records to S3."""

        try:
            Logger.log_banner(logger, "UPLOAD RAW DATA TO S3")

            s3_key = self.s3_loader.upload(
                records=records,
                dataset_name=Config.RAW_DATASET_NAME
            )

            logger.info("Raw data uploaded successfully.")
            logger.info("S3 Key: %s", s3_key)

            return s3_key

        except Exception as e:
            logger.exception(f"S3 upload failed: {e}")
            raise

    def run(self):
        """Execute complete extract pipeline."""

        start_time = perf_counter()

        try:
            Logger.log_banner(logger, "EXTRACT JOB STARTED")

            logger.info("Extracting data from CoinGecko API.")

            # Extract records
            records = self.extractor.extract()

            # Validate records
            self.validate_records(records)

            # Save locally
            if self.output_path:
                self.save_local(records)

            # Upload raw data to S3
            s3_key = self.save_s3(records)

            # Calculate execution time
            duration = round(perf_counter() - start_time, 2)

            Logger.log_banner(logger, "EXTRACT JOB COMPLETED")

            logger.info("Extract job completed successfully.")
            logger.info("Extracted records: %d", len(records))
            logger.info("Raw S3 Key: %s", s3_key)
            logger.info("Execution Time: %.2f seconds", duration)

            return {
                "record_count": len(records),
                "s3_key": s3_key,
                "execution_time": duration
            }

        except Exception as e:
            logger.exception("Extract job failed: %s", e)
            raise