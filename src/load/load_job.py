from time import perf_counter

from src.load.redshift_loader import RedshiftLoader
from src.utils.logger import Logger

logger = Logger.get_logger("load_job", "load_job.log")

class LoadJob:
    """Execute complete Redshift loading pipeline."""

    def __init__(
        self,
        s3_processed_path: str,
        schema
    ):
        """Initialize Load Job."""

        self.s3_processed_path = s3_processed_path
        self.schema = schema

        self.loader = RedshiftLoader()

    def connect(self):
        """Connect to Amazon Redshift."""

        try:
            Logger.log_banner(logger, "CONNECT TO REDSHIFT")

            logger.info("Connecting to Amazon Redshift.")

            self.loader.connect()

            logger.info("Connected successfully.")

        except Exception as e:
            logger.exception("Redshift connection failed: %s", e)
            raise

    def copy_to_redshift(self):
        """Load processed parquet from S3 into Redshift."""

        try:
            Logger.log_banner(logger, "COPY DATA TO REDSHIFT")

            self.loader.copy_from_s3(
                self.s3_processed_path
            )

            logger.info("Processed S3 Path : %s", self.s3_processed_path)

            logger.info("Data loaded into Redshift successfully.")

        except Exception as e:
            logger.exception("COPY command failed: %s", e)
            raise

    def validate_load(self):
        """Validate loaded records."""

        try:
            Logger.log_banner(logger, "VALIDATE REDSHIFT LOAD")

            logger.info("Validating loaded records.")

            loaded_rows = (
                self.loader.validate_load()
            )

            logger.info("Loaded Rows : %d", loaded_rows)

            return loaded_rows

        except Exception as e:
            logger.exception("Load validation failed: %s", e)
            raise

    def close(self):
        """Close Redshift resources."""

        try:
            Logger.log_banner(logger, "CLOSE REDSHIFT CONNECTION")

            self.loader.close()

            logger.info("Redshift resources closed successfully.")

        except Exception as e:
            logger.exception("Failed closing Redshift resources: %s", e)
            raise

    def run(self):
        """Execute complete Load Job."""

        start_time = perf_counter()

        try:
            Logger.log_banner(logger, "LOAD JOB STARTED")

            # Connect
            self.connect()

            # Create table from transformed dataframe schema
            self.loader.create_table(
                self.schema
            )

            logger.info("Redshift table schema is ready.")

            # Copy Data
            self.copy_to_redshift()

            # Validate
            loaded_rows = self.validate_load()

            duration = round(
                perf_counter() - start_time,
                2
            )

            logger.info("Execution Time : %.2f seconds", duration)

            Logger.log_banner(logger, "LOAD JOB COMPLETED")

            return {
                "loaded_rows": loaded_rows,
                "redshift_table": (
                    f"{self.loader.schema}."
                    f"{self.loader.table}"
                ),
                "s3_processed_path": self.s3_processed_path,
                "execution_time": duration
            }

        except Exception as e:
            logger.exception("Load Job failed: %s", e)
            raise

        finally:
            self.close()