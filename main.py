from time import perf_counter

from config.config import Config

from src.extract.extract_job import ExtractJob
from src.transform.transform_job import TransformJob
from src.load.load_job import LoadJob

from src.utils.spark_session import SparkSessionManager
from src.utils.logger import Logger

from src.notifications.sns_notifier import SNSNotifier
from src.utils.email_template import EmailTemplate

logger = Logger.get_logger("crypto_etl_pipeline", "pipeline.log")

class CryptoETLPipeline:
    """Execute complete Crypto ETL Pipeline."""

    def __init__(self):
        self.spark = None

    # Notification Helpers
    def notify_success(self):
        """Send success notification."""

        subject, message = EmailTemplate.sns_success(
            job_name="Crypto ETL Pipeline"
        )

        SNSNotifier().publish(
            subject=subject,
            message=message
        )

    def notify_failure(self, stage: str, error: Exception):
        """Send failure notification."""

        subject, message = EmailTemplate.sns_failure(
            job_name="Crypto ETL Pipeline",
            stage=stage,
            error=str(error)
        )

        SNSNotifier().publish(
            subject=subject,
            message=message
        )

    # Spark
    def create_spark(self):

        try:
            Logger.log_banner(logger, "CREATE SPARK SESSION")

            self.spark = SparkSessionManager.get_session()

            logger.info("Spark session created successfully.")

        except Exception as e:
            logger.exception("Spark creation failed: %s", e)
            self.notify_failure("Spark Session", e)
            raise

    # Extract
    def extract(self):

        try:
            Logger.log_banner(logger, "EXTRACT STAGE")

            result = ExtractJob().run()

            logger.info("Extract completed successfully.")

            return result

        except Exception as e:
            logger.exception("Extract stage failed: %s", e)
            self.notify_failure("Extract", e)
            raise

    # Transform
    def transform(self, raw_s3_path):

        try:
            Logger.log_banner(logger, "TRANSFORM STAGE")

            result = TransformJob(
                spark=self.spark,
                schema_file=Config.SCHEMA_FILE,
                input_path=raw_s3_path,
                output_path=Config.PROCESSED_PATH
            ).run()

            logger.info("Transform completed successfully.")

            return result

        except Exception as e:
            logger.exception("Transform stage failed: %s", e)
            self.notify_failure("Transform", e)
            raise

    # Load
    def load(self, s3_processed_path, schema):

        try:
            Logger.log_banner(logger, "LOAD STAGE")

            result = LoadJob(
                s3_processed_path=s3_processed_path,
                schema=schema
            ).run()

            logger.info("Load completed successfully.")

            return result

        except Exception as e:
            logger.exception("Load stage failed: %s", e)
            self.notify_failure("Load", e)
            raise

    # Stop Spark
    def stop(self):

        if self.spark is not None:

            Logger.log_banner(logger, "STOP SPARK SESSION")

            SparkSessionManager.stop_session()

            self.spark = None

            logger.info("Spark session stopped successfully.")

    # Pipeline
    def run(self):

        start_time = perf_counter()

        try:

            Logger.log_banner(logger, "CRYPTO ETL PIPELINE STARTED")

            # Stage 1
            self.create_spark()

            # Stage 2
            extract_result = self.extract()

            raw_s3_path = (
                f"{Config.RAW_PATH}"
                f"{extract_result['s3_key'].replace('raw_data/', '')}"
            )

            logger.info("Raw S3 Path : %s", raw_s3_path)

            # Stage 3
            transform_result = self.transform(raw_s3_path)

            # Stage 4
            load_result = self.load(
                transform_result["s3_processed_path"],
                transform_result["schema"]
            )

            execution_time = round(
                perf_counter() - start_time,
                2
            )

            self.notify_success()

            logger.info(
                "Pipeline completed successfully in %.2f seconds.",
                execution_time
            )

            Logger.log_banner(logger, "CRYPTO ETL PIPELINE COMPLETED")

            return {
                "extract": extract_result,
                "transform": transform_result,
                "load": load_result,
                "execution_time": execution_time
            }

        finally:
            self.stop()

if __name__ == "__main__":
    CryptoETLPipeline().run()