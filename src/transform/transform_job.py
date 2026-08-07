from time import perf_counter
from config.config import Config
from src.utils.path_builder import PathBuilder

from pyspark.sql import SparkSession, DataFrame

from src.schemas.schema_manager import SchemaManager
from src.load.parquet_writer import ParquetWriter
from src.transform.validator import DataValidator
from src.transform.normalizer import Normalizer
from src.transform.crypto_transform import CryptoTransformer
from src.utils.logger import Logger

logger = Logger.get_logger("transform_job", "transform_job.log")

class TransformJob:
    """Execute complete Spark transformation pipeline."""

    def __init__(
        self,
        spark: SparkSession,
        schema_file: str,
        input_path: str,
        output_path: str
    ):
        """Initialize transformation job."""

        self.spark = spark
        self.schema_file = schema_file
        self.input_path = input_path
        self.output_path = output_path

        self.schema = None

    def read_raw_data(self) -> DataFrame:
        """Read raw NDJSON data from S3."""

        try:
            Logger.log_banner(logger, "READ RAW DATA")

            logger.info("Loading Spark schema.")

            self.schema = (
                SchemaManager(
                    self.schema_file
                ).build_schema()
            )

            logger.info("Schema Columns : %d", len(self.schema.fields))
            logger.info("Reading raw dataset from: %s", self.input_path)

            df = (
                self.spark.read
                .schema(self.schema)
                .json(self.input_path)
            )

            logger.info("Rows Loaded: %d", df.count())

            logger.info("Columns Loaded: %d", len(df.columns))

            logger.info("Raw dataset loaded successfully.")

            return df

        except Exception as e:
            logger.exception("Failed to read raw dataset: %s", e)
            raise

    def validate(self, df: DataFrame) -> DataFrame:
        """Validate raw dataframe."""

        try:
            Logger.log_banner(logger, "DATA VALIDATION")

            logger.info("Running dataframe validation.")

            required_columns = [
                field.name
                for field in self.schema.fields
            ]

            DataValidator.validate(
                df=df,
                required_columns=required_columns,
                expected_schema=self.schema,
                null_threshold=50.0
            )

            logger.info("Validation completed successfully.")

            return df

        except Exception as e:
            logger.exception("Validation failed: %s", e)
            raise

    def normalize(self, df: DataFrame) -> tuple[DataFrame, int]:
        """Normalize dataframe."""

        try:
            Logger.log_banner(logger, "NORMALIZATION")

            logger.info("Running dataframe normalization.")

            df, duplicate_count = (
                Normalizer.normalize(
                    df=df,
                    expected_schema=self.schema,
                    duplicate_subset=["id"]
                )
            )

            logger.info("Normalization completed successfully.")

            return df, duplicate_count

        except Exception as e:
            logger.exception("Normalization failed: %s", e)
            raise

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply business transformations."""

        try:
            Logger.log_banner(logger, "BUSINESS TRANSFORMATION")

            logger.info("Applying business transformation.")

            df = CryptoTransformer.transform(df)

            logger.info("Business transformation completed successfully.")

            return df

        except Exception as e:
            logger.exception("Transformation failed: %s", e)
            raise

    def write_parquet(self, df: DataFrame, mode: str = "overwrite") -> str:
        """Write transformed dataframe to partitioned parquet."""

        try:
            Logger.log_banner(logger, "WRITE PROCESSED PARQUET")
            
            processed_path = (
                f"{self.output_path}"
                f"{PathBuilder.processed_path(Config.RAW_DATASET_NAME)}"
            )
            logger.info("Processed Output Path: %s", processed_path)

            writer = ParquetWriter(
                output_path=processed_path
            )

            logger.info("Processed parquet written successfully.")

            return writer.write(
                df=df,
                mode=mode,
                compression="snappy"
            )

        except Exception as e:
            logger.exception("Failed to write parquet: %s", e)
            raise

    def run(self) -> dict:
        """Execute complete transformation pipeline."""

        start_time = perf_counter()

        try:
            Logger.log_banner(logger, "TRANSFORM JOB STARTED")

            # Read Raw Data
            df = self.read_raw_data()

            # Validate
            df = self.validate(df)

            # Normalize
            df, rejected = self.normalize(df)

            # Business Transformation
            df = self.transform(df)
            
            logger.info("Processed DataFrame Columns : %d", len(df.columns))

            # Build processed schema from transformed dataframe
            processed_schema = (
                SchemaManager.build_schema_from_dataframe(df)
            )

            logger.info("Processed Schema Columns : %d", len(processed_schema.fields))
            
            df = df.cache()

            # Row Count
            transformed = df.count()

            # Write Processed Parquet
            processed_path = self.write_parquet(df)

            duration = round(perf_counter() - start_time, 2)

            logger.info("Processed Rows: %d", transformed)
            logger.info("Rejected Rows: %d", rejected)
            logger.info("Final Columns: %d", len(df.columns))
            logger.info("Processed S3 Path: %s", processed_path)
            logger.info("Execution Time: %.2f seconds", duration)
            Logger.log_banner(logger, "TRANSFORM JOB COMPLETED")

            return {
                "processed_rows": transformed,
                "rejected_rows": rejected,
                "s3_processed_path": processed_path,
                "schema": processed_schema,
                "execution_time": duration
            }

        except Exception as e:
            logger.exception("Transform job failed: %s", e)
            raise