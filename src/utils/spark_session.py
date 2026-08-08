from pyspark.sql import SparkSession
import logging

from config.config import Config
from src.utils.logger import Logger

logger = Logger.get_logger("spark_session", "spark_session.log")

py4j_logger = logging.getLogger("py4j")
py4j_logger.setLevel(logging.WARNING)
py4j_logger.propagate = False

class SparkSessionManager:
    """Creates and manages a reusable SparkSession."""

    _spark = None

    @classmethod
    def get_session(cls):
        """Create or return an existing SparkSession."""

        try:
            if cls._spark is not None:
                return cls._spark

            Logger.log_banner(logger, "Creating Spark Session")

            builder = (
                SparkSession.builder
                .appName(Config.APP_NAME)
                .config("spark.sql.session.timeZone", Config.SPARK_TIMEZONE)
                .config(
                    "spark.sql.shuffle.partitions",
                    Config.SPARK_SHUFFLE_PARTITIONS
                )
                .config(
                    "spark.driver.memory",
                    Config.SPARK_DRIVER_MEMORY
                )
                .config(
                    "spark.executor.memory",
                    Config.SPARK_EXECUTOR_MEMORY
                )
                .config(
                    "spark.serializer",
                    "org.apache.spark.serializer.KryoSerializer"
                )
                .config(
                    "spark.sql.adaptive.enabled",
                    "true"
                )
                .config(
                    "spark.sql.execution.arrow.pyspark.enabled",
                    "true"
                )

                # Hadoop AWS Packages
                .config(
                    "spark.jars.packages",
                    Config.HADOOP_AWS_PACKAGE
                )

                # S3A Configuration
                .config(
                    "spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem"
                )
                .config(
                    "spark.hadoop.fs.s3a.endpoint",
                    f"s3.{Config.AWS_REGION}.amazonaws.com"
                )
            )

            # Local Environment
            if Config.ENV.lower() == "local":

                logger.info("Running Spark in LOCAL mode.")

                builder = (
                    builder
                    .master(Config.SPARK_MASTER)
                    .config(
                        "spark.hadoop.fs.s3a.aws.credentials.provider",
                        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
                    )
                )

            # Production Environment
            elif Config.ENV.lower() == "production":

                logger.info("Running Spark in PRODUCTION mode.")

                builder = builder.config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider"
                )

            cls._spark = builder.getOrCreate()

            cls._spark.sparkContext.setLogLevel("WARN")

            logger.info("Spark Session created successfully.")

            return cls._spark

        except Exception:
            logger.exception("Failed to create Spark Session.")
            raise

    @classmethod
    def stop_session(cls):
        """Stop the SparkSession."""

        spark = cls._spark

        if spark is None:
            return

        # Clear reference first
        cls._spark = None

        try:
            logger.info("Stopping Spark Session.")

            spark.stop()

            logger.info("Spark Session stopped successfully.")

        except Exception:
            logger.exception("Failed to stop Spark Session.")
            raise