import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    """Application configration."""

    # Environment
    ENV = os.getenv("ENV", "local")

    # Spark
    SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
    SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    SPARK_EXECUTOR_MEMORY = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    SPARK_SHUFFLE_PARTITIONS = os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")
    SPARK_TIMEZONE = os.getenv("SPARK_TIMEZONE", "UTC")

    # Hadoop / S3
    HADOOP_AWS_PACKAGE = os.getenv(
        "HADOOP_AWS_PACKAGE",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # Application
    APP_NAME = "CryptoFlow-ETL"

    # AWS
    AWS_REGION = os.getenv("AWS_REGION")

    # SNS
    SNS_TOPIC_ARN = os.getenv("SNS_TOPIC_ARN")

    # Coingecko API
    COINGECKO_BASE_URL = os.getenv("COINGECKO_BASE_URL")
    
    # Data Paths
    S3_BUCKET = os.getenv("S3_BUCKET")
    RAW_DATA_PATH = os.getenv("RAW_DATA_PATH")
    PROCESSED_DATA_PATH = os.getenv("PROCESSED_DATA_PATH")

    # Pipeline
    PIPELINE_NAME = "Crypto ETL Pipeline"
    SOURCE_NAME = "CoinGecko API"
    TARGET_TABLE = "crypto_market"
    RAW_DATASET_NAME = "crypto_market"

    # Redshift
    REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
    REDSHIFT_PORT = int(os.getenv("REDSHIFT_PORT", 5439))
    REDSHIFT_DATABASE = os.getenv("REDSHIFT_DATABASE")
    REDSHIFT_USER = os.getenv("REDSHIFT_USER")
    REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
    REDSHIFT_SCHEMA = os.getenv("REDSHIFT_SCHEMA")
    REDSHIFT_TABLE = os.getenv("REDSHIFT_TABLE")
    REDSHIFT_IAM_ROLE = os.getenv("REDSHIFT_IAM_ROLE")

    # Full S3 Paths
    RAW_PATH = f"s3a://{S3_BUCKET}/{RAW_DATA_PATH}/"
    PROCESSED_PATH = f"s3a://{S3_BUCKET}/{PROCESSED_DATA_PATH}/"

    # Schema file
    SCHEMA_FILE = "schemas/crypto_schema.json"