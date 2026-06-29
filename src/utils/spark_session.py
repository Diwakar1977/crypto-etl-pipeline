import os
import sys
from pyspark.sql import SparkSession
from src.utils.logger import get_logger,log_banner

logger = get_logger(__name__)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def get_spark(app_name: str = "Crypto ETL"):
    """Create and sparkssion."""

    log_banner(logger,"SPARK SESSION")
    logger.info("Creating Spark Session...")
    
    spark = (
        SparkSession.builder
        .config("spark.local.dir", "C:/spark-temp")
        .config("spark.driver.memory", "4g")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .appName(app_name)
        .getOrCreate()
    )

    logger.info("Spark Session created successfully.")

    return spark
