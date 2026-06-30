import os
import sys
from pyspark.sql import SparkSession
from src.utils.logger import get_logger

logger = get_logger(__name__)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def crypto_spark_session(app_name: str = "Crypto ETL"):
    """Create and sparkssion."""
    
    spark = (
        SparkSession.builder
        .config("spark.local.dir", "C:/spark-temp")
        .config("spark.driver.memory", "4g")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .appName(app_name)
        .getOrCreate()
    )

    return spark

