from src.utils.spark_session import SparkSessionManager


def test_create_spark_session():
    spark = SparkSessionManager.get_session()

    assert spark is not None
    assert spark.sparkContext.appName == "CryptoFlow-ETL"

    SparkSessionManager.stop_session()