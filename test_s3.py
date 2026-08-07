from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("S3 Test")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    .getOrCreate()
)

df = spark.read.json(
    "s3a://crypto-etl-dev/raw_data/crypto_market/year=2026/month=08/day=06/run_time=135756.ndjson"
)

df.show(5)