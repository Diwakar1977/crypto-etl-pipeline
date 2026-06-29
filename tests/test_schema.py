import os
from pyspark.sql import SparkSession
from src.extract.coingecko_extractor import CoinGeckoExtractor
from src.transform.normalizer import normalize
from src.schemas.schema_manager import SchemaManager

os.environ["PYSPARK_PYTHON"] = "python"

spark = (
    SparkSession.builder
    .appName("crypto ETL schema")
    .getOrCreate()
)

# Extract
extractor = CoinGeckoExtractor()
data = extractor.extract()

# Transform (normalize)
data = normalize(data)

# Schema
schema_manger = SchemaManager()

try:
    schema = schema_manger.load_schema()
except:
    schema = schema_manger.create_and_save(data, spark)

# Spark Dataframe
df = spark.createDataFrame(data)

print(type(data))
print(df.printSchema())
print(df.show())
