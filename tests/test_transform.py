from src.utils.spark_session import get_spark

from src.extract.coingecko_extractor import CoinGeckoExtractor
from src.transform.normalizer import normalize
from src.schemas.schema_manager import SchemaManager
from src.transform.data_cleaner import DataCleaner
from src.transform.validator import DataValidator

spark = get_spark("Crypto ETL Test")

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
df = spark.createDataFrame(
    data,
    schema=schema
)

print("\nOriginal Schema")
df.printSchema()

print("\nOriginal Data")
df.show(5, truncate=False)

clean_df = DataCleaner.clean(df)

print("\nCleaned Data")
clean_df.show(5, truncate=False)

DataValidator.validate(clean_df)

print("\nData Validation Passed.")

print(f"\nFinal Record Count: {clean_df.count()}")


