"""
main.py

Production Crypto ETL Pipeline

Flow
-----
Extract
    ↓
Load RAW S3
    ↓
Normalize
    ↓
Validate
    ↓
Clean
    ↓
Load Processed S3
"""
from src.extract.coingecko_extractor import CoinGeckoExtractor
from src.transform.normalizer import normalize
from src.schemas.schema_manager import SchemaManager
from src.transform.validator import DataValidator
from src.transform.crypto_transformer import CryptoTransformer

from src.loads.s3_raw_loader import S3RawLoader
from src.loads.s3_processed_loader import S3ProcessedLoader

from src.utils.spark_session import crypto_spark_session
from src.utils.logger import get_logger,log_banner

logger = get_logger(__name__)

def main(test_mode=False):
    
    spark = None

    try:
        # Start pipeline
        log_banner(logger,"CRYPTO ETL PIPELINE STARTED")

        # create spark session
        logger.info("Creating spark session....")
        
        spark = crypto_spark_session()
        
        logger.info("Spark session created successfully.")

        # Extract
        log_banner(logger,"EXTRACT STAGE")

        extractor = CoinGeckoExtractor()

        data = extractor.extract()

        if not data:
            raise ValueError("No data received from coingecko API.")
        
        logger.info(f"Successfully extracted {len(data)} records.")

        # Save raw data
        log_banner(logger,"RAW LOAD STAGE")
        
        raw_loader = S3RawLoader()
        raw_loader.load_json(
            data=data
        )

        logger.info("Raw data saved successfully.")

        # Normalization
        log_banner(logger,"NORMALIZATION STAGE")

        df = normalize(
            spark=spark,
            data=data
        )

        record_count = df.count()

        logger.info(f"Normalization completed. Records: {record_count}")

        # Schema validation 
        log_banner(logger,"SCHEMA VALIDATION STAGE")

        if test_mode:
            logger.info("TEST MODE ENABLED")
            logger.info("Skipping schema validation.")
            
        else:
            schema_manager = SchemaManager()
            schema = schema_manager.load_schema()
            
            DataValidator.validate(
                df=df,
                expected_schema=schema
            )
            
            logger.info("Schema validation passed.")

        # Trasformation 
        log_banner(logger,"TRANSFORMATION STAGE")

        transformer = CryptoTransformer()
        processed_df = transformer.transform(df).cache()
        processed_count = processed_df.count()

        logger.info(f"Transformation completed. Records: {processed_count}")


        # Save processed dt
        log_banner(logger,"PROCESSED LOAD STAGE")

        processed_loader = S3ProcessedLoader()
        processed_loader.load_parquet(
            df=processed_df,
            dataset_name="crypto_market"
        )

        logger.info("Processed data saved successfully.")

        # Pipeline success
        log_banner(logger,"ETL PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.exception(f"ETL pipeline failed: {str(e)}")
        raise

    finally:
        if spark:
            logger.info("Stopping spark session.....")
            spark.stop()
            logger.info("Spark session stopped.")

if __name__ == "__main__":
    main() 







