from pyspark.sql import DataFrame
from pyspark.sql.functions import (col,trim)
from pyspark.sql.types import (StructType,StringType)
from src.utils.logger import Logger

logger = Logger.get_logger("normalizer", "normalizer.log")

class Normalizer:
    """Normalize spark dataframe before transformation."""

    @staticmethod
    def trim_strings(df: DataFrame):
        """Trim whitespace from all string columns."""

        try:
            logger.info("Trimming string columns...")

            for field in df.schema.fields:
                if isinstance(field.dataType, StringType):
                    df = df.withColumn(
                        field.name,
                        trim(
                            col(field.name)
                        )
                    )
            
            logger.info("String trimming completed.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to trim string columns: {e}")
            raise

    @staticmethod
    def cast_columns(df: DataFrame, expected_schema: StructType):
        """Cast dataframe columns according to expected columns."""
        try:
            logger.info("Casting dataframe columns...")

            for field in expected_schema.fields:
                if field.name not in df.columns:
                    logger.warning("Column %s not found. Skipping cast.", field.name)
                    continue
            
                df = df.withColumn(
                    field.name,
                    col(
                        field.name
                    ).cast(
                        field.dataType
                    )
                )
            
            logger.info("Column casting completed.")

            return df
        
        except Exception as e:
            logger.exception(f"Column casting failed: {e}")
            raise
    
    @staticmethod
    def convert_timestamps(df: DataFrame, expected_schema: StructType):
        """Convert string timestamp columns to spark timestampType."""

        try:
            logger.info("Converting timestamp columns...")

            for field in expected_schema.fields:
                if field.dataType.typeName() == "timestamp":
                    if field.name not in df.columns:
                        logger.warning("Column '%s' not found. skipping timestamp conversion.", field.name) 
                        continue
                    
                    df = df.withColumn(
                        field.name,
                        col(
                            field.name
                        ).cast(
                            field.dataType
                        )
                    )

            logger.info("Timestamp conversion completed.")
            
            return df
        
        except Exception as e:
            logger.exception(f"Timestmap conversion failed: {e}")
            raise

    @staticmethod
    def remove_duplicates(df: DataFrame, subset: list[str]):
        """Remove duplicate rows."""

        try:
            logger.info("Removing duplicates rows...")

            before_count = df.count()
            
            if subset:
                df = df.dropDuplicates(subset)
            
            else:
                df = df.dropDuplicates()
            
            after_count = df.count()

            duplicate_count = before_count - after_count
            
            logger.info("Removed %d duplicate rows.", duplicate_count)

            return df,duplicate_count
        
        except Exception as e:
            logger.exception(f"Duplicates removal failed: {e}")
            raise
    
    @classmethod
    def normalize(cls, df: DataFrame, expected_schema: StructType, duplicate_subset: list[str] | None = None):
        """Execute complete normalization pipeline."""

        try:
            Logger.log_banner(logger, "DATA NORMALIZATION STARTED")
            
            df = cls.trim_strings(df)
            df = cls.cast_columns(df, expected_schema)
            df = cls.convert_timestamps(df,expected_schema)
            df, duplicate_count = cls.remove_duplicates(
                df,
                duplicate_subset
            )

            Logger.log_banner(logger, "DATA NORMALIZATION COMPLETED")
            
            return df, duplicate_count
        
        except Exception as e:
            logger.exception(f"Normalization failed: {e}")
            raise