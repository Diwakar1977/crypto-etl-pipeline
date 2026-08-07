from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from src.utils.logger import Logger

logger = Logger.get_logger("data_validator", "validator.log")

class DataValidator:
    """Validate spark DataFrame beforetransfomation."""
    
    @staticmethod
    def validate_required_columns(df: DataFrame, required_columns: list[str]):
        """Vlidation all required columns exists."""

        try:
            logger.info("Validating required columns.")
        
            dataframe_columns = set(df.columns)
            
            missing_columns = list(
                set(required_columns) - dataframe_columns
            )

            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
        
            logger.info("Required columns validation passed.")

        except Exception as e:
            logger.info(f"Required column validation failed: {e}")
            raise

    @staticmethod
    def validate_duplicate_columns(df: DataFrame):
        """Validate duplicate columns."""
        
        try:
            logger.info("Checking duplicates columns.")

            seen = set()
            duplicates = []

            for column in df.columns:
                if column in seen:
                    duplicates.append(column)

                seen.add(column)

            if duplicates:
                raise ValueError(f"Duplicate column found: {duplicates}")
                
            logger.info("Duplicate column validation passed.")

        except Exception as e:
            logger.exception(f"Duplicate column vlidation failed: {e}")
            raise
    
    @staticmethod
    def validate_empty_dataframe(df: DataFrame):
        """Validate dataframe is not empty."""
        
        try:
            logger.info("Checking dataframe is empty.")

            if df.rdd.isEmpty():
                raise ValueError("Input dataframe is empty.")
            
            logger.info("Dataframe contains records.")
        
        except Exception as e:
            logger.exception(f"Empty dataframe validation failed: {e}")
            raise
    
    @staticmethod
    def validate_data_types(df: DataFrame, expected_schema: StructType):
        """Validate dataframe datatypes against expected schema."""
        
        try:
            logger.info("Validating dataframe datatypes.")
            
            actual_schema = {
                field.name: type(field.dataType)
                for field in df.schema.fields
            }

            for field in expected_schema.fields:
                
                expected_type = type(field.dataType)
                
                actual_type = actual_schema.get(field.name)
                
                if actual_type is None:
                    raise ValueError(f"Column '{field.name}' not found.")
                
                if actual_type != expected_type:
                    raise TypeError(
                        f"Datatype mismatch for '{field.name}'."
                        f"Expected={expected_type.__name__},"
                        f"Actual={actual_type.__name__}"
                    )
                
            logger.info("Datatype validation passed.")
        
        except Exception as e:
            logger.exception(f"Datatype validation failed: {e}")

    @staticmethod
    def validate_null_percentage(df: DataFrame, threshold: float = 50.0):
        """Validate null percentage for every column."""

        try:
            logger.info("Checking null Percentage.")
            
            total_rows = df.count()
            
            if total_rows == 0:
                logger.warning("Skpping null validation because dataframe is empty.")
                return
            
            for column in df.columns:
                
                null_rows = df.filter(
                    df[column].isNull()
                ).count()

                null_pct = (
                    null_rows / total_rows
                ) * 100

                logger.info("%s : %.2f%% Null", column, null_pct)

                if null_pct > threshold:
                    logger.warning("%s exceeded null threshold (%.2f%%)", column, null_pct)

            logger.info("Null validation completed.")

        except Exception as e:
            logger.exception(f"Null validation failed: {e}")
            raise

    @classmethod
    def validate(cls, df: DataFrame, required_columns: list[str], expected_schema: StructType, null_threshold: float = 50.0):
        """Run all validations."""

        try:

            Logger.log_banner(logger,"DATA VALIDATION STARTED")
            
            cls.validate_empty_dataframe(df)
            cls.validate_required_columns(df, required_columns)
            cls.validate_duplicate_columns(df)
            cls.validate_data_types(df, expected_schema)
            cls.validate_null_percentage(df, null_threshold)
            
            Logger.log_banner(logger, "DATA VALIDATION COMPLETED")

        except Exception as e:
            logger.exception(f"Validatin failed: {e}")
            raise

