from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils.logger import get_logger

logger = get_logger(__name__)

class DataValidator:
    """
    Perform data quality validation on the input dataframe.
    """

    ID_COLUMN = "id"
    PRICE_COLUMN = "current_price"
    MRKET_CAP_COLUMN = "market_cap"

    @staticmethod
    def validate_null_ids(df:DataFrame):
        """Validate that ID columns contains no null values."""

        if DataValidator.ID_COLUMN not in df.columns:
            raise ValueError(f"Missing required columns: {DataValidator.ID_COLUMN}")
        
        null_count = df.filter(
            col(DataValidator.ID_COLUMN).isNull()
        ).count()

        if null_count:
            logger.error("Found % null IDs.",null_count)
            raise ValueError(f"Found {null_count} null IDs.")
        
    @staticmethod
    def valid_duplicate_ids(df: DataFrame):
        """Valid duplicate IDs."""

        duplicate_count = (
            df.groupBy(DataValidator.ID_COLUMN)
            .count()
            .filter(col("count") > 1)
            .count()
        )

        if duplicate_count:
            logger.error("Found % duplicte IDs.",duplicate_count)
            raise ValueError(f"Found {duplicate_count} duplicate IDs.")
        
        logger.info("Duplicte ID validation passed.")
        
    @staticmethod
    def validate_negative_prices(df: DataFrame):
        """Validate negative prices."""
        
        if DataValidator.PRICE_COLUMN not in df.columns:
            logger.warning("%s column not found.", DataValidator.PRICE_COLUMN)
            return
        
        invalid_count = (
            df.filter(col(DataValidator.PRICE_COLUMN) < 0)
            .count()
        )

        if invalid_count:
            logger.error("Founds % negative prices", invalid_count)
            raise  ValueError(f"Found {invalid_count} negative prices.")
        
        logger.info("Price validation passed.")

    @staticmethod
    def valid_market_cap(df: DataFrame):
        """Value negative market cap"""

        if DataValidator.MRKET_CAP_COLUMN not in df.columns:
            logger.warning("% colume not found.", DataValidator.MRKET_CAP_COLUMN)
            return
        
        invalid_count = (
            df.filter(col(DataValidator.MRKET_CAP_COLUMN) < 0)
            .count()
        )

        if invalid_count:
            logger.error("Founds % invalid market cap values.", invalid_count)
            raise ValueError(f"Found {invalid_count} invalid market cap values.")
        
        logger.info("Market cap validation passed.")

    @classmethod
    def validate(cls, df:DataFrame):
        """Execute all validations."""

        logger.info("=" * 80)
        logger.info("Starting data validation")
        logger.info("=" * 80)

        try:
            cls.validate_null_ids(df)
            cls.valid_duplicate_ids(df)
            cls.validate_negative_prices(df)
            cls.valid_market_cap(df)

            logger.info("All data quality validations passed.")

        except Exception:
            logger.exception("Data validation failed.")
            raise


 
 
 
