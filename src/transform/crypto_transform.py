from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    round,
    current_timestamp,
    current_date,
    datediff,
    expr,
    when
)
from src.utils.logger import Logger

logger = Logger.get_logger("crypto_transformer", "crypto_transform.log")

class CryptoTransformer:

    @staticmethod
    def drop_columns(df: DataFrame, columns: list[str]):
        """Drop unwanted columns."""

        try:
            logger.info("Dropping columns...")

            existing_columns = [
                column for column in columns if column in df.columns
            ]

            if existing_columns:
                df = df.drop(*existing_columns)

                logger.info("Dropped columns: %s", existing_columns)

            else:
                logger.info("No columns dropped.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to drop columns: {e}")
            raise

    @staticmethod
    def round_numeric_columns(df: DataFrame, columns: list[str], decimal_prices: int = 2):
        "Round numeric columns."

        try:
            logger.info("Rounding numeric columns...")

            for column in columns:
                if column not in df.columns:
                    logger.warning("Column '%s' not found.", column)
                    continue

                df = df.withColumn(
                    column,
                    round(
                        col(column),
                        decimal_prices
                    )
                )
            logger.info("Numeric rounding completed.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to round numeric columns: {e}")
            raise

    @staticmethod
    def add_ingest_timestamp(df: DataFrame):
        """Add ingestion timestamp."""

        try:
            logger.info("Adding ingest timestamp.")   
            
            df = df.withColumn(
                "ingest_timestamp",
                current_timestamp()
            )

            logger.info("Ingest timestamp added.")
            
            return df
        
        except Exception as e:
            logger.exception(f"Failed to add ingest timestamp: {e}")
            raise

    @staticmethod
    def add_days_since_ath(df: DataFrame):
        """Calculate days since ATH."""

        try:
            logger.info("Calculating days_since_ath.")

            df = df.withColumn(
                "days_since_ath",
                datediff(
                    current_date(),
                    col("ath_date")
                )
            )

            logger.info("days_since_ath created.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to crate days_since_ath: {e}")
            raise
    
    @staticmethod
    def add_days_since_atl(df: DataFrame):
        """Calculated days since ATL."""

        try:
            logger.info("Calculating days_since_atl.")

            df = df.withColumn(
                "days_since_atl",
                datediff(
                    current_date(),
                    col("atl_date")
                )
            )

            logger.info("days_since_atl created.")
            
            return df

        except Exception as e:
            logger.exception(f"Failed to create days_since_atl: {e}")
            raise

    @staticmethod
    def add_daily_volatility(df: DataFrame):
        """Calculate daily volatility percentage."""
        
        try:
            logger.info("Calculating daily volatility.")

            df = df.withColumn(
                "daily_volatility_percentage",
                round(
                    expr("try_divide(high_24h - low_24h, current_price)"),
                    2
                )
            )
            
            logger.info("daily_volatility_percentage created")

            return df

        except Exception as e:
            logger.exception(f"Falied to calculate volatility: {e}")
            raise

    @staticmethod
    def add_distance_from_ath(df: DataFrame):
        """Calculate distance from ATH."""
        
        try:
            logger.info("Calculating distance_from_ath.")

            df = df.withColumn(
                "distance_from_ath",
                round(
                    expr("try_divide(ath - current_price, ath)"),
                    2
                )
            )
            
            logger.info("distance_from_ath created.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to create distance_from_ath: {e}")
            raise

    @staticmethod
    def add_distance_from_atl(df: DataFrame):
        """Calculate distance from ATL."""

        try:
            logger.info("Calculating distance_from_atl.")

            df = df.withColumn(
                "distance_from_atl",
                round(
                    expr("try_divide(current_price - atl, atl)"),
                    2
                )
            )

            logger.info("distance_from_atl created.")

            return df

        except Exception as e:
            logger.exception(f"Failed to create distance_from_atl: {e}")
            raise

    @staticmethod
    def add_volume_market_cap_ratio(df: DataFrame):
        """Calculated volume to market cap ration."""

        try:
            logger.info("Calculating volume_market_cap_ratio.")

            df = df.withColumn(
                "volume_market_cap_ratio",
                round(
                    expr("try_divide(total_volume, market_cap)"),
                    4
                )
            )
            
            logger.info("volume_market_cap_ratio created.")

            return df

        except Exception as e:
            logger.exception(f"Failed to create volume_market_cap_ratio: {e}")
            raise

    @staticmethod
    def add_supply_utilization(df: DataFrame):
        """Calculate supply utilization percentage."""

        try:
            logger.info("Calculating supply_utilization_pct.")

            df = df.withColumn(
                "supply_utilization_pct",
                round(
                    expr("try_divide(circulating_supply, max_supply)"),
                    2
                )
            )

            logger.info("supply_utilization_pct created.")

            return df

        except Exception as e:
            logger.exception(f"Failed to create supply_utilization: {e}")
            raise

    @staticmethod
    def add_price_direction(df: DataFrame):
        """Add price direction"""

        try:
            logger.info("Creating price_direction.")

            df = df.withColumn(
                "price_direction",
                when(
                    col("price_change_24h") > 0, "UP"
                ).when(
                    col("price_change_24h") < 0, "DOWN"
                ).otherwise("FLAT")
            )

            logger.info("price_direction created.")

            return df
        
        except Exception as e:
            logger.exception(f"Failed to create price_direction: {e}")
            raise

    @classmethod
    def transform(cls, df: DataFrame):
        """Execute complete business transformation."""

        try:
            Logger.log_banner(logger, "CRYPTO TRANSFORMATION STARTED")
            
            df = cls.drop_columns(df, ["roi"])
            df = cls.round_numeric_columns(
                df, [
                    "current_price",
                    "high_24h",
                    "low_24h",
                    "price_change_24h",
                    "price_change_percentage_24h",
                    "market_cap_change_24h",
                    "market_cap_change_percentage_24h",
                    "ath",
                    "ath_change_percentage",
                    "atl",
                    "atl_change_percentage"
                ]
            )

            df = cls.add_ingest_timestamp(df)
            df = cls.add_days_since_ath(df)
            df = cls.add_days_since_atl(df)
            df = cls.add_daily_volatility(df)
            df = cls.add_distance_from_ath(df)
            df = cls.add_distance_from_atl(df)
            df = cls.add_volume_market_cap_ratio(df)
            df = cls.add_supply_utilization(df)
            df = cls.add_price_direction(df)

            Logger.log_banner(logger, "CRYPTO TRANSFORMATION COMPLETED")

            return df
        
        except Exception as e:
            logger.exception(f"Transformation failed: {e}")
            raise
