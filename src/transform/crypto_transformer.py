from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,when,round,current_timestamp,current_date,datediff,to_timestamp,lit
    )
from pyspark.sql.utils import AnalysisException
from src.utils.logger import get_logger

logger = get_logger(__name__)

class CryptoTransformer:
    """"
    Data cleaning pipeline
    Responsibilities:
    ----------------
    1.Remove duplicate records.
    2.Drop unwanted columns.
    3.Fill missing values.
    4.Cast data types.
    5.Round numeric values.
    6.Create derived business features.
    """

    DUPLICATE_KEY = ["id"]

    DROP_COLUMNS = ["roi"]

    NUMERIE_IMPUTE_COLUMNS = [
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "market_cap_change_24h",
        "market_cap_change_percentage_24h"
    ]

    DATE_COLUMNS = [
        "ath_date",
        "atl_date",
        "last_updated"
    ]
    
    ROUND_COLUMNS = [
        "current_price",
        "market_cap",
        "fully_diluted_valuation",
        "total_volume",
        "high_24h",
        "low_24h",
        "price_change_24h",
        "price_change_percentage_24h",
        "market_cap_change_percentage_24h",
        "circulating_supply",
        "total_supply",
        "max_supply",
        "ath",
        "ath_change_percentage",
        "atl",
        "atl_change_percentage"
    ]
    
    TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"

    @classmethod
    def remove_duplicates(cls, df: DataFrame):
        logger.info(f"Removing duplicate records using key %s: {cls.DUPLICATE_KEY}.")
        return df.dropDuplicates(cls.DUPLICATE_KEY)
    
    @classmethod
    def drop_unwanted_columns(cls, df: DataFrame):
        existing = [c for c in cls.DROP_COLUMNS if c in df.columns]
        if existing:
            logger.info(f"Dropping columns %s: {existing}.")
            df = df.drop(*existing)
        return df
    
    @classmethod
    def fill_missing_values(cls, df: DataFrame):
        logger.info("Starting median imputation.")

        for column in cls.NUMERIE_IMPUTE_COLUMNS:
            if column not in df.columns:
                logger.info(f"%s column not found. skipping {column}")
                continue
            median = df.approxQuantile(column, [0.5], 0.01)[0]
            logger.info("Median %s = %s", column, median)

            df = df.fillna({column: median})

        return df
    
    @classmethod
    def cast_columns(cls, df:DataFrame):
        logger.info("Casting timestamp columns.")
        
        for column in cls.DATE_COLUMNS:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    to_timestamp(col(column),cls.TIMESTAMP_FORMAT)
                )

        if "market_cap_rank" in df.columns:
            df = df.withColumn(
                "market_cap_rank",
                col("market_cap_rank").cast("int")
            )
        return df
    
    @classmethod
    def round_columns(cls, df: DataFrame):
        logger.info("Rounding numeric columns.")
        
        for column in cls.ROUND_COLUMNS:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    round(col(column),2)
                )
        return df
    
    @classmethod
    def create_feature(cls, df:DataFrame):
        logger.info("Creating business features.")

        df = (
            df
            .withColumn(
                "ingest_timestamp",
                current_timestamp()
            )
            .withColumn(
                "days_since_ath",
                datediff(current_date(), col("ath_date"))
            )
            .withColumn(
                "days_since_atl",
                datediff(current_date(), col("atl_date"))
            )
            .withColumn(
                "daily_volatility_pct",
                when(
                    col("current_price") != 0,
                    round(
                        (
                            (col("high_24h") - col("low_24h"))
                            / col("current_price")
                        ),
                        2
                    )
                ).otherwise(lit(None))
            )
            .withColumn(
                "distance_from_ath",
                when(
                    col("ath") != 0,
                    round(
                        (
                            (col("ath") - col("current_price"))
                            / col("ath")
                        ),
                        2
                    )
                ).otherwise(lit(None))
            )
            .withColumn(
                "volume_market_cap_ratio",
                when(
                    col("market_cap") != 0,
                    round(
                        col("total_volume")
                        / col("market_cap"),
                        4
                    )
                ).otherwise(lit(None))
            )
            .withColumn(
                "supply_utilization_pct",
                when(
                    col("max_supply") > 0,
                    round(
                        col("circulating_supply")
                        / col("max_supply"),
                        2
                    )
                ).otherwise(lit(None))
            )
            .withColumn(
                "near_ath",
                when(
                    col("ath_change_percentage") > -10,
                    1
                ).otherwise(0)
            )
            .withColumn(
                "price_direction",
                when(
                    col("price_change_24h") > 0,
                    "UP"
                )
                .when(
                    col("price_change_24h") < 0,
                    "DOWN"
                ).otherwise("FLAT")
            )
        )
        return df
    
    @classmethod
    def transform(cls, df: DataFrame):
        logger.info("Starting data cleaning pipeline.")

        try:
            df = cls.remove_duplicates(df)

            df = cls.drop_unwanted_columns(df)

            df = cls.fill_missing_values(df)

            df = cls.cast_columns(df)

            df = cls.round_columns(df)

            df = cls.create_feature(df)

            logger.info("Data transformation completed successfully.")

            return df
        
        except AnalysisException as e:
            logger.exception(f"Spark analysis exception: %s {e}")
            raise

        except Exception as e:
            logger.exception(f"Unexpected exception: %s {e}")
            raise

            
        
        



