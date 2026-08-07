from pyspark.sql import DataFrame
from src.utils.logger import Logger

logger = Logger.get_logger("parquet_writer", "parquet_writer.log")

class ParquetWriter:
    """Write Spark DataFrame into Parquet format."""

    VALID_WRITE_MODE = {
        "overwrite",
        "append",
        "ignore",
        "error",
        "errorifexists"
    }

    def __init__(self, output_path: str):
        """Initialize parquet writer."""

        self.output_path = output_path

    def write(
        self,
        df: DataFrame,
        mode: str = "overwrite",
        partition_columns: list[str] | None = None,
        compression: str = "snappy"
    ):
        """
        Write dataframe to parquet.
        """

        try:
            Logger.log_banner(logger, "WRITE PARQUET")

            if mode not in self.VALID_WRITE_MODE:
                raise ValueError(f"Invalid write mode: {mode}")
            
            logger.info("Writing parquet dataset.")
            logger.info("Output path: %s", self.output_path)
            logger.info("Write mode: %s", mode)
            logger.info("Compression: %s", compression)
            logger.info("Columns : %d", len(df.columns))

            writer = (
                df.write
                .mode(mode)
                .option("compression", compression)
            )

            if partition_columns:
                logger.info("Partition columns: %s", partition_columns)

                writer = writer.partitionBy(*partition_columns)

            writer.parquet(self.output_path)

            logger.info("Parquet written successfully.")

            return self.output_path

        except Exception as e:
            logger.exception("Failed to write parquet: %s", e)
            raise