from pyspark.sql.types import (
    StructType,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    ArrayType,
    MapType,
    DateType,
    TimestampType,
    IntegerType,
    FloatType
)

from src.utils.logger import Logger

logger = Logger.get_logger("redshift_schema_mapper", "redshift_schema_mapper.log")

class RedShiftSchemaMapper:
    """
    Convert Spark StructType schema
    into Amazon Redshift CREATE TABLE SQL.
    """

    TYPE_MAPPING = {
        StringType: "VARCHAR(65535)",
        LongType: "BIGINT",
        IntegerType: "INTEGER",
        DoubleType: "DOUBLE PRECISION",
        FloatType: "REAL",
        BooleanType: "BOOLEAN",
        TimestampType: "TIMESTAMP",
        DateType: "DATE",
        ArrayType: "SUPER",
        MapType: "SUPER"
    }

    def __init__(self, table_name: str):
        """
        Initialize Redshift schema mapper.

        Args:
            table_name: Target Redshift table name
        """

        self.table_name = table_name

    def spark_type_to_redshift(self, data_type):
        """
        Convert Spark datatype into Redshift datatype.
        """

        try:

            for spark_type, redshift_type in self.TYPE_MAPPING.items():

                if isinstance(data_type, spark_type):
                    return redshift_type

            logger.warning(
                "Unknown Spark datatype %s. Using VARCHAR.",
                data_type
            )

            return "VARCHAR(65535)"

        except Exception as e:
            logger.exception("Datatype conversion failed: %s", e)
            raise

    def generate_columns(self, schema: StructType):
        """
        Generate Redshift column definitions.
        """

        try:

            columns = []

            for field in schema.fields:

                redshift_type = (
                    self.spark_type_to_redshift(
                        field.dataType
                    )
                )

                column_definition = (
                    f'"{field.name}" {redshift_type}'
                )

                columns.append(
                    column_definition
                )

            logger.info("Generated %d Redshift columns", len(columns))

            return columns

        except Exception as e:
            logger.exception("Failed generating Redshift columns: %s", e)
            raise

    def generate_create_table(self, schema: StructType):
        """
        Generate CREATE TABLE statement.
        """

        try:

            Logger.log_banner(logger, "GENERATE REDSHIFT CREATE TABLE SQL")

            columns = self.generate_columns(schema)

            sql = f"""
CREATE TABLE IF NOT EXISTS {self.table_name}
(
    {",\n    ".join(columns)}
);
"""
            logger.info("Redshift CREATE TABLE SQL generated successfully.")

            return sql.strip()

        except Exception as e:
            logger.exception("Failed generating CREATE TABLE SQL: %s", e)
            raise