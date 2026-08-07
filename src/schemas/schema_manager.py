import json
from pathlib import Path
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
    BooleanType,
    TimestampType,
    ArrayType,
    MapType
)

from src.utils.logger import Logger

logger = Logger.get_logger("schema_manager", "schema_manager.log")

class SchemaManager:
    """Build spark StructType from schema.json."""

    TYPE_MAPPING = {
        "StringType": StringType(),
        "LongType": LongType(),
        "DoubleType": DoubleType(),
        "BooleanType": BooleanType(),
        "TimestampType": TimestampType()
    }

    def __init__(self, schema_file: str):
        self.schema_file = Path(schema_file)

    def load_schema(self):
        """Load schema JSON."""

        try:
            if not self.schema_file.exists():
                raise FileNotFoundError(
                    f"Schema file not found: {self.schema_file}"
                )

            logger.info("Loading schema: %s", self.schema_file)

            with self.schema_file.open(
                "r",
                encoding="utf-8"
            ) as file:

                schema = json.load(file)

            if not isinstance(schema, dict):
                raise ValueError("Schema.json must contain a JSON object.")

            logger.info("Schema loaded successfully.")

            return schema

        except Exception as e:
            logger.exception("Failed to load schema: %s", e)
            raise

    def convert_type(self, dtype):
        """Recursively convert JSON datatype into Spark datatype."""

        try:

            if isinstance(dtype, str):
                return self.TYPE_MAPPING.get(
                    dtype,
                    StringType()
                )

            if not isinstance(dtype, dict):
                return StringType()

            data_type = dtype.get("type")

            if data_type == "StructType":

                fields = []

                for name, value in dtype.get(
                    "fields",
                    {}
                ).items():

                    fields.append(
                        StructField(
                            name,
                            self.convert_type(value),
                            True
                        )
                    )

                return StructType(fields)

            if data_type == "ArrayType":

                return ArrayType(
                    self.convert_type(
                        dtype.get(
                            "elementType",
                            "StringType"
                        )
                    ),
                    containsNull=True
                )

            if data_type == "MapType":

                return MapType(

                    self.convert_type(
                        dtype.get(
                            "keyType",
                            "StringType"
                        )
                    ),

                    self.convert_type(
                        dtype.get(
                            "valueType",
                            "StringType"
                        )
                    ),

                    valueContainsNull=True
                )

            logger.warning("Unknown datatype '%s'. Using StringType.", data_type)

            return StringType()

        except Exception as e:
            logger.exception("Failed to convert datatype: %s", e)
            raise

    def build_schema(self):
        """
        Build Spark StructType
        from raw schema JSON.
        """

        try:

            schema = self.load_schema()

            fields = []

            for column, dtype in schema.items():

                fields.append(
                    StructField(
                        column,
                        self.convert_type(dtype),
                        nullable=True
                    )
                )

            spark_schema = StructType(fields)

            logger.info("Raw Spark schema created successfully.")
            Logger.log_banner(logger, "RAW SPARK SCHEMA CREATED SUCCESSFULLY")

            return spark_schema

        except Exception as e:
            logger.exception("Failed to build raw schema: %s", e)
            raise

    @staticmethod
    def build_schema_from_dataframe(df):
        """
        Build Spark StructType
        from processed DataFrame.

        Used after transformation
        before Redshift table creation.
        """

        try:

            logger.info("Building schema from processed dataframe.")

            processed_schema = df.schema

            logger.info(
                "Processed schema created. Columns: %d",
                len(processed_schema.fields)
            )

            Logger.log_banner(logger, "PROCESSED SPARK SCHEMA CREATED SUCCESSFULLY")

            return processed_schema

        except Exception as e:
            logger.exception("Failed to build processed schema: %s", e)
            raise