import json
import re
from pathlib import Path
from collections import defaultdict

from src.utils.logger import Logger

logger = Logger.get_logger("schema_infer", "schema_infer.log")

class SchemaInfer:
    """Infer schema from JSON records."""

    TIMESTAMP_REGEX = r"^\d{4}-\d{2}-\d{2}T"

    def __init__(self):
        """Initialize schema infer."""
        self.column_types = defaultdict(set)

    # Detect datatype
    def detect_type(self, value):
        """Detect datatype of a value."""

        try:

            if value is None:
                return None

            if isinstance(value, bool):
                return "BooleanType"

            if isinstance(value, int):
                return "LongType"

            if isinstance(value, float):
                return "DoubleType"

            if isinstance(value, dict):
                return "StructType"

            if isinstance(value, list):
                return "ArrayType"

            if isinstance(value, str):

                if re.match(self.TIMESTAMP_REGEX, value):
                    return "TimestampType"

                return "StringType"

            return "StringType"

        except Exception as e:
            logger.exception(f"Failed to detect datatype: {e}")
            raise

    # Scan all records
    def scan_records(self, records):
        """Scan all JSON records."""

        try:

            logger.info("Scanning records...")

            for row in records:

                if not isinstance(row, dict):
                    logger.warning("Skipping invalid record.")
                    continue

                for column, value in row.items():

                    dtype = self.detect_type(value)

                    if dtype is not None:
                        self.column_types[column].add(dtype)

            logger.info("Scanning completed.")

        except Exception as e:
            logger.exception(f"Record scanning failed: {e}")
            raise

    # Resolve datatype conflicts
    def resolve_schema(self):
        """Resolve datatype conflicts."""

        try:

            logger.info("Resolving schema...")

            schema = {}

            for column, types in self.column_types.items():

                logger.info(f"{column} -> {types}")

                # String wins
                if "StringType" in types:
                    schema[column] = "StringType"

                # Nested JSON
                elif "StructType" in types:
                    schema[column] = "StructType"

                # Array
                elif "ArrayType" in types:
                    schema[column] = "ArrayType"

                # Long + Double -> Double
                elif "DoubleType" in types:
                    schema[column] = "DoubleType"

                elif "LongType" in types:
                    schema[column] = "LongType"

                elif "BooleanType" in types:
                    schema[column] = "BooleanType"

                elif "TimestampType" in types:
                    schema[column] = "TimestampType"

                else:
                    schema[column] = "StringType"

            logger.info("Schema resolved successfully.")

            return schema

        except Exception as e:
            logger.exception(f"Schema resolve failed: {e}")
            raise

    # Save schema
    def save_schema(self, schema, output_file):
        """Save schema.json."""

        try:

            output_file = Path(output_file)

            output_file.parent.mkdir(
                parents=True,
                exist_ok=True
            )

            with output_file.open(
                "w",
                encoding="utf-8"
            ) as file:

                json.dump(
                    schema,
                    file,
                    indent=4
                )

            logger.info(f"Schema saved successfully -> {output_file}")

        except Exception as e:
            logger.exception(f"Failed to save schema: {e}")
            raise

    # Run infernce
    def infer(self, records, output_file):
        """Infer schema and save it."""

        try:

            Logger.log_banner(logger, "SCHEMA INFERENCE INITIALIZED")

            self.scan_records(records)

            schema = self.resolve_schema()

            self.save_schema(
                schema,
                output_file
            )

            Logger.log_banner(logger, "SCHAMA INFERENCE COMPLETED")

            return schema

        except Exception as e:
            logger.exception(f"Schema inference failed: {e}")
            raise