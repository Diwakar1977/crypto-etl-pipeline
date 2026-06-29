import json
from pathlib import Path
from pyspark.sql.types import StructType
from src.utils.logger import get_logger

logger = get_logger(__name__)

class SchemaManager:
    """
    Handling schema creation, persistance and loading.
    """

    def __init__(self, path="src/schemas/crypto_schema.json"):
        self.path = Path(path)

    def create_and_save(self, data, spark):
        try:
            logger.info("Starating schema creationg.")
            if not data:
                raise ValueError("Input data is empty.")
                
            # Create parent directory if it doesn't exit
            self.path.parent.mkdir(parents=True, exist_ok=True)
            df = spark.createDataFrame(data)

            # Preserve API column order
            if isinstance(data, dict):
                api_columns = list(data.keys())
            else:
                api_columns = list(data[0].keys())
            
            df = df.select(*api_columns)
            scheam = df.schema

            with open(self.path, "w", encoding="utf-8") as f:
                f.write(scheam.json())
                    
            logger.info(f"Schema successfully saved: {self.path}")

            return scheam
        
        except Exception as e:
            logger.exception(f"Scheam creation failed: {e}")
            raise

    def load_schema(self):
        try:
            logger.info(f"Load schema from: {self.path}")

            if not self.path.exists():
                raise FileNotFoundError(f"Schema not found: {self.path}")
            
            with open(self.path, "r", encoding="utf-8") as f:
                schema_json = json.load(f)

            schema = StructType.fromJson(schema_json)

            logger.info("Schema loaded successfully.")

            return schema

        except Exception as e:
            logger.exception(f"Schema loading failed: {e}")
            raise
        