import psycopg2
from psycopg2 import OperationalError
from src.load.redshift_schema_mapper import RedShiftSchemaMapper

from config.config import Config
from src.utils.logger import Logger

logger = Logger.get_logger("redshift_loader", "redshift_loader.log")

class RedshiftLoader:
    """Redshift Loader."""
    
    def __init__(self):
        """Initialize Redshift Loader."""

        self.host = Config.REDSHIFT_HOST
        self.port = Config.REDSHIFT_PORT
        self.database = Config.REDSHIFT_DATABASE
        self.schema = Config.REDSHIFT_SCHEMA
        self.table = Config.REDSHIFT_TABLE

        self.user = Config.REDSHIFT_USER
        self.password = Config.REDSHIFT_PASSWORD
        
        self.iam_role = Config.REDSHIFT_IAM_ROLE
        
        self.connection = None
        self.cursor = None

        logger.info("Host : %s", self.host)
        logger.info("Database : %s", self.database)
        logger.info("Schema : %s", self.schema)
        logger.info("Table : %s", self.table)
    
    def connect(self):
        """Create redshift connection."""

        try:
            Logger.log_banner(logger, "CONNECT TO AMAZON REDSHIFT")

            logger.info("Connecting to Redshift...")

            logger.info("Host     : %s", self.host)
            logger.info("Port     : %s", self.port)
            logger.info("Database : %s", self.database)
            logger.info("User     : %s", self.user)
            logger.info("Schema   : %s", self.schema)
            logger.info("Table    : %s", self.table)

            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                connect_timeout=30,
                application_name="crypto-etl-pipeline"
            )

            self.connection.autocommit = False
            self.cursor = self.connection.cursor()

            self.cursor.execute(
                "SELECT current_database(), current_user;"
            )

            db_name, user_name = self.cursor.fetchone()

            logger.info("Connected Database : %s", db_name)
            logger.info("Connected User     : %s", user_name)

            logger.info("Redshift connection established successfully.")

            return self.connection

        except OperationalError as e:
            logger.exception("Unable to connect to Redshift: %s", e)
            raise

    def execute_sql(self, sql: str):
        """Execute SQL statement."""

        try:
            Logger.log_banner(logger, "EXECUTE SQL")
            logger.info("Executing SQL statement.")

            self.cursor.execute(sql)
            self.connection.commit()
            
            logger.info("SQL executed successfully.")

        except Exception as e:
            self.connection.rollback()
            logger.exception("SQL execution failed: %s", e)
            raise

    def create_table(self, schema):
        """Create redshift table if it does not exist."""

        try:
            Logger.log_banner(logger, "CREATE REDSHIFT TABLE")

            table_name = f"{self.schema}.{self.table}"

            logger.info("Generating CREATE TABLE statement for %s", table_name)

            check_sql = f"""
SELECT COUNT(*) 
FROM information_schema.tables
WHERE table_schema = '{self.schema}'
AND table_name = '{self.table}'
"""
            self.cursor.execute(check_sql)

            exists = self.cursor.fetchone()[0] > 0

            mapper = RedShiftSchemaMapper(
                table_name=table_name
            )
            
            create_table_sql = (
                mapper.generate_create_table(schema)
            )

            if not exists:
                logger.info("Table does not exist. Creating new table.")

                self.execute_sql(
                    create_table_sql
                )

            else:
                logger.info("Table alreay exists. Checking schema.")

                self.handle_schema_evolution(schema)

                logger.info("Redshift table ready.")
        

        except Exception as e:
            logger.exception("Failed creating redshift table: %s", e)
            raise

    def handle_schema_evolution(self, schema):
        """
        Add new columns if processed dataframe
        contains new columns.
        """
        try:
            existing_sql = f"""
SELECT column_name
FROM information_schema.columns
WHERE table_schema = '{self.schema}'
AND table_name = '{self.table}';
"""
            self.cursor.execute(existing_sql)

            existing_columns = {
                row[0]
                for row in self.cursor.fetchall()
            }

            for field in schema.fields:

                if field.name not in existing_columns:

                    redshift_type = (
                        RedShiftSchemaMapper(
                            table_name=""
                            )
                            .spark_type_to_redshift(
                                field.dataType
                            )
                        )

                    alter_sql = f"""
ALTER TABLE {self.schema}.{self.table}
ADD COLUMN "{field.name}" {redshift_type};
"""
                    logger.info("Adding new column: %s", field.name)

                    self.execute_sql(alter_sql)

            logger.info("Schema evolution completed.")


        except Exception as e:
            logger.exception("Schema evolution failed: %s", e)
            raise

    def copy_from_s3(self, s3_path):
        """Load processed parquet from s3 into Redshift."""
        
        try:
            Logger.log_banner(logger, "COPY PARQUET FORM S3")

            s3_path = s3_path.replace("s3a://", "s3://", 1)
            
            logger.info("Preparing COPY command.")

            copy_sql = f""" 
COPY {self.schema}.{self.table}
FROM '{s3_path}'
IAM_ROLE '{self.iam_role}'
FORMAT AS PARQUET;
"""
            logger.info("Loading data from: %s", s3_path)
            
            self.execute_sql(
                copy_sql
            )

            logger.info("COPY command completed successfully.")

        except Exception as e:
            logger.exception("COPY command failed: %s", e)
            raise
    
    def validate_load(self):
        """Validate loaded records in redshift."""

        try:
            Logger.log_banner(logger, "VALIDATE REDSHIFT LOAD")

            logger.info("Validating loaded records.")

            validation_sql = f"""
SELECT COUNT(*)
FROM {self.schema}.{self.table};
"""
            self.cursor.execute(
                validation_sql
            )

            row_count = self.cursor.fetchone()[0]
            
            logger.info("Validation completed successfully.")

            logger.info("Loaded Records: %d", row_count)

            return row_count
        
        except Exception as e:
            logger.exception("Load validation failed: %s", e)
            raise

    def close(self):
        """Close redshift resource."""
        
        try:
            Logger.log_banner(logger, "CLOSE REDSHIFT CONNECTION")

            if self.cursor is not None:
                
                logger.info("Closing cursor.")

                self.cursor.close()

                self.cursor = None
            
            if self.connection is not None:

                logger.info("Closing database connection.")

                self.connection.close()

                self.connection = None

            logger.info("Redshift connection closed successfully.")

        except Exception as e:
            logger.exception(f"Failed closing redshift connection: %s", e)
            raise
