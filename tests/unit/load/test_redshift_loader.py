from unittest.mock import MagicMock, patch

import pytest
from psycopg2 import OperationalError

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

from src.load.redshift_loader import RedshiftLoader

@pytest.fixture
def loader():
    return RedshiftLoader()

@patch("src.load.redshift_loader.psycopg2.connect")
def test_connect(mock_connect, loader):
    """Test successful Redshift connection."""

    mock_conn = MagicMock()
    mock_cursor = MagicMock()

    mock_conn.cursor.return_value = mock_cursor

    mock_cursor.fetchone.return_value = (
        "dev",
        "awsuser",
    )

    mock_connect.return_value = mock_conn

    connection = loader.connect()

    assert connection == mock_conn

    mock_connect.assert_called_once()

@patch("src.load.redshift_loader.psycopg2.connect")
def test_connect_failure(mock_connect, loader):
    """Test Redshift connection failure."""

    mock_connect.side_effect = OperationalError(
        "Connection failed"
    )

    with pytest.raises(OperationalError):
        loader.connect()

def test_execute_sql(loader):
    """Test SQL execution."""

    loader.connection = MagicMock()
    loader.cursor = MagicMock()

    loader.execute_sql("SELECT 1")
    loader.cursor.execute.assert_called_once_with("SELECT 1")

    loader.connection.commit.assert_called_once()

def test_execute_sql_failure(loader):
    """Test SQL execution failure."""

    loader.connection = MagicMock()
    loader.cursor = MagicMock()

    loader.cursor.execute.side_effect = Exception("SQL Error")

    with pytest.raises(Exception):
        loader.execute_sql(
            "SELECT 1"
        )

    loader.connection.rollback.assert_called_once()

@patch("src.load.redshift_loader.RedShiftSchemaMapper")
def test_create_table(mock_mapper, loader):
    """Test create table."""

    loader.cursor = MagicMock()

    # Table does not exist
    loader.cursor.fetchone.return_value = (
        0,
    )

    loader.execute_sql = MagicMock()

    schema = StructType([
        StructField(
            "id",
            StringType(),
            True
        )
    ])

    mapper = MagicMock()

    mapper.generate_create_table.return_value = (
        "CREATE TABLE public.crypto_market(id VARCHAR);"
    )

    mock_mapper.return_value = mapper

    loader.create_table(schema)

    mapper.generate_create_table.assert_called_once_with(schema)

    loader.execute_sql.assert_called_once_with(
        "CREATE TABLE public.crypto_market(id VARCHAR);"
    )

@patch("src.load.redshift_loader.RedShiftSchemaMapper")
def test_create_table_existing_schema(
    mock_mapper,
    loader
):
    """Test schema evolution when table exists."""

    loader.cursor = MagicMock()

    # Table already exists
    loader.cursor.fetchone.return_value = (
        1,
    )

    loader.handle_schema_evolution = MagicMock()

    schema = StructType([
        StructField(
            "id",
            StringType(),
            True
        )
    ])

    mock_mapper.return_value = MagicMock()

    loader.create_table(schema)
    loader.handle_schema_evolution.assert_called_once_with(schema)

def test_copy_from_s3(loader):
    """Test COPY command."""

    loader.execute_sql = MagicMock()
    loader.copy_from_s3("s3://bucket/data/")
    loader.execute_sql.assert_called_once()

def test_copy_from_s3_s3a_path(loader):
    """Test s3a path conversion."""

    loader.execute_sql = MagicMock()
    loader.copy_from_s3("s3a://bucket/data/")

    sql = (
        loader.execute_sql
        .call_args[0][0]
    )

    assert "s3://bucket/data/" in sql

def test_validate_load(loader):
    """Test row validation."""

    loader.cursor = MagicMock()
    loader.cursor.fetchone.return_value = (150,)

    rows = loader.validate_load()

    assert rows == 150

def test_close(loader):
    """Test closing resources."""

    loader.cursor = MagicMock()
    loader.connection = MagicMock()

    cursor = loader.cursor
    connection = loader.connection

    loader.close()

    cursor.close.assert_called_once()

    connection.close.assert_called_once()

    assert loader.cursor is None
    assert loader.connection is None