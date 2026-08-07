from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession

from src.transform.transform_job import TransformJob


@patch("src.transform.transform_job.SchemaManager")
@patch("src.transform.transform_job.ParquetWriter")
@patch("src.transform.transform_job.CryptoTransformer")
@patch("src.transform.transform_job.Normalizer")
@patch("src.transform.transform_job.DataValidator")
def test_transform_job_run_success(
    mock_validator,
    mock_normalizer,
    mock_transformer,
    mock_writer,
    mock_schema_manager,
):
    """Test successful transform job."""

    spark = MagicMock(
        spec=SparkSession
    )

    # Schema Mock
    schema = MagicMock()

    field1 = MagicMock()
    field1.name = "id"

    field2 = MagicMock()
    field2.name = "symbol"

    schema.fields = [
        field1,
        field2
    ]

    mock_schema_manager.return_value.build_schema.return_value = schema

    # build_schema_from_dataframe
    processed_schema = MagicMock()

    processed_schema.fields = [
        field1,
        field2
    ]

    mock_schema_manager.build_schema_from_dataframe.return_value = (
        processed_schema
    )

    # DataFrame Mock
    df = MagicMock()

    df.columns = [
        "id",
        "symbol"
    ]

    df.count.return_value = 10

    df.cache.return_value = df

    spark.read.schema.return_value.json.return_value = df

    # Validator
    mock_validator.validate.return_value = None

    # Normalizer
    mock_normalizer.normalize.return_value = (
        df,
        2
    )

    # Transformer
    mock_transformer.transform.return_value = df

    # Parquet Writer
    mock_writer.return_value.write.return_value = (
        "processed/output"
    )

    job = TransformJob(
        spark=spark,
        schema_file="tests/fixtures/crypto_schema.json",
        input_path="tests/fixtures/sample_crypto.ndjson",
        output_path="output/",
    )

    result = job.run()

    # Assertions
    assert result["processed_rows"] == 10
    assert result["rejected_rows"] == 2
    assert (
        result["s3_processed_path"]
        == "processed/output"
    )
    assert "schema" in result
    assert "execution_time" in result

    mock_validator.validate.assert_called_once()
    mock_normalizer.normalize.assert_called_once()
    mock_transformer.transform.assert_called_once()
    mock_writer.return_value.write.assert_called_once()