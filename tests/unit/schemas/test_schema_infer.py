import json
from pathlib import Path

from src.schemas.schema_infer import SchemaInfer


INPUT_FILE = "tests/fixtures/sample_crypto.ndjson"
OUTPUT_FILE = "tests/fixtures/crypto_schema.json"


def test_schema_infer():

    input_path = Path(INPUT_FILE)
    output_path = Path(OUTPUT_FILE)

    assert input_path.exists(), f"Input file not found: {INPUT_FILE}"

    records = []

    with input_path.open("r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if line:
                records.append(json.loads(line))

    assert len(records) > 0, "No records found in input file."

    infer = SchemaInfer()

    schema = infer.infer(
        records=records,
        output_file=OUTPUT_FILE
    )

    assert schema is not None
    assert output_path.exists(), f"Schema file was not created: {OUTPUT_FILE}"

    print("\nSchema inferred successfully.")
    print(schema)