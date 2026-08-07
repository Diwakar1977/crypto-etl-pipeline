import json
from pathlib import Path

from src.extract.crypto_extractor import CoinGeckoExtractor

def main():
    """Generate a sample NDJSON fixture for tests."""

    extractor = CoinGeckoExtractor()
    data = extractor.extract()

    fixtures_dir = Path("tests/fixtures")
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    file_path = fixtures_dir / "sample_crypto.ndjson"

    with open(file_path, "w", encoding="utf-8") as file:
        for record in data:
            file.write(json.dumps(record) + "\n")

    print(f"Fixture created successfully: {file_path}")

if __name__ == "__main__":
    main()