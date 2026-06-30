import main
import json


class MockCoinGeckoExtractor:
    def extract(self):
        print("MOCK: Reading JSON file")

        with open("tests/data/coingecko_response.json", "r") as f:
            data = json.load(f)

        return data


class MockS3RawLoader:
    def load_json(self, data):
        print(f"MOCK RAW LOAD → {len(data)} records")


class MockS3ProcessedLoader:
    def load_parquet(self, df, dataset_name):
        print("MOCK PROCESSED LOAD → NO S3 UPLOAD")
        print(f"Dataset: {dataset_name}")
        print(f"Rows: {df.count()}")

# attach mocks
main.CoinGeckoExtractor = MockCoinGeckoExtractor
main.S3RawLoader = MockS3RawLoader
main.S3ProcessedLoader = MockS3ProcessedLoader


if __name__ == "__main__":
    print("START TEST PIPELINE")

    main.main(test_mode=True)

    print("TEST COMPLETED")