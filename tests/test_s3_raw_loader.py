from src.extract.coingecko_extractor import CoinGeckoExtractor
from src.loads.s3_raw_loader import S3RawLoader

extractor = CoinGeckoExtractor()
data = extractor.extract()

loader = S3RawLoader()
loader.s3.put_object = lambda **kwargs: print("\nMock s3 upload sucessful.")

path = loader.load_json(data)
print(f"\nReturned Path: {path}")
print("\nTest Completed Successfully")