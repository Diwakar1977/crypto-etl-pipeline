import json
from pathlib import Path
from src.extract.coingecko_extractor import CoinGeckoExtractor

extractor = CoinGeckoExtractor()
data = extractor.extract()

print(type(data))
print(len(data))
print(data[0])

Path("tests/data").mkdir(parents=True,exist_ok=True)

with open("tests/data/coingecko_response.json", "w", encoding="utf-8") as f:
    json.dump(data, f, indent=4)

print("Mock data saved successfully!")