from src.extract.coingecko_extractor import CoinGeckoExtractor

extractor = CoinGeckoExtractor()

data = extractor.extract()

print(type(data))
print(len(data))
print(data[0])