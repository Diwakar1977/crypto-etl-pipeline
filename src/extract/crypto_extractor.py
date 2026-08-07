import requests

from config.config import Config
from src.utils.logger import Logger

logger = Logger.get_logger("crypto_extractor", "crypto_extractor.log")

class CoinGeckoExtractor:
    """
    Extract cryptocurrency market data from the CoinGecko API.
    """

    def __init__(self, timeout: int = 30):
        self.url = Config.COINGECKO_BASE_URL
        self.timeout = timeout

    def extract(self):
        """
        Fetch cryptocurrency market data from the CoinGecko API.

        Returns:
            list[dict]: JSON response from the API.
        """

        try:
            Logger.log_banner(logger, "EXTRACT JOB INITIALIZED")
            logger.info("Fetching data from CoinGecko API...")

            response = requests.get(
                self.url,
                timeout=self.timeout
            )

            response.raise_for_status()

            data = response.json()

            logger.info(f"Successfully fetched {len(data)} records.")

            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {e}")
            raise