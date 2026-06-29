import requests
from config.config import COINGECKO_API_URL
from src.utils.logger import get_logger

logger = get_logger(__name__)

class CoinGeckoExtractor:
    """
    Extract cryptocurrency market data extranct form Coingecko API.
    """

    def __init__(self,timeout=30):
        self.url = COINGECKO_API_URL
        self.timeout = timeout
    
    def extract(self):
        """
        Fetch market data from coingecko.
        Returns:
            LIST[Dict]: JSON response form API
        """

        try:
            logger.info("Fetch data from coingecko API.")
            response = requests.get(
                self.url,
                timeout=self.timeout
                )
            response.raise_for_status()
            data = response.json()
            logger.info(f"Successfully fetch {len(data)} records")
            return data
        
        except requests.exceptions.RequestException as e:
            logger.error(f"API error: {e}")
            raise
