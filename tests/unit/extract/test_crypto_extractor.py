from unittest.mock import MagicMock, patch

import pytest
import requests

from src.extract.crypto_extractor import CoinGeckoExtractor

@patch("src.extract.crypto_extractor.requests.get")
def test_extract_success(mock_get):
    """Test successful API extraction."""

    sample_data = [
        {
            "id": "bitcoin",
            "symbol": "btc",
            "current_price": 118500.25
        },
        {
            "id": "ethereum",
            "symbol": "eth",
            "current_price": 4250.40
        }
    ]

    mock_response = MagicMock()
    mock_response.json.return_value = sample_data
    mock_response.raise_for_status.return_value = None

    mock_get.return_value = mock_response

    extractor = CoinGeckoExtractor()

    data = extractor.extract()

    assert len(data) == 2
    assert data[0]["id"] == "bitcoin"
    assert data[1]["symbol"] == "eth"

    mock_get.assert_called_once_with(
        extractor.url,
        timeout=30
    )

@patch("src.extract.crypto_extractor.requests.get")
def test_extract_request_exception(mock_get):
    """Test API request failure."""

    mock_get.side_effect = requests.exceptions.RequestException(
        "Connection failed"
    )

    extractor = CoinGeckoExtractor()

    with pytest.raises(requests.exceptions.RequestException):
        extractor.extract()