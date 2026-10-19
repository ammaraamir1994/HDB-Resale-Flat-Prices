"""Unit tests for resilient HTTP client, exponential backoff, and retry logic."""

from unittest.mock import MagicMock, patch
import pytest
import requests
from requests.exceptions import Timeout, ConnectionError

from src.client import APIClient
from src.config import BackoffConfig


def test_backoff_calculation_exponential_growth():
    """Verify exponential growth and maximum delay capping."""
    cfg = BackoffConfig(base_delay=1.0, backoff_factor=2.0, max_delay=10.0, jitter=False)
    client = APIClient(cfg)

    # 1.0 * (2^0) = 1.0
    assert client._calculate_backoff(0) == 1.0
    # 1.0 * (2^1) = 2.0
    assert client._calculate_backoff(1) == 2.0
    # 1.0 * (2^2) = 4.0
    assert client._calculate_backoff(2) == 4.0
    # 1.0 * (2^3) = 8.0
    assert client._calculate_backoff(3) == 8.0
    # 1.0 * (2^4) = 16.0 -> capped at max_delay=10.0
    assert client._calculate_backoff(4) == 10.0


def test_backoff_respects_retry_after_header():
    """Verify that HTTP Retry-After header overrides exponential calculation."""
    cfg = BackoffConfig(base_delay=1.0, max_delay=60.0, jitter=False)
    client = APIClient(cfg)

    delay = client._calculate_backoff(attempt=1, retry_after="15")
    assert delay == 15.0


def test_retry_on_429_then_success():
    """Verify client retries on HTTP 429 and succeeds on subsequent attempt."""
    cfg = BackoffConfig(max_retries=3, base_delay=0.01, jitter=False)
    client = APIClient(cfg)

    mock_resp_429 = MagicMock()
    mock_resp_429.status_code = 429
    mock_resp_429.headers = {"Retry-After": "0.01"}

    mock_resp_200 = MagicMock()
    mock_resp_200.status_code = 200
    mock_resp_200.json.return_value = {"success": True, "result": {"records": [{"id": 1}]}}

    with patch.object(client.session, "get", side_effect=[mock_resp_429, mock_resp_200]) as mock_get:
        payload = client.get_json("https://fake-api/search")
        assert payload["success"] is True
        assert mock_get.call_count == 2


def test_retry_exhaustion_raises_error():
    """Verify client raises exception when max retries are exceeded on 500 status."""
    cfg = BackoffConfig(max_retries=2, base_delay=0.01, jitter=False)
    client = APIClient(cfg)

    mock_resp_500 = MagicMock()
    mock_resp_500.status_code = 500
    mock_resp_500.text = "Internal Server Error"
    mock_resp_500.raise_for_status.side_effect = requests.HTTPError("500 Server Error")

    with patch.object(client.session, "get", return_value=mock_resp_500):
        with pytest.raises(requests.HTTPError):
            client.get_json("https://fake-api/search")


def test_non_retryable_404_fails_immediately():
    """Verify client fails immediately on 404 without retrying."""
    cfg = BackoffConfig(max_retries=3, base_delay=0.01)
    client = APIClient(cfg)

    mock_resp_404 = MagicMock()
    mock_resp_404.status_code = 404
    mock_resp_404.raise_for_status.side_effect = requests.HTTPError("404 Not Found")

    with patch.object(client.session, "get", return_value=mock_resp_404) as mock_get:
        with pytest.raises(requests.HTTPError):
            client.get_json("https://fake-api/search")
        assert mock_get.call_count == 1
