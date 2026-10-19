"""Robust HTTP client with exponential backoff, jitter, and rate-limit handling."""

import logging
import random
import time
from typing import Any, Dict, Optional
import requests
from requests.exceptions import RequestException, Timeout, ConnectionError

from src.config import BackoffConfig

logger = logging.getLogger(__name__)


class APIClient:
    """
    HTTP client configured for high resilience against API throttling,
    HTTP 429 rate limits, and transient gateway failures (5xx).
    """

    # Status codes qualifying for retry with exponential backoff
    RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

    def __init__(self, backoff_config: Optional[BackoffConfig] = None):
        self.config = backoff_config or BackoffConfig()
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mandai-Data-Engineering-Ingestion/1.0",
            "Accept": "application/json",
        })

    def _calculate_backoff(self, attempt: int, retry_after: Optional[str] = None) -> float:
        """
        Calculate backoff sleep duration using exponential backoff with full jitter,
        respecting HTTP 'Retry-After' header if present.
        """
        if retry_after:
            try:
                # Retry-After can be integer seconds
                header_delay = float(retry_after)
                logger.info(f"API specified Retry-After header: {header_delay}s")
                return min(header_delay, self.config.max_delay)
            except ValueError:
                pass  # Fall back to standard exponential calculation if non-numeric/HTTP-date

        # Exponential backoff formula: base * factor^(attempt)
        exp_delay = self.config.base_delay * (self.config.backoff_factor ** attempt)
        capped_delay = min(exp_delay, self.config.max_delay)

        if self.config.jitter:
            # Full Jitter (AWS architecture recommended pattern): uniform(0, capped_delay)
            # Prevents thundering herd problems when multiple workers retry simultaneously
            sleep_duration = random.uniform(capped_delay / 2.0, capped_delay)
        else:
            sleep_duration = capped_delay

        return sleep_duration

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute an HTTP GET request with exponential backoff retries.

        Args:
            url: Target API endpoint.
            params: Query parameters.

        Returns:
            Parsed JSON dictionary from successful response.

        Raises:
            requests.HTTPError: If max retries are exhausted or non-retryable 4xx encountered.
        """
        attempt = 0
        while True:
            try:
                logger.debug(f"Executing GET {url} with params {params} (attempt {attempt + 1}/{self.config.max_retries + 1})")
                response = self.session.get(
                    url,
                    params=params,
                    timeout=self.config.timeout_seconds
                )

                # Check if status code represents rate limiting or transient server error
                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    attempt += 1
                    if attempt > self.config.max_retries:
                        logger.error(
                            f"Exceeded max retries ({self.config.max_retries}) for {url}. "
                            f"Status: {response.status_code}, Body: {response.text[:200]}"
                        )
                        response.raise_for_status()

                    retry_after = response.headers.get("Retry-After")
                    sleep_time = self._calculate_backoff(attempt, retry_after)
                    logger.warning(
                        f"Received HTTP {response.status_code} from API. "
                        f"Backing off for {sleep_time:.2f}s before attempt {attempt + 1}/{self.config.max_retries + 1}."
                    )
                    time.sleep(sleep_time)
                    continue

                # Raise exception for non-retryable 4xx client errors (e.g. 400 Bad Request, 404 Not Found)
                response.raise_for_status()

                # Parse and validate JSON payload
                payload = response.json()
                if not payload.get("success", False):
                    raise ValueError(f"API response returned success=False: {payload}")

                return payload

            except (Timeout, ConnectionError) as net_err:
                attempt += 1
                if attempt > self.config.max_retries:
                    logger.error(f"Network error on GET {url} exceeded max retries: {net_err}")
                    raise

                sleep_time = self._calculate_backoff(attempt)
                logger.warning(
                    f"Transient network exception ({type(net_err).__name__}). "
                    f"Backing off for {sleep_time:.2f}s before attempt {attempt + 1}/{self.config.max_retries + 1}."
                )
                time.sleep(sleep_time)

    def close(self):
        """Close the underlying requests Session."""
        self.session.close()
