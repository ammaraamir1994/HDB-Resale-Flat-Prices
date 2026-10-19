"""Pipeline configuration and dataset registry for data.gov.sg HDB Resale data extraction."""

from dataclasses import dataclass, field
import os
import re
from typing import Dict, Optional


# Singapore data.gov.sg resource IDs for HDB Resale Flat Prices across historical epochs
# Source: data.gov.sg CKAN datastore catalog
RESOURCE_REGISTRY: Dict[str, str] = {
    # Jan 2017 to Present (includes remaining_lease in years + months format)
    "2017_CURRENT": "d_8b84c4ee58e3cfc0ece0d773c8ca6abc",
    # Jan 2015 to Dec 2016 (includes remaining_lease)
    "2015_2016": "d_ea9ed51da2787afaf8e51f827c304208",
    # Mar 2012 to Dec 2014 (schema lacks remaining_lease column)
    "2012_2014": "d_2d5ff9ea31397b66239f245f57751537",
}

DEFAULT_BASE_URL: str = "https://data.gov.sg/api/action/datastore_search"


def resolve_resource_id(month: str) -> str:
    """
    Resolve the correct data.gov.sg CKAN resource_id based on target month (YYYY-MM).
    Demonstrates proactive handling of multi-epoch historical dataset schema variations.
    """
    if not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", month):
        raise ValueError(f"Invalid month format: '{month}'. Must be in 'YYYY-MM' format (e.g., '2024-01').")

    if month >= "2017-01":
        return RESOURCE_REGISTRY["2017_CURRENT"]
    elif month >= "2015-01":
        return RESOURCE_REGISTRY["2015_2016"]
    elif month >= "2012-03":
        return RESOURCE_REGISTRY["2012_2014"]
    else:
        # Fall back to modern dataset if older periods are queried or custom mapped
        return RESOURCE_REGISTRY["2017_CURRENT"]


@dataclass
class BackoffConfig:
    """Exponential backoff and retry parameters for resilient HTTP fetching."""
    max_retries: int = 5
    base_delay: float = 1.0       # Base backoff in seconds
    max_delay: float = 60.0       # Maximum capped delay in seconds
    backoff_factor: float = 2.0   # Multiplier per attempt
    jitter: bool = True           # Apply randomized jitter to prevent thundering herd
    timeout_seconds: int = 30     # Network socket timeout per request


@dataclass
class PipelineConfig:
    """Global configuration for the extraction and staging pipeline."""
    target_month: str
    base_url: str = DEFAULT_BASE_URL
    resource_id: Optional[str] = None
    batch_size: int = 1000        # Page limit per API request (max 1000 per page for safe pagination)
    output_dir: str = "data/output"
    backoff: BackoffConfig = field(default_factory=BackoffConfig)

    def __post_init__(self):
        # Validate month
        if not re.match(r"^\d{4}-(0[1-9]|1[0-2])$", self.target_month):
            raise ValueError(
                f"Target month '{self.target_month}' is invalid. Expected format: YYYY-MM (e.g. 2024-01)"
            )
        # Auto-resolve resource_id if not explicitly provided
        if not self.resource_id:
            self.resource_id = resolve_resource_id(self.target_month)
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
