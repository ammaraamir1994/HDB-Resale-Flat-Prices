"""Extraction module for programmatically querying and paginating data.gov.sg datastore."""

import json
import logging
from typing import Any, Dict, Generator, List, Tuple
from src.client import APIClient
from src.config import PipelineConfig

logger = logging.getLogger(__name__)


class HDBResaleExtractor:
    """
    Handles robust pagination and extraction of HDB Resale Flat Price records
    from data.gov.sg CKAN datastore API.
    """

    def __init__(self, config: PipelineConfig, client: APIClient):
        self.config = config
        self.client = client

    def fetch_all_records(self) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Paginates through the entire result set for the target month.

        Returns:
            Tuple containing:
                - List of raw JSON record dictionaries.
                - Metadata dictionary containing total count, fields schema, and resource ID.
        """
        all_records: List[Dict[str, Any]] = []
        offset = 0
        total_records: int = 0
        fields_metadata: List[Dict[str, Any]] = []
        page_num = 1

        # data.gov.sg datastore expects filter parameter as serialized JSON
        filters = json.dumps({"month": self.config.target_month})

        logger.info(
            f"Starting extraction for month '{self.config.target_month}' "
            f"using resource '{self.config.resource_id}' (page limit: {self.config.batch_size})..."
        )

        while True:
            params = {
                "resource_id": self.config.resource_id,
                "filters": filters,
                "limit": self.config.batch_size,
                "offset": offset,
            }

            response_json = self.client.get_json(self.config.base_url, params=params)
            result = response_json.get("result", {})

            # Capture dataset metadata on the first page
            if offset == 0:
                total_records = result.get("total", 0)
                fields_metadata = result.get("fields", [])
                logger.info(
                    f"Reported total records for month '{self.config.target_month}': {total_records}"
                )

            page_records = result.get("records", [])
            num_page_records = len(page_records)
            all_records.extend(page_records)

            logger.info(
                f"Page {page_num}: Fetched {num_page_records} records "
                f"(cumulative: {len(all_records)}/{total_records}, offset: {offset})"
            )

            # Check pagination termination conditions:
            # 1. Returned records are empty
            # 2. Total accumulated records reach or exceed API reported total
            # 3. Number of records returned is less than requested limit (last page)
            if num_page_records == 0 or len(all_records) >= total_records or num_page_records < self.config.batch_size:
                logger.info(
                    f"Extraction complete for '{self.config.target_month}'. "
                    f"Total records retrieved: {len(all_records)}."
                )
                break

            offset += self.config.batch_size
            page_num += 1

        metadata = {
            "target_month": self.config.target_month,
            "resource_id": self.config.resource_id,
            "total_available": total_records,
            "total_extracted": len(all_records),
            "source_fields": fields_metadata,
        }

        return all_records, metadata
