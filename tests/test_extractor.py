"""Unit tests for pagination and extraction logic."""

from unittest.mock import MagicMock
import pytest

from src.client import APIClient
from src.config import PipelineConfig
from src.extractor import HDBResaleExtractor


def test_pagination_fetches_multiple_pages():
    """Verify that the extractor correctly increments offset and aggregates records across pages."""
    config = PipelineConfig(target_month="2024-01", batch_size=2)
    mock_client = MagicMock(spec=APIClient)

    # Simulate 2 pages with a total of 3 records
    page1 = {
        "success": True,
        "result": {
            "total": 3,
            "fields": [{"id": "month"}, {"id": "resale_price"}],
            "records": [{"_id": 1, "month": "2024-01"}, {"_id": 2, "month": "2024-01"}],
        },
    }
    page2 = {
        "success": True,
        "result": {
            "total": 3,
            "fields": [{"id": "month"}, {"id": "resale_price"}],
            "records": [{"_id": 3, "month": "2024-01"}],
        },
    }

    mock_client.get_json.side_effect = [page1, page2]

    extractor = HDBResaleExtractor(config=config, client=mock_client)
    records, meta = extractor.fetch_all_records()

    assert len(records) == 3
    assert meta["total_available"] == 3
    assert meta["total_extracted"] == 3
    assert mock_client.get_json.call_count == 2


def test_pagination_stops_on_empty_records():
    """Verify pagination terminates cleanly if the API returns an empty record set."""
    config = PipelineConfig(target_month="2024-01", batch_size=5)
    mock_client = MagicMock(spec=APIClient)

    empty_page = {
        "success": True,
        "result": {
            "total": 0,
            "fields": [],
            "records": [],
        },
    }
    mock_client.get_json.return_value = empty_page

    extractor = HDBResaleExtractor(config=config, client=mock_client)
    records, meta = extractor.fetch_all_records()

    assert len(records) == 0
    assert meta["total_extracted"] == 0
    assert mock_client.get_json.call_count == 1
