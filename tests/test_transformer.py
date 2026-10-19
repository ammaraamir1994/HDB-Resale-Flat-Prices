"""Unit tests for data normalization, surrogate key generation, and schema drift handling."""

from datetime import datetime, timezone
import pytest

from src.transformer import (
    HDBResaleTransformer,
    compute_surrogate_key,
    flatten_record,
    STANDARD_PYARROW_SCHEMA,
)


def test_compute_surrogate_key_is_deterministic():
    """Verify that identical natural business keys yield identical SHA-256 surrogate hashes."""
    record_a = {
        "month": "2024-01",
        "town": "ANG MO KIO",
        "flat_type": "3 ROOM",
        "block": "123",
        "street_name": "ANG MO KIO AVE 3",
        "storey_range": "04 TO 06",
        "floor_area_sqm": "68.00",
        "resale_price": "350000",
        "lease_commence_date": "1980",
    }
    # Slight whitespace / casing difference that should normalize to the same hash
    record_b = {
        "month": " 2024-01 ",
        "town": "ang mo kio",
        "flat_type": "3 room",
        "block": "123",
        "street_name": "Ang Mo Kio Ave 3",
        "storey_range": "04 TO 06",
        "floor_area_sqm": 68.0,
        "resale_price": 350000.00,
        "lease_commence_date": "1980",
    }

    hash_a = compute_surrogate_key(record_a)
    hash_b = compute_surrogate_key(record_b)

    assert hash_a == hash_b
    assert len(hash_a) == 64  # Standard SHA-256 hex string


def test_deduplication_in_transformer():
    """Verify transformer removes duplicate records based on surrogate key."""
    raw_records = [
        {
            "_id": 1,
            "month": "2024-01",
            "town": "BEDOK",
            "flat_type": "4 ROOM",
            "block": "501",
            "street_name": "BEDOK NORTH ST 3",
            "storey_range": "07 TO 09",
            "floor_area_sqm": "92.00",
            "flat_model": "New Generation",
            "lease_commence_date": "1978",
            "remaining_lease": "53 years",
            "resale_price": "450000",
        },
        # Exact duplicate with different transient _id
        {
            "_id": 9999,
            "month": "2024-01",
            "town": "BEDOK",
            "flat_type": "4 ROOM",
            "block": "501",
            "street_name": "BEDOK NORTH ST 3",
            "storey_range": "07 TO 09",
            "floor_area_sqm": "92.00",
            "flat_model": "New Generation",
            "lease_commence_date": "1978",
            "remaining_lease": "53 years",
            "resale_price": "450000",
        },
    ]

    transformer = HDBResaleTransformer(resource_id="test_resource_id")
    arrow_table, metrics = transformer.transform(raw_records)

    assert metrics["input_rows"] == 2
    assert metrics["output_rows"] == 1
    assert metrics["duplicates_removed"] == 1
    assert arrow_table.num_rows == 1


def test_schema_drift_handling_missing_remaining_lease():
    """Verify transformer gracefully handles pre-2015 records where remaining_lease is absent."""
    raw_record_pre_2015 = {
        "_id": 42,
        "month": "2013-05",
        "town": "CLEMENTI",
        "flat_type": "3 ROOM",
        "block": "301",
        "street_name": "CLEMENTI AVE 2",
        "storey_range": "01 TO 03",
        "floor_area_sqm": "67.0",
        "flat_model": "Improved",
        "lease_commence_date": "1977",
        # 'remaining_lease' completely missing
        "resale_price": "310000",
    }

    transformer = HDBResaleTransformer(resource_id="pre_2015_resource")
    arrow_table, metrics = transformer.transform([raw_record_pre_2015])

    assert arrow_table.num_rows == 1
    pydict = arrow_table.to_pydict()
    assert pydict["remaining_lease"][0] is None
    assert pydict["month"][0] == "2013-05"
    assert pydict["resale_price"][0] == 310000.0
