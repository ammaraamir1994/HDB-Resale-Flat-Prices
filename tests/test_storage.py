"""Unit tests for atomic Parquet writing and storage idempotency."""

from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.storage import ParquetStorageWriter
from src.transformer import STANDARD_PYARROW_SCHEMA


@pytest.fixture
def sample_arrow_table():
    """Generates a small PyArrow Table matching the standard schema."""
    data = {
        "_record_hash": ["hash1", "hash2"],
        "month": ["2024-01", "2024-01"],
        "town": ["TAMPINES", "TAMPINES"],
        "flat_type": ["4 ROOM", "5 ROOM"],
        "block": ["101", "102"],
        "street_name": ["TAMPINES ST 11", "TAMPINES ST 11"],
        "storey_range": ["04 TO 06", "07 TO 09"],
        "floor_area_sqm": [90.0, 110.0],
        "flat_model": ["Model A", "Improved"],
        "lease_commence_date": [1985, 1986],
        "remaining_lease": ["60 years", "61 years"],
        "resale_price": [500000.0, 650000.0],
        "_source_id": [1, 2],
        "_resource_id": ["res_123", "res_123"],
        "_extracted_at_utc": [
            pa.scalar(1700000000000000, type=pa.timestamp("us", tz="UTC")),
            pa.scalar(1700000000000000, type=pa.timestamp("us", tz="UTC")),
        ],
        "_raw_payload": ['{"test": 1}', '{"test": 2}'],
    }
    return pa.Table.from_pydict(data, schema=STANDARD_PYARROW_SCHEMA)


def test_atomic_write_creates_valid_parquet(tmp_path, sample_arrow_table):
    """Verify that Parquet file is written cleanly and readable with matching rows."""
    storage = ParquetStorageWriter(base_output_dir=str(tmp_path))
    file_path, meta = storage.write_atomic_parquet(sample_arrow_table, month="2024-01")

    assert file_path.exists()
    assert file_path.name == "hdb_resale_2024_01.parquet"
    assert meta["row_count"] == 2
    assert meta["file_size_bytes"] > 0
    assert len(meta["sha256_checksum"]) == 64

    # Verify no leftover temporary files in directory
    temp_files = list(tmp_path.glob(".*.tmp.*"))
    assert len(temp_files) == 0

    # Read back table and assert row count
    read_table = pq.read_table(file_path)
    assert read_table.num_rows == 2


def test_idempotent_overwrite(tmp_path, sample_arrow_table):
    """Verify that multiple writes for the same month safely overwrite without file accumulation."""
    storage = ParquetStorageWriter(base_output_dir=str(tmp_path))

    path_1, meta_1 = storage.write_atomic_parquet(sample_arrow_table, month="2024-01")
    path_2, meta_2 = storage.write_atomic_parquet(sample_arrow_table, month="2024-01")

    assert path_1 == path_2
    all_files = list(tmp_path.glob("*.parquet"))
    # Exactly one parquet file exists
    assert len(all_files) == 1
    assert meta_2["row_count"] == 2
