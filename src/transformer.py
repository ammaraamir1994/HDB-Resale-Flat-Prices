"""Data transformation, schema normalization, flattening, and deterministic hashing."""

from datetime import datetime, timezone
import hashlib
import json
import logging
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# Standard PyArrow Schema for HDB Resale Flat Prices
# Designed to be compatible with Snowflake external tables and Parquet columnar storage
STANDARD_PYARROW_SCHEMA = pa.schema([
    pa.field("_record_hash", pa.string(), nullable=False),        # Deterministic SHA-256 surrogate key
    pa.field("month", pa.string(), nullable=False),               # Transaction month: YYYY-MM
    pa.field("town", pa.string(), nullable=False),                # Town name, e.g. ANG MO KIO
    pa.field("flat_type", pa.string(), nullable=False),           # e.g. 3 ROOM, 4 ROOM, EXECUTIVE
    pa.field("block", pa.string(), nullable=False),               # Block number (string, e.g. 116, 401A)
    pa.field("street_name", pa.string(), nullable=False),         # Street name
    pa.field("storey_range", pa.string(), nullable=False),        # Storey range, e.g. 07 TO 09
    pa.field("floor_area_sqm", pa.float64(), nullable=False),     # Floor area in square meters
    pa.field("flat_model", pa.string(), nullable=False),          # Model, e.g. Improved, Model A
    pa.field("lease_commence_date", pa.int64(), nullable=False),  # Year lease commenced, e.g. 1978
    pa.field("remaining_lease", pa.string(), nullable=True),      # Formatted string, nullable for pre-2015 records
    pa.field("resale_price", pa.float64(), nullable=False),       # Resale price in SGD
    pa.field("_source_id", pa.int64(), nullable=True),            # Original CKAN _id
    pa.field("_resource_id", pa.string(), nullable=False),        # Source data.gov.sg resource UUID
    pa.field("_extracted_at_utc", pa.timestamp("us", tz="UTC"), nullable=False), # Ingestion lineage timestamp
    pa.field("_raw_payload", pa.string(), nullable=False),        # Preserved JSON payload for Snowflake VARIANT / drift audit
])


def compute_surrogate_key(record: Dict[str, Any]) -> str:
    """
    Computes a deterministic SHA-256 surrogate primary key from the composite natural business key.
    
    In data.gov.sg, the internal '_id' column is a transient database sequence number that resets
    whenever the government refreshes or rebuilds the datastore. To achieve true idempotency and
    avoid duplicate insertions in Snowflake, we hash the immutable physical transaction attributes:
      month | town | flat_type | block | street_name | storey_range | floor_area_sqm | resale_price | lease_commence_date
    """
    natural_key_components = [
        str(record.get("month", "")).strip().upper(),
        str(record.get("town", "")).strip().upper(),
        str(record.get("flat_type", "")).strip().upper(),
        str(record.get("block", "")).strip().upper(),
        str(record.get("street_name", "")).strip().upper(),
        str(record.get("storey_range", "")).strip().upper(),
        f"{float(record.get('floor_area_sqm', 0.0)):.2f}",
        f"{float(record.get('resale_price', 0.0)):.2f}",
        str(record.get("lease_commence_date", "")).strip(),
    ]
    raw_key = "|".join(natural_key_components)
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()


def flatten_record(raw_record: Dict[str, Any], resource_id: str, extraction_timestamp: datetime) -> Dict[str, Any]:
    """
    Flattens and normalizes a single raw JSON record into a standardized dictionary.
    Handles nested structures, data type coercions, and schema drift.
    """
    # Defensive copy & preservation of raw payload for Snowflake VARIANT ingestion
    raw_payload_json = json.dumps(raw_record, sort_keys=True)

    # Normalize remaining_lease (present from 2015 onwards; None in earlier epochs)
    remaining_lease_val = raw_record.get("remaining_lease")
    if remaining_lease_val is not None:
        remaining_lease_val = str(remaining_lease_val).strip()
        if not remaining_lease_val:
            remaining_lease_val = None

    # Cast numerical values with safe fallback
    try:
        floor_area_sqm = float(raw_record.get("floor_area_sqm", 0.0))
    except (ValueError, TypeError):
        floor_area_sqm = 0.0

    try:
        resale_price = float(raw_record.get("resale_price", 0.0))
    except (ValueError, TypeError):
        resale_price = 0.0

    try:
        lease_commence_date = int(raw_record.get("lease_commence_date", 0))
    except (ValueError, TypeError):
        lease_commence_date = 0

    source_id = raw_record.get("_id")
    if source_id is not None:
        try:
            source_id = int(source_id)
        except (ValueError, TypeError):
            source_id = None

    surrogate_key = compute_surrogate_key(raw_record)

    flattened = {
        "_record_hash": surrogate_key,
        "month": str(raw_record.get("month", "")).strip(),
        "town": str(raw_record.get("town", "")).strip().upper(),
        "flat_type": str(raw_record.get("flat_type", "")).strip().upper(),
        "block": str(raw_record.get("block", "")).strip(),
        "street_name": str(raw_record.get("street_name", "")).strip().upper(),
        "storey_range": str(raw_record.get("storey_range", "")).strip().upper(),
        "floor_area_sqm": floor_area_sqm,
        "flat_model": str(raw_record.get("flat_model", "")).strip(),
        "lease_commence_date": lease_commence_date,
        "remaining_lease": remaining_lease_val,
        "resale_price": resale_price,
        "_source_id": source_id,
        "_resource_id": resource_id,
        "_extracted_at_utc": extraction_timestamp,
        "_raw_payload": raw_payload_json,
    }

    return flattened


class HDBResaleTransformer:
    """Transforms raw API records into strongly typed, deduplicated PyArrow Tables."""

    def __init__(self, resource_id: str):
        self.resource_id = resource_id

    def transform(self, raw_records: List[Dict[str, Any]]) -> Tuple[pa.Table, Dict[str, Any]]:
        """
        Transforms and flattens a list of raw records.
        Deduplicates by surrogate key and returns PyArrow Table with lineage metrics.
        """
        extraction_ts = datetime.now(timezone.utc)
        flattened_list: List[Dict[str, Any]] = []
        seen_hashes = set()
        duplicate_count = 0

        for r in raw_records:
            item = flatten_record(r, self.resource_id, extraction_ts)
            rec_hash = item["_record_hash"]
            if rec_hash in seen_hashes:
                duplicate_count += 1
                continue
            seen_hashes.add(rec_hash)
            flattened_list.append(item)

        if duplicate_count > 0:
            logger.warning(f"Deduplicated {duplicate_count} identical records during transformation.")

        if not flattened_list:
            # Create an empty table respecting the schema
            empty_dict = {f.name: [] for f in STANDARD_PYARROW_SCHEMA}
            table = pa.Table.from_pydict(empty_dict, schema=STANDARD_PYARROW_SCHEMA)
            return table, {"input_rows": 0, "output_rows": 0, "duplicates_removed": duplicate_count}

        df = pd.DataFrame(flattened_list)
        
        # Ensure timestamp column is UTC timezone-aware for PyArrow conversion
        df["_extracted_at_utc"] = pd.to_datetime(df["_extracted_at_utc"], utc=True)

        table = pa.Table.from_pandas(df, schema=STANDARD_PYARROW_SCHEMA, preserve_index=False)

        metrics = {
            "input_rows": len(raw_records),
            "output_rows": table.num_rows,
            "duplicates_removed": duplicate_count,
            "columns": table.column_names,
        }

        return table, metrics
