"""Idempotent storage layer for writing atomic Parquet files to local storage or object store staging."""

import hashlib
import logging
import os
import uuid
from pathlib import Path
from typing import Dict, Any, Tuple
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class ParquetStorageWriter:
    """
    Manages deterministic, atomic writing of Parquet files to ensure complete pipeline idempotency.

    Idempotency Guarantees:
    1. Deterministic File Naming: Every target month maps to a unique, deterministic partition path.
    2. Atomic Write Pattern: Data is initially written to a hidden temporary file (.tmp.<uuid>) on the same
       filesystem, flushed to disk, and then atomically swapped into place via os.replace().
    3. Failure Isolation: If the process crashes, times out, or is terminated mid-write, the target Parquet
       file is never corrupted or left in a partial state.
    4. Safe Re-execution: Re-running the pipeline for a given month cleanly overwrites the previous run
       with the latest deduplicated, validated dataset.
    """

    def __init__(self, base_output_dir: str = "data/output"):
        self.base_output_dir = Path(base_output_dir)
        self.base_output_dir.mkdir(parents=True, exist_ok=True)

    def get_target_path(self, month: str) -> Path:
        """
        Derives the deterministic target file path for a given month (YYYY-MM).
        Example: data/output/hdb_resale_2024_01.parquet
        """
        safe_month_str = month.replace("-", "_")
        filename = f"hdb_resale_{safe_month_str}.parquet"
        return self.base_output_dir / filename

    def write_atomic_parquet(self, table: pa.Table, month: str) -> Tuple[Path, Dict[str, Any]]:
        """
        Writes a PyArrow Table to a local Parquet file using atomic rename.

        Args:
            table: PyArrow Table containing normalized records.
            month: Target month string (YYYY-MM).

        Returns:
            Tuple of (final_path, file_metadata_dict).
        """
        target_path = self.get_target_path(month)
        temp_filename = f".{target_path.stem}.tmp.{uuid.uuid4().hex}.parquet"
        temp_path = self.base_output_dir / temp_filename

        logger.info(f"Writing {table.num_rows} records atomically to staging file: {temp_path.name}")

        try:
            # Write with SNAPPY compression and Parquet 2.6 data page format
            # Snappy provides high decompression throughput optimal for Snowflake COPY INTO
            pq.write_table(
                table,
                temp_path,
                compression="SNAPPY",
                version="2.6",
                flavor=None,
            )

            # Atomically rename temporary file to destination path
            # Under POSIX semantics, os.replace is atomic and replaces existing files seamlessly
            os.replace(temp_path, target_path)
            logger.info(f"Successfully committed Parquet file to: {target_path}")

        except Exception as e:
            # Clean up temporary artifact in case of failure
            if temp_path.exists():
                try:
                    temp_path.unlink()
                except OSError:
                    pass
            logger.error(f"Failed to write Parquet file for month '{month}': {e}")
            raise

        # Verify integrity and calculate checksum
        file_size_bytes = target_path.stat().st_size
        sha256_checksum = self._compute_file_checksum(target_path)

        # Validate that the file is readable by PyArrow
        parquet_meta = pq.read_metadata(target_path)

        metadata = {
            "file_path": str(target_path),
            "file_name": target_path.name,
            "row_count": parquet_meta.num_rows,
            "column_count": parquet_meta.num_columns,
            "file_size_bytes": file_size_bytes,
            "file_size_kb": round(file_size_bytes / 1024, 2),
            "sha256_checksum": sha256_checksum,
            "compression": "SNAPPY",
        }

        return target_path, metadata

    @staticmethod
    def _compute_file_checksum(file_path: Path) -> str:
        """Compute SHA-256 checksum of the generated Parquet file."""
        hasher = hashlib.sha256()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()
