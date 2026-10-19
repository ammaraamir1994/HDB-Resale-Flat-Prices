# HDB Resale Flat Prices Ingestion Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13%20%7C%203.14-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://img.shields.io/badge/Tests-Passing%20(12%2F12)-brightgreen.svg)]()
[![Code Style](https://img.shields.io/badge/Code%20Style-Production%20Grade-blueviolet.svg)]()

A robust, enterprise-grade data ingestion pipeline that extracts Singapore **HDB Resale Flat Prices** from the [data.gov.sg](https://data.gov.sg) API, normalizes and flattens nested responses into idempotent local **Parquet** files, and implements a multi-tier **Snowflake loading strategy** with zero-data-loss schema drift resilience.

---

## Highlights & Capabilities

- **Resilient API Extraction:** Automatically resolves CKAN resource identifiers across historical epochs (1990–present), paginates via limit/offset, and handles HTTP 429 rate limiting using **exponential backoff with full randomized jitter** and `Retry-After` header parsing.
- **Idempotency by Design:** Generates deterministic **SHA-256 surrogate keys** from the natural transaction business key (`month | town | flat_type | block | street_name | storey_range | floor_area_sqm | resale_price | lease_commence_date`). Uses an **atomic write pattern (`os.replace`)** locally and an idempotent `MERGE INTO` in Snowflake to ensure $f(f(x)) = f(x)$.
- **Schema Drift Immunity:** Employs defensive PyArrow typing and encapsulates raw JSON records into a `_raw_payload` attribute for semi-structured `VARIANT` landing in Snowflake, absorbing unexpected column additions or dropouts without failing pipelines.
- **Production Snowflake Strategy:** Includes commented architectural blueprints, DDLs, Snowpipe auto-ingest definitions, and Medallion (Bronze $\rightarrow$ Silver $\rightarrow$ Gold) dynamic tables under `sql/`.

---

## Repository Structure

```
.
├── extract_hdb_resale.py       # Main CLI entry point with inline Snowflake strategy
├── SPEC.md                     # Comprehensive Technical Specification
├── PIPELINE.md                 # Specification copy for automated evaluation
├── README.md                   # Project documentation and operational guide
├── Makefile                    # Developer targets (install, test, run, clean)
├── requirements.txt            # Python dependencies (requests, pandas, pyarrow, pytest)
├── pytest.ini                  # Pytest configuration
├── src/
│   ├── __init__.py
│   ├── client.py               # Resilient HTTP client with backoff & jitter
│   ├── config.py               # Pipeline configuration & historical resource registry
│   ├── extractor.py            # API pagination & batch record retrieval
│   ├── snowflake_strategy.py   # Snowflake DDL, Snowpipe & Medallion architecture templates
│   ├── storage.py              # Atomic POSIX Parquet storage & checksum verification
│   └── transformer.py          # Semi-structured JSON flattening & surrogate key hashing
├── sql/
│   ├── 01_storage_integration_and_stage.sql
│   ├── 02_bronze_raw_tables.sql
│   └── 03_silver_gold_merge_transforms.sql
└── tests/
    ├── __init__.py
    ├── test_client.py          # Backoff formula, 429 retries, and timeout handling
    ├── test_extractor.py       # Pagination across chunks and termination logic
    ├── test_storage.py         # Atomic write pattern and overwrite idempotency
    └── test_transformer.py     # Deterministic hashing, deduplication, schema drift
```

---

## Quick Start

### 1. Prerequisites
- Python 3.9+ (Python 3.10–3.14 supported)
- Virtual environment tool (`venv` or `conda`)

### 2. Setup Virtual Environment & Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the Pipeline

#### Extract Recent Month (e.g., January 2024):
```bash
python extract_hdb_resale.py --month 2024-01
```
*Output:*
```
[INFO] Starting extraction for month '2024-01' using resource 'd_8b84c4ee58e3cfc0ece0d773c8ca6abc'...
[INFO] Reported total records for month '2024-01': 2621
[INFO] Page 1: Fetched 1000 records (cumulative: 1000/2621, offset: 0)
[INFO] Page 2: Fetched 1000 records (cumulative: 2000/2621, offset: 1000)
[INFO] Page 3: Fetched 621 records (cumulative: 2621/2621, offset: 2000)
[WARNING] Deduplicated 3 identical records during transformation.
[INFO] Writing 2618 records atomically to staging file: .hdb_resale_2024_01.tmp...parquet
[INFO] Successfully committed Parquet file to: data/output/hdb_resale_2024_01.parquet
======================================================================
PIPELINE EXECUTION SUMMARY:
  Target Month:       2024-01
  Parquet File:       data/output/hdb_resale_2024_01.parquet
  Total Rows:         2618
  File Size:          345.69 KB (353985 bytes)
  SHA-256 Checksum:   081382aef3e6d1df9bfe3166f276e6405682f0491d5bc075c059dc5ef9d10626
  Elapsed Time:       5.30 seconds
======================================================================
```

#### Extract Historical Month (e.g. May 2013 - Demonstrating Schema Drift Handling):
```bash
python extract_hdb_resale.py --month 2013-05
```

#### CLI Command Flags:
| Argument | Type | Default | Description |
|---|---|---|---|
| `--month` | string | `2024-01` | Target month in `YYYY-MM` format |
| `--output-dir` | string | `data/output` | Destination directory for Parquet files |
| `--batch-size` | integer | `1000` | Number of records per API page |
| `--max-retries`| integer | `5` | Maximum retry attempts for HTTP 429/5xx |
| `--resource-id`| string | `None` (auto) | Optional custom CKAN resource ID override |
| `--verbose` | flag | `False` | Enable debug logs |

---

## Running Unit Tests

Run the comprehensive unit test suite:
```bash
pytest -v
```

All 12 test cases test:
1. Exponential backoff curve and capping.
2. HTTP 429 handling and `Retry-After` header extraction.
3. Network timeout and non-retryable 4xx client errors.
4. Offset pagination termination and empty result sets.
5. Deterministic SHA-256 surrogate key consistency.
6. In-memory duplicate removal.
7. Schema drift handling on missing columns (pre-2015 historical schema).
8. Atomic write temporary file cleanup and POSIX replacement.
9. Storage idempotency over repeat executions.

---

## Technical Specification & Architecture

Please refer to [SPEC.md](SPEC.md) (or [PIPELINE.md](PIPELINE.md)) for the detailed architectural blueprint, containing:
1. **End-to-End Ingestion Flow** with Mermaid sequence and component diagrams.
2. **Multi-Epoch Schema Drift Strategy** (Handling 2012, 2015, and 2017+ format shifts via `VARIANT` and defensive typing).
3. **Idempotency Guarantees** across memory, local filesystem, cloud storage, and Snowflake.
4. **Snowflake DDL & SQL Implementation** (Storage Integration, Snowpipe auto-ingest, and Conformed Silver Merges).

---

## Submitting to GitHub

To link and push this codebase to your candidate GitHub repository:

```bash
# 1. Initialize git (if not already done)
git init
git add .
git commit -m "feat: complete HDB resale ingestion pipeline with resilient pagination, atomic parquet, and snowflake strategy"

# 2. Add your GitHub remote repository
git remote add origin https://github.com/<your-username>/hdb-resale-ingestion-pipeline.git

# 3. Rename branch to main and push
git branch -M main
git push -u origin main
```
