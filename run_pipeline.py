#!/usr/bin/env python3
"""
run_pipeline.py
===============
Complete ETL pipeline for industrial IoT sensor readings.

Stages
------
1. extract()   - read the configured CSV source.
2. validate()  - execute the data-quality expectation suite.
3. transform() - clean, standardize, deduplicate, and enrich records.
4. load()      - atomically replace the SQLite target using a staging table.

Idempotency and load safety
---------------------------
The load stage does NOT delete the production table before a replacement is
ready. It first writes and verifies a staging table. Only after staging passes
row-count, null-key, and duplicate-key checks does the pipeline start a SQLite
transaction that swaps staging into the target name and adds a UNIQUE index on
reading_id. If the swap fails, SQLite rolls the transaction back, preserving
the previous target table.

Re-running the pipeline with the same source therefore produces the same set of
business keys (reading_id) without accumulating duplicate rows.

Configuration is loaded from .env (see .env.example).
Run with:
    python run_pipeline.py
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from validation import run_suite


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent


def _resolve_path(value: str) -> Path:
    """Resolve relative configuration paths from the repository root."""
    path = Path(value).expanduser()
    return path if path.is_absolute() else BASE_DIR / path


SOURCE_DATA_PATH = _resolve_path(
    os.getenv("SOURCE_DATA_PATH", "data/source/raw_sensor_data.csv")
)
TARGET_DB_PATH = _resolve_path(
    os.getenv("TARGET_DB_PATH", "data/processed/warehouse.db")
)
TARGET_TABLE = os.getenv("TARGET_TABLE", "sensor_readings")
GE_SUITE_PATH = _resolve_path(
    os.getenv(
        "GE_SUITE_PATH",
        "great_expectations/expectations/sensor_data_suite.json",
    )
)

LOG_FILE = _resolve_path(os.getenv("LOG_FILE", "pipeline.log"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
HALT_ON_VALIDATION_FAILURE = (
    os.getenv("HALT_ON_VALIDATION_FAILURE", "true").strip().lower() == "true"
)

MIN_TEMPERATURE_C = float(os.getenv("MIN_TEMPERATURE_C", "-40"))
MAX_TEMPERATURE_C = float(os.getenv("MAX_TEMPERATURE_C", "100"))
MIN_PRESSURE_KPA = float(os.getenv("MIN_PRESSURE_KPA", "0"))
MAX_PRESSURE_KPA = float(os.getenv("MAX_PRESSURE_KPA", "300"))
MIN_HUMIDITY_PCT = float(os.getenv("MIN_HUMIDITY_PCT", "0"))
MAX_HUMIDITY_PCT = float(os.getenv("MAX_HUMIDITY_PCT", "100"))

REQUIRED_COLUMNS = {
    "reading_id",
    "sensor_id",
    "timestamp",
    "temperature_c",
    "pressure_kpa",
    "humidity_pct",
}


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
logger = logging.getLogger("etl_pipeline")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.handlers.clear()
logger.propagate = False

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)


class PipelineError(Exception):
    """Raised when a pipeline stage cannot safely continue."""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _sql_identifier(name: str) -> str:
    """Validate and quote a SQLite identifier supplied by configuration."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise PipelineError(
            f"Unsafe SQL identifier {name!r}. Use letters, numbers, and underscores "
            "and do not start with a number."
        )
    return f'"{name}"'


def _assert_required_columns(df: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_COLUMNS.difference(df.columns))
    if missing:
        raise PipelineError(
            "Source data is missing required column(s): " + ", ".join(missing)
        )


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract(source_path: Path) -> pd.DataFrame:
    """Read source CSV data and verify that the expected schema is present."""
    logger.info("EXTRACT: reading source file %s", source_path)

    if not source_path.exists():
        raise PipelineError(f"Source file not found: {source_path}")
    if not source_path.is_file():
        raise PipelineError(f"Source path is not a file: {source_path}")

    try:
        df = pd.read_csv(source_path)
    except (OSError, UnicodeError, pd.errors.ParserError) as exc:
        raise PipelineError(f"Failed to read source file {source_path}: {exc}") from exc

    # Normalize header whitespace so accidental spaces do not break later stages.
    df.columns = [str(column).strip() for column in df.columns]
    _assert_required_columns(df)

    logger.info("EXTRACT: read %d rows and %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# VALIDATE
# ---------------------------------------------------------------------------
def validate(df: pd.DataFrame, suite_path: Path) -> None:
    """Run the expectation suite and enforce the configured quality gate."""
    logger.info("VALIDATE: running suite %s", suite_path)

    if not suite_path.exists():
        raise PipelineError(f"Validation suite not found: {suite_path}")

    try:
        report = run_suite(df, str(suite_path))
    except Exception as exc:  # validation errors are pipeline errors, not silent skips
        raise PipelineError(f"Validation suite could not be executed: {exc}") from exc

    for line in report.summary().splitlines():
        if report.success:
            logger.info(line)
        else:
            logger.warning(line)

    if report.success:
        logger.info("VALIDATE: all expectations passed")
        return

    if HALT_ON_VALIDATION_FAILURE:
        raise PipelineError(
            "Data quality validation FAILED. The pipeline stopped before "
            "transform/load so invalid data cannot replace the warehouse table."
        )

    logger.warning(
        "VALIDATE: failures detected, but HALT_ON_VALIDATION_FAILURE=false; "
        "continuing in warn-only mode."
    )


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean, standardize, deduplicate, and enrich extracted sensor readings.

    Transform rules:
    - normalize identifiers and numeric fields;
    - remove exact duplicate rows;
    - remove unusable rows with missing business keys/timestamps/measurements;
    - resolve duplicate reading_id values deterministically by keeping the most
      recent timestamp for that reading_id (primarily defensive for warn-only
      validation mode);
    - convert temperature Celsius to Fahrenheit;
    - flag any measurement outside configured physical ranges;
    - stamp the batch with one UTC ingestion timestamp.

    Physical measurements are never clipped or silently changed.
    """
    _assert_required_columns(df)
    transformed = df.copy(deep=True)
    before = len(transformed)
    logger.info("TRANSFORM: starting with %d rows", before)

    # Strip identifier whitespace and convert blank identifiers to null.
    for column in ("reading_id", "sensor_id"):
        transformed[column] = transformed[column].astype("string").str.strip()
        transformed.loc[transformed[column].eq(""), column] = pd.NA

    # Parse timestamp and measurements. Bad parses become NaN/NaT and are removed
    # below rather than being loaded as malformed values.
    transformed["timestamp"] = pd.to_datetime(
        transformed["timestamp"], errors="coerce", utc=True
    )
    for column in ("temperature_c", "pressure_kpa", "humidity_pct"):
        transformed[column] = pd.to_numeric(transformed[column], errors="coerce")

    exact_duplicates = int(transformed.duplicated().sum())
    if exact_duplicates:
        logger.warning(
            "TRANSFORM: removing %d exact duplicate row(s)", exact_duplicates
        )
    transformed = transformed.drop_duplicates()

    required_non_null = [
        "reading_id",
        "sensor_id",
        "timestamp",
        "temperature_c",
        "pressure_kpa",
        "humidity_pct",
    ]
    unusable_mask = transformed[required_non_null].isna().any(axis=1)
    unusable_count = int(unusable_mask.sum())
    if unusable_count:
        logger.warning(
            "TRANSFORM: dropping %d unusable row(s) with missing/invalid required values",
            unusable_count,
        )
        transformed = transformed.loc[~unusable_mask].copy()

    # Business-key deduplication. This is deterministic and protects the load
    # stage if the validation gate is intentionally configured as warn-only.
    duplicate_key_mask = transformed.duplicated(subset=["reading_id"], keep=False)
    duplicate_key_rows = int(duplicate_key_mask.sum())
    if duplicate_key_rows:
        duplicate_key_count = int(
            transformed.loc[duplicate_key_mask, "reading_id"].nunique()
        )
        logger.warning(
            "TRANSFORM: %d rows share %d duplicate reading_id value(s); "
            "keeping the latest timestamp for each reading_id",
            duplicate_key_rows,
            duplicate_key_count,
        )
        transformed = (
            transformed.sort_values(["reading_id", "timestamp"], kind="stable")
            .drop_duplicates(subset=["reading_id"], keep="last")
            .copy()
        )

    transformed["temperature_f"] = (
        transformed["temperature_c"] * 9.0 / 5.0
    ) + 32.0

    transformed["is_out_of_range"] = ~(
        transformed["temperature_c"].between(
            MIN_TEMPERATURE_C, MAX_TEMPERATURE_C, inclusive="both"
        )
        & transformed["pressure_kpa"].between(
            MIN_PRESSURE_KPA, MAX_PRESSURE_KPA, inclusive="both"
        )
        & transformed["humidity_pct"].between(
            MIN_HUMIDITY_PCT, MAX_HUMIDITY_PCT, inclusive="both"
        )
    )

    batch_ingested_at = datetime.now(timezone.utc).isoformat()
    transformed["ingested_at"] = batch_ingested_at

    # SQLite stores datetimes portably as ISO-8601 strings.
    transformed["timestamp"] = transformed["timestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    transformed = transformed.sort_values(
        ["timestamp", "reading_id"], kind="stable"
    ).reset_index(drop=True)

    after = len(transformed)
    logger.info(
        "TRANSFORM: finished with %d rows (%d removed from the extract)",
        after,
        before - after,
    )

    if transformed.empty:
        raise PipelineError("Transformation produced zero loadable rows")

    return transformed


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------
def load(df: pd.DataFrame, db_path: Path, table: str) -> int:
    """
    Load the transformed batch into SQLite using a staging-and-swap strategy.

    Idempotency:
    - every run represents a full refresh of the configured source;
    - a UNIQUE index on reading_id prevents duplicate business keys;
    - the previous target is replaced only after staging data has been written
      and verified successfully.

    Safety:
    - staging is prepared while the current production table remains untouched;
    - the DROP/RENAME/index creation happens inside one SQLite transaction;
    - on any failure, rollback keeps the previous production table intact.
    """
    if df.empty:
        raise PipelineError("LOAD: refusing to replace the target with an empty batch")
    if "reading_id" not in df.columns:
        raise PipelineError("LOAD: transformed data does not contain reading_id")

    table_sql = _sql_identifier(table)
    staging_table = f"{table}__staging"
    staging_sql = _sql_identifier(staging_table)
    unique_index = f"idx_{table}_reading_id"
    unique_index_sql = _sql_identifier(unique_index)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(
        "LOAD: staging %d rows for %s (target table=%s)",
        len(df),
        db_path,
        table,
    )

    try:
        conn = sqlite3.connect(db_path, timeout=30)
    except sqlite3.Error as exc:
        raise PipelineError(f"LOAD: unable to open SQLite database: {exc}") from exc

    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")

        # Prepare staging only. The live target remains untouched during this step.
        conn.execute(f"DROP TABLE IF EXISTS {staging_sql}")
        conn.commit()

        try:
            df.to_sql(staging_table, conn, if_exists="replace", index=False)
        except Exception as exc:
            raise PipelineError(f"LOAD: failed while writing staging table: {exc}") from exc

        staged_count = int(
            conn.execute(f"SELECT COUNT(*) FROM {staging_sql}").fetchone()[0]
        )
        if staged_count != len(df):
            raise PipelineError(
                f"LOAD: staging row-count mismatch: expected {len(df)}, got {staged_count}"
            )

        null_key_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM {staging_sql} WHERE reading_id IS NULL"
            ).fetchone()[0]
        )
        if null_key_count:
            raise PipelineError(
                f"LOAD: staging contains {null_key_count} null reading_id value(s)"
            )

        duplicate_key_count = int(
            conn.execute(
                f"SELECT COUNT(*) FROM ("
                f"SELECT reading_id FROM {staging_sql} "
                f"GROUP BY reading_id HAVING COUNT(*) > 1"
                f")"
            ).fetchone()[0]
        )
        if duplicate_key_count:
            raise PipelineError(
                "LOAD: staging contains duplicate reading_id values; "
                "production table was not modified"
            )

        conn.commit()

        # Atomic target replacement. SQLite DDL is transactional here; a failure
        # rolls this block back and preserves the previous target table.
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(f"DROP TABLE IF EXISTS {table_sql}")
            conn.execute(f"ALTER TABLE {staging_sql} RENAME TO {table_sql}")
            conn.execute(
                f"CREATE UNIQUE INDEX {unique_index_sql} "
                f"ON {table_sql}(reading_id)"
            )

            loaded_count = int(
                conn.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]
            )
            if loaded_count != len(df):
                raise PipelineError(
                    f"LOAD: final row-count mismatch: expected {len(df)}, got {loaded_count}"
                )

            conn.commit()
        except Exception:
            conn.rollback()
            raise

    except PipelineError:
        raise
    except sqlite3.Error as exc:
        raise PipelineError(f"LOAD: SQLite operation failed: {exc}") from exc
    except Exception as exc:
        raise PipelineError(f"LOAD: unexpected load failure: {exc}") from exc
    finally:
        conn.close()

    logger.info(
        "LOAD: target table %s contains %d rows; atomic replacement committed",
        table,
        loaded_count,
    )
    return loaded_count


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------
def run() -> int:
    """Execute one complete ETL run and return a process exit code."""
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 78)
    logger.info("PIPELINE START: %s", start_time.isoformat())

    extracted_rows = 0
    transformed_rows = 0
    loaded_rows = 0
    exit_code = 0

    try:
        raw_df = extract(SOURCE_DATA_PATH)
        extracted_rows = len(raw_df)

        validate(raw_df, GE_SUITE_PATH)

        clean_df = transform(raw_df)
        transformed_rows = len(clean_df)

        loaded_rows = load(clean_df, TARGET_DB_PATH, TARGET_TABLE)

        logger.info(
            "PIPELINE SUCCESS: extracted=%d transformed=%d loaded=%d",
            extracted_rows,
            transformed_rows,
            loaded_rows,
        )

    except PipelineError as exc:
        logger.error("PIPELINE HALTED: %s", exc)
        exit_code = 1
    except Exception as exc:  # top-level safety net with traceback
        logger.exception("PIPELINE CRASHED with an unexpected error: %s", exc)
        exit_code = 2
    finally:
        end_time = datetime.now(timezone.utc)
        logger.info(
            "PIPELINE END: %s | duration=%s | extracted=%d transformed=%d loaded=%d",
            end_time.isoformat(),
            end_time - start_time,
            extracted_rows,
            transformed_rows,
            loaded_rows,
        )
        logger.info("=" * 78)

    return exit_code


if __name__ == "__main__":
    sys.exit(run())
