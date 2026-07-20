#!/usr/bin/env python3
"""
run_pipeline.py
================
Production-grade ETL pipeline for IDOP sensor readings.

Pipeline stages
---------------
1. extract()   - read raw data from SOURCE_DATA_PATH
2. validate()  - run the Great Expectations-style quality suite; halt on failure
3. transform() - clean, standardize, and enrich the data
4. load()      - idempotently write the result into the target SQLite table

Run:
    python run_pipeline.py

Configuration is loaded from a `.env` file (see .env.example) via python-dotenv.
Every run appends a structured entry to `pipeline.log` (path configurable),
recording start time, end time, row counts at each stage, and any errors.
"""

from __future__ import annotations

import os
import sqlite3
import sys
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from validation import run_suite

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()  # populates os.environ from .env if present

BASE_DIR = Path(__file__).resolve().parent

SOURCE_DATA_PATH = BASE_DIR / os.getenv("SOURCE_DATA_PATH", "data/source/raw_sensor_data.csv")
TARGET_DB_PATH = BASE_DIR / os.getenv("TARGET_DB_PATH", "data/processed/warehouse.db")
TARGET_TABLE = os.getenv("TARGET_TABLE", "sensor_readings")
GE_SUITE_PATH = BASE_DIR / os.getenv("GE_SUITE_PATH", "great_expectations/expectations/sensor_data_suite.json")

LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "pipeline.log")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

HALT_ON_VALIDATION_FAILURE = os.getenv("HALT_ON_VALIDATION_FAILURE", "true").lower() == "true"

MIN_TEMPERATURE_C = float(os.getenv("MIN_TEMPERATURE_C", -40))
MAX_TEMPERATURE_C = float(os.getenv("MAX_TEMPERATURE_C", 100))

# ---------------------------------------------------------------------------
# Logging setup - writes to both pipeline.log (file) and stdout (console)
# ---------------------------------------------------------------------------
logger = logging.getLogger("etl_pipeline")
logger.setLevel(LOG_LEVEL)
logger.handlers.clear()

_formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
logger.addHandler(_console_handler)


class PipelineError(Exception):
    """Raised when a pipeline stage cannot proceed."""


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract(source_path: Path) -> pd.DataFrame:
    """Read raw data from the source CSV. Raises PipelineError if unreadable."""
    logger.info("EXTRACT: reading source file %s", source_path)
    if not source_path.exists():
        raise PipelineError(f"Source file not found: {source_path}")

    try:
        df = pd.read_csv(source_path)
    except Exception as exc:
        raise PipelineError(f"Failed to read source file: {exc}") from exc

    logger.info("EXTRACT: read %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# VALIDATE (Data Quality Gate)
# ---------------------------------------------------------------------------
def validate(df: pd.DataFrame, suite_path: Path) -> None:
    """
    Run the Great Expectations-style suite against the raw extract.
    Halts the pipeline (raises PipelineError) if any expectation fails
    and HALT_ON_VALIDATION_FAILURE is true.
    """
    logger.info("VALIDATE: running data quality suite from %s", suite_path)
    report = run_suite(df, str(suite_path))

    for line in report.summary().splitlines():
        logger.info(line) if report.success else logger.warning(line)

    if not report.success:
        if HALT_ON_VALIDATION_FAILURE:
            raise PipelineError(
                "Data quality validation FAILED - halting pipeline before load. "
                "See pipeline.log for the full expectation report."
            )
        logger.warning(
            "Data quality validation FAILED but HALT_ON_VALIDATION_FAILURE is false; "
            "continuing anyway."
        )
    else:
        logger.info("VALIDATE: all expectations passed")


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and enrich the raw extract:
      - drop exact duplicate rows
      - drop rows with a null sensor_id or reading_id (already caught by
        validation, but transform is defensive in case validation was
        configured to warn-only)
      - clip out-of-range readings is NOT done here (we never silently alter
        physical measurements) - instead we flag them
      - parse timestamp, add a derived temperature_f column and a
        human-readable ingestion timestamp
    """
    before = len(df)
    logger.info("TRANSFORM: starting with %d rows", before)

    df = df.drop_duplicates()
    df = df.dropna(subset=["reading_id", "sensor_id"])

    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df = df.dropna(subset=["timestamp"])

    df["temperature_f"] = (df["temperature_c"] * 9 / 5) + 32
    df["is_out_of_range"] = ~df["temperature_c"].between(MIN_TEMPERATURE_C, MAX_TEMPERATURE_C)
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    after = len(df)
    logger.info(
        "TRANSFORM: finished with %d rows (%d dropped as unusable)",
        after, before - after,
    )
    return df


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------
def load(df: pd.DataFrame, db_path: Path, table: str) -> int:
    """
    Idempotent load: the target table is fully cleared before the new batch
    is inserted, so re-running the pipeline on the same (or a corrected)
    source file never produces duplicate rows. reading_id is enforced as a
    PRIMARY KEY as a second layer of duplicate protection.
    """
    logger.info("LOAD: writing %d rows to %s (table=%s)", len(df), db_path, table)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # idempotency: clear before load
        conn.commit()

        df.to_sql(table, conn, if_exists="replace", index=False)

        # Enforce reading_id uniqueness at the database level going forward.
        cur.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_reading_id "
                    f"ON {table}(reading_id)")
        conn.commit()

        row_count = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    finally:
        conn.close()

    logger.info("LOAD: table %s now contains %d rows", table, row_count)
    return row_count


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------
def run() -> int:
    start_time = datetime.now(timezone.utc)
    logger.info("=" * 70)
    logger.info("PIPELINE START: %s", start_time.isoformat())

    exit_code = 0
    try:
        raw_df = extract(SOURCE_DATA_PATH)
        validate(raw_df, GE_SUITE_PATH)
        clean_df = transform(raw_df)
        loaded_rows = load(clean_df, TARGET_DB_PATH, TARGET_TABLE)

        logger.info("PIPELINE SUCCESS: extracted=%d, loaded=%d", len(raw_df), loaded_rows)

    except PipelineError as exc:
        logger.error("PIPELINE HALTED: %s", exc)
        exit_code = 1
    except Exception as exc:  # noqa: BLE001 - top-level safety net, always logged
        logger.exception("PIPELINE CRASHED with an unexpected error: %s", exc)
        exit_code = 2
    finally:
        end_time = datetime.now(timezone.utc)
        logger.info("PIPELINE END: %s (duration: %s)", end_time.isoformat(), end_time - start_time)
        logger.info("=" * 70)

    return exit_code


if __name__ == "__main__":
    sys.exit(run())
