#!/usr/bin/env python3
"""
run_pipeline.py
===============
Complete, executable ETL entrypoint for the Week 4 sensor-data pipeline.

Pipeline stages
---------------
1. extract()   - read the source CSV and confirm the required schema exists.
2. validate()  - run the repository's modular validation interpreter.
3. transform() - clean types, remove unusable rows, resolve duplicate keys,
                 enrich measurements, and prepare warehouse-ready records.
4. load()      - load through a staging table and transactionally UPSERT into
                 SQLite using reading_id as the business key.

Idempotency
-----------
The target table enforces a UNIQUE business key on ``reading_id``. The load
stage uses SQLite UPSERT semantics, so running the same input repeatedly does
not create duplicate warehouse rows. Existing rows with the same reading_id
are updated; new reading_ids are inserted.

Safety
------
All target changes are executed inside a transaction. Data is first inserted
into a temporary staging table and checked for null/duplicate business keys.
If staging, validation, transformation, or the merge fails, the transaction is
rolled back and the previously committed target data remains intact.

Configuration
-------------
Values can come from .env and may be overridden by command-line arguments.
See .env.example in the repository for the normal defaults.

Typical use:
    python run_pipeline.py

Optional overrides:
    python run_pipeline.py \
        --source data/source/raw_sensor_data.csv \
        --suite great_expectations/expectations/sensor_data_suite.json \
        --db data/processed/warehouse.db \
        --table sensor_readings
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd
from dotenv import load_dotenv

from validation import run_suite


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REQUIRED_COLUMNS = (
    "reading_id",
    "sensor_id",
    "timestamp",
    "temperature_c",
    "pressure_kpa",
    "humidity_pct",
)

LOAD_COLUMNS = (
    "reading_id",
    "sensor_id",
    "timestamp",
    "temperature_c",
    "pressure_kpa",
    "humidity_pct",
    "temperature_f",
    "is_out_of_range",
    "ingested_at",
)

BASE_DIR = Path(__file__).resolve().parent


# ---------------------------------------------------------------------------
# Errors and configuration
# ---------------------------------------------------------------------------
class PipelineError(RuntimeError):
    """Raised for an expected ETL failure that should stop the pipeline safely."""


@dataclass(frozen=True)
class Settings:
    source_path: Path
    suite_path: Path
    db_path: Path
    table: str
    log_file: Path
    log_level: str
    halt_on_validation_failure: bool
    min_temperature_c: float
    max_temperature_c: float
    min_pressure_kpa: float
    max_pressure_kpa: float
    min_humidity_pct: float
    max_humidity_pct: float


def _resolve_path(value: str) -> Path:
    path = Path(value).expanduser()
    if path.is_absolute():
        return path
    return (BASE_DIR / path).resolve()


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    raise PipelineError(
        f"Environment variable {name} must be true/false, yes/no, 1/0, or on/off; "
        f"received {raw!r}."
    )


def _safe_sql_name(name: str) -> str:
    """Return a quoted SQLite identifier after strict allow-list validation."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise PipelineError(
            f"Unsafe SQL identifier {name!r}. Use only letters, digits, and "
            "underscores, and do not start with a digit."
        )
    return f'"{name}"'


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the complete Week 4 sensor ETL pipeline."
    )
    parser.add_argument("--source", help="Source CSV path")
    parser.add_argument("--suite", help="Validation-suite JSON path")
    parser.add_argument("--db", help="SQLite warehouse database path")
    parser.add_argument("--table", help="SQLite target table name")
    parser.add_argument("--log-file", help="Pipeline log-file path")
    return parser.parse_args(argv)


def build_settings(args: argparse.Namespace) -> Settings:
    load_dotenv(BASE_DIR / ".env")

    source = args.source or os.getenv(
        "SOURCE_DATA_PATH", "data/source/raw_sensor_data.csv"
    )
    suite = args.suite or os.getenv(
        "GE_SUITE_PATH",
        "great_expectations/expectations/sensor_data_suite.json",
    )
    database = args.db or os.getenv(
        "TARGET_DB_PATH", "data/processed/warehouse.db"
    )
    table = args.table or os.getenv("TARGET_TABLE", "sensor_readings")
    log_file = args.log_file or os.getenv("LOG_FILE", "pipeline.log")

    # Validate the configured SQL identifier before any database work occurs.
    _safe_sql_name(table)

    return Settings(
        source_path=_resolve_path(source),
        suite_path=_resolve_path(suite),
        db_path=_resolve_path(database),
        table=table,
        log_file=_resolve_path(log_file),
        log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
        halt_on_validation_failure=_env_bool(
            "HALT_ON_VALIDATION_FAILURE", True
        ),
        min_temperature_c=float(os.getenv("MIN_TEMPERATURE_C", "-40")),
        max_temperature_c=float(os.getenv("MAX_TEMPERATURE_C", "100")),
        min_pressure_kpa=float(os.getenv("MIN_PRESSURE_KPA", "0")),
        max_pressure_kpa=float(os.getenv("MAX_PRESSURE_KPA", "300")),
        min_humidity_pct=float(os.getenv("MIN_HUMIDITY_PCT", "0")),
        max_humidity_pct=float(os.getenv("MAX_HUMIDITY_PCT", "100")),
    )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
def configure_logging(settings: Settings) -> logging.Logger:
    settings.log_file.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("week4_etl")
    logger.handlers.clear()
    logger.propagate = False
    logger.setLevel(getattr(logging, settings.log_level, logging.INFO))

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(settings.log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ---------------------------------------------------------------------------
# Shared validation helpers
# ---------------------------------------------------------------------------
def _assert_required_columns(df: pd.DataFrame) -> None:
    missing = [column for column in REQUIRED_COLUMNS if column not in df.columns]
    if missing:
        raise PipelineError(
            "Source data is missing required column(s): " + ", ".join(missing)
        )


def _assert_transformed_columns(df: pd.DataFrame) -> None:
    missing = [column for column in LOAD_COLUMNS if column not in df.columns]
    if missing:
        raise PipelineError(
            "Transformed data is missing load column(s): " + ", ".join(missing)
        )


# ---------------------------------------------------------------------------
# EXTRACT
# ---------------------------------------------------------------------------
def extract(source_path: Path, logger: logging.Logger) -> pd.DataFrame:
    """Extract source records from CSV without mutating the input file."""
    logger.info("EXTRACT: reading %s", source_path)

    if not source_path.exists():
        raise PipelineError(f"Source file does not exist: {source_path}")
    if not source_path.is_file():
        raise PipelineError(f"Source path is not a file: {source_path}")

    try:
        df = pd.read_csv(source_path)
    except (OSError, UnicodeError, pd.errors.ParserError) as exc:
        raise PipelineError(f"Unable to read source CSV: {exc}") from exc

    # Header whitespace is common in manually prepared CSVs. Normalize it once.
    df.columns = [str(column).strip() for column in df.columns]
    _assert_required_columns(df)

    logger.info("EXTRACT: %d rows, %d columns", len(df), len(df.columns))
    return df


# ---------------------------------------------------------------------------
# VALIDATE
# ---------------------------------------------------------------------------
def validate(
    df: pd.DataFrame,
    suite_path: Path,
    halt_on_failure: bool,
    logger: logging.Logger,
) -> None:
    """
    Run the repository's modular validation interpreter against the raw batch.

    The pipeline halts before transformation/loading when the suite fails and
    HALT_ON_VALIDATION_FAILURE is enabled. In warn-only mode, failures are
    logged and transformation is still allowed to apply its defensive cleaning.
    """
    logger.info("VALIDATE: evaluating suite %s", suite_path)

    if not suite_path.exists():
        raise PipelineError(f"Validation suite does not exist: {suite_path}")
    if not suite_path.is_file():
        raise PipelineError(f"Validation suite path is not a file: {suite_path}")

    try:
        report = run_suite(df, str(suite_path))
    except Exception as exc:
        raise PipelineError(f"Validation interpreter failed: {exc}") from exc

    summary = report.summary()
    for line in summary.splitlines():
        if report.success:
            logger.info("VALIDATE: %s", line)
        else:
            logger.warning("VALIDATE: %s", line)

    if report.success:
        logger.info("VALIDATE: PASSED")
        return

    if halt_on_failure:
        raise PipelineError(
            "Validation FAILED and HALT_ON_VALIDATION_FAILURE=true. "
            "No warehouse changes were attempted."
        )

    logger.warning(
        "VALIDATE: FAILED, but warn-only mode is enabled; continuing with "
        "defensive transformation."
    )


# ---------------------------------------------------------------------------
# TRANSFORM
# ---------------------------------------------------------------------------
def transform(
    df: pd.DataFrame,
    settings: Settings,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Transform raw sensor records into warehouse-ready rows.

    Rules
    -----
    * trim identifier whitespace and treat blank IDs as missing;
    * parse timestamps as UTC;
    * coerce the three measurements to numeric values;
    * remove exact duplicate rows;
    * remove rows missing any required key/timestamp/measurement;
    * resolve duplicate reading_id values by keeping the latest timestamp;
    * calculate temperature_f;
    * flag measurements outside configured physical ranges;
    * stamp the batch with one UTC ingestion timestamp;
    * return a deterministic column order and row order.

    Measurements outside the configured ranges are flagged, not silently
    clipped or changed.
    """
    _assert_required_columns(df)
    out = df.copy(deep=True)
    input_rows = len(out)
    logger.info("TRANSFORM: starting with %d rows", input_rows)

    # Normalize identifiers.
    for column in ("reading_id", "sensor_id"):
        out[column] = out[column].astype("string").str.strip()
        out.loc[out[column].eq(""), column] = pd.NA

    # Normalize timestamps and measurements. Invalid parses become NaT/NaN and
    # are removed by the required-value gate below.
    out["timestamp"] = pd.to_datetime(out["timestamp"], errors="coerce", utc=True)
    for column in ("temperature_c", "pressure_kpa", "humidity_pct"):
        out[column] = pd.to_numeric(out[column], errors="coerce")

    exact_duplicate_count = int(out.duplicated().sum())
    if exact_duplicate_count:
        logger.warning(
            "TRANSFORM: removing %d exact duplicate row(s)",
            exact_duplicate_count,
        )
        out = out.drop_duplicates().copy()

    required_non_null = list(REQUIRED_COLUMNS)
    unusable_mask = out[required_non_null].isna().any(axis=1)
    unusable_count = int(unusable_mask.sum())
    if unusable_count:
        logger.warning(
            "TRANSFORM: removing %d unusable row(s) with invalid/missing "
            "required values",
            unusable_count,
        )
        out = out.loc[~unusable_mask].copy()

    if out.empty:
        raise PipelineError("Transformation produced zero usable rows")

    # A business key must map to one warehouse row. If warn-only validation lets
    # duplicates through, keep the most recent event deterministically.
    duplicate_key_mask = out.duplicated(subset=["reading_id"], keep=False)
    duplicate_key_rows = int(duplicate_key_mask.sum())
    if duplicate_key_rows:
        duplicate_key_count = int(
            out.loc[duplicate_key_mask, "reading_id"].nunique()
        )
        logger.warning(
            "TRANSFORM: %d row(s) contain %d duplicate reading_id value(s); "
            "keeping the latest timestamp for each key",
            duplicate_key_rows,
            duplicate_key_count,
        )
        out = (
            out.sort_values(["reading_id", "timestamp"], kind="stable")
            .drop_duplicates(subset=["reading_id"], keep="last")
            .copy()
        )

    out["temperature_f"] = (out["temperature_c"] * 9.0 / 5.0) + 32.0

    in_range = (
        out["temperature_c"].between(
            settings.min_temperature_c,
            settings.max_temperature_c,
            inclusive="both",
        )
        & out["pressure_kpa"].between(
            settings.min_pressure_kpa,
            settings.max_pressure_kpa,
            inclusive="both",
        )
        & out["humidity_pct"].between(
            settings.min_humidity_pct,
            settings.max_humidity_pct,
            inclusive="both",
        )
    )
    out["is_out_of_range"] = (~in_range).astype("int64")

    batch_timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    out["ingested_at"] = batch_timestamp

    # Store timestamps in an unambiguous portable ISO-8601 UTC representation.
    out["timestamp"] = out["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    out = (
        out.loc[:, list(LOAD_COLUMNS)]
        .sort_values(["timestamp", "reading_id"], kind="stable")
        .reset_index(drop=True)
    )

    logger.info(
        "TRANSFORM: produced %d rows (%d removed from extracted batch)",
        len(out),
        input_rows - len(out),
    )
    return out


# ---------------------------------------------------------------------------
# LOAD helpers
# ---------------------------------------------------------------------------
def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


def _create_target_table(conn: sqlite3.Connection, table: str) -> None:
    table_sql = _safe_sql_name(table)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_sql} (
            reading_id       TEXT PRIMARY KEY,
            sensor_id        TEXT NOT NULL,
            timestamp        TEXT NOT NULL,
            temperature_c    REAL NOT NULL,
            pressure_kpa     REAL NOT NULL,
            humidity_pct     REAL NOT NULL,
            temperature_f    REAL NOT NULL,
            is_out_of_range  INTEGER NOT NULL CHECK (is_out_of_range IN (0, 1)),
            ingested_at      TEXT NOT NULL
        )
        """
    )


def _assert_target_schema(conn: sqlite3.Connection, table: str) -> None:
    """
    Fail safely if an existing target does not match the expected schema.

    The pipeline deliberately refuses destructive, implicit schema migration.
    That protects previously committed warehouse data from being silently
    reshaped by an application-code change.
    """
    table_sql = _safe_sql_name(table)
    rows = conn.execute(f"PRAGMA table_info({table_sql})").fetchall()
    actual_columns = {row[1] for row in rows}
    missing = [column for column in LOAD_COLUMNS if column not in actual_columns]
    if missing:
        raise PipelineError(
            "Existing target table has an incompatible schema; missing: "
            + ", ".join(missing)
            + ". No data was deleted. Migrate the schema explicitly before rerunning."
        )


def _create_staging_table(conn: sqlite3.Connection, staging_table: str) -> None:
    staging_sql = _safe_sql_name(staging_table)
    conn.execute(f"DROP TABLE IF EXISTS temp.{staging_sql}")
    conn.execute(
        f"""
        CREATE TEMP TABLE {staging_sql} (
            reading_id       TEXT NOT NULL,
            sensor_id        TEXT NOT NULL,
            timestamp        TEXT NOT NULL,
            temperature_c    REAL NOT NULL,
            pressure_kpa     REAL NOT NULL,
            humidity_pct     REAL NOT NULL,
            temperature_f    REAL NOT NULL,
            is_out_of_range  INTEGER NOT NULL CHECK (is_out_of_range IN (0, 1)),
            ingested_at      TEXT NOT NULL
        )
        """
    )


def _rows_for_sqlite(df: pd.DataFrame) -> Iterable[tuple]:
    # Convert pandas/numpy scalar types to regular Python values for sqlite3.
    for row in df.loc[:, list(LOAD_COLUMNS)].itertuples(index=False, name=None):
        yield tuple(value.item() if hasattr(value, "item") else value for value in row)


# ---------------------------------------------------------------------------
# LOAD
# ---------------------------------------------------------------------------
def load(
    df: pd.DataFrame,
    db_path: Path,
    table: str,
    logger: logging.Logger,
) -> dict[str, int]:
    """
    Transactionally stage and UPSERT transformed records into SQLite.

    Idempotency mechanism
    ---------------------
    ``reading_id`` is the target PRIMARY KEY. The merge uses
    ``ON CONFLICT(reading_id) DO UPDATE``. Re-running the same batch therefore
    changes the existing logical rows instead of inserting duplicates.

    Safety mechanism
    ----------------
    The batch is first loaded into a TEMP staging table and checked for null and
    duplicate business keys. Target-table creation/schema validation, staging,
    and UPSERT happen inside one transaction. Any exception triggers ROLLBACK.
    """
    if df.empty:
        raise PipelineError("LOAD: refusing to load an empty transformed batch")
    _assert_transformed_columns(df)
    _safe_sql_name(table)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    table_sql = _safe_sql_name(table)
    staging_table = f"{table}__staging"
    staging_sql = _safe_sql_name(staging_table)

    logger.info(
        "LOAD: opening %s and staging %d row(s) for table %s",
        db_path,
        len(df),
        table,
    )

    try:
        conn = sqlite3.connect(db_path, timeout=30)
    except sqlite3.Error as exc:
        raise PipelineError(f"LOAD: unable to open SQLite database: {exc}") from exc

    try:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA busy_timeout = 30000")

        try:
            conn.execute("BEGIN IMMEDIATE")

            if not _table_exists(conn, table):
                _create_target_table(conn, table)
            _assert_target_schema(conn, table)

            # A unique index is redundant for a correctly created PRIMARY KEY,
            # but makes the idempotency constraint explicit when inspecting DB DDL.
            index_sql = _safe_sql_name(f"ux_{table}_reading_id")
            conn.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_sql} "
                f"ON {table_sql}(reading_id)"
            )

            _create_staging_table(conn, staging_table)

            placeholders = ", ".join(["?"] * len(LOAD_COLUMNS))
            column_sql = ", ".join(_safe_sql_name(c) for c in LOAD_COLUMNS)
            conn.executemany(
                f"INSERT INTO {staging_sql} ({column_sql}) VALUES ({placeholders})",
                _rows_for_sqlite(df),
            )

            staged_count = int(
                conn.execute(f"SELECT COUNT(*) FROM {staging_sql}").fetchone()[0]
            )
            if staged_count != len(df):
                raise PipelineError(
                    f"LOAD: staging row-count mismatch: expected {len(df)}, "
                    f"found {staged_count}"
                )

            null_keys = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {staging_sql} WHERE reading_id IS NULL "
                    "OR TRIM(reading_id) = ''"
                ).fetchone()[0]
            )
            if null_keys:
                raise PipelineError(
                    f"LOAD: staging contains {null_keys} null/blank reading_id value(s)"
                )

            duplicate_keys = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT reading_id FROM {staging_sql} "
                    "GROUP BY reading_id HAVING COUNT(*) > 1"
                    ")"
                ).fetchone()[0]
            )
            if duplicate_keys:
                raise PipelineError(
                    f"LOAD: staging contains {duplicate_keys} duplicate reading_id "
                    "value(s); target was not changed"
                )

            existing_matches = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {staging_sql} s "
                    f"JOIN {table_sql} t ON t.reading_id = s.reading_id"
                ).fetchone()[0]
            )
            changed_matches = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM {staging_sql} s "
                    f"JOIN {table_sql} t ON t.reading_id = s.reading_id "
                    "WHERE t.sensor_id IS NOT s.sensor_id "
                    "OR t.timestamp IS NOT s.timestamp "
                    "OR t.temperature_c IS NOT s.temperature_c "
                    "OR t.pressure_kpa IS NOT s.pressure_kpa "
                    "OR t.humidity_pct IS NOT s.humidity_pct "
                    "OR t.temperature_f IS NOT s.temperature_f "
                    "OR t.is_out_of_range IS NOT s.is_out_of_range"
                ).fetchone()[0]
            )
            inserted = staged_count - existing_matches
            updated = changed_matches
            unchanged = existing_matches - changed_matches

            # SQLite's parser can confuse ON in INSERT...SELECT with a JOIN clause.
            # WHERE 1=1 removes that ambiguity before the UPSERT clause.
            merge_sql = f"""
                INSERT INTO {table_sql} ({column_sql})
                SELECT {column_sql}
                FROM {staging_sql}
                WHERE 1 = 1
                ON CONFLICT(reading_id) DO UPDATE SET
                    sensor_id       = excluded.sensor_id,
                    timestamp       = excluded.timestamp,
                    temperature_c   = excluded.temperature_c,
                    pressure_kpa    = excluded.pressure_kpa,
                    humidity_pct    = excluded.humidity_pct,
                    temperature_f   = excluded.temperature_f,
                    is_out_of_range = excluded.is_out_of_range,
                    ingested_at     = excluded.ingested_at
                WHERE {table_sql}.sensor_id IS NOT excluded.sensor_id
                   OR {table_sql}.timestamp IS NOT excluded.timestamp
                   OR {table_sql}.temperature_c IS NOT excluded.temperature_c
                   OR {table_sql}.pressure_kpa IS NOT excluded.pressure_kpa
                   OR {table_sql}.humidity_pct IS NOT excluded.humidity_pct
                   OR {table_sql}.temperature_f IS NOT excluded.temperature_f
                   OR {table_sql}.is_out_of_range IS NOT excluded.is_out_of_range
            """
            conn.execute(merge_sql)

            final_total = int(
                conn.execute(f"SELECT COUNT(*) FROM {table_sql}").fetchone()[0]
            )
            duplicate_target_keys = int(
                conn.execute(
                    f"SELECT COUNT(*) FROM ("
                    f"SELECT reading_id FROM {table_sql} "
                    "GROUP BY reading_id HAVING COUNT(*) > 1"
                    ")"
                ).fetchone()[0]
            )
            if duplicate_target_keys:
                raise PipelineError(
                    "LOAD: post-merge duplicate business keys detected; rolling back"
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
        "LOAD: committed successfully | staged=%d inserted=%d updated=%d "
        "unchanged=%d total=%d",
        staged_count,
        inserted,
        updated,
        unchanged,
        final_total,
    )
    return {
        "staged": staged_count,
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "target_total": final_total,
    }


# ---------------------------------------------------------------------------
# ORCHESTRATION
# ---------------------------------------------------------------------------
def run_pipeline(settings: Settings, logger: logging.Logger) -> int:
    """Run extract -> validate -> transform -> load exactly once."""
    started_at = datetime.now(timezone.utc)
    logger.info("=" * 78)
    logger.info("PIPELINE START: %s", started_at.isoformat())
    logger.info("SOURCE: %s", settings.source_path)
    logger.info("DATABASE: %s | TABLE: %s", settings.db_path, settings.table)

    extracted_rows = 0
    transformed_rows = 0
    load_stats: dict[str, int] = {
        "staged": 0,
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "target_total": 0,
    }

    try:
        raw_df = extract(settings.source_path, logger)
        extracted_rows = len(raw_df)

        validate(
            raw_df,
            settings.suite_path,
            settings.halt_on_validation_failure,
            logger,
        )

        transformed_df = transform(raw_df, settings, logger)
        transformed_rows = len(transformed_df)

        load_stats = load(
            transformed_df,
            settings.db_path,
            settings.table,
            logger,
        )

        logger.info(
            "PIPELINE SUCCESS: extracted=%d transformed=%d staged=%d "
            "inserted=%d updated=%d unchanged=%d target_total=%d",
            extracted_rows,
            transformed_rows,
            load_stats["staged"],
            load_stats["inserted"],
            load_stats["updated"],
            load_stats["unchanged"],
            load_stats["target_total"],
        )
        return 0

    except PipelineError as exc:
        logger.error("PIPELINE FAILED SAFELY: %s", exc)
        return 1
    except Exception as exc:
        logger.exception("PIPELINE CRASHED: %s", exc)
        return 2
    finally:
        ended_at = datetime.now(timezone.utc)
        logger.info(
            "PIPELINE END: %s | duration=%s | extracted=%d transformed=%d",
            ended_at.isoformat(),
            ended_at - started_at,
            extracted_rows,
            transformed_rows,
        )
        logger.info("=" * 78)


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    try:
        args = parse_args(argv)
        settings = build_settings(args)
        logger = configure_logging(settings)
    except Exception as exc:
        # Configuration failures can occur before the normal logger is ready.
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    return run_pipeline(settings, logger)


if __name__ == "__main__":
    sys.exit(main())

# END OF FILE — run_pipeline.py is complete and intentionally ends here.
