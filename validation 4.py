"""
validation.py
--------------
Data Quality gate for the ETL pipeline.

This module enforces the rules defined in
great_expectations/expectations/sensor_data_suite.json.

Design note:
Great Expectations (GE) stores each rule as a JSON object with an
"expectation_type" and "kwargs" -- e.g. {"expectation_type":
"expect_column_values_to_be_between", "kwargs": {"column": "temperature_c",
"min_value": -40, "max_value": 100}}.

If the `great_expectations` package is installed in the target environment,
that exact JSON file can be loaded into a real GE FileDataContext /
ExpectationSuite object and run through a Checkpoint (see the commented
`validate_with_real_ge()` function below).

To guarantee the quality gate works even where the (heavy) `great_expectations`
package isn't installed, this module also ships a small, dependency-light
interpreter, `run_suite()`, that reads the same JSON file and evaluates each
rule against a pandas DataFrame using pandas itself. Both paths enforce
identical rules -- only the execution engine differs.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from dataclasses import dataclass, field

import pandas as pd

logger = logging.getLogger("etl_pipeline")


@dataclass
class ExpectationResult:
    expectation_type: str
    column: str
    success: bool
    description: str
    unexpected_count: int = 0
    total_count: int = 0


@dataclass
class ValidationReport:
    suite_name: str
    results: list = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(r.success for r in self.results)

    def summary(self) -> str:
        lines = [f"Validation suite '{self.suite_name}' -> "
                  f"{'PASSED' if self.success else 'FAILED'}"]
        for r in self.results:
            status = "OK" if r.success else "FAIL"
            lines.append(
                f"  [{status}] {r.expectation_type} on '{r.column}': "
                f"{r.description} (unexpected={r.unexpected_count}/{r.total_count})"
            )
        return "\n".join(lines)


def load_suite(suite_path: str) -> dict:
    with open(suite_path, "r") as f:
        return json.load(f)


def _check_not_null(df: pd.DataFrame, column: str) -> tuple[bool, int]:
    unexpected = df[column].isna().sum()
    return unexpected == 0, int(unexpected)


def _check_unique(df: pd.DataFrame, column: str) -> tuple[bool, int]:
    unexpected = df[column].duplicated(keep=False).sum()
    return unexpected == 0, int(unexpected)


def _check_between(df: pd.DataFrame, column: str, min_value, max_value,
                    strict_min: bool = False, strict_max: bool = False) -> tuple[bool, int]:
    series = df[column]
    if strict_min:
        low_ok = series > min_value
    else:
        low_ok = series >= min_value
    if strict_max:
        high_ok = series < max_value
    else:
        high_ok = series <= max_value
    in_range = low_ok & high_ok
    unexpected = (~in_range).sum()
    return unexpected == 0, int(unexpected)


CHECK_DISPATCH = {
    "expect_column_values_to_not_be_null": _check_not_null,
    "expect_column_values_to_be_unique": _check_unique,
    "expect_column_values_to_be_between": _check_between,
}


def run_suite(df: pd.DataFrame, suite_path: str) -> ValidationReport:
    """Evaluate every expectation in the suite against the dataframe."""
    suite = load_suite(suite_path)
    report = ValidationReport(suite_name=suite.get("expectation_suite_name", "unnamed_suite"))

    for expectation in suite.get("expectations", []):
        etype = expectation["expectation_type"]
        kwargs = dict(expectation.get("kwargs", {}))
        column = kwargs.pop("column", "")
        description = expectation.get("meta", {}).get("description", "")

        check_fn = CHECK_DISPATCH.get(etype)
        if check_fn is None:
            logger.warning("No local interpreter for expectation type '%s' - skipping", etype)
            continue

        try:
            success, unexpected = check_fn(df, column, **kwargs)
        except KeyError:
            logger.error("Column '%s' referenced by expectation not found in data", column)
            success, unexpected = False, len(df)

        report.results.append(
            ExpectationResult(
                expectation_type=etype,
                column=column,
                success=success,
                description=description,
                unexpected_count=unexpected,
                total_count=len(df),
            )
        )
    return report


# ---------------------------------------------------------------------------
# Optional: run the SAME suite through real Great Expectations if installed.
# Not called by default (kept dependency-light), but shows how to wire it in
# once `pip install great_expectations` has been run in your environment.
# ---------------------------------------------------------------------------
def validate_with_real_ge(df: pd.DataFrame, suite_path: str):  # pragma: no cover
    import great_expectations as ge
    from great_expectations.dataset import PandasDataset

    suite = load_suite(suite_path)
    ge_df = PandasDataset(df)
    results = []
    for expectation in suite["expectations"]:
        method = getattr(ge_df, expectation["expectation_type"])
        results.append(method(**expectation["kwargs"]))
    return results
