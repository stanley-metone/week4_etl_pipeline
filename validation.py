"""validation.py - dependency-light interpreter for the GE-style expectation suite."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd


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
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return bool(self.results) and all(result.success for result in self.results)

    def summary(self) -> str:
        lines = [
            f"Validation suite '{self.suite_name}' -> "
            f"{'PASSED' if self.success else 'FAILED'}"
        ]
        for result in self.results:
            status = "OK" if result.success else "FAIL"
            lines.append(
                f"  [{status}] {result.expectation_type} on '{result.column}': "
                f"{result.description} "
                f"(unexpected={result.unexpected_count}/{result.total_count})"
            )
        return "\n".join(lines)


def load_suite(suite_path: str) -> dict:
    path = Path(suite_path)
    with path.open("r", encoding="utf-8") as handle:
        suite = json.load(handle)
    if not suite.get("expectations"):
        raise ValueError(f"Expectation suite contains no expectations: {path}")
    return suite


def _check_not_null(df: pd.DataFrame, column: str) -> tuple[bool, int]:
    unexpected = int(df[column].isna().sum())
    return unexpected == 0, unexpected


def _check_unique(df: pd.DataFrame, column: str) -> tuple[bool, int]:
    unexpected = int(df[column].duplicated(keep=False).sum())
    return unexpected == 0, unexpected


def _check_between(
    df: pd.DataFrame,
    column: str,
    min_value,
    max_value,
    strict_min: bool = False,
    strict_max: bool = False,
) -> tuple[bool, int]:
    series = pd.to_numeric(df[column], errors="coerce")
    low_ok = series > min_value if strict_min else series >= min_value
    high_ok = series < max_value if strict_max else series <= max_value
    in_range = low_ok & high_ok & series.notna()
    unexpected = int((~in_range).sum())
    return unexpected == 0, unexpected


CHECK_DISPATCH = {
    "expect_column_values_to_not_be_null": _check_not_null,
    "expect_column_values_to_be_unique": _check_unique,
    "expect_column_values_to_be_between": _check_between,
}


def run_suite(df: pd.DataFrame, suite_path: str) -> ValidationReport:
    """Evaluate every expectation in the JSON suite against a DataFrame."""
    suite = load_suite(suite_path)
    report = ValidationReport(
        suite_name=suite.get("expectation_suite_name", "unnamed_suite")
    )

    for expectation in suite.get("expectations", []):
        expectation_type = expectation["expectation_type"]
        kwargs = dict(expectation.get("kwargs", {}))
        column = kwargs.pop("column", "")
        description = expectation.get("meta", {}).get("description", "")

        check = CHECK_DISPATCH.get(expectation_type)
        if check is None:
            raise ValueError(
                f"Unsupported expectation type: {expectation_type}. "
                "Validation rules must not be silently skipped."
            )
        if column not in df.columns:
            success, unexpected = False, len(df)
        else:
            success, unexpected = check(df, column, **kwargs)

        report.results.append(
            ExpectationResult(
                expectation_type=expectation_type,
                column=column,
                success=success,
                description=description,
                unexpected_count=unexpected,
                total_count=len(df),
            )
        )

    return report
