"""A small Spark-native expectations framework.

Rather than fight Great Expectations 1.0's Spark integration for cross-table
referential checks (which don't fit its single-batch column model), we
implement GE-style expectations directly against Spark DataFrames. Each
expectation returns a GE-shaped result (success + observed metrics), so suites
read like GE suites and results are dashboard-ready.

The depth here is in the expectation *logic* — referential integrity and
business rules specific to this schema — which is exactly what the build's
depth rule calls for.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from pyspark.sql import DataFrame


@dataclass(frozen=True)
class ExpectationResult:
    expectation: str
    table: str
    success: bool
    severity: str                 # "critical" | "warning"
    observed: dict = field(default_factory=dict)
    detail: str = ""

    def as_row(self) -> dict:
        return {
            "expectation": self.expectation,
            "table": self.table,
            "success": self.success,
            "severity": self.severity,
            "observed": self.observed,
            "detail": self.detail,
        }


class ValidationContext:
    """Holds the Silver DataFrames a suite validates, keyed by table name."""

    def __init__(self, tables: dict[str, DataFrame]):
        self._tables = tables

    def table(self, name: str) -> DataFrame:
        if name not in self._tables:
            raise KeyError(f"table '{name}' not available in this validation context")
        return self._tables[name]

    def has(self, name: str) -> bool:
        return name in self._tables


class Expectation(ABC):
    """Base class. Subclasses validate against a ValidationContext."""

    severity: str = "critical"

    @property
    def name(self) -> str:
        return type(self).__name__

    @abstractmethod
    def validate(self, ctx: ValidationContext) -> ExpectationResult:
        ...

    def _result(self, table: str, success: bool, observed: dict, detail: str = "") -> ExpectationResult:
        return ExpectationResult(
            expectation=self.name, table=table, success=success,
            severity=self.severity, observed=observed, detail=detail,
        )
