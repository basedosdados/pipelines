"""BEA API client + pure transforms for us_bea — re-export shim.

The implementation moved to ``pipelines.datasets.us_bea.utils`` so the recurring
pipeline and this one-shot bootstrap share a single copy (DRY). This module
re-exports the names the bootstrap historically used, so ``from . import bea``
keeps working unchanged.
"""

from __future__ import annotations

from pipelines.datasets.us_bea.utils import (
    BASE,
    MISSING_TOKENS,
    BEAError,
    call,
    clean_value,
    get_data,
    line_from_code,
    norm_gi_quarter,
    param_values,
    param_values_filtered,
    split_time_period,
)

__all__ = [
    "BASE",
    "MISSING_TOKENS",
    "BEAError",
    "call",
    "clean_value",
    "get_data",
    "line_from_code",
    "norm_gi_quarter",
    "param_values",
    "param_values_filtered",
    "split_time_period",
]


if __name__ == "__main__":
    # smoke test
    print(
        "datasets:",
        [d["DatasetName"] for d in call("GetDataSetList")["Dataset"]][:3],
        "...",
    )
    print(
        "clean_value tests:",
        clean_value("1,234.5"),
        clean_value("(NA)"),
        clean_value("(D)"),
        clean_value(""),
    )
    print(
        "split_time_period:",
        split_time_period("2020"),
        split_time_period("2020Q3"),
        split_time_period("2020M07"),
    )
    print(
        "line_from_code:",
        line_from_code("SAGDP2N-1"),
        line_from_code("CAINC1-30"),
    )
