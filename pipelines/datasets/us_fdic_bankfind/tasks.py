"""Prefect 3 tasks for us_fdic_bankfind — thin wrappers over utils.py."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
from prefect import task

from pipelines.datasets.us_fdic_bankfind import utils
from pipelines.datasets.us_fdic_bankfind.constants import constants
from pipelines.datasets.us_fdic_bankfind.institution_spec import SPEC

CODE_DIR = Path(constants.CODE_DIR.value)
ARCHITECTURE_DIR = Path(constants.ARCHITECTURE_DIR.value)


def _columns(table: str) -> list[str]:
    with (ARCHITECTURE_DIR / f"{table}.csv").open() as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def _catalog() -> dict:
    return json.loads((CODE_DIR / "indicator_catalog.json").read_text())


def _write(frame: pd.DataFrame, table: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        utils.to_string_table(frame, _columns(table)),
        path,
        compression="snappy",
    )


@task(retries=2, retry_delay_seconds=60)
def source_max_report_date() -> str:
    """Latest quarter the FDIC has financial data for, as ``YYYYMMDD``."""
    return utils.list_report_dates()[-1]


@task(retries=2, retry_delay_seconds=60)
def build_static_tables(work_dir: str) -> dict:
    """Rebuild the institution directory and the indicator dictionary."""
    out = Path(work_dir) / "output"
    docs = Path(work_dir) / "docs"
    utils.download_docs(docs)

    raw = utils.fetch_institutions()
    extraction_date = pd.Timestamp.today().strftime("%Y-%m-%d")
    _write(
        utils.clean_institutions(raw, SPEC, extraction_date),
        "institution",
        out / "institution/data.parquet",
    )

    catalog = {
        k: v for k, v in _catalog().items() if v["source_type"] == "number"
    }
    names = json.loads((CODE_DIR / "wide_column_names.json").read_text())
    indicator = pd.DataFrame(
        [
            {
                "indicator_id": code,
                "name": record["name"],
                "description": record["description"],
                "measurement_unit": (
                    "USD"
                    if record["unit_of_measure"] == "USD_thousand"
                    else record["unit_of_measure"]
                ),
                "is_ratio": record["is_ratio"],
                "is_quarterly": record["is_quarterly"],
                "is_flag": record["is_flag"],
                "financials_column": names.get(code, ""),
            }
            for code, record in sorted(catalog.items())
        ]
    )
    _write(indicator, "indicator", out / "indicator/data.parquet")
    return {
        "institution": str(out / "institution"),
        "indicator": str(out / "indicator"),
    }


@task(retries=2, retry_delay_seconds=60)
def build_recent_quarters(work_dir: str) -> dict:
    """Rebuild the trailing window of quarters into hive partitions."""
    out = Path(work_dir) / "output"
    docs = Path(work_dir) / "docs"
    utils.download_docs(docs)

    catalog = _catalog()
    names = json.loads((CODE_DIR / "wide_column_names.json").read_text())
    batches = utils.financial_field_batches(utils.load_field_catalog(docs))
    quarters = utils.list_report_dates()[-constants.TRAILING_QUARTERS.value :]

    for report_date in quarters:
        raw = utils.fetch_quarter(report_date, batches)
        if raw.empty:
            continue
        stamp = pd.Timestamp(report_date)
        quarter = (stamp.month - 1) // 3 + 1
        iso = stamp.strftime("%Y-%m-%d")
        suffix = f"year={stamp.year}/data_q{quarter}.parquet"

        _write(
            utils.clean_financials(raw, iso, names, catalog),
            "financials",
            out / "financials" / suffix,
        )
        long = utils.clean_financials_indicator(raw, iso, catalog)
        _write(
            long.sort_values(["indicator_id", "cert"]),
            "financials_indicator",
            out / "financials_indicator" / suffix,
        )
        del raw, long

    return {
        "financials": str(out / "financials"),
        "financials_indicator": str(out / "financials_indicator"),
        "quarters": quarters,
    }
