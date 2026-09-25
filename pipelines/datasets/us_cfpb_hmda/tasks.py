"""Prefect 3 tasks for us_cfpb_hmda - thin wrappers over utils.py."""

import tempfile
from datetime import UTC, date, datetime
from pathlib import Path

from prefect import task

from pipelines.datasets.us_cfpb_hmda.constants import constants
from pipelines.datasets.us_cfpb_hmda.utils import clean_all, latest_source_year
from pipelines.utils.metadata.domain import AllFree, DateFormat, YearOnly
from pipelines.utils.stage_dispatch import CheckResult, DownloadResult


@task
def resolve_years(this_year: int) -> dict:
    """Find the latest published modern year and the full 2018..latest range.

    Args:
        this_year: Current calendar year (passed by the flow).

    Returns:
        {"max_year": int, "years": list[int]} covering FIRST_YEAR..max_year.
    """
    max_year = latest_source_year(this_year)
    years = list(range(constants.FIRST_YEAR.value, max_year + 1))
    return {"max_year": max_year, "years": years}


@task
def build_tables(work_dir: str, years: list[int]) -> dict:
    """Download + clean every modern year into all-STRING partitioned parquet.

    Years are streamed one at a time (raw CSV deleted after each clean), so peak
    disk stays near a single ~4-5 GB file.

    Args:
        work_dir: Scratch dir; input under <work_dir>/input, output under <work_dir>/output.
        years: Modern years to (re)build.

    Returns:
        {"loan_application_register": <partition dir str>, "max_year": "<YYYY>"}.
    """
    base = Path(work_dir)
    return clean_all(base / "output", years, base / "input")


# ──────────────────────────────────────────────────────────────────────────────
# loan_application_register (issue #1867)
#
# `check_for_update` (`latest_source_year`) é uma checagem de verdade leve:
# GET com stream=True, lê só os primeiros 2048 bytes do CSV por ano sondado
# (não baixa o arquivo inteiro) — diferente de br_ibge_ipca, aqui o check é
# genuinamente independente do download.
#
# `download_data` continua reconstruindo o histórico inteiro
# (FIRST_YEAR..max_year, dump_mode="overwrite") a cada run — decisão de
# design já existente (schema all-STRING consistente), não alterada aqui.
# Vários GB por ano; fica isolado no próprio pod (`job_variables` no
# `@flow` de download, não no de check_update).
# ──────────────────────────────────────────────────────────────────────────────


def check_for_update() -> CheckResult:
    this_year = datetime.now(UTC).year
    resolved = resolve_years(this_year)
    max_year = resolved["max_year"]
    return CheckResult(
        reference_date=date(max_year, 1, 1),
        extra_download_params={"years": resolved["years"]},
    )


def download_data(download_params: dict) -> DownloadResult:
    years = download_params["years"]
    work_dir = tempfile.mkdtemp(prefix="us_cfpb_hmda_")
    result = build_tables(work_dir=work_dir, years=years)
    data_path = result[constants.TABLE_ID.value]

    return DownloadResult(
        coverage=AllFree(
            date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
        ).model_dump(),
        data_path=data_path,
        bq_project="basedosdados",
        dump_mode="overwrite",
        source_format="parquet",
    )
