"""Prefect 3 tasks for us_sec_edgar — thin wrappers over utils.py."""

import os

import basedosdados as bd
from prefect import task

from pipelines.datasets.us_sec_edgar.constants import constants
from pipelines.datasets.us_sec_edgar.utils import (
    build_dicionario,
    clean_quarter,
    download_quarter,
    latest_source_quarter,
    merge_observed,
    observed_from_rows,
)

DATASET_ID = constants.DATASET_ID.value


@task(retries=2, retry_delay_seconds=60)
def resolve_latest_quarter() -> dict:
    """Find the newest quarter the SEC actually serves.

    Not the newest one it *links*: the landing page lags the files, so this
    goes through `latest_source_quarter`, which probes forward by URL. See
    `list_source_quarters`.

    Returns:
        ``{"year": int, "quarter": int, "max_date": "YYYY-MM"}``. ``max_date``
        is the quarter's last month, matching how `read_max_date` reads a
        year/quarter column (``DATE(year, quarter * 3, 1)``), so the source
        poll compares like with like against the table's coverage.
    """
    year, quarter = latest_source_quarter()
    return {
        "year": year,
        "quarter": quarter,
        "max_date": f"{year}-{quarter * 3:02d}",
    }


@task(retries=2, retry_delay_seconds=60)
def download_and_clean(work_dir: str, year: int, quarter: int) -> dict:
    """Download one quarterly ZIP and write its partitioned parquet.

    Only the new quarter is cleaned; earlier quarters already sit in the
    staging bucket, which is why the upload appends rather than overwrites.

    Args:
        work_dir: Directory to work in; input lands in ``<work_dir>/input`` and
            output under ``<work_dir>/output``.
        year: Release year.
        quarter: Release quarter, 1-4.

    Returns:
        A mapping of table slug to its partitioned output directory, plus
        ``"counts"`` (rows written per table) and ``"observed"`` (the distinct
        dictionary-covered values seen in this quarter, as JSON-safe lists).
    """
    input_dir = os.path.join(work_dir, "input")
    output_dir = os.path.join(work_dir, "output")
    os.makedirs(input_dir, exist_ok=True)

    zip_path = download_quarter(year, quarter, input_dir)
    observed: dict = {}
    counts = clean_quarter(
        zip_path, year, quarter, output_dir, observed=observed
    )
    os.remove(zip_path)

    result: dict[str, object] = {
        table: os.path.join(output_dir, table)
        for table in constants.SOURCE_FILES.value.values()
    }
    result["counts"] = counts
    # Prefect serializes task results, so the tuple keys and sets have to go.
    result["observed"] = [
        {"id_tabela": table, "nome_coluna": column, "chaves": sorted(values)}
        for (table, column), values in sorted(observed.items())
    ]
    print(
        f"{year}Q{quarter}: "
        + ", ".join(f"{t}={c:,}" for t, c in counts.items())
    )
    return result


@task
def build_dicionario_task(
    work_dir: str, observed: list, billing_project_id: str
) -> str:
    """Rebuild the dictionary as the union of the published one and this quarter.

    The published table is the accumulated record of every code seen so far;
    this quarter may add new ones. Rebuilding from the quarter alone would drop
    codes last seen years ago and break `custom_dictionary_coverage` on the
    older partitions.

    Args:
        work_dir: Same directory used by :func:`download_and_clean`.
        observed: The ``"observed"`` list from :func:`download_and_clean`.
        billing_project_id: GCP project to bill the read of the current table.

    Returns:
        The dictionary's output directory.
    """
    fresh = {
        (row["id_tabela"], row["nome_coluna"]): set(row["chaves"])
        for row in observed
    }
    try:
        frame = bd.read_sql(
            f"select id_tabela, nome_coluna, chave "
            f"from `{billing_project_id}.{DATASET_ID}.dicionario`",
            billing_project_id=billing_project_id,
            from_file=True,
        )
        published = [
            {str(k): str(v) for k, v in record.items()}
            for record in frame.to_dict("records")
        ]
    except Exception as error:
        # First run, or the table is not materialized yet in this project.
        print(
            f"could not read the published dicionario ({error}); rebuilding from this quarter only"
        )
        published: list[dict[str, str]] = []

    merged = merge_observed(observed_from_rows(published), fresh)
    output_dir = os.path.join(work_dir, "output")
    rows = build_dicionario(output_dir, merged)
    print(
        f"dicionario: {rows:,} rows ({len(published):,} published + this quarter)"
    )
    return os.path.join(output_dir, "dicionario")
