"""Prefect tasks for cl_res_empresas — thin wrappers over the pure utils."""

from pathlib import Path

from prefect import task

from pipelines.datasets.cl_res_empresas.utils import clean_all, download_all


@task
def download_res(work_dir: str) -> str:
    """Download every yearly CSV published on datos.gob.cl."""
    input_dir = Path(work_dir) / "input"
    paths = download_all(input_dir)
    print(f"downloaded {len(paths)} file(s) to {input_dir}")
    return str(input_dir)


@task
def clean_res(work_dir: str, input_dir: str) -> dict:
    """Clean every CSV and return the output paths plus the source max period.

    The returned dict maps each table slug to the path ``upload_to_gcs`` should
    take, and carries ``max_year_month`` for the source poll.
    """
    output_dir = Path(work_dir) / "output"
    counts = clean_all(input_dir, output_dir)

    years = sorted(int(k) for k in counts if k.isdigit())
    max_year = years[-1]

    # The source poll compares against Coverage.DateTimeRange, which is
    # month-granular, so the max period must be too.
    import pyarrow.parquet as pq

    latest = pq.read_table(
        output_dir / "sociedad" / f"ano={max_year}" / "data.parquet",
        columns=["mes"],
    ).column("mes")
    max_month = max(int(v.as_py()) for v in latest if v.as_py() is not None)

    total = counts["sociedad_total"]
    print(
        f"cleaned {total:,} rows; source max period {max_year}-{max_month:02d}"
    )

    return {
        "sociedad": str(output_dir / "sociedad"),
        "dicionario": str(output_dir / "dicionario"),
        "max_year_month": f"{max_year}-{max_month:02d}",
    }
