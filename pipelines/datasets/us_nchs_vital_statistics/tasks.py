"""Prefect task wrappers over the pure functions in utils.py."""

from pathlib import Path

from prefect import task

from pipelines.datasets.us_nchs_vital_statistics import utils
from pipelines.datasets.us_nchs_vital_statistics.constants import constants


@task
def latest_source_year_task(product: str) -> int | None:
    """Newest final data year NCHS publishes for this product."""
    return utils.latest_source_year(product)


@task
def download_and_clean_task(product: str, year: int, data_dir: str) -> int:
    """Download one product-year and write its parquet partition. Returns rows."""
    root = Path(data_dir)
    zip_path = utils.download_year(product, year, root / "input")
    layouts = utils.load_layouts()
    return utils.write_year(
        utils.parse_year(product, year, zip_path, layouts),
        product,
        year,
        root / "output",
    )


@task
def write_dicionario_task(data_dir: str) -> int:
    return utils.write_dicionario(
        constants.CODE_DIR.value, Path(data_dir) / "output"
    )
