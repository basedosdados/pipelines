"""Pure download and cleaning functions for the us_ed_nces_ccd pipeline.

No Prefect imports. The transform itself is **not** reimplemented here: it lives
in ``models/us_ed_nces_ccd/code/utils.py`` alongside the column specs in
``schema.py``, and this module imports it, so the recurring pipeline and the
one-shot onboarding bootstrap can never drift apart.
"""

from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

from pipelines.datasets.us_ed_nces_ccd.constants import constants

CODE_DIR: Path = constants.CODE_DIR.value


def _load(name: str) -> ModuleType:
    """Import a module from models/us_ed_nces_ccd/code/ by path.

    That directory is not a package (it holds one-shot bootstrap scripts, not
    importable library code), so it is loaded by file path rather than added to
    the package tree. `schema` must be importable as a bare name because
    `utils` there does `import schema as S`.
    """
    if name in sys.modules:
        return sys.modules[name]
    spec = importlib.util.spec_from_file_location(
        name, CODE_DIR / f"{name}.py"
    )
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"cannot load {name} from {CODE_DIR}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def load_modules() -> tuple[ModuleType, ModuleType]:
    """Return the shared (schema, transform) modules, importing them once."""
    schema = _load("schema")
    transform = _load("utils")
    return schema, transform


def source_max_year(input_dir: Path) -> int:
    """Latest school year present in the school directory extract.

    Drives the source poll: the portal republishes the whole panel each release,
    so "is there anything new" is "does the extract now reach a later year".
    """
    _schema, transform = load_modules()
    con = transform.connect()
    try:
        path = input_dir / transform.BULK_FILES["school"]
        row = con.execute(
            f"select max(cast(try_cast(year as double) as int)) "
            f"from {transform._read(path)}"
        ).fetchone()
    finally:
        con.close()
    if row is None or row[0] is None:
        raise ValueError(f"no usable year column in {path}")
    return int(row[0])


def download_directories(input_dir: Path) -> Path:
    """Fetch the two directory extracts, which the poll and three tables need."""
    _, transform = load_modules()
    input_dir.mkdir(parents=True, exist_ok=True)
    for slug in ("school", "school_district"):
        transform.download(
            transform.BULK_FILES[slug], input_dir / transform.BULK_FILES[slug]
        )
    return input_dir


def clean_year(input_dir: Path, output_dir: Path, year: int) -> dict[str, str]:
    """Rebuild the four refreshable tables for a single school year.

    Only one year is built. The portal ships the full panel, but every prior
    year is already materialized and unchanged, so rebuilding them would move
    tens of gigabytes to write the same rows. `upload_to_gcs` runs in
    ``dump_mode="append"``, which adds the new Hive partition without touching
    the others.
    """
    schema, transform = load_modules()
    con = transform.connect()
    out: dict[str, str] = {}
    try:
        school_csv = input_dir / transform.BULK_FILES["school"]
        lea_csv = input_dir / transform.BULK_FILES["school_district"]

        transform.clean_wide_table(
            con, schema.TABLE_SCHOOL, school_csv, output_dir, year
        )
        out["school"] = str(output_dir / "school")

        transform.clean_wide_table(
            con, schema.TABLE_DISTRICT, lea_csv, output_dir, year
        )
        out["school_district"] = str(output_dir / "school_district")

        transform.clean_staff(con, lea_csv, output_dir, year)
        out["staff"] = str(output_dir / "staff")

        enrollment_csv = transform.fetch_enrollment(year)
        transform.clean_enrollment(con, enrollment_csv, output_dir, year)
        out["school_enrollment"] = str(output_dir / "school_enrollment")
    finally:
        con.close()
    return out


def rebuild_dictionary(output_dir: Path) -> str:
    """Rewrite the value dictionary from the committed architecture CSV."""
    _, transform = load_modules()
    con = transform.connect()
    try:
        transform.write_dictionary(
            con,
            CODE_DIR / "architecture" / "dicionario_values.csv",
            output_dir,
        )
    finally:
        con.close()
    return str(output_dir / "dicionario")


def finance_table_spec():
    """The district_finance table spec, built from the source header."""
    schema, transform = load_modules()
    header = next(
        csv.reader(
            (transform.input_dir() / "districts_ccd_finance.csv").open(
                encoding="utf-8"
            )
        )
    )
    labels = {
        x["variable"]: x["label"]
        for x in json.loads((CODE_DIR / "varlist_29.json").read_text())
    }
    return schema.finance_table(header, labels)
