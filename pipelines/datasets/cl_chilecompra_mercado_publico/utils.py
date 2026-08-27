"""Pure download and cleaning helpers for cl_chilecompra_mercado_publico.

No Prefect imports here: the one-shot onboarding bootstrap under
``models/cl_chilecompra_mercado_publico/code/`` imports these same functions, so the
transform lives in exactly one place.

Source: ChileCompra bulk downloads, one ZIP per month per container.

    https://transparenciachc.blob.core.windows.net/oc-da/<year>-<month>.zip
    https://transparenciachc.blob.core.windows.net/lic-da/<year>-<month>.zip

The files are MONTHLY, not semestral -- ``<month>`` runs 1..12.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE_URL = "https://transparenciachc.blob.core.windows.net"
CONTAINER = {"orden_compra": "oc-da", "licitacion": "lic-da"}

ARCHITECTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "models"
    / "cl_chilecompra_mercado_publico"
    / "code"
    / "architecture"
)

# The source is Windows-1252, not Latin-1: bytes 0x92-0x97 are smart quotes and dashes,
# which decode to control characters under Latin-1. A few files also carry 0x81, which
# is undefined in cp1252, hence errors="replace".
ENCODING = "cp1252"
ENCODING_ERRORS = "replace"

# Rows per parquet row group. See write_partitioned.
ROW_GROUP_SIZE = 50_000

# Values the publisher uses to mean "absent".
NULL_TOKENS = {"", " ", "NA", "N/A", "NULL", "null", "-"}
NULL_DATE = "1900-01-01"

# Header spellings that changed across eras. ChileCompra stripped accents from the
# licitaciones header in 2015, but inconsistently: "Adquisición" lost its accent to a
# plain "o", while "genérico" had the "e" dropped entirely. Neither accent-folding nor
# accent-deletion reconciles both, so the mapping is explicit. Keys are the 2007-2014
# spellings; values are the canonical names used in the architecture CSVs.
HEADER_ALIASES = {
    "Tipo de Adquisición": "Tipo de Adquisicion",
    "Moneda Adquisición": "Moneda Adquisicion",
    "Nombre producto genérico": "Nombre producto genrico",
    "Nombre línea Adquisición": "Nombre linea Adquisicion",
    "Descripción línea Adquisición": "Descripcion linea Adquisicion",
}

# Deliberately excluded from every table.
#
# lic-da/2014-3 and lic-da/2014-4 -- and only those two of the 236 licitaciones files --
# carry nine extra columns naming individual public officials: their RUT, name, job
# title, email and telephone. They are fully populated there (about 4,000 distinct
# people across 1.5M rows each) and absent everywhere else, so including them would add
# personal data that is 99% null and that the procurement record does not otherwise
# publish. Chile's Ley 19.628 governs this kind of data and ChileCompra's own terms of
# use invoke it.
#
# This is an explicit decision, not an oversight: any source column that is neither in
# an architecture table nor listed here raises in _read_csv_from_zip, so a column the
# publisher adds later cannot be dropped silently.
EXCLUDED_COLUMNS = {
    "RutUsuario",
    "CodigoUsuario",
    "NombreUsuario",
    "CargoUsuario",
    "NombreResponsablePago",
    "EmailResponsablePago",
    "NombreResponsableContrato",
    "EmailResponsableContrato",
    "FonoResponsableContrato",
}

# Which table each licitaciones column belongs to was determined empirically, by
# measuring how many distinct values each column takes within one tender and within one
# tender-item. See PLAN.md section 8.
PARTITION_SOURCE = {
    "orden_compra_item": "FechaEnvio",
    "licitacion_item": "FechaPublicacion",
    "licitacion_oferta": "FechaPublicacion",
}

TABLES_BY_KIND = {
    "orden_compra": ["orden_compra_item"],
    "licitacion": ["licitacion_item", "licitacion_oferta"],
}

PRIMARY_KEYS = {
    "orden_compra_item": ["codigo_orden_compra", "id_item"],
    "licitacion_item": ["codigo_licitacion", "codigo_item"],
    "licitacion_oferta": [
        "codigo_licitacion",
        "codigo_item",
        "codigo_proveedor",
        "nombre_oferta",
    ],
}


def read_architecture(table: str) -> pd.DataFrame:
    """Return the architecture CSV for ``table``: the single source of truth for
    column order, types and the mapping back to the source header."""
    return pd.read_csv(ARCHITECTURE_DIR / f"{table}.csv", dtype=str).fillna("")


def month_url(kind: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{CONTAINER[kind]}/{year}-{month}.zip"


def head_month(
    kind: str, year: int, month: int, timeout: int = 60
) -> dict | None:
    """HEAD one blob. Returns None when the month does not exist (404).

    ``last_modified`` and ``etag`` are what the recurring pipeline diffs against its
    stored state: ChileCompra rewrites *old* months retroactively, so "only refresh the
    current month" would miss real revisions.
    """
    r = requests.head(month_url(kind, year, month), timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return {
        "kind": kind,
        "year": year,
        "month": month,
        "bytes": int(r.headers.get("Content-Length", 0)),
        "last_modified": r.headers.get("Last-Modified", ""),
        "etag": r.headers.get("ETag", "").strip('"'),
    }


def download_month(kind: str, year: int, month: int, dest_dir: Path) -> Path:
    """Stream one monthly ZIP to disk. Returns the local path."""
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"{kind}_{year}-{month:02d}.zip"
    tmp = out.with_suffix(".zip.part")
    with requests.get(
        month_url(kind, year, month), stream=True, timeout=900
    ) as r:
        r.raise_for_status()
        with open(tmp, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    tmp.rename(out)
    return out


def _read_csv_from_zip(path: Path) -> pd.DataFrame:
    """Read the single CSV inside a monthly ZIP.

    Everything arrives as string; typing happens in the dbt model. The parser must be a
    real CSV parser -- quoted fields contain both embedded ``;`` and embedded newlines.
    """
    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if len(names) != 1:
            raise ValueError(
                f"{path.name}: expected exactly one CSV, found {names}"
            )
        with zf.open(names[0]) as raw:
            text = io.TextIOWrapper(
                raw, encoding=ENCODING, errors=ENCODING_ERRORS, newline=""
            )
            df = pd.read_csv(
                text,
                sep=";",
                quotechar='"',
                dtype=str,
                keep_default_na=False,
                na_values=[],
                engine="c",
                low_memory=False,
            )
    df.columns = [HEADER_ALIASES.get(c.strip(), c.strip()) for c in df.columns]
    _assert_columns_known(df.columns, path.name)
    return df


def _known_source_columns() -> set[str]:
    known: set[str] = set()
    for tables in TABLES_BY_KIND.values():
        for table in tables:
            arch = read_architecture(table)
            known |= {s for s in arch["original_name"] if s}
    return known | EXCLUDED_COLUMNS


def _assert_columns_known(columns, filename: str) -> None:
    """Refuse to silently drop a column the architecture does not know about.

    The source has four header signatures per table across 2007-2026, and two of the
    236 licitaciones files carry columns no other file has. A quiet projection onto the
    architecture would hide the next such change instead of surfacing it.
    """
    unknown = sorted(set(columns) - _known_source_columns())
    if unknown:
        raise ValueError(
            f"{filename}: source columns not present in any architecture table and not "
            f"in EXCLUDED_COLUMNS: {unknown}. Add them to the architecture or to "
            f"EXCLUDED_COLUMNS before loading this month."
        )


def _clean_strings(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace, and map the publisher's null tokens to NA.

    Source values carry trailing spaces (``"Región del Maule "``), which would otherwise
    split one region into two categories.
    """
    out = s.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    return out.mask(out.isin(NULL_TOKENS))


def _to_numeric_text(s: pd.Series) -> pd.Series:
    """Normalise the decimal comma to a point, keeping the result as text.

    Staging is all-STRING by house convention and the dbt model does the ``safe_cast``;
    this only fixes the separator so that ``safe_cast`` can succeed. ``892,5`` must not
    become ``8925``.
    """
    out = _clean_strings(s)
    # Only the comma is translated. Verified across every numeric column of a full
    # month: the source never uses a dot as a thousands separator, so stripping dots
    # would silently corrupt any point-decimal that did appear. Scientific notation
    # ("1,5e+07") survives the swap and safe_cast reads it as a float.
    return out.str.replace(",", ".", regex=False)


def _to_date_text(s: pd.Series) -> pd.Series:
    """Return ISO date text, mapping the 1900-01-01 sentinel to NA."""
    out = _clean_strings(s)
    out = out.str.slice(0, 10)
    out = out.mask(out == NULL_DATE)
    return out.mask(~out.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False))


def _sort_semicolon_list(s: pd.Series) -> pd.Series:
    """Order the tokens of a ``;``-separated list.

    ChileCompra emits the same evaluation criteria for one tender in different orders
    across rows, which otherwise manufactures duplicate tender-item rows.
    """

    def _one(v):
        if pd.isna(v):
            return v
        parts = sorted(p.strip() for p in str(v).split(";") if p.strip())
        return "; ".join(parts) if parts else pd.NA

    return s.map(_one).astype("string")


def build_table(raw: pd.DataFrame, table: str) -> pd.DataFrame:
    """Project one raw monthly frame onto one architecture table.

    Columns absent from this era's file are created empty, so every month yields the
    same schema regardless of which of the three header signatures it came from.
    """
    arch = read_architecture(table)
    out = pd.DataFrame(index=raw.index)

    for _, col in arch.iterrows():
        name, src, btype = (
            col["name"],
            col["original_name"],
            col["bigquery_type"],
        )
        if name in ("ano", "mes"):
            continue
        if not src or src not in raw.columns:
            out[name] = pd.Series(pd.NA, index=raw.index, dtype="string")
            continue
        s = raw[src]
        if btype == "DATE":
            out[name] = _to_date_text(s)
        elif btype in ("INT64", "FLOAT64"):
            out[name] = _to_numeric_text(s)
        elif name == "criterios_evaluacion":
            out[name] = _sort_semicolon_list(_clean_strings(s))
        else:
            out[name] = _clean_strings(s)

    part_src = PARTITION_SOURCE[table]
    part = (
        _to_date_text(raw[part_src])
        if part_src in raw.columns
        else pd.Series(pd.NA, index=raw.index, dtype="string")
    )
    out.insert(0, "mes", part.str.slice(5, 7))
    out.insert(0, "ano", part.str.slice(0, 4))

    keys = PRIMARY_KEYS[table]
    out = out.drop_duplicates(subset=keys)
    return out.reindex(columns=list(arch["name"]))


def clean_month(kind: str, zip_path: Path) -> dict[str, pd.DataFrame]:
    """Turn one monthly ZIP into one frame per table it feeds."""
    raw = _read_csv_from_zip(Path(zip_path))
    return {t: build_table(raw, t) for t in TABLES_BY_KIND[kind]}


def arrow_schema(table: str) -> pa.Schema:
    """All-STRING arrow schema in architecture order.

    Both upload paths -- the one-shot onboarding upload and the pipeline's
    ``upload_to_gcs`` -- must write all-STRING parquet. They share one staging dataset,
    so a typed external table left behind by one collides with the other's overwrite.
    The schema carries column order, not types; the dbt model does the casting.
    """
    arch = read_architecture(table)
    return pa.schema(
        [(n, pa.string()) for n in arch["name"] if n not in ("ano", "mes")]
    )


def write_partitioned(
    df: pd.DataFrame, table: str, output_dir: Path
) -> list[Path]:
    """Write hive-partitioned parquet: ``<table>/ano=YYYY/mes=MM/data.parquet``.

    Cast to string through arrow rather than ``astype(str)``: the latter renders NULL as
    the literal "nan", which ``safe_cast`` will not turn back into NULL.
    """
    output_dir = Path(output_dir)
    schema = arrow_schema(table)
    written = []
    df = df[df["ano"].notna() & df["mes"].notna()]
    for (ano, mes), part in df.groupby(["ano", "mes"], sort=True):
        target = output_dir / table / f"ano={ano}" / f"mes={mes}"
        target.mkdir(parents=True, exist_ok=True)
        body = part.drop(columns=["ano", "mes"])
        arrays = [
            pa.array(body[f.name].astype("string"), type=pa.string())
            for f in schema
        ]
        pq.write_table(
            pa.Table.from_arrays(arrays, schema=schema),
            target / "data.parquet",
            compression="snappy",
            # Bounded row groups keep gcs.dump_header cheap: it builds the staging
            # table by reading row group 0 of the first parquet it walks into, so one
            # giant row group there is an out-of-memory risk on the worker.
            row_group_size=ROW_GROUP_SIZE,
        )
        written.append(target / "data.parquet")
    return written
