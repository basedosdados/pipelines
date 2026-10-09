"""Pure download and cleaning functions for cl_res_empresas.

No Prefect imports here: the one-shot onboarding bootstrap under
``models/cl_res_empresas/code/`` imports these same functions, so the transform
lives in exactly one place.
"""

import re
import unicodedata
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.cl_res_empresas.constants import constants


def _c(name: str):
    return constants[name].value


# --------------------------------------------------------------------------
# source discovery and download
# --------------------------------------------------------------------------
def discover_resources() -> list[dict]:
    """Return one entry per yearly CSV published in the CKAN package.

    Each entry is ``{"year": int, "url": str, "name": str}``. The package is
    republished monthly: the current year's resource is replaced in place with a
    later cut-off date, so the year parsed from the resource name is the key.
    """
    url = f"{_c('CKAN_BASE')}/package_show"
    response = requests.get(
        url,
        params={"id": _c("CKAN_PACKAGE_ID")},
        headers=_c("HEADERS"),
        timeout=120,
    )
    response.raise_for_status()
    result = response.json()["result"]

    resources = []
    for resource in result["resources"]:
        match = re.search(r"(\d{4})", resource.get("name") or "")
        if not match:
            continue
        resources.append(
            {
                "year": int(match.group(1)),
                "url": resource["url"],
                "name": resource["name"],
            }
        )
    if not resources:
        raise ValueError("no yearly CSV resources found in the CKAN package")
    return sorted(resources, key=lambda r: r["year"])


def download_all(
    input_dir: str | Path, years: list[int] | None = None
) -> list[Path]:
    """Download every yearly CSV into ``input_dir`` as ``<year>.csv``."""
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)

    paths = []
    for resource in discover_resources():
        if years and resource["year"] not in years:
            continue
        path = input_dir / f"{resource['year']}.csv"
        with requests.get(
            resource["url"], headers=_c("HEADERS"), timeout=600, stream=True
        ) as response:
            response.raise_for_status()
            with open(path, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1 << 20):
                    handle.write(chunk)
        paths.append(path)
    return paths


# --------------------------------------------------------------------------
# comuna name -> CUT code
# --------------------------------------------------------------------------
def normalize_comuna(name: str) -> str:
    """Upper-case, strip accents and fold punctuation to spaces."""
    text = unicodedata.normalize("NFD", name)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.upper().replace("'", " ").replace("-", " ").replace(".", " ")
    return re.sub(r"\s+", " ", text).strip()


def build_comuna_lookup() -> dict[str, str]:
    """Normalised comuna name -> CUT code, from the shipped directory snapshot."""
    directory = pd.read_csv(_c("COMUNA_CUT_PATH"), dtype=str)
    lookup = {
        normalize_comuna(n): c
        for n, c in zip(
            directory["nombre"], directory["id_comuna"], strict=True
        )
    }
    lookup.update(_c("COMUNA_OVERRIDES"))
    return lookup


def map_comuna(
    values: pd.Series, lookup: dict[str, str], label: str
) -> pd.Series:
    """Map source comuna names to CUT codes, raising on anything unmapped.

    Failing loudly matters: a silently unmapped comuna would arrive as NULL and
    pass every downstream test.
    """
    normalized = values.map(
        lambda v: normalize_comuna(v) if pd.notna(v) else None
    )
    mapped = normalized.map(lambda v: lookup.get(v) if v is not None else None)

    unmapped = sorted(set(normalized[mapped.isna() & normalized.notna()]))
    if unmapped:
        raise ValueError(
            f"{label}: {len(unmapped)} comuna name(s) not in br_bd_diretorios_cl: {unmapped}"
        )
    return mapped


# --------------------------------------------------------------------------
# cleaning
# --------------------------------------------------------------------------
def architecture_columns(table: str) -> list[str]:
    """Column order for ``table``, read from its architecture CSV."""
    path = Path(_c("ARCHITECTURE_DIR")) / f"{table}.csv"
    return pd.read_csv(path)["name"].tolist()


def build_sociedad(path: str | Path, lookup: dict[str, str]) -> pd.DataFrame:
    """Clean one yearly CSV into the ``sociedad`` schema."""
    raw = pd.read_csv(
        path,
        sep=_c("CSV_SEP"),
        encoding=_c("CSV_ENCODING"),
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    raw = raw.rename(columns=_c("RENAMES"))

    out = pd.DataFrame(index=raw.index)
    out["ano"] = pd.to_numeric(raw["ano"], errors="coerce").astype("Int64")
    out["mes"] = raw["mes"].map(_c("MONTHS")).astype("Int64")

    comuna_tributaria = map_comuna(
        raw["comuna_tributaria"], lookup, "comuna_tributaria"
    )
    comuna_social = map_comuna(raw["comuna_social"], lookup, "comuna_social")

    # Region is derived from the CUT code rather than taken from the source, so
    # that region and comuna are always consistent with each other and with the
    # current directory. This reassigns the 21 Ñuble comunas registered before
    # the region was created (Ley 21.033, September 2018) from region 08 to 16.
    out["id_region_tributaria"] = comuna_tributaria.str[:2]
    out["id_comuna_tributaria"] = comuna_tributaria
    out["id_region_social"] = comuna_social.str[:2]
    out["id_comuna_social"] = comuna_social

    out["rut"] = raw["rut"].str.strip()
    out["id_actuacion"] = raw["id_actuacion"].str.strip()
    out["razon_social"] = raw["razon_social"].str.strip()
    out["tipo_sociedad"] = raw["tipo_sociedad"].str.strip()
    out["tipo_actuacion"] = raw["tipo_actuacion"].str.strip()

    for column in (
        "fecha_actuacion",
        "fecha_registro",
        "fecha_aprobacion_sii",
    ):
        out[column] = pd.to_datetime(
            raw[column], format=_c("SOURCE_DATE_FORMAT"), errors="coerce"
        ).dt.date

    out["capital"] = pd.to_numeric(raw["capital"], errors="coerce").astype(
        "Float64"
    )

    return out[architecture_columns("sociedad")]


def build_dicionario() -> pd.DataFrame:
    """Build the dictionary table from the value maps in ``constants``."""
    rows = []
    for (table, column), mapping in _c("DICTIONARY").items():
        for key, value in sorted(mapping.items()):
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )
    return pd.DataFrame(rows)[architecture_columns("dicionario")]


# --------------------------------------------------------------------------
# parquet output
# --------------------------------------------------------------------------
def _to_string_table(frame: pd.DataFrame) -> pa.Table:
    """Cast every column to string via arrow, preserving NULL.

    Staging is all-STRING by house convention and ``gcs.py::dump_header``
    stringifies the header anyway, so typed parquet is rejected on read. Casting
    through arrow (rather than ``astype(str)``) keeps NULL as NULL instead of
    rendering it as the literal ``"nan"``, which ``safe_cast`` cannot undo.
    """
    table = pa.Table.from_pandas(frame, preserve_index=False)
    return table.cast(
        pa.schema([pa.field(name, pa.string()) for name in table.column_names])
    )


def write_partitioned(
    frame: pd.DataFrame, output_dir: str | Path, table: str
) -> Path:
    """Write ``sociedad`` as ``<table>/ano=<year>/data.parquet``."""
    output_dir = Path(output_dir)
    year = int(frame["ano"].dropna().iloc[0])
    target = output_dir / table / f"ano={year}"
    target.mkdir(parents=True, exist_ok=True)

    payload = frame.drop(columns=["ano"])
    pq.write_table(
        _to_string_table(payload),
        target / "data.parquet",
        compression="snappy",
    )
    return target / "data.parquet"


def write_flat(
    frame: pd.DataFrame, output_dir: str | Path, table: str
) -> Path:
    """Write an unpartitioned table as ``<table>/data.parquet``."""
    output_dir = Path(output_dir)
    target = output_dir / table
    target.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _to_string_table(frame), target / "data.parquet", compression="snappy"
    )
    return target / "data.parquet"


def clean_all(input_dir: str | Path, output_dir: str | Path) -> dict[str, int]:
    """Clean every downloaded CSV and write the partitioned parquet output."""
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    lookup = build_comuna_lookup()

    counts: dict[str, int] = {}
    total = 0
    for path in sorted(input_dir.glob("*.csv")):
        frame = build_sociedad(path, lookup)
        write_partitioned(frame, output_dir, "sociedad")
        counts[path.stem] = len(frame)
        total += len(frame)

    dicionario = build_dicionario()
    write_flat(dicionario, output_dir, "dicionario")
    counts["dicionario"] = len(dicionario)
    counts["sociedad_total"] = total
    return counts
