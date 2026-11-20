"""Clean V-Dem v16 (Country-Year: V-Dem Full+Others) into all-STRING staging parquet.

Usage:
    Rscript models/se_gu_vdem/code/extract_rdata.R
    python models/se_gu_vdem/code/clean_data.py

Reads ``$SE_GU_VDEM_DATA_DIR/input/{vdem,codebook}.parquet`` (written by
``extract_rdata.R`` from the vdemdata R package) and writes

* ``output/country_year/year=YYYY/data.parquet`` (long: one row per country,
  year and variable, with every published variant of the estimate)
* ``output/country_year_index/data.parquet`` (wide: the 118 ``v2x*`` indices)
* ``output/country/data.parquet``
* ``output/indicator/data.parquet``
* ``output/dicionario/data.parquet``

``SE_GU_VDEM_DATA_DIR`` defaults to ``~/Library/Caches/se_gu_vdem_data``.

V-Dem publishes each variable as up to 13 columns: the point estimate plus
suffixed variants (``_codelow``, ``_osp``, ``_ord``, ``_nr`` and so on). The long
table reshapes them into one column per variant. A column is read as a variant
only when stripping its suffix leaves another column or a codebook tag, so
variables whose own name ends in a suffix (``e_uds_mean``) stay whole. The 25
text variables (names of parties, office-holders and institutions) go to
``value_text``. Rows where every variant is missing are dropped; the script
checks that the non-null cell count matches the source.

The output directory is deleted before it is rewritten, so a re-run never keeps
parquet produced by an older version of this code. Row counts go to
``output/_manifest_<table>.json``.
"""

import csv
import json
import os
import re
import shutil
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "SE_GU_VDEM_DATA_DIR",
        Path.home() / "Library" / "Caches" / "se_gu_vdem_data",
    )
)
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"
ARCH_DIR = Path(__file__).parent / "architecture"

# Source suffix -> long-table column. Longer suffixes first, so _osp_codelow is
# not read as _codelow.
VARIANTS = {
    "_osp_codelow": "osp_code_low",
    "_osp_codehigh": "osp_code_high",
    "_osp_sd": "osp_sd",
    "_ord_codelow": "ord_code_low",
    "_ord_codehigh": "ord_code_high",
    "_codelow": "code_low",
    "_codehigh": "code_high",
    "_osp": "osp",
    "_ord": "ord",
    "_mean": "mean",
    "_nr": "coder_count",
    "_sd": "sd",
}
VALUE_COLUMNS = ["value", "value_text", *dict.fromkeys(VARIANTS.values())]

# Unit identifiers and coding periods: everything before the first variable.
ID_COLUMNS = [
    "country_name",
    "country_text_id",
    "country_id",
    "year",
    "historical_date",
    "project",
    "historical",
    "histname",
    "codingstart",
    "codingend",
    "codingstart_contemp",
    "codingend_contemp",
    "codingstart_hist",
    "codingend_hist",
    "gapstart1",
    "gapstart2",
    "gapstart3",
    "gapend1",
    "gapend2",
    "gapend3",
    "gap_index",
    "COWcode",
]
COUNTRY_RENAME = {
    "codingstart": "coding_start",
    "codingend": "coding_end",
    "codingstart_contemp": "coding_start_contemporary",
    "codingend_contemp": "coding_end_contemporary",
    "codingstart_hist": "coding_start_historical",
    "codingend_hist": "coding_end_historical",
    "gapstart1": "gap_start_1",
    "gapstart2": "gap_start_2",
    "gapstart3": "gap_start_3",
    "gapend1": "gap_end_1",
    "gapend2": "gap_end_2",
    "gapend3": "gap_end_3",
}
INDEX_RENAME = {
    "histname": "historical_name",
    "COWcode": "cow_code",
}
CODEBOOK_RENAME = {
    "tag": "codebook_tag",
    "vartype": "variable_type",
    "crosscoder_aggregation": "cross_coder_aggregation",
    "cy_aggregation": "country_year_aggregation",
    "datarelease": "data_release",
    "projectmanager": "project_manager",
}
# A response category of a multiple-selection question (v2eltype_0) or an
# ordinal recode of an index (e_v2x_api_3C): its codebook entry is the parent.
CATEGORY_RE = re.compile(r"^(?P<parent>.+)_(?P<category>\d+|\dC)$")

# The source rounds nothing: these are the v16 Full+Others dimensions.
EXPECTED_SHAPE = (28_092, 4_618)

# column -> {key: Portuguese label}. Labels follow the codebook's responses.
NO_YES = {"0": "Não", "1": "Sim"}
DICTIONARY: dict[tuple[str, str], dict[str, str]] = {
    ("country", "historical"): NO_YES,
    ("country_year_index", "project"): {
        "0": "V-Dem contemporâneo",
        "1": "V-Dem histórico",
        "2": "Ambos (período de sobreposição)",
    },
    ("country_year_index", "v2x_regime"): {
        "0": "Autocracia fechada",
        "1": "Autocracia eleitoral",
        "2": "Democracia eleitoral",
        "3": "Democracia liberal",
    },
    ("country_year_index", "v2x_regime_amb"): {
        "0": "Autocracia fechada",
        "1": "Autocracia fechada, limite superior",
        "2": "Autocracia eleitoral, limite inferior",
        "3": "Autocracia eleitoral",
        "4": "Autocracia eleitoral, limite superior",
        "5": "Democracia eleitoral, limite inferior",
        "6": "Democracia eleitoral",
        "7": "Democracia eleitoral, limite superior",
        "8": "Democracia liberal, limite inferior",
        "9": "Democracia liberal",
    },
    **{
        ("country_year_index", col): NO_YES
        for col in (
            "v2x_elecreg",
            "v2xex_elecreg",
            "v2xlg_elecreg",
            "v2xel_elecparl",
            "v2xlg_leginter",
            "v2xel_elecpres",
            "v2x_hosinter",
            "v2x_hosabort",
            "v2x_legabort",
        )
    },
    ("indicator", "variable_type"): {
        "A": "Factual, codificada por gerentes de projeto e assistentes de pesquisa a partir de fontes existentes",
        "A*": "Factual, codificada por gerentes de projeto e assistentes de pesquisa, com grau de confiança indicado por especialistas no país",
        "A,C": "Codificada por especialistas no país e verificada por assistentes de pesquisa",
        "C": "Codificada por especialistas no país e agregada pelo modelo de medição",
        "D": "Índice composto de variáveis do tipo A ou C",
        "E": "Importada de outra fonte, sem codificação própria do V-Dem",
    },
}


def read_columns(table: str) -> list[str]:
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as f:
        return [row["name"] for row in csv.DictReader(f)]


def as_strings(df: pd.DataFrame, cols: list[str]) -> pa.Table:
    """Cast every column to STRING via arrow, so NULL stays NULL.

    Whole-number floats (years, codes, counts) are cast to int64 first so they
    serialise as "1789", not "1789.0".
    """
    missing = set(cols) - set(df.columns)
    if missing:
        raise ValueError(f"Columns missing from the frame: {sorted(missing)}")
    arrays = []
    for c in cols:
        s = df[c]
        if pd.api.types.is_float_dtype(s) and (s.dropna() % 1 == 0).all():
            s = s.astype("Int64")
        arrays.append(pa.array(s, from_pandas=True).cast(pa.string()))
    return pa.Table.from_arrays(arrays, names=cols)


def write_single(table: str, df: pd.DataFrame, **extra) -> None:
    out = OUTPUT / table
    shutil.rmtree(out, ignore_errors=True)
    out.mkdir(parents=True)
    pq.write_table(
        as_strings(df, read_columns(table)),
        out / "data.parquet",
        compression="snappy",
    )
    write_manifest(table, len(df), **extra)


def write_manifest(table: str, rows: int, **extra) -> None:
    (OUTPUT / f"_manifest_{table}.json").write_text(
        json.dumps({"table": table, "rows": rows, **extra}, indent=2)
    )
    print(f"  {table}: {rows:,} rows", flush=True)


def split_columns(
    columns: list[str], tags: set[str], text: set[str]
) -> dict[str, tuple[str, str]]:
    """Map each variable column to (variable_id, long-table column).

    Text variables (party, office-holder and institution names) go to
    value_text, since value is numeric.
    """
    names = set(columns)
    out = {}
    for c in columns:
        out[c] = (c, "value_text" if c in text else "value")
        for suffix, variant in VARIANTS.items():
            base = c[: -len(suffix)]
            if c.endswith(suffix) and (base in names or base in tags):
                out[c] = (base, variant)
                break
    pairs = list(out.values())
    if len(pairs) != len(set(pairs)):
        dup = sorted({p for p in pairs if pairs.count(p) > 1})
        raise ValueError(f"Two columns map to the same variant: {dup[:10]}")
    return out


def clean_country_year(vdem: pd.DataFrame, mapping: dict) -> dict[str, str]:
    """Write the long table; return each variable's populated variants."""
    out = OUTPUT / "country_year"
    shutil.rmtree(out, ignore_errors=True)
    cols = read_columns("country_year")
    by_variant: dict[str, list[str]] = {}
    for c, (_, variant) in mapping.items():
        by_variant.setdefault(variant, []).append(c)

    seen: dict[str, set[str]] = {}
    rows_by_year = {}
    cells = 0
    for key, part in vdem.groupby("year", sort=True):
        year = int(cast(float, key))
        frames = []
        for variant, vcols in by_variant.items():
            long = part[["country_id", *vcols]].melt(
                id_vars="country_id", var_name="column", value_name=variant
            )
            long = long.dropna(subset=[variant])
            long["variable_id"] = long.pop("column").map(
                lambda c: mapping[c][0]
            )
            frames.append(long.set_index(["country_id", "variable_id"]))
        merged = pd.concat(frames, axis=1).reset_index()
        merged = merged.reindex(
            columns=["country_id", "variable_id", *VALUE_COLUMNS]
        )
        merged = merged.sort_values(["country_id", "variable_id"])
        for variant in VALUE_COLUMNS:
            for v in merged.loc[
                merged[variant].notna(), "variable_id"
            ].unique():
                seen.setdefault(v, set()).add(variant)
        merged["year"] = year
        pdir = out / f"year={year}"
        pdir.mkdir(parents=True)
        # year lives in the hive path; BigQuery rebuilds it from there.
        table = as_strings(merged, cols).drop_columns(["year"])
        pq.write_table(table, pdir / "data.parquet", compression="snappy")
        rows_by_year[year] = len(merged)
        cells += int(merged[VALUE_COLUMNS].notna().sum().sum())
    expected = int(vdem[list(mapping)].notna().sum().sum())
    if cells != expected:
        raise ValueError(
            f"{cells:,} non-null cells written, source has {expected:,}"
        )
    write_manifest(
        "country_year", sum(rows_by_year.values()), rows_by_year=rows_by_year
    )
    return {
        v: ",".join(x for x in VALUE_COLUMNS if x in variants)
        for v, variants in seen.items()
    }


def clean_country_year_index(vdem: pd.DataFrame) -> None:
    df = vdem.rename(columns=INDEX_RENAME).copy()
    df["historical_date"] = pd.to_datetime(df["historical_date"]).dt.date
    df = df.sort_values(["year", "country_id"])
    write_single("country_year_index", df)


def clean_country(vdem: pd.DataFrame) -> None:
    attrs = ["country_id", "country_text_id", "country_name", "historical"]
    attrs += list(COUNTRY_RENAME)
    df = vdem[attrs].drop_duplicates()
    if df["country_id"].duplicated().any():
        raise ValueError("A country attribute varies across years")
    df = df.rename(columns=COUNTRY_RENAME).sort_values("country_id")
    write_single("country", df)


def clean_indicator(
    codebook: pd.DataFrame, variables: list[str], variants: dict[str, str]
) -> None:
    cb = codebook.rename(columns=CODEBOOK_RENAME).set_index("codebook_tag")
    text_cols = [c for c in cb.columns if cb[c].dtype == object]
    for c in text_cols:
        # The codebook is exported from LaTeX; drop its spacing commands.
        cb[c] = (
            cb[c]
            .str.replace(r"\s*\\?(small|med|big)skip\b\s*", " ", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
    rows = []
    for v in variables:
        tag, category = v, None
        m = CATEGORY_RE.match(v)
        if v not in cb.index and m and m["parent"] in cb.index:
            tag, category = m["parent"], m["category"]
        row: dict[str, Any] = cb.loc[tag].to_dict() if tag in cb.index else {}
        row.update(
            variable_id=v,
            codebook_tag=tag if tag in cb.index else None,
            category=category,
            variants=variants.get(v),
        )
        rows.append(row)
    df = pd.DataFrame(rows).sort_values("variable_id")
    unknown = set(df["variable_type"].dropna()) - set(
        DICTIONARY[("indicator", "variable_type")]
    )
    if unknown:
        raise ValueError(f"Unknown variable_type codes: {unknown}")
    print(
        f"  indicator: {df['codebook_tag'].isna().sum()} variables "
        "without a codebook entry",
        flush=True,
    )
    write_single("indicator", df)


def build_dicionario(vdem: pd.DataFrame) -> None:
    for (table, col), labels in DICTIONARY.items():
        if table == "indicator":
            continue
        observed = set(vdem[col].dropna().astype(int).astype(str).unique())
        if observed - set(labels):
            raise ValueError(
                f"{table}.{col}: unlabelled {observed - set(labels)}"
            )
    rows = [
        {
            "id_tabela": table,
            "nome_coluna": col,
            "chave": key,
            "cobertura_temporal": None,
            "valor": label,
        }
        for (table, col), labels in DICTIONARY.items()
        for key, label in labels.items()
    ]
    write_single("dicionario", pd.DataFrame(rows))


def main():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    print(f"=== cleaning {INPUT} -> {OUTPUT} ===", flush=True)
    vdem = pd.read_parquet(INPUT / "vdem.parquet")
    codebook = pd.read_parquet(INPUT / "codebook.parquet")
    if vdem.shape != EXPECTED_SHAPE:
        raise ValueError(f"vdem is {vdem.shape}, expected {EXPECTED_SHAPE}")
    if list(vdem.columns[: len(ID_COLUMNS)]) != ID_COLUMNS:
        raise ValueError("The identifier columns changed order or name")
    if vdem.duplicated(["country_id", "year"]).any():
        raise ValueError("Duplicate country-year rows")

    variables = list(vdem.columns[len(ID_COLUMNS) :])
    text = {c for c in variables if vdem[c].dtype == object}
    mapping = split_columns(variables, set(codebook["tag"]), text)
    variable_ids = sorted({v for v, _ in mapping.values()})
    print(f"  {len(variables)} columns -> {len(variable_ids)} variables")

    clean_country(vdem)
    clean_country_year_index(vdem)
    variants = clean_country_year(vdem, mapping)
    clean_indicator(codebook, variable_ids, variants)
    build_dicionario(vdem)
    print("DONE")


if __name__ == "__main__":
    main()
