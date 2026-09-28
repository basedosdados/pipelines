"""Clean the Pop-72 Access database into partitioned Parquet for Data Basis.

Reads the CSV dump of ``Pop-1872-Brasil_versao1_0.mdb`` produced by
``extract.py`` and writes one partitioned Parquet dataset per published table:

    output/<table_slug>/ano=1872/data.parquet

Every column is written as STRING, per the house staging convention: the dbt
model ``safe_cast``s each column to its architecture type, and ``dump_header``
stringifies the staging header anyway, so typed Parquet is rejected on read.
Casting goes through Arrow rather than ``astype(str)``, which would render NULL
as the literal "nan".

Run:  uv run python models/br_ufmg_censo_demografico_1872/code/clean.py
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from spec import ANO, DICIONARIOS, LEVELS, TABLES, table_slug

DATA_DIR = Path(
    os.environ.get(
        "CENSO_1872_DATA",
        Path.home() / "Downloads" / "br_ufmg_censo_demografico_1872_data",
    )
)
CSV_DIR = DATA_DIR / "input" / "csv"
OUT_DIR = DATA_DIR / "output"


def _read(name: str) -> pd.DataFrame:
    return pd.read_csv(CSV_DIR / f"{name}.csv")


def _write(df: pd.DataFrame, slug: str) -> int:
    """Write one table as all-STRING Parquet, partitioned by ``ano``."""
    dest = OUT_DIR / slug / f"ano={ANO}"
    if dest.exists():
        shutil.rmtree(dest)
    dest.mkdir(parents=True, exist_ok=True)

    # Real types first, so integers serialise as "1872" and not "1872.0".
    table = pa.Table.from_pandas(df, preserve_index=False)
    table = table.cast(
        pa.schema([(f.name, pa.string()) for f in table.schema])
    )
    pq.write_table(table, dest / "data.parquet", compression="snappy")
    return len(df)


def build_geografia(
    par: pd.DataFrame, mun: pd.DataFrame, pro: pd.DataFrame
) -> dict[str, int]:
    """Parish / municipality / province lookup tables."""
    counts = {}

    provincia = (
        pro[["ProvId", "Provincia"]]
        .rename(
            columns={"ProvId": "id_provincia", "Provincia": "nome_provincia"}
        )
        .astype({"id_provincia": "Int64"})
        .sort_values("id_provincia")
    )
    counts["provincia"] = _write(provincia, "provincia")

    municipio = (
        par[["ProvId", "MunicId"]]
        .drop_duplicates()
        .merge(mun, on="MunicId", how="left")
        .rename(
            columns={
                "ProvId": "id_provincia",
                "MunicId": "id_municipio_1872",
                "Municipio": "nome_municipio",
            }
        )
        .astype({"id_provincia": "Int64", "id_municipio_1872": "Int64"})
        .sort_values("id_municipio_1872")
    )
    counts["municipio"] = _write(municipio, "municipio")

    paroquia = (
        par[["ProvId", "MunicId", "ParoquiaId", "Paroquia"]]
        .rename(
            columns={
                "ProvId": "id_provincia",
                "MunicId": "id_municipio_1872",
                "ParoquiaId": "id_paroquia",
                "Paroquia": "nome_paroquia",
            }
        )
        .astype(
            {
                "id_provincia": "Int64",
                "id_municipio_1872": "Int64",
                "id_paroquia": "Int64",
            }
        )
        .sort_values("id_paroquia")
    )
    counts["paroquia"] = _write(paroquia, "paroquia")
    return counts


def build_dicionario() -> int:
    """One ``dicionario`` row per (table, id_categoria) across every table.

    ``valor`` carries the group and the label together ("Racas: Branco") when
    the group adds information, because the Data Basis dictionary schema has a
    single value column and the label alone is ambiguous across groups.
    """
    rows = []
    for source, t in TABLES.items():
        if t["dicionario"] is None:
            continue
        cfg = DICIONARIOS[t["dicionario"]]
        d = _read(t["dicionario"])
        for _, r in d.iterrows():
            desc = str(r[cfg["desc"]]).strip()
            grupo = str(r[cfg["grupo"]]).strip() if cfg["grupo"] else ""
            if grupo and grupo.lower() not in ("nan", "", desc.lower()):
                valor = f"{grupo}: {desc}"
            else:
                valor = desc
            key = r[cfg["key"]]
            if pd.isna(key):
                continue
            for level in LEVELS:
                rows.append(
                    {
                        "id_tabela": table_slug(source, level),
                        "nome_coluna": "id_categoria",
                        "chave": str(int(key)),
                        "cobertura_temporal": str(ANO),
                        "valor": valor,
                    }
                )
    dic = (
        pd.DataFrame(rows)
        .drop_duplicates()
        .sort_values(["id_tabela", "nome_coluna", "chave"])
    )
    return _write(dic, "dicionario")


def build_data_tables(par: pd.DataFrame) -> dict[str, int]:
    """Every source table, at every geographic level."""
    hierarchy = par[["ParoquiaId", "MunicId", "ProvId"]].drop_duplicates()
    counts = {}

    for source, t in TABLES.items():
        df = _read(source)
        measures = t["measures"]

        missing = set(measures) - set(df.columns)
        if missing:
            raise KeyError(
                f"{source}: measure columns absent from source: {sorted(missing)}"
            )

        keep = [t["local_col"]] + (
            [t["categoria_col"]] if t["categoria_col"] else []
        )
        out = df[keep + list(measures)].rename(columns=measures)
        out = out.rename(
            columns={
                t["local_col"]: "id_paroquia",
                **(
                    {t["categoria_col"]: "id_categoria"}
                    if t["categoria_col"]
                    else {}
                ),
            }
        )

        out = out.merge(
            hierarchy.rename(
                columns={
                    "ParoquiaId": "id_paroquia",
                    "MunicId": "id_municipio_1872",
                    "ProvId": "id_provincia",
                }
            ),
            on="id_paroquia",
            how="left",
        )
        if out["id_provincia"].isna().any():
            raise ValueError(f"{source}: parish ids absent from cod_paroquias")

        value_cols = list(dict.fromkeys(measures.values()))
        cat = ["id_categoria"] if t["categoria_col"] else []

        for level, geo in LEVELS.items():
            grouped = (
                out.groupby(geo + cat, as_index=False, dropna=False)[
                    value_cols
                ]
                .sum(min_count=1)
                .sort_values(geo + cat)
            )
            grouped.insert(0, "ano", ANO)
            frame = grouped[["ano", *geo, *cat, *value_cols]]
            frame = frame.astype(
                {c: "Int64" for c in ["ano", *geo, *cat, *value_cols]}
            )
            counts[table_slug(source, level)] = _write(
                frame, table_slug(source, level)
            )

    return counts


def main() -> None:
    par = _read("cod_paroquias")
    mun = _read("cod_municipios")
    pro = _read("cod_prov_id")

    # The whole three-level design rests on the parish id encoding the
    # hierarchy. Fail loudly rather than silently mis-aggregating.
    if not (par.ParoquiaId // 100 == par.MunicId).all():
        raise ValueError(
            "ParoquiaId // 100 does not equal MunicId for every parish"
        )
    if not (par.MunicId // 100 == par.ProvId).all():
        raise ValueError(
            "MunicId // 100 does not equal ProvId for every municipality"
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    counts = build_geografia(par, mun, pro)
    counts["dicionario"] = build_dicionario()
    counts.update(build_data_tables(par))

    for slug in sorted(counts):
        print(f"{counts[slug]:>8,}  {slug}")
    print(f"\n{len(counts)} tables written to {OUT_DIR}")


if __name__ == "__main__":
    main()
