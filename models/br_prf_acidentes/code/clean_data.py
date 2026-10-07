"""Build the br_prf_acidentes cleaned tables as partitioned Parquet.

Usage:
    uv run models/br_prf_acidentes/code/clean_data.py                     # all three tables, all years
    uv run models/br_prf_acidentes/code/clean_data.py --table ocorrencia  # one table
    uv run models/br_prf_acidentes/code/clean_data.py --years 2016 2017   # one or more years
    uv run models/br_prf_acidentes/code/clean_data.py --download          # fetch archives first

Output: <PRF_DATA_ROOT>/output/<table_slug>/ano=<year>/data.parquet, every
column STRING (staging is all-STRING by house convention; the dbt model
safe_casts each column to its architecture type).
"""

from __future__ import annotations

import argparse
import collections
import json
import shutil
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import models.br_prf_acidentes.code.utils as utils
from models.br_prf_acidentes.code.constants import (
    INPUT_DIR,
    OUTPUT_DIR,
    SHAPES,
)

# ------------------------------------------------------------------ schemas

OCORRENCIA_COLUMNS = [
    "ano",
    "data",
    "horario",
    "dia_semana",
    "sigla_uf",
    "id_municipio",
    "id_ocorrencia",
    "br",
    "km",
    "latitude",
    "longitude",
    "causa_acidente",
    "tipo_acidente",
    "classificacao_acidente",
    "fase_dia",
    "sentido_via",
    "condicao_metereologica",
    "tipo_pista",
    "tracado_via",
    "uso_solo",
    "quantidade_pessoas",
    "quantidade_mortos",
    "quantidade_feridos_leves",
    "quantidade_feridos_graves",
    "quantidade_ilesos",
    "quantidade_ignorados",
    "quantidade_feridos",
    "quantidade_veiculos",
    "regional",
    "delegacia",
    "uop",
]

PESSOA_COLUMNS = [
    "ano",
    "data",
    "horario",
    "dia_semana",
    "sigla_uf",
    "id_municipio",
    "id_ocorrencia",
    "id_veiculo",
    "id_pessoa",
    "br",
    "km",
    "latitude",
    "longitude",
    "causa_acidente",
    "tipo_acidente",
    "classificacao_acidente",
    "fase_dia",
    "sentido_via",
    "condicao_metereologica",
    "tipo_pista",
    "tracado_via",
    "uso_solo",
    "tipo_veiculo",
    "marca",
    "ano_fabricacao_veiculo",
    "tipo_envolvido",
    "estado_fisico",
    "idade",
    "sexo",
    "indicador_ileso",
    "indicador_ferido_leve",
    "indicador_ferido_grave",
    "indicador_morto",
    "nacionalidade",
    "naturalidade",
    "regional",
    "delegacia",
    "uop",
]

PESSOA_CAUSA_TIPO_COLUMNS = [
    "ano",
    "data",
    "horario",
    "dia_semana",
    "sigla_uf",
    "id_municipio",
    "id_ocorrencia",
    "id_veiculo",
    "id_pessoa",
    "br",
    "km",
    "latitude",
    "longitude",
    "causa_principal",
    "causa_acidente",
    "ordem_tipo_acidente",
    "tipo_acidente",
    "classificacao_acidente",
    "fase_dia",
    "sentido_via",
    "condicao_metereologica",
    "tipo_pista",
    "tracado_via",
    "uso_solo",
    "tipo_veiculo",
    "marca",
    "ano_fabricacao_veiculo",
    "tipo_envolvido",
    "estado_fisico",
    "idade",
    "sexo",
    "indicador_ileso",
    "indicador_ferido_leve",
    "indicador_ferido_grave",
    "indicador_morto",
    "regional",
    "delegacia",
    "uop",
]

TABLE_COLUMNS = {
    "ocorrencia": OCORRENCIA_COLUMNS,
    "pessoa": PESSOA_COLUMNS,
    "pessoa_causa_tipo": PESSOA_CAUSA_TIPO_COLUMNS,
}

# Person-level shared block, built once for both person tables.
_PERSON_FLAG_SOURCE = {
    "indicador_ileso": "ilesos",
    "indicador_ferido_leve": "feridos_leves",
    "indicador_ferido_grave": "feridos_graves",
    "indicador_morto": "mortos",
}


def _shared(row, shape, year, mun_index, case_maps, stats):
    """Columns common to all three tables."""
    date = utils.parse_date(row.get("data_inversa"), shape, year)
    id_municipio, status = utils.resolve_municipality(
        row.get("uf"), row.get("municipio"), mun_index
    )
    stats["municipality"][status] += 1
    lat = utils.parse_coordinate(row.get("latitude"), year, "lat")
    lon = utils.parse_coordinate(row.get("longitude"), year, "lon")
    verdict = utils.coordinate_in_brazil(lat, lon)
    if verdict is False:
        stats["coordinate_outside_brazil"] += 1
    if date is not None and date.year != year:
        stats["date_year_mismatch"] += 1
    out = {
        "ano": year,
        "data": date,
        "horario": utils.parse_time(row.get("horario")),
        "dia_semana": utils.harmonize_value(
            "dia_semana", utils.clean_str(row.get("dia_semana")), case_maps
        ),
        "sigla_uf": utils.clean_str(row.get("uf")),
        "id_municipio": id_municipio,
        "id_ocorrencia": utils.clean_str(row.get("id")),
        "br": utils.clean_str(row.get("br")),
        "km": utils.parse_float(row.get("km"), year),
        "latitude": lat,
        "longitude": lon,
        "regional": utils.clean_str(row.get("regional")),
        "delegacia": utils.clean_str(row.get("delegacia")),
        "uop": utils.clean_str(row.get("uop")),
    }
    for col in (
        "causa_acidente",
        "tipo_acidente",
        "classificacao_acidente",
        "fase_dia",
        "sentido_via",
        "condicao_metereologica",
        "tipo_pista",
        "tracado_via",
        "uso_solo",
    ):
        out[col] = utils.harmonize_value(
            col, utils.clean_str(row.get(col)), case_maps
        )
    return out


def _person_block(row, shape, year, case_maps):
    out = {
        "id_veiculo": utils.clean_str(row.get("id_veiculo")),
        "id_pessoa": utils.clean_str(row.get("pesid")),
        "tipo_veiculo": utils.harmonize_value(
            "tipo_veiculo", utils.clean_str(row.get("tipo_veiculo")), case_maps
        ),
        "marca": utils.clean_str(row.get("marca")),
        "ano_fabricacao_veiculo": utils.parse_vehicle_year(
            row.get("ano_fabricacao_veiculo")
        ),
        "tipo_envolvido": utils.harmonize_value(
            "tipo_envolvido",
            utils.clean_str(row.get("tipo_envolvido")),
            case_maps,
        ),
        "estado_fisico": utils.harmonize_value(
            "estado_fisico",
            utils.clean_str(row.get("estado_fisico")),
            case_maps,
        ),
        "idade": utils.parse_age(row.get("idade"), shape, year),
        "sexo": utils.harmonize_value(
            "sexo", utils.clean_str(row.get("sexo")), case_maps
        ),
    }
    for target, source in _PERSON_FLAG_SOURCE.items():
        out[target] = utils.parse_flag(row.get(source))
    return out


def build_rows(
    shape: str, year: int, mun_index: dict, case_maps: dict, stats: dict
):
    """Clean one (shape, year) into a list of dicts in architecture column order."""
    table = SHAPES[shape][0]
    columns = TABLE_COLUMNS[table]
    rows = []
    for raw in utils.read_source(shape, year, INPUT_DIR):
        rec = _shared(raw, shape, year, mun_index, case_maps, stats)
        if shape != "ocorrencia":
            rec.update(_person_block(raw, shape, year, case_maps))
            rec["nacionalidade"] = utils.clean_str(raw.get("nacionalidade"))
            rec["naturalidade"] = utils.clean_str(raw.get("naturalidade"))
        else:
            rec["quantidade_pessoas"] = utils.parse_int(raw.get("pessoas"))
            rec["quantidade_mortos"] = utils.parse_int(raw.get("mortos"))
            rec["quantidade_feridos_leves"] = utils.parse_int(
                raw.get("feridos_leves")
            )
            rec["quantidade_feridos_graves"] = utils.parse_int(
                raw.get("feridos_graves")
            )
            rec["quantidade_ilesos"] = utils.parse_int(raw.get("ilesos"))
            rec["quantidade_ignorados"] = utils.parse_int(raw.get("ignorados"))
            rec["quantidade_feridos"] = utils.parse_int(raw.get("feridos"))
            rec["quantidade_veiculos"] = utils.parse_int(raw.get("veiculos"))
        if shape == "pessoa_todas_causas":
            rec["causa_principal"] = utils.harmonize_value(
                "causa_principal",
                utils.clean_str(raw.get("causa_principal")),
                case_maps,
            )
            rec["ordem_tipo_acidente"] = utils.clean_str(
                raw.get("ordem_tipo_acidente")
            )
        rows.append({c: rec.get(c) for c in columns})
    return rows


# ------------------------------------------------------------------ output


def to_string_frame(rows: list[dict], columns: list[str]) -> pd.DataFrame:
    """Typed -> all-STRING frame, via arrow so NULL never becomes the text 'nan'.

    Staging is all-STRING by house convention, and `gcs.py::dump_header`
    stringifies the pipeline's staging header anyway. Passing the real types
    through arrow first keeps `ano` as "2016" rather than "2016.0".
    """
    df = pd.DataFrame(rows, columns=columns)
    table = pa.Table.from_pandas(df, preserve_index=False)
    casted = table.cast(
        pa.schema([(c, pa.string()) for c in columns]), safe=False
    )
    return casted.to_pandas()


def write_partition(rows: list[dict], table: str, year: int) -> Path:
    columns = TABLE_COLUMNS[table]
    df = to_string_frame(rows, columns)
    # `ano` is the hive partition key, so it is not written inside the file.
    part_dir = OUTPUT_DIR / table / f"ano={year}"
    part_dir.mkdir(parents=True, exist_ok=True)
    body = df.drop(columns=["ano"])
    schema = pa.schema([(c, pa.string()) for c in columns if c != "ano"])
    pq.write_table(
        pa.Table.from_pandas(body, schema=schema, preserve_index=False),
        part_dir / "data.parquet",
        compression="snappy",
    )
    return part_dir / "data.parquet"


# ------------------------------------------------------------------ runner


def load_municipality_directory() -> pd.DataFrame:
    """Read the BD municipality directory, caching it beside the input data."""
    cache = INPUT_DIR.parent / "municipio_directory.csv"
    if cache.exists():
        return pd.read_csv(cache, dtype=str)
    from google.cloud import bigquery

    client = bigquery.Client(project="basedosdados-dev")
    sql = """
        select id_municipio, nome, sigla_uf
        from `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    df = client.query(sql).result().to_dataframe().astype(str)
    cache.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache, index=False)
    return df


def clean_all(tables=None, years=None, do_download=False) -> dict:
    directory = load_municipality_directory()
    mun_index = utils.build_municipality_index(directory)
    # Two sections rather than one mixed mapping: the per-partition stats and the
    # harmonization maps have different shapes and different readers.
    partitions: dict[str, dict] = {}
    harmonization: dict[str, dict[str, dict[str, str]]] = {}
    for shape, (table, first, last) in SHAPES.items():
        if tables and table not in tables:
            continue
        shape_years = [
            y for y in range(first, last + 1) if years is None or y in years
        ]
        if do_download:
            # The full range, not just shape_years: collect_vocabularies below
            # reads every year of the shape to pick each category's canonical
            # spelling, so `--download --years 2017` would otherwise leave
            # read_source raising FileNotFoundError on a fresh PRF_DATA_ROOT.
            for year in range(first, last + 1):
                utils.download(shape, year, INPUT_DIR)
        # Vocabularies must be collected across ALL of a shape's years before any
        # year is written, or the canonical spelling would differ per file.
        vocab = utils.collect_vocabularies(
            shape, range(first, last + 1), INPUT_DIR
        )
        case_maps = {
            col: utils.build_case_variant_map(v) for col, v in vocab.items()
        }
        for year in shape_years:
            stats = {
                "municipality": collections.Counter(),
                "coordinate_outside_brazil": 0,
                "date_year_mismatch": 0,
            }
            rows = build_rows(shape, year, mun_index, case_maps, stats)
            path = write_partition(rows, table, year)
            partitions[f"{table}|{year}"] = {
                "rows": len(rows),
                "path": str(path),
                "municipality": dict(stats["municipality"]),
                "coordinate_outside_brazil": stats[
                    "coordinate_outside_brazil"
                ],
                "date_year_mismatch": stats["date_year_mismatch"],
            }
            print(
                f"{table:20}{year}  rows={len(rows):>9,}  "
                f"mun_unresolved={stats['municipality']['unresolved'] + stats['municipality']['uf_mismatch'] + stats['municipality']['no_municipality']:>4}  "
                f"coord_bad={stats['coordinate_outside_brazil']:>4}  "
                f"date_year_mismatch={stats['date_year_mismatch']:>4}",
                flush=True,
            )
        harmonization[table] = {c: m for c, m in case_maps.items() if m}
    return {"partitions": partitions, "harmonization": harmonization}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--table", nargs="*", default=None, choices=list(TABLE_COLUMNS)
    )
    ap.add_argument("--years", nargs="*", type=int, default=None)
    ap.add_argument("--download", action="store_true")
    ap.add_argument(
        "--clean-output", action="store_true", help="delete output/ first"
    )
    args = ap.parse_args()
    if args.clean_output and OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    report = clean_all(
        args.table, set(args.years) if args.years else None, args.download
    )
    out = OUTPUT_DIR / "_clean_report.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1, default=str))
    print(f"\nreport -> {out}")


if __name__ == "__main__":
    main()
