"""Cleaning code for world_dasanaike_sage (SAGE — Small-Area Global Elections).

Source: Noah Dasanaike, Harvard Dataverse doi:10.7910/DVN/YGJR1L (v2.0), CC0 1.0.

Tables:
    1. election_returns        — party/candidate returns at each electoral unit
                                 (284,271,492 rows; 56 cols). Streamed row-group
                                 by row-group from the consolidated sage.parquet.
    2. spatial_admin_crosswalk — coordinate -> GADM/NUTS/FIPS admin crosswalk.
    3. netherlands_preference_votes — candidate preference votes by stembureau
                                 (union of the 2017 and 2021 files).
    4. germany_erststimme      — constituency first-vote (Erststimme) results.

Global rules:
    - Column names, types, and order follow the architecture spec exactly.
    - Types by arithmetic meaning: counts/ratios/coordinates -> FLOAT64/INT64;
      categorical codes, flags, and identifiers -> STRING (even when numeric-
      looking, e.g. round, geometry_level, party_id, wahlkreis_nr).
    - Values are kept faithful to the source (no filtering; negative votes and
      turnout outliers are preserved).
    - Output: snappy Parquet, hive-partitioned by year:
      output/<table_slug>/year=<YYYY>/part-*.parquet
      (the `year` column is encoded in the path, not stored in the file).
"""

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "input"
OUTPUT = ROOT / "output"

EXPECTED_ROWS = {
    "election_returns": 284_271_492,
    "spatial_admin_crosswalk": 4_928_712,
    "netherlands_preference_votes": 30_671_332,
    "germany_erststimme": 5_996,
}

# ----------------------------------------------------------------------------
# Column specs: (target_name, source_name, kind)
# kind: str | str_from_int | str_from_floatint | i64 | f64
# ----------------------------------------------------------------------------

MAIN_COLS = [
    ("year", "year", "i64"),
    ("country_iso3_code", "iso3", "str"),
    ("country", "country", "str"),
    ("name_1", "NAME1", "str"),
    ("name_1_b", "NAME1_b", "str"),
    ("name_1_c", "NAME1_c", "str"),
    ("name_1_d", "NAME1_d", "str"),
    ("name_1_imp", "NAME1_imp", "str"),
    ("name_1_raw", "NAME1_raw", "str"),
    ("name_2", "NAME2", "str"),
    ("name_2_b", "NAME2_b", "str"),
    ("name_2_c", "NAME2_c", "str"),
    ("name_2_d", "NAME2_d", "str"),
    ("name_2_imp", "NAME2_imp", "str"),
    ("name_3", "NAME3", "str"),
    ("name_3_b", "NAME3_b", "str"),
    ("name_3_c", "NAME3_c", "str"),
    ("name_3_imp", "NAME3_imp", "str"),
    ("name_4", "NAME4", "str"),
    ("name_4_b", "NAME4_b", "str"),
    ("name_5", "NAME5", "str"),
    ("name_5_b", "NAME5_b", "str"),
    ("name_5_c", "NAME5_c", "str"),
    ("name_6", "NAME6", "str"),
    ("name_6_b", "NAME6_b", "str"),
    ("name_7", "NAME7", "str"),
    ("name_7_b", "NAME7_b", "str"),
    ("latitude", "latitude", "f64"),
    ("longitude", "longitude", "f64"),
    ("election_type", "election_type", "str"),
    ("round", "round", "str_from_int"),
    ("month", "month", "str"),
    ("special_type", "special_type", "str"),
    ("special_type_b", "special_type_b", "str"),
    ("special_type_c", "special_type_c", "str"),
    ("party", "party", "str"),
    ("party_b", "party_b", "str"),
    ("party_c", "party_c", "str"),
    ("party_d", "party_d", "str"),
    ("candidate", "candidate", "str"),
    ("candidate_b", "candidate_b", "str"),
    ("candidate_c", "candidate_c", "str"),
    ("coalition", "coalition", "str"),
    ("partyfacts_id", "partyfacts_id", "str"),
    ("partyfacts_name", "partyfacts_name", "str"),
    ("match_confidence", "match_confidence", "str"),
    ("votes", "votes", "f64"),
    ("total_votes", "total_votes", "f64"),
    ("registered_voters", "reg", "f64"),
    ("eligible_voters", "evp", "f64"),
    ("turnout_registered", "turnout_reg", "f64"),
    ("turnout_eligible", "turnout_evp", "f64"),
    ("geometry_type", "geometry_type", "str"),
    ("geometry_type_b", "geometry_type_b", "str"),
    ("geometry_level", "geometry_level", "str_from_floatint"),
    ("geocode_duplicates", "geocode_duplicates", "i64"),
]

CROSSWALK_COLS = [
    ("year", "year", "i64"),
    ("country_iso3_code", "country_iso3_code", "str"),
    ("latitude", "latitude", "f64"),
    ("longitude", "longitude", "f64"),
    ("gadm_1_gid", "gadm_1_gid", "str"),
    ("gadm_1_hasc", "gadm_1_hasc", "str"),
    ("gadm_1_name", "gadm_1_name", "str"),
    ("gadm_2_gid", "gadm_2_gid", "str"),
    ("gadm_2_hasc", "gadm_2_hasc", "str"),
    ("gadm_2_name", "gadm_2_name", "str"),
    ("gadm_3_gid", "gadm_3_gid", "str"),
    ("gadm_3_hasc", "gadm_3_hasc", "str"),
    ("gadm_3_name", "gadm_3_name", "str"),
    ("iso_3166_2", "iso_3166_2", "str"),
    ("nuts_1_id", "nuts_1_id", "str"),
    ("nuts_1_name", "nuts_1_name", "str"),
    ("nuts_2_id", "nuts_2_id", "str"),
    ("nuts_2_name", "nuts_2_name", "str"),
    ("nuts_3_id", "nuts_3_id", "str"),
    ("nuts_3_name", "nuts_3_name", "str"),
    ("fips_state", "fips_state", "str"),
    ("fips_county", "fips_county", "str"),
]

NL_COLS = [
    ("year", "year", "i64"),
    ("gemeente_code", "gemeente_code", "str_from_int"),
    ("stembureau_id", "stembureau_id", "str"),
    ("party_id", "party_id", "str_from_int"),
    ("party", "party", "str"),
    ("candidate_id", "candidate_id", "str_from_int"),
    ("candidate", "candidate", "str"),
    ("votes", "votes", "i64"),
]

DE_COLS = [
    ("year", "year", "i64"),
    ("wahlkreis_nr", "wahlkreis_nr", "str_from_int"),
    ("wahlkreis_name", "wahlkreis_name", "str"),
    ("party", "party", "str"),
    ("candidate", "candidate", "str"),
    ("constituency_votes", "constituency_votes", "i64"),
    ("constituency_share_pct", "constituency_share_pct", "f64"),
]

TYPE_MAP = {
    "str": pa.string(),
    "str_from_int": pa.string(),
    "str_from_floatint": pa.string(),
    "i64": pa.int64(),
    "f64": pa.float64(),
}


def target_schema(cols):
    return pa.schema([(t, TYPE_MAP[k]) for t, _, k in cols])


def build_array(batch, source_name, kind):
    col = batch.column(batch.schema.get_field_index(source_name))
    if kind == "str":
        if pa.types.is_string(col.type) or pa.types.is_large_string(col.type):
            return pc.cast(col, pa.string())
        return pc.cast(col, pa.string())
    if kind == "str_from_int":
        return pc.cast(pc.cast(col, pa.int64()), pa.string())
    if kind == "str_from_floatint":
        # whole-number float -> int64 (nulls preserved) -> string
        as_int = pc.cast(
            col,
            options=pc.CastOptions(
                target_type=pa.int64(), allow_float_truncate=True
            ),
        )
        return pc.cast(as_int, pa.string())
    if kind == "i64":
        return pc.cast(col, pa.int64())
    if kind == "f64":
        return pc.cast(col, pa.float64())
    raise ValueError(kind)


def transform_batch(batch, cols, schema):
    arrays = [build_array(batch, src, kind) for _, src, kind in cols]
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


def build_iso3_map():
    """country name -> ISO3 code, from the consolidated returns file."""
    pf = pq.ParquetFile(INPUT / "sage.parquet")
    mapping = {}
    for b in pf.iter_batches(
        batch_size=1_000_000, columns=["country", "iso3"]
    ):
        d = b.to_pydict()
        for c, i in zip(d["country"], d["iso3"], strict=True):
            if c is not None and i is not None and c not in mapping:
                mapping[c] = i
    print(f"  built country->iso3 map: {len(mapping)} countries", flush=True)
    return mapping


def _write_batches(batch_gen, table_slug, cols):
    out_dir = OUTPUT / table_slug
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = target_schema(cols)
    partitioning = ds.partitioning(
        pa.schema([("year", pa.int64())]), flavor="hive"
    )
    file_opts = ds.ParquetFileFormat().make_write_options(compression="snappy")

    ds.write_dataset(
        batch_gen(),
        base_dir=str(out_dir),
        format="parquet",
        partitioning=partitioning,
        schema=schema,
        file_options=file_opts,
        existing_data_behavior="delete_matching",
        max_rows_per_file=3_000_000,
        max_rows_per_group=200_000,
        basename_template="part-{i}.parquet",
    )
    written = ds.dataset(str(out_dir), partitioning="hive").count_rows()
    exp = EXPECTED_ROWS[table_slug]
    status = "OK" if written == exp else "MISMATCH"
    print(
        f"  wrote {written:,} rows (expected {exp:,}) — {status}", flush=True
    )
    if written != exp:
        raise RuntimeError(f"{table_slug}: row mismatch {written} vs {exp}")


def write_partitioned(source_paths, table_slug, cols, batch_size=250_000):
    """Stream source parquet(s) -> hive-partitioned output by year."""
    schema = target_schema(cols)

    def gen():
        n = 0
        for path in source_paths:
            pf = pq.ParquetFile(path)
            for b in pf.iter_batches(batch_size=batch_size):
                n += b.num_rows
                yield transform_batch(b, cols, schema)
        print(f"  streamed {n:,} source rows", flush=True)

    _write_batches(gen, table_slug, cols)


def clean_crosswalk():
    """Crosswalk: derive country_iso3_code from the country name (no iso3 in
    the source), then write partitioned by year."""
    iso = build_iso3_map()
    t = pq.read_table(INPUT / "spatial_admin_crosswalk.parquet")
    countries = t.column("country").to_pylist()
    iso3 = [iso.get(c) for c in countries]
    missing = sorted(
        {c for c, i in zip(countries, iso3, strict=True) if i is None}
    )
    if missing:
        raise RuntimeError(f"crosswalk countries with no ISO3: {missing}")
    t = t.append_column("country_iso3_code", pa.array(iso3, pa.string()))
    schema = target_schema(CROSSWALK_COLS)

    def gen():
        n = 0
        for b in t.to_batches(max_chunksize=250_000):
            n += b.num_rows
            yield transform_batch(b, CROSSWALK_COLS, schema)
        print(f"  streamed {n:,} source rows", flush=True)

    _write_batches(gen, "spatial_admin_crosswalk", CROSSWALK_COLS)


DICIONARIO_ROWS = [
    # (id_tabela, nome_coluna, chave, valor)
    (
        "election_returns",
        "match_confidence",
        "high",
        "Correspondência de alta confiança no Party Facts.",
    ),
    (
        "election_returns",
        "match_confidence",
        "medium",
        "Correspondência de confiança média no Party Facts.",
    ),
    (
        "election_returns",
        "match_confidence",
        "low",
        "Correspondência de baixa confiança no Party Facts.",
    ),
    (
        "election_returns",
        "match_confidence",
        "long_tail",
        "Partido de baixa frequência agrupado no resíduo de cauda longa.",
    ),
    (
        "election_returns",
        "match_confidence",
        "no_match",
        "Nenhuma correspondência encontrada no Party Facts.",
    ),
    (
        "election_returns",
        "geometry_type",
        "Actual",
        "Polígono de fronteira oficial real da unidade.",
    ),
    (
        "election_returns",
        "geometry_type",
        "Thiessen",
        "Polígono de Thiessen/Voronoi gerado ao redor do ponto.",
    ),
    (
        "election_returns",
        "geometry_type",
        "Thiessen Voter Addresses",
        "Polígono de Thiessen construído a partir de endereços de eleitores.",
    ),
    (
        "election_returns",
        "geometry_type",
        "Point",
        "Apenas geometria de ponto (sem polígono).",
    ),
    (
        "election_returns",
        "geometry_type",
        "Manual",
        "Geometria desenhada ou atribuída manualmente.",
    ),
    (
        "election_returns",
        "geometry_type",
        "Missing",
        "Geometria indisponível.",
    ),
]

DICIONARIO_COVERAGE = "1948(1)2026"


def clean_dicionario():
    out_dir = OUTPUT / "dicionario"
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    schema = pa.schema(
        [
            ("id_tabela", pa.string()),
            ("nome_coluna", pa.string()),
            ("chave", pa.string()),
            ("cobertura_temporal", pa.string()),
            ("valor", pa.string()),
        ]
    )
    table = pa.table(
        {
            "id_tabela": [r[0] for r in DICIONARIO_ROWS],
            "nome_coluna": [r[1] for r in DICIONARIO_ROWS],
            "chave": [r[2] for r in DICIONARIO_ROWS],
            "cobertura_temporal": [DICIONARIO_COVERAGE] * len(DICIONARIO_ROWS),
            "valor": [r[3] for r in DICIONARIO_ROWS],
        },
        schema=schema,
    )
    pq.write_table(table, out_dir / "dicionario.parquet", compression="snappy")
    print(f"  wrote dicionario: {table.num_rows} rows", flush=True)


def main():
    print("=== election_returns (streaming 284M rows) ===", flush=True)
    write_partitioned([INPUT / "sage.parquet"], "election_returns", MAIN_COLS)

    print("=== spatial_admin_crosswalk ===", flush=True)
    clean_crosswalk()

    # The *_2021.parquet file is an exact duplicate of the 2021 subset already
    # present in the main file (2017, 2021, 2023); use only the main file.
    print(
        "=== netherlands_preference_votes (2017, 2021, 2023) ===", flush=True
    )
    write_partitioned(
        [INPUT / "netherlands_preference_votes_by_stembureau.parquet"],
        "netherlands_preference_votes",
        NL_COLS,
    )

    print("=== germany_erststimme ===", flush=True)
    write_partitioned(
        [INPUT / "germany_erststimme_candidates_by_wahlkreis.parquet"],
        "germany_erststimme",
        DE_COLS,
    )

    print("=== dicionario ===", flush=True)
    clean_dicionario()

    print("done.", flush=True)


if __name__ == "__main__":
    main()
