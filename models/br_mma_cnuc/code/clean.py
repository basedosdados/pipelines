"""
br_mma_cnuc — Unidades de Conservação (CNUC)
Cleans all biannual CSV snapshots into a single stacked parquet table.
Geometry (WKT) is merged from shapefiles where available.
Partitioned by ano + semestre.

Usage:
    python clean.py                # full run
    python clean.py --subset       # 2018_1 only for validation
"""

import argparse
import re
from pathlib import Path

import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output" / "unidades_conservacao"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Shapefile sources: (ano, semestre) → polygon shapefile path ────────────
# Points-only files (shp_2024_1) are excluded; 2025 shapefiles not yet available.
SHAPEFILES = {
    (2024, 2): INPUT_DIR / "shp_2024_2" / "shp_cnuc_2024_10_pol.shp",
}

# ── Column mapping: raw → bd ───────────────────────────────────────────────
COLUMN_MAP = {
    "ID_UC": "id_uc",
    "Código UC": "codigo_uc",
    "Nome da UC": "nome_uc",
    "NOME DA UC": "nome_uc",
    "Informações Gerais": "informacoes_gerais",
    "Esfera Administrativa": "esfera_administrativa",
    "Categoria de Manejo": "categoria_manejo",
    "Categoria IUCN": "categoria_iucn",
    "Grupo": "grupo",
    "PI": "protecao_integral",
    "US": "uso_sustentavel",
    "UF": "sigla_uf",
    "Municípios Abrangidos": "municipios_abrangidos",
    "Ano de criação": "ano_criacao",
    "Ano de Criação": "ano_criacao",
    "Ano do ato legal mais recente": "ano_ato_legal_recente",
    "Ato Legal de Criação": "ato_legal_criacao",
    "Outros atos legais": "outros_atos_legais",
    "Plano de Manejo": "plano_manejo",
    "Conselho Gestor": "conselho_gestor",
    "Órgão Gestor": "orgao_gestor",
    "Fonte da Área: (1 = SHP, 0 = Ato legal)": "fonte_area",
    # Area totals
    "Área (ha)": "area_soma_biomas",  # 2018 only — treated as total
    "Área soma biomas": "area_soma_biomas",
    "Bioma Área (ha)": None,  # drop — duplicate of area_soma_biomas
    "Área soma Biomas Continental": "area_soma_biomas_continental",
    "Área Ato Legal de Criação": "area_ato_legal_criacao",
    # Biome areas (unit = ha, tracked in measurement_unit field)
    "Área (ha) Amazônia": "area_amazonia",
    "Amazônia": "area_amazonia",
    "Área (ha) Caatinga": "area_caatinga",
    "Caatinga": "area_caatinga",
    "Área (ha) Cerrado": "area_cerrado",
    "Cerrado": "area_cerrado",
    "Área (ha) Mata Atlântica": "area_mata_atlantica",
    "Mata Atlântica": "area_mata_atlantica",
    "Área (ha) Pampa": "area_pampa",
    "Pampa": "area_pampa",
    "Área (ha) Pantanal": "area_pantanal",
    "Pantanal": "area_pantanal",
    "Área (ha) Área Marinha": "area_marinha",
    "Área Marinha": "area_marinha",
    # Biome metadata
    "Bioma declarado": "bioma_declarado",
    "Biomas Abrangidos": "biomas_abrangidos",
    "% Além da linha de costa": "percentual_alem_linha_costa",
    # Recortes
    "Recortes (ha)": "recortes",
    "Mar Territorial": "mar_territorial",
    "Município Costeiro": "municipio_costeiro",
    "Município Costeiro + Área Marinha": "municipio_costeiro_area_marinha",
    "Amazônia Legal": "amazonia_legal",
    "Lei da Mata Atlântica": "lei_mata_atlantica",
    "Sobreposição com TI ou TQ": "sobreposicao_ti_tq",
    # Programs / flags
    "Programa/Projeto": "programa_projeto",
    "Sítios do Patrimônio Mundial": "sitios_patrimonio_mundial",
    "Sítios do Patrimônio Natural": "sitios_patrimonio_mundial",  # old name
    "Sítios Ramsar": "sitios_ramsar",
    "Mosaico": "mosaico",
    "Reserva da Biosfera": "reserva_biosfera",
    # IDs
    "Código WDPA": "codigo_wdpa",
    "WDPAID": "codigo_wdpa",
    # QA / admin (2024+)
    "Região": "regiao",
    "Qualidade dos dados georreferenciados": "qualidade_dados_georreferenciados",
    "Presente na versão anterior": "presente_versao_anterior",
    "Diferença Área": "diferenca_area",
    "Razão Diferença Área": "razao_diferenca_area",
    "Data da publicação no CNUC": "data_publicacao_cnuc",
    "Data da última certificação dos dados pelo Órgão Gestor": "data_ultima_certificacao",
}

# ── Final column order ─────────────────────────────────────────────────────
OUTPUT_COLUMNS = [
    "ano",
    "semestre",
    "id_uc",
    "codigo_uc",
    "nome_uc",
    "esfera_administrativa",
    "categoria_manejo",
    "categoria_iucn",
    "grupo",
    "protecao_integral",
    "uso_sustentavel",
    "sigla_uf",
    "municipios_abrangidos",
    "ano_criacao",
    "ano_ato_legal_recente",
    "ato_legal_criacao",
    "outros_atos_legais",
    "plano_manejo",
    "conselho_gestor",
    "orgao_gestor",
    "informacoes_gerais",
    "fonte_area",
    "area_soma_biomas",
    "area_soma_biomas_continental",
    "area_ato_legal_criacao",
    "area_amazonia",
    "area_caatinga",
    "area_cerrado",
    "area_mata_atlantica",
    "area_pampa",
    "area_pantanal",
    "area_marinha",
    "bioma_declarado",
    "biomas_abrangidos",
    "percentual_alem_linha_costa",
    "recortes",
    "mar_territorial",
    "municipio_costeiro",
    "municipio_costeiro_area_marinha",
    "amazonia_legal",
    "lei_mata_atlantica",
    "sobreposicao_ti_tq",
    "programa_projeto",
    "sitios_patrimonio_mundial",
    "sitios_ramsar",
    "mosaico",
    "reserva_biosfera",
    "codigo_wdpa",
    "regiao",
    "qualidade_dados_georreferenciados",
    "presente_versao_anterior",
    "diferenca_area",
    "razao_diferenca_area",
    "data_publicacao_cnuc",
    "data_ultima_certificacao",
    "geometria",
]

FLOAT_COLS = {
    "area_soma_biomas",
    "area_soma_biomas_continental",
    "area_ato_legal_criacao",
    "area_amazonia",
    "area_caatinga",
    "area_cerrado",
    "area_mata_atlantica",
    "area_pampa",
    "area_pantanal",
    "area_marinha",
    "percentual_alem_linha_costa",
    "recortes",
    "mar_territorial",
    "municipio_costeiro",
    "municipio_costeiro_area_marinha",
    "amazonia_legal",
    "lei_mata_atlantica",
    "sobreposicao_ti_tq",
    "diferenca_area",
    "razao_diferenca_area",
}

INT_COLS = {
    "id_uc",
    "ano_criacao",
    "ano_ato_legal_recente",
    "fonte_area",
    "protecao_integral",
    "uso_sustentavel",
}

DATE_COLS = {"data_publicacao_cnuc", "data_ultima_certificacao"}


def parse_filename(path: Path) -> tuple[int, int]:
    m = re.search(r"cnuc_(\d{4})_(\d)", path.name)
    if not m:
        raise ValueError(f"Cannot parse ano/semestre from {path.name}")
    return int(m.group(1)), int(m.group(2))


def read_csv(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "latin1"):
        try:
            return pd.read_csv(
                path, sep=";", encoding=enc, dtype=str, low_memory=False
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path}")


def clean_string(s: pd.Series) -> pd.Series:
    return (
        s.astype(str)
        .str.strip()
        .replace({"nan": pd.NA, "-": pd.NA, "": pd.NA})
    )


def load_geometry(shp_path: Path) -> dict[str, str]:
    """Return {cd_cnuc: wkt_string} from a polygon shapefile."""
    gdf = gpd.read_file(shp_path, encoding="latin1")
    if gdf.crs is None or gdf.crs.to_epsg() != 4674:
        gdf = gdf.to_crs(epsg=4674)
    gdf["_wkt"] = gdf.geometry.to_wkt()
    return dict(
        zip(gdf["cd_cnuc"].astype(str).str.strip(), gdf["_wkt"], strict=False)
    )


def clean_file(path: Path, geo_lookup: dict[int, str] | None) -> pd.DataFrame:
    ano, semestre = parse_filename(path)
    df = read_csv(path)

    # Drop pandas-generated duplicate columns (.1, .2 suffixes)
    df = df.loc[:, ~df.columns.str.match(r".*\.\d+$")]

    # Rename columns
    rename = {c: COLUMN_MAP[c] for c in df.columns if c in COLUMN_MAP}
    df = df.rename(columns=rename)

    # Drop columns mapped to None
    drop = [c for c in df.columns if c in COLUMN_MAP and COLUMN_MAP[c] is None]
    df = df.drop(columns=drop, errors="ignore")

    # Keep only known columns
    known = set(OUTPUT_COLUMNS)
    df = df[[c for c in df.columns if c in known]]

    # Add partition columns
    df["ano"] = ano
    df["semestre"] = semestre

    # Add missing output columns as NA
    for col in OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA

    # Type casts — int
    for col in INT_COLS:
        if col in df.columns:
            cleaned = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.strip()
            )
            s = pd.to_numeric(cleaned, errors="coerce")
            mask = s.isna()
            arr = s.fillna(0).astype(int).astype("Int64")
            arr[mask] = pd.NA
            df[col] = arr

    # Type casts — float (Brazilian formatting: period=thousands, comma=decimal)
    for col in FLOAT_COLS:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(".", "", regex=False)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Type casts — date
    for col in DATE_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], dayfirst=True, errors="coerce"
            ).dt.date

    # Type casts — string
    str_cols = [
        c
        for c in OUTPUT_COLUMNS
        if c not in INT_COLS | FLOAT_COLS | DATE_COLS
        and c not in {"ano", "semestre", "geometria"}
    ]
    for col in str_cols:
        if col in df.columns:
            df[col] = clean_string(df[col])

    # Merge geometry on codigo_uc (cd_cnuc in shapefile — 100% coverage)
    if geo_lookup is not None and "codigo_uc" in df.columns:
        df["geometria"] = (
            df["codigo_uc"].astype(str).str.strip().map(geo_lookup)
        )
    else:
        df["geometria"] = pd.NA

    return df[OUTPUT_COLUMNS]


def _build_schema() -> pa.Schema:
    """Build a fixed pyarrow schema so all partitions are type-consistent."""
    fields = []
    for col in OUTPUT_COLUMNS:
        if col in ("ano", "semestre"):
            continue
        if col in INT_COLS:
            fields.append(pa.field(col, pa.int64()))
        elif col in FLOAT_COLS:
            fields.append(pa.field(col, pa.float64()))
        elif col in DATE_COLS:
            fields.append(pa.field(col, pa.date32()))
        else:
            fields.append(pa.field(col, pa.string()))
    return pa.schema(fields)


_SCHEMA = _build_schema()


def write_partition(df: pd.DataFrame, ano: int, semestre: int):
    out = OUTPUT_DIR / f"ano={ano}" / f"semestre={semestre}"
    out.mkdir(parents=True, exist_ok=True)
    data = df.drop(columns=["ano", "semestre"])
    table = pa.Table.from_pandas(data, schema=_SCHEMA, preserve_index=False)
    pq.write_table(table, out / "data.parquet", compression="snappy")


def main(subset: bool = False):
    files = sorted(INPUT_DIR.glob("cnuc_*.csv"))
    if subset:
        files = [f for f in files if "2018_1" in f.name]

    # Pre-load geometry lookups
    geo_cache: dict[tuple[int, int], dict[int, str]] = {}
    for key, shp_path in SHAPEFILES.items():
        if shp_path.exists():
            print(f"  Loading geometry for {key} ...", flush=True)
            geo_cache[key] = load_geometry(shp_path)
            print(f"    {len(geo_cache[key]):,} polygons loaded")

    total_rows = 0
    for path in files:
        ano, semestre = parse_filename(path)
        geo = geo_cache.get((ano, semestre))
        print(f"  Processing {path.name} ...", end=" ", flush=True)
        df = clean_file(path, geo)
        write_partition(df, ano, semestre)
        n_geo = df["geometria"].notna().sum()
        print(
            f"{len(df):,} rows (geometry: {n_geo:,}) → ano={ano}/semestre={semestre}"
        )
        total_rows += len(df)

    print(f"\nDone. Total rows: {total_rows:,}")
    print(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--subset", action="store_true", help="Run on 2018_1 only"
    )
    args = parser.parse_args()
    main(subset=args.subset)
