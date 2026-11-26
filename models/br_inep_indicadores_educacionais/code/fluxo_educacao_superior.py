"""
Indicadores de fluxo da educação superior (Inep) — Brasil, Região e UF.

Downloads the three files published by Inep, cleans them according to the
architecture tables and writes Hive-partitioned CSV (by `ano_inicial`):

    fluxo_educacao_superior_brasil
    fluxo_educacao_superior_regiao
    fluxo_educacao_superior_uf

Then uploads each table to staging with `bd.Table.create`.

Each source file has four sheets (TX_EVASAO, TX_CONCLUSAO, TX_RETENCAO,
TX_PERMANENCIA) in a wide layout with a two-level header (group: Sexo /
Cor-Raça / PPI / Faixa de Idade / Deficiência, then category). The sheets are
merged side by side on the key (Ano Fluxo[, region/state]).

Cleaning notes:
- `Ano Fluxo` ("2010-2011") becomes `ano_inicial` and `ano_final`.
- Dropout (evasão) starts at 2010-2011; completion, retention and permanence
  only at 2016-2017 — earlier windows are null in those columns.
- UF, sheet TX_EVASAO: in the published file (2010-2024) the Deficiência
  (Sim/Não) columns repeat the PPI (Sim/Não) columns exactly on every row —
  an Inep publication error. Those two columns are written as null (see
  `null_uf_dropout_disability`).
- UF, sheet TX_RETENCAO: Inep does not publish a disability breakdown.

Usage:
    uv run python -m models.br_inep_indicadores_educacionais.code.fluxo_educacao_superior

Data is written to models/br_inep_indicadores_educacionais/code/output/
fluxo_educacao_superior (git-ignored): raw files under input/, one directory
per table next to it. Set SKIP_DOWNLOAD=1 to reuse the downloaded files and
SKIP_UPLOAD=1 to only write the CSVs.
"""

import os
import shutil
import unicodedata
import zipfile
from pathlib import Path

import basedosdados as bd
import certifi
import pandas as pd
import requests

DATASET_ID = "br_inep_indicadores_educacionais"

DATA_DIR = Path(__file__).resolve().parent / "fluxo_educacao_superior"
INPUT = DATA_DIR / "input"
OUTPUT = DATA_DIR / "output"

BASE_URL = "https://download.inep.gov.br/informacoes_estatisticas/indicadores_educacionais"
PERIOD = "2010_2024"

# level -> (name in zip/URL, xlsx name, geographic breakdown header)
LEVELS = {
    "brasil": ("brasil", "INDIC_BRASIL", None),
    "regiao": ("regiao", "INDIC_REGIAO", "Região Geográfica"),
    "uf": ("UF", "INDIC_UF", "Unidade da Federação"),
}

SHEETS = {
    "TX_EVASAO": "taxa_evasao",
    "TX_CONCLUSAO": "taxa_conclusao",
    "TX_RETENCAO": "taxa_retencao",
    "TX_PERMANENCIA": "taxa_permanencia",
}

# (header group, header category) -> column suffix
CATEGORIES = {
    ("Total", None): "total",
    ("Sexo", "Feminino"): "feminino",
    ("Sexo", "Masculino"): "masculino",
    ("Cor-Raça", "Amarela"): "amarela",
    ("Cor-Raça", "Branca"): "branca",
    ("Cor-Raça", "Indígena"): "indigena",
    ("Cor-Raça", "Parda"): "parda",
    ("Cor-Raça", "Preta"): "preta",
    ("Cor-Raça", "Sem Declaração 1"): "sem_declaracao",
    ("Preto/Pardo/Indígena (PPI)", "Sim"): "ppi_sim",
    ("Preto/Pardo/Indígena (PPI)", "Não 1"): "ppi_nao",
    ("Faixa de Idade", "Até 19 anos"): "ate_19_anos",
    ("Faixa de Idade", "20 a 22 anos"): "20_22_anos",
    ("Faixa de Idade", "23 e 24 anos"): "23_24_anos",
    ("Faixa de Idade", "25 a 29 anos"): "25_29_anos",
    ("Faixa de Idade", "30 a 39 anos"): "30_39_anos",
    ("Faixa de Idade", "40 a 49 anos"): "40_49_anos",
    ("Faixa de Idade", "50 anos ou mais"): "50_anos_ou_mais",
    ("Deficiência", "Sim"): "deficiencia_sim",
    ("Deficiência", "Não"): "deficiencia_nao",
}

STATE_ABBREVIATION = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}

REGIONS = {"Centro-Oeste", "Nordeste", "Norte", "Sudeste", "Sul"}

# Column order = architecture tables (Drive).
_RACE_SUFFIXES = [
    "amarela",
    "branca",
    "indigena",
    "parda",
    "preta",
    "sem_declaracao",
]
_AGE_SUFFIXES = [
    "ate_19_anos",
    "20_22_anos",
    "23_24_anos",
    "25_29_anos",
    "30_39_anos",
    "40_49_anos",
    "50_anos_ou_mais",
]
_GEO_KEY = {"brasil": [], "regiao": ["regiao"], "uf": ["sigla_uf"]}


def _architecture_columns(level: str) -> list[str]:
    race = ["ppi_sim", "ppi_nao"] if level == "uf" else _RACE_SUFFIXES
    columns = ["ano_inicial", "ano_final", *_GEO_KEY[level]]
    for prefix in SHEETS.values():
        suffixes = ["total", "feminino", "masculino", *race, *_AGE_SUFFIXES]
        # UF/retention has no disability breakdown, in the source or the architecture
        if not (level == "uf" and prefix == "taxa_retencao"):
            suffixes += ["deficiencia_sim", "deficiencia_nao"]
        columns += [f"{prefix}_{s}" for s in suffixes]
    return columns


COLUMNS = {level: _architecture_columns(level) for level in LEVELS}


# ---------------------------------------------------------------- download


def _inep_ca_bundle() -> str:
    """
    download.inep.gov.br does not send its intermediate certificate (RNP
    ICPEdu), so TLS verification fails. Instead of disabling verification,
    complete the chain with the intermediate named in the certificate (AIA).
    """
    bundle = INPUT / "inep_ca_bundle.pem"
    if bundle.exists():
        return str(bundle)
    der = requests.get(
        "http://secure.globalsign.com/cacert/rnpicpedugr46ovtlsca2025.crt",
        timeout=60,
    )
    der.raise_for_status()
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import Encoding

    pem = x509.load_der_x509_certificate(der.content).public_bytes(
        Encoding.PEM
    )
    bundle.write_bytes(Path(certifi.where()).read_bytes() + pem)
    return str(bundle)


def download() -> None:
    INPUT.mkdir(parents=True, exist_ok=True)
    verify = _inep_ca_bundle()
    for url_name, _, _ in LEVELS.values():
        url = f"{BASE_URL}/indicadores_fluxo_{url_name}_{PERIOD}.zip"
        zip_path = INPUT / f"fluxo_{url_name}.zip"
        print(url)
        for attempt in range(5):
            try:
                response = requests.get(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    verify=verify,
                    timeout=120,
                )
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                if attempt == 4:
                    raise
                print(f"  retry {attempt + 1}/5 ({e})")
        zip_path.write_bytes(response.content)  # pyrefly: ignore[unbound-name]
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(INPUT / zip_path.stem)


# ---------------------------------------------------------------- cleaning


def _normalize(text) -> str | None:
    if pd.isna(text):
        return None
    return unicodedata.normalize("NFC", str(text)).strip()


def read_sheet(
    xlsx: Path, sheet: str, prefix: str, geo_header: str | None
) -> pd.DataFrame:
    raw = pd.read_excel(xlsx, sheet_name=sheet, header=None, dtype=str)

    header_row = raw.index[raw[0].map(_normalize) == "Ano Fluxo"][0]
    groups = raw.iloc[header_row].map(_normalize).ffill()
    categories = raw.iloc[header_row + 1].map(_normalize)

    n_keys = 1 if geo_header is None else 2
    if geo_header is not None:
        found = _normalize(raw.iloc[header_row, 1])
        assert found == geo_header, (xlsx, sheet, found)

    names = ["ano_fluxo"] + ([] if geo_header is None else ["geo"])
    positions = list(range(n_keys))
    for j in range(n_keys, raw.shape[1]):
        group, category = groups[j], categories[j]
        if group is None and category is None:
            continue
        # Stray cells after the last category (e.g. a lone "deficiência" in
        # column 53 of the region file) are ignored.
        if category is None and group != "Total":
            continue
        key = (group, None if group == "Total" else category)
        if key not in CATEGORIES:
            raise ValueError(f"{xlsx.name}/{sheet}: unexpected header {key}")
        names.append(f"{prefix}_{CATEGORIES[key]}")
        positions.append(j)

    df = raw.iloc[header_row + 2 :, positions].copy()
    df.columns = names
    df = df.map(_normalize)

    if geo_header is not None:
        df["ano_fluxo"] = df["ano_fluxo"].ffill()
        df = df[df["geo"].notna()]
    df = df[df["ano_fluxo"].fillna("").str.fullmatch(r"\d{4}-\d{4}")]

    values = df.columns[n_keys:]
    df[values] = df[values].apply(pd.to_numeric, errors="raise")
    return df


def null_uf_dropout_disability(df: pd.DataFrame) -> pd.DataFrame:
    """In the UF file the dropout disability columns copy PPI; null them."""
    yes, no = "taxa_evasao_deficiencia_sim", "taxa_evasao_deficiencia_nao"
    is_copy = (df[yes] == df["taxa_evasao_ppi_sim"]) & (
        df[no] == df["taxa_evasao_ppi_nao"]
    )
    print(
        f"  uf/TX_EVASAO: disability == PPI on {is_copy.sum()} of "
        f"{len(df)} rows — writing null"
    )
    if is_copy.mean() > 0.9:
        df[[yes, no]] = None
    return df


def clean(level: str) -> pd.DataFrame:
    url_name, xlsx_name, geo_header = LEVELS[level]
    xlsx = INPUT / f"fluxo_{url_name}" / f"{xlsx_name}_{PERIOD}.xlsx"
    keys = ["ano_fluxo"] + ([] if geo_header is None else ["geo"])

    df = None
    for sheet, prefix in SHEETS.items():
        part = read_sheet(xlsx, sheet, prefix, geo_header)
        assert not part.duplicated(keys).any(), (level, sheet)
        if level == "uf" and sheet == "TX_EVASAO":
            part = null_uf_dropout_disability(part)
        df = (
            part
            if df is None
            else df.merge(part, on=keys, how="outer", validate="1:1")
        )
    assert df is not None

    years = df["ano_fluxo"].str.split("-", expand=True).astype(int)
    df["ano_inicial"], df["ano_final"] = years[0], years[1]
    assert (df["ano_final"] == df["ano_inicial"] + 1).all()

    if level == "regiao":
        df["regiao"] = df["geo"]
        assert set(df["regiao"]) == REGIONS, set(df["regiao"])
    elif level == "uf":
        unknown = set(df["geo"]) - STATE_ABBREVIATION.keys()
        assert not unknown, unknown
        df["sigla_uf"] = df["geo"].map(STATE_ABBREVIATION)
        assert df["sigla_uf"].nunique() == 27

    missing = set(COLUMNS[level]) - set(df.columns)
    extra = set(df.columns) - set(COLUMNS[level]) - set(keys)
    assert not missing and not extra, (level, missing, extra)

    return (
        df[COLUMNS[level]]
        .sort_values(["ano_inicial", *_GEO_KEY[level]])
        .reset_index(drop=True)
    )


def write_csv(df: pd.DataFrame, level: str) -> Path:
    """Hive-partitioned CSV by ano_inicial (the partition column is in the path)."""
    table_id = f"fluxo_educacao_superior_{level}"
    table_dir = OUTPUT / table_id
    shutil.rmtree(table_dir, ignore_errors=True)
    for year, part in df.groupby("ano_inicial"):
        partition = table_dir / f"ano_inicial={year}"
        partition.mkdir(parents=True, exist_ok=True)
        part.drop(columns="ano_inicial").to_csv(
            partition / f"{table_id}.csv", index=False
        )
    print(
        f"  {table_id}: {len(df)} rows, {df.shape[1]} columns -> {table_dir}"
    )
    return table_dir


def upload(table_dir: Path) -> None:
    bd.Table(dataset_id=DATASET_ID, table_id=table_dir.name).create(
        path=table_dir,
        if_storage_data_exists="replace",
        if_table_exists="replace",
    )


def main(skip_download: bool = False, skip_upload: bool = False) -> None:
    if not skip_download:
        download()
    for level in LEVELS:
        print(level)
        table_dir = write_csv(clean(level), level)
        if not skip_upload:
            upload(table_dir)


if __name__ == "__main__":
    main(
        skip_download=os.environ.get("SKIP_DOWNLOAD") == "1",
        skip_upload=os.environ.get("SKIP_UPLOAD") == "1",
    )
