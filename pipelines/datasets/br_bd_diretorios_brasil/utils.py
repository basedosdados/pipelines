"""Download + cleaning transform for br_bd_diretorios_brasil.escola.

Pure functions (no Prefect) shared by the one-shot bootstrap
(models/br_bd_diretorios_brasil/code/update_escola.py).

Download strategy
-----------------
The INEP school catalog lives in an OBIEE (Oracle BI) portal that requires an
active browser session for the async Download action.  The synchronous Extract
action works with the anonymous session cookies that the portal hands out on
the first GET — no login needed.

Flow:
  1. GET  saw.dll?dashboard  →  server sets JSESSIONID + ORA_BIPS_NQID cookies
  2. POST saw.dll?Go  with Action=Extract&Format=csv  →  returns ~85 MB CSV

This is implemented via subprocess curl (not requests) because the server
resets TLS connections from Python's ssl library but accepts curl's fingerprint.
"""

from __future__ import annotations

import logging
import subprocess
import tempfile
import unicodedata
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger("br_bd_diretorios_brasil")

# ── OBIEE constants ──────────────────────────────────────────────────────────

_BASE_URL = "https://anonymousdata.inep.gov.br/analytics/saw.dll"
_DASHBOARD_URL = f"{_BASE_URL}?dashboard"
_GO_URL = f"{_BASE_URL}?Go"
_CATALOG_PATH = (
    "/shared/Censo da Educação Básica/Catálogo das Escolas/Análises"
    "/Lista das Escolas/Análise - Tabela da lista das escolas - Detalhado"
)
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0"
)

# CSV source column → staging column name
_COL_RENAME = {
    "Restrição de Atendimento": "restricao_atendimento",
    "Escola": "nome",
    "Código INEP": "id_escola",
    "UF": "sigla_uf",
    "Município": "nome_municipio",  # kept for the id_municipio join below
    "Localização": "localizacao",
    "Localidade Diferenciada": "localidade_diferenciada",
    "Categoria Administrativa": "categoria_administrativa",
    "Endereço": "endereco",
    "Telefone": "telefone",
    "Dependência Administrativa": "dependencia_administrativa",
    "Categoria Escola Privada": "categoria_privada",
    "Conveniada Poder Público": "conveniada_poder_publico",
    "Regulamentação pelo Conselho de Educação": "regulacao_conselho_educacao",
    "Porte da Escola": "porte",
    "Etapas e Modalidade de Ensino Oferecidas": "etapas_modalidades_oferecidas",
    "Outras Ofertas Educacionais": "outras_ofertas_educacionais",
    "Latitude": "latitude",
    "Longitude": "longitude",
}

# Final column order matching the dbt model (staging must preserve this order)
_STAGING_COLS = [
    "id_escola",
    "nome",
    "id_municipio",
    "sigla_uf",
    "restricao_atendimento",
    "localizacao",
    "localidade_diferenciada",
    "categoria_administrativa",
    "endereco",
    "telefone",
    "dependencia_administrativa",
    "categoria_privada",
    "conveniada_poder_publico",
    "regulacao_conselho_educacao",
    "porte",
    "etapas_modalidades_oferecidas",
    "outras_ofertas_educacionais",
    "latitude",
    "longitude",
]

# PyArrow schema — all STRING (staging convention; dbt safe_casts to final types)
_PA_SCHEMA = pa.schema([(col, pa.string()) for col in _STAGING_COLS])

# Municipalities whose name in OBIEE differs from the BD+ directory even after
# accent normalization (historical renames, spelling reforms, hyphen variants).
# Maps (OBIEE_nome_exact, sigla_uf) → id_municipio (IBGE 7-digit string).
_MUNICIPIO_NAME_FIXES: dict[tuple[str, str], str] = {
    ("Muquém do São Francisco", "BA"): "2922250",  # "do" → "de"
    ("Santa Terezinha", "BA"): "2928505",  # z → s (Terezinha → Teresinha)
    ("Itapajé", "CE"): "2306306",  # j → g (Itapajé → Itapagé)
    (
        "Barão do Monte Alto",
        "MG",
    ): "3105509",  # "do" → "de" (Barão de Monte Alto)
    ("Dona Euzébia", "MG"): "3122900",  # z → s (Eusébia)
    ("Poxoréu", "MT"): "5107008",  # u → o (Poxoréo)
    ("Santo Antônio de Leverger", "MT"): "5107800",  # "de" → "do"
    ("Santa Izabel do Pará", "PA"): "1506500",  # z → s (Isabel)
    ("Iguaracy", "PE"): "2606903",  # y → i (Iguaraci)
    ("Arez", "RN"): "2401206",  # z → s (Arês — accent only handled by norm)
    ("Assú", "RN"): "2400208",  # Assú → Açu (different word)
    ("Olho d'Água do Borges", "RN"): "2408409",  # space → hyphen before d'Água
    (
        "São Luiz do Anauá",
        "RR",
    ): "1400605",  # BQ stores as "São Luiz" (without "do Anauá")
    ("Grão-Pará", "SC"): "4206108",  # hyphen → space (Grão Pará)
    ("Amparo do São Francisco", "SE"): "2800100",  # "do" → "de"
    ("Graccho Cardoso", "SE"): "2802601",  # cc → c (Gracho Cardoso)
    ("Biritiba Mirim", "SP"): "3506607",  # space → hyphen (Biritiba-Mirim)
    ("Florínea", "SP"): "3516101",  # nea → nia (Florínia)
    ("São Luiz do Paraitinga", "SP"): "3550001",  # z → s (São Luís)
    ("Tabocão", "TO"): "1708254",  # renamed to Fortaleza do Tabocão
    # MG: Passa Vinte, São Tomé das Letras — not in BD+ municipio; left NULL
    # RN: Januário Cicco — not in BD+ municipio; left NULL
}


# ── download ─────────────────────────────────────────────────────────────────


def download_catalogo(input_dir: Path) -> Path:
    """Download the INEP school catalog CSV from OBIEE.

    Uses curl via subprocess to work around the server's TLS fingerprint check.
    Two requests are issued:
      1. GET dashboard page to obtain an anonymous session cookie.
      2. POST with Action=Extract to receive the full CSV (~85 MB, ~212k rows).

    Args:
        input_dir: Directory where the raw CSV will be saved.

    Returns:
        Path to the downloaded CSV file.

    Raises:
        RuntimeError: If either curl call fails or the response is not CSV.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    csv_path = input_dir / "catalogo_escolas.csv"

    with tempfile.NamedTemporaryFile(
        suffix=".txt", delete=False
    ) as cookie_file:
        cookie_path = Path(cookie_file.name)

    try:
        log.info(
            "Step 1/2: obtaining anonymous session cookies from INEP OBIEE..."
        )
        _curl(
            [
                "curl",
                "-s",
                "-L",
                "-c",
                str(cookie_path),
                "-b",
                str(cookie_path),
                "-H",
                f"User-Agent: {_USER_AGENT}",
                "-H",
                "Accept: text/html,*/*",
                "-o",
                "/dev/null",
                _DASHBOARD_URL,
            ],
            label="GET dashboard (cookie acquisition)",
        )

        log.info(
            "Step 2/2: downloading school catalog via OBIEE Extract (~85 MB)..."
        )
        result = _curl(
            [
                "curl",
                "-s",
                "-b",
                str(cookie_path),
                "-w",
                "\n%{http_code} %{content_type}",
                "-H",
                f"User-Agent: {_USER_AGENT}",
                "-H",
                "Content-Type: application/x-www-form-urlencoded",
                "-H",
                f"Referer: {_DASHBOARD_URL}",
                "-X",
                "POST",
                _GO_URL,
                "--data-urlencode",
                "Go=",
                "--data-urlencode",
                "Action=Extract",
                "--data-urlencode",
                "Format=csv",
                "--data-urlencode",
                f"path={_CATALOG_PATH}",
                "-o",
                str(csv_path),
            ],
            label="POST Extract",
            capture_stdout=True,
        )
    finally:
        cookie_path.unlink(missing_ok=True)

    # last line of stdout: "<status_code> <content_type>"
    last_line = result.stdout.strip().rsplit("\n", 1)[-1]
    status_code = last_line.split()[0] if last_line else "?"
    content_type = last_line[len(status_code) :].strip() if last_line else "?"

    if status_code != "200" or "csv" not in content_type.lower():
        raise RuntimeError(
            f"OBIEE Extract failed: HTTP {status_code}, content-type={content_type!r}. "
            "Check that the INEP portal is reachable and the catalog path is still valid."
        )

    size_mb = csv_path.stat().st_size / 1_048_576
    log.info("Downloaded %.1f MB → %s", size_mb, csv_path)
    return csv_path


def _curl(
    cmd: list[str], *, label: str, capture_stdout: bool = False
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd,
        capture_output=capture_stdout,
        text=capture_stdout,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"curl failed ({label}): exit {result.returncode}")
    return result


# ── cleaning ─────────────────────────────────────────────────────────────────


def _norm(s: str) -> str:
    """Remove accents and uppercase — used for accent-insensitive lookup."""
    return (
        unicodedata.normalize("NFD", s)
        .encode("ascii", "ignore")
        .decode()
        .upper()
        .strip()
    )


def build_municipio_lookup_from_df(
    mun: pd.DataFrame,
) -> dict[tuple[str, str], str]:
    """Build (nome_norm, sigla_uf) → id_municipio from a DataFrame.

    Keys are accent-normalized (via ``_norm``) so that minor accent differences
    between the OBIEE source and the BD+ directory are resolved automatically.
    """
    nome_col = "nome" if "nome" in mun.columns else "nome_municipio"
    lookup = {
        (_norm(str(row[nome_col])), str(row["sigla_uf"]).strip()): str(
            row["id_municipio"]
        ).strip()
        for _, row in mun.iterrows()
        if pd.notna(row.get("id_municipio")) and pd.notna(row.get(nome_col))
    }
    log.info("municipio lookup: %d entries", len(lookup))
    return lookup


def build_municipio_lookup(municipio_csv: Path) -> dict[tuple[str, str], str]:
    """Build lookup from a local CSV (id_municipio, nome, sigla_uf)."""
    mun = pd.read_csv(municipio_csv, dtype=str)
    return build_municipio_lookup_from_df(mun)


def build_municipio_lookup_from_bq(
    billing_project_id: str = "basedosdados-dev",
    credentials_path: str | None = None,
) -> dict[tuple[str, str], str]:
    """Build lookup reading ``basedosdados.br_bd_diretorios_brasil.municipio`` from BigQuery.

    Uses google-cloud-bigquery directly (not basedosdados.read_table) to avoid
    the browser-based OAuth flow that blocks headless environments.

    Args:
        billing_project_id: GCP project to bill the query to (default: basedosdados-dev).
        credentials_path: Path to a service account JSON key. If None, falls back to
            ``~/.basedosdados/credentials/staging.json`` then ADC.

    Returns:
        Dict mapping (nome_upper, sigla_uf) to the 7-digit IBGE code string.
    """
    from google.cloud import bigquery
    from google.oauth2 import service_account

    if credentials_path is None:
        default = (
            Path.home() / ".basedosdados" / "credentials" / "staging.json"
        )
        credentials_path = str(default) if default.exists() else None

    credentials = None
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/bigquery"],
        )
        log.info("Using credentials from %s", credentials_path)

    client = bigquery.Client(
        project=billing_project_id, credentials=credentials
    )
    log.info(
        "Reading municipio from BigQuery (billing=%s)...", billing_project_id
    )
    query = """
        SELECT id_municipio, nome, sigla_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    mun = client.query(query).to_dataframe().astype(str)
    return build_municipio_lookup_from_df(mun)


def clean_catalogo(
    csv_path: Path,
    output_dir: Path,
    municipio_lookup: dict[tuple[str, str], str] | None = None,
) -> Path:
    """Clean the raw INEP catalog CSV and write a Parquet file for staging.

    Column mapping:
        CSV columns → _COL_RENAME → staging columns (see _STAGING_COLS).

    id_municipio resolution:
        Derived from (nome_municipio, sigla_uf) via ``municipio_lookup``.
        If the lookup is None or a name is not found, the column is left null.
        The dbt model accepts nulls here; add a ``relationships`` test once the
        lookup covers >95% of rows.

    Output:
        ``output_dir/escola/data.parquet``  (no partition; escola is a static
        directory table, not partitioned by year).

    Args:
        csv_path: Path to the raw CSV from ``download_catalogo``.
        output_dir: Root output directory.
        municipio_lookup: Optional mapping from ``build_municipio_lookup``.

    Returns:
        Path to the written Parquet file.
    """
    log.info("Reading %s...", csv_path)
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", na_values=[""])

    # Rename columns
    df = df.rename(columns=_COL_RENAME)

    # id_municipio — three-pass resolution:
    #   1. Manual corrections for known name divergences (_MUNICIPIO_NAME_FIXES)
    #   2. Accent-normalized lookup against BD+ municipio directory
    #   3. NULL for the few municipalities not found in the directory
    if municipio_lookup:

        def _resolve_id_municipio(row: pd.Series) -> str | None:
            nome = str(row["nome_municipio"]).strip()
            uf = str(row["sigla_uf"]).strip()
            # pass 1: manual fix
            fix = _MUNICIPIO_NAME_FIXES.get((nome, uf))
            if fix:
                return fix
            # pass 2: normalized lookup
            return municipio_lookup.get((_norm(nome), uf))

        df["id_municipio"] = df.apply(_resolve_id_municipio, axis=1)
        matched = df["id_municipio"].notna().sum()
        log.info(
            "id_municipio: %d/%d rows matched (%.1f%%)",
            matched,
            len(df),
            100 * matched / len(df),
        )
        if matched < len(df):
            unmatched = df[df["id_municipio"].isna()][
                ["nome_municipio", "sigla_uf"]
            ].drop_duplicates()
            log.warning(
                "Unmatched municipalities (%d):\n%s",
                len(unmatched),
                unmatched.to_string(index=False),
            )
    else:
        df["id_municipio"] = None
        log.warning(
            "municipio_lookup not provided — id_municipio will be null. "
            "Pass a lookup built from the municipio directory."
        )

    # Drop the raw name column (not in staging schema)
    df = df.drop(columns=["nome_municipio"], errors="ignore")

    # Ensure all staging columns exist (fill missing with None)
    for col in _STAGING_COLS:
        if col not in df.columns:
            df[col] = None

    df = df[_STAGING_COLS]

    # Cast to all-STRING PyArrow table (staging convention — dbt safe_casts later)
    table = pa.Table.from_pandas(df, schema=_PA_SCHEMA, preserve_index=False)

    out_path = output_dir / "escola" / "data.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path, compression="snappy")

    log.info(
        "Wrote %d rows → %s (%.1f MB)",
        len(df),
        out_path,
        out_path.stat().st_size / 1_048_576,
    )
    return out_path
