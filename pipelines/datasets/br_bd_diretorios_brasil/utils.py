"""Download + cleaning transform for br_bd_diretorios_brasil.escola.

Pure functions (no Prefect), wrapped by the tasks in ``tasks.py``.

Download strategy
-----------------
The INEP school catalog lives in an OBIEE (Oracle BI) portal that requires an
active browser session for the async Download action.  The synchronous Extract
action works with the anonymous session cookies that the portal hands out on
the first GET — no login needed.

Flow:
  1. GET  saw.dll?dashboard  →  server sets JSESSIONID + ORA_BIPS_NQID cookies
  2. POST saw.dll?Go  with Action=Extract&Format=csv and a P0 to P3 filter on one
     UF  →  returns that UF's CSV

Extract returns at most 100,000 rows, fewer than half the catalog, so the
download runs once per UF and the parts are merged.

This is implemented via subprocess curl (not requests) because the server
resets TLS connections from Python's ssl library but accepts curl's fingerprint.

Directory accumulation
----------------------
The catalog carries only the schools currently in the INEP register — the
source prunes schools extinct in earlier years. Loading it as-is would drop
those schools from the directory, breaking joins from datasets that still
reference their ``id_escola``. So ``clean_catalogo`` unions the catalog with
the published directory and with the schools that only appear in the censo
escolar, and records the outcome per school in ``situacao_catalogo``.
"""

from __future__ import annotations

import datetime
import logging
import subprocess
import tempfile
import time
import unicodedata
from pathlib import Path

import basedosdados as bd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_bd_diretorios_brasil.constants import constants

log = logging.getLogger("br_bd_diretorios_brasil")


# ── source date ──────────────────────────────────────────────────────────────


def get_source_max_date() -> str:
    """Devolve a data que o Catálogo extraído representa.

    O Catálogo é o registro do Inep no momento da extração e não publica data
    de atualização, então a data do download faz esse papel.

    Returns:
        A data de hoje, no formato ``%Y-%m-%d``.
    """
    return datetime.date.today().strftime("%Y-%m-%d")


# ── download ─────────────────────────────────────────────────────────────────


def download_catalogo(input_dir: Path) -> Path:
    """Baixa o Catálogo de Escolas UF por UF e junta as partes num CSV só.

    O ``Extract`` do portal devolve no máximo
    ``constants.EXTRACT_ROW_LIMIT`` linhas, menos da metade do Catálogo, então
    cada UF é baixada separadamente.

    Args:
        input_dir: Diretório onde ficam as partes e o CSV final.

    Returns:
        O caminho do CSV com o Catálogo inteiro.

    Raises:
        RuntimeError: Se alguma UF esgotar as tentativas.
        ValueError: Se alguma UF vier vazia ou bater no limite de linhas.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    part_paths = [
        download_catalogo_uf(input_dir, uf) for uf in constants.UFS.value
    ]
    csv_path = input_dir / "catalogo_escolas.csv"
    merge_csv_parts(part_paths, csv_path)
    return csv_path


def download_catalogo_uf(input_dir: Path, uf: str) -> Path:
    """Baixa as escolas de uma UF, repetindo a rodada inteira quando falha.

    A rodada de cookie e ``Extract`` é repetida inteira porque o portal falha
    de dois jeitos: uma conexão derrubada faz o curl sair com erro, enquanto
    um 502 chega como curl bem-sucedido trazendo HTML.

    Args:
        input_dir: Diretório onde a parte é gravada.
        uf: Sigla da UF.

    Returns:
        O caminho do CSV da UF.

    Raises:
        RuntimeError: Se todas as tentativas falharem.
        ValueError: Se a UF vier vazia ou com ``constants.EXTRACT_ROW_LIMIT``
            linhas, sinal de que o filtro não foi aplicado ou de que o limite
            cortou a UF.
    """
    part_path = input_dir / f"catalogo_escolas_{uf}.csv"

    for attempt in range(1, constants.DOWNLOAD_ATTEMPTS.value + 1):
        try:
            _download_catalogo_once(part_path, uf)
            break
        except RuntimeError as error:
            # Apaga o corpo parcial antes de tentar de novo.
            part_path.unlink(missing_ok=True)
            if attempt == constants.DOWNLOAD_ATTEMPTS.value:
                raise
            log.warning(
                "%s: attempt %d/%d failed (%s). Retrying in %ds...",
                uf,
                attempt,
                constants.DOWNLOAD_ATTEMPTS.value,
                error,
                constants.RETRY_WAIT_SECONDS.value,
            )
            time.sleep(constants.RETRY_WAIT_SECONDS.value)

    rows = len(pd.read_csv(part_path, dtype=str, encoding="utf-8-sig"))
    if rows == 0 or rows >= constants.EXTRACT_ROW_LIMIT.value:
        raise ValueError(
            f"O Catálogo de {uf} veio com {rows} escolas. Zero indica UF "
            f"faltando; {constants.EXTRACT_ROW_LIMIT.value} indica que o "
            "filtro não foi aplicado ou que o limite do portal cortou a UF."
        )
    log.info("%s: %d schools", uf, rows)
    return part_path


def merge_csv_parts(part_paths: list[Path], csv_path: Path) -> None:
    """Junta os CSVs das UFs num só, mantendo o cabeçalho uma vez.

    Args:
        part_paths: CSVs baixados por ``download_catalogo_uf``.
        csv_path: Destino do CSV juntado.

    Raises:
        ValueError: Se alguma parte tiver cabeçalho diferente da primeira.
    """
    first_header = None
    with open(csv_path, "wb") as merged:
        for part_path in part_paths:
            with open(part_path, "rb") as part:
                header = part.readline()
                body = part.read()
            if first_header is None:
                first_header = header
                merged.write(header)
            elif header != first_header:
                raise ValueError(
                    f"Cabeçalho de {part_path.name} difere do da primeira UF."
                )
            merged.write(body)
            if body and not body.endswith(b"\n"):
                merged.write(b"\n")


def _download_catalogo_once(csv_path: Path, uf: str) -> Path:
    """Faz uma rodada de cookie e ``Extract`` no portal, filtrando uma UF.

    Usa curl em subprocesso porque o servidor recusa a impressão digital TLS
    do ``ssl`` do Python. São duas requisições:
      1. GET no painel, para obter o cookie de sessão anônima.
      2. POST com ``Action=Extract`` e o filtro ``P0`` a ``P3`` na coluna
         ``constants.UF_FILTER_COLUMN``, que devolve o CSV da UF.

    Args:
        csv_path: Destino do CSV baixado.
        uf: Sigla da UF usada no filtro.

    Returns:
        ``csv_path``.

    Raises:
        RuntimeError: Se alguma chamada do curl falhar ou a resposta não for
            CSV.
    """
    with tempfile.NamedTemporaryFile(
        suffix=".txt", delete=False
    ) as cookie_file:
        cookie_path = Path(cookie_file.name)

    try:
        log.info("%s: obtaining anonymous session cookies...", uf)
        run_curl(
            [
                "curl",
                "-s",
                "-L",
                "-c",
                str(cookie_path),
                "-b",
                str(cookie_path),
                "-H",
                f"User-Agent: {constants.USER_AGENT.value}",
                "-H",
                "Accept: text/html,*/*",
                "-o",
                "/dev/null",
                constants.DASHBOARD_URL.value,
            ],
            label="GET dashboard (cookie acquisition)",
        )

        log.info("%s: downloading the school catalog via Extract...", uf)
        result = run_curl(
            [
                "curl",
                "-s",
                "-b",
                str(cookie_path),
                "-w",
                "\n%{http_code} %{content_type}",
                "-H",
                f"User-Agent: {constants.USER_AGENT.value}",
                "-H",
                "Content-Type: application/x-www-form-urlencoded",
                "-H",
                f"Referer: {constants.DASHBOARD_URL.value}",
                "-X",
                "POST",
                constants.GO_URL.value,
                "--data-urlencode",
                "Go=",
                "--data-urlencode",
                "Action=Extract",
                "--data-urlencode",
                "Format=csv",
                "--data-urlencode",
                f"path={constants.CATALOG_PATH.value}",
                "--data-urlencode",
                "P0=1",
                "--data-urlencode",
                "P1=eq",
                "--data-urlencode",
                f"P2={constants.UF_FILTER_COLUMN.value}",
                "--data-urlencode",
                f"P3={uf}",
                "-o",
                str(csv_path),
            ],
            label=f"POST Extract {uf}",
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


def run_curl(
    cmd: list[str], *, label: str, capture_stdout: bool = False
) -> subprocess.CompletedProcess:
    """Run a curl command, raising on a non-zero exit or a timeout.

    Args:
        cmd: The full curl argv.
        label: Step name used in the error message.
        capture_stdout: Whether to capture stdout, needed to read ``-w``.

    Returns:
        The completed process.

    Raises:
        RuntimeError: If curl exits non-zero or exceeds the timeout.
    """
    try:
        result = subprocess.run(
            cmd,
            capture_output=capture_stdout,
            text=capture_stdout,
            check=False,
            timeout=constants.CURL_TIMEOUT_SECONDS.value,
        )
    except subprocess.TimeoutExpired as error:
        raise RuntimeError(
            f"curl timed out ({label}) after {constants.CURL_TIMEOUT_SECONDS.value}s"
        ) from error
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
) -> dict[tuple[str, str], str]:
    """Build lookup reading the municipio directory from BigQuery.

    Args:
        billing_project_id: GCP project to bill the query to (default:
            basedosdados-dev).

    Returns:
        Dict mapping (nome_upper, sigla_uf) to the 7-digit IBGE code string.
    """
    log.info(
        "Reading municipio from BigQuery (billing=%s)...", billing_project_id
    )
    query = """
        SELECT id_municipio, nome, sigla_uf
        FROM `basedosdados.br_bd_diretorios_brasil.municipio`
    """
    mun = bd.read_sql(
        query, billing_project_id=billing_project_id, from_file=True
    ).astype(str)
    return build_municipio_lookup_from_df(mun)


def fetch_diretorio_publicado(
    billing_project_id: str = "basedosdados-dev",
) -> pd.DataFrame:
    """Read the published escola directory from BigQuery.

    Feeds the union in ``clean_catalogo``: schools the INEP catalog no longer
    carries are kept from here instead of being dropped.

    Args:
        billing_project_id: GCP project to bill the query to (default:
            basedosdados-dev).

    Returns:
        One row per ``id_escola`` in
        ``basedosdados.br_bd_diretorios_brasil.escola``, holding every staging
        column. ``situacao_catalogo`` feeds the size check in
        ``clean_catalogo``.
    """
    query = f"""
        SELECT {", ".join(constants.COLUMNS.value)}
        FROM `basedosdados.br_bd_diretorios_brasil.escola`
    """
    log.info(
        "Reading published escola directory (billing=%s)...",
        billing_project_id,
    )
    diretorio = bd.read_sql(
        query, billing_project_id=billing_project_id, from_file=True
    )
    log.info("published directory: %d rows", len(diretorio))
    return diretorio


def fetch_censo_escolar(
    billing_project_id: str = "basedosdados-dev",
) -> pd.DataFrame:
    """Lê do Censo Escolar uma linha por escola, com município e UF.

    Alimenta a terceira camada da união em ``clean_catalogo``: as escolas que
    aparecem no Censo Escolar e não estão nem no Catálogo nem no diretório
    publicado. O município e a UF são os do último ano em que a escola aparece.

    Args:
        billing_project_id: Projeto do GCP que paga a consulta (padrão:
            basedosdados-dev).

    Returns:
        Uma linha por ``id_escola`` de
        ``basedosdados.br_inep_censo_escolar.escola``, com ``id_escola``,
        ``id_municipio`` e ``sigla_uf``.
    """
    query = """
        SELECT
            CAST(id_escola AS STRING) AS id_escola,
            CAST(id_municipio AS STRING) AS id_municipio,
            CAST(sigla_uf AS STRING) AS sigla_uf
        FROM `basedosdados.br_inep_censo_escolar.escola`
        WHERE id_escola IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (PARTITION BY id_escola ORDER BY ano DESC) = 1
    """
    log.info(
        "Reading censo escolar schools (billing=%s)...", billing_project_id
    )
    censo = bd.read_sql(
        query, billing_project_id=billing_project_id, from_file=True
    )
    log.info("censo escolar: %d schools", len(censo))
    return censo


def clean_catalogo(
    csv_path: Path,
    output_dir: Path,
    municipio_lookup: dict[tuple[str, str], str] | None = None,
    diretorio_publicado: pd.DataFrame | None = None,
    censo_escolar: pd.DataFrame | None = None,
) -> Path:
    """Limpa o CSV do Catálogo e grava o parquet da staging.

    Colunas:
        As do CSV passam por ``constants.RENAME`` e saem na ordem de
        ``constants.COLUMNS``.

    id_municipio:
        Derivado de (nome_municipio, sigla_uf) pelo ``municipio_lookup``.
        Sem o mapa, ou quando o nome não é encontrado, a coluna fica nula. O
        modelo dbt aceita nulo aqui; acrescentar um teste ``relationships``
        quando o mapa cobrir mais de 95% das linhas.

    situacao_catalogo:
        ``Presente`` para toda escola do Catálogo. As escolas do
        ``diretorio_publicado`` que saíram do Catálogo entram como
        ``Ausente``, com os atributos da última vez em que apareceram. As do
        ``censo_escolar`` que não estão em nenhum dos dois também entram como
        ``Ausente``, só com ``id_escola``, ``id_municipio`` e ``sigla_uf``.
        Quando um id aparece em mais de uma fonte, vale o Catálogo, depois o
        diretório publicado, depois o Censo Escolar.

    Trava de tamanho:
        Com ``diretorio_publicado``, a limpeza falha se o Catálogo trouxer
        menos de ``constants.MIN_CATALOG_SHARE`` das escolas ``Presente`` do
        diretório publicado.

    Valores em branco:
        Toda coluna de texto passa por strip, e o que fica vazio vira nulo: o
        OBIEE grava coordenada ausente como uma sequência de espaços, que o
        ``na_values`` não trata como ausente.

    Saída:
        ``output_dir/escola/data.parquet``, sem partição: a ``escola`` é um
        diretório, não é particionada por ano.

    Args:
        csv_path: CSV baixado por ``download_catalogo``.
        output_dir: Diretório raiz da saída.
        municipio_lookup: Mapa devolvido por ``build_municipio_lookup``.
        diretorio_publicado: Tabela devolvida por
            ``fetch_diretorio_publicado``. Sem ela, as escolas que saíram do
            Catálogo saem também do diretório.
        censo_escolar: Tabela devolvida por ``fetch_censo_escolar``. Sem ela,
            o diretório fica sem as escolas que só aparecem no Censo Escolar.

    Returns:
        O caminho do parquet gravado.

    Raises:
        ValueError: Se o Catálogo vier menor que a trava de tamanho.
    """
    log.info("Reading %s...", csv_path)
    df = pd.read_csv(csv_path, dtype=str, encoding="utf-8-sig", na_values=[""])

    if diretorio_publicado is not None:
        published_presente = (
            diretorio_publicado[constants.SITUACAO_CATALOGO.value]
            == constants.PRESENTE.value
        ).sum()
        minimum = constants.MIN_CATALOG_SHARE.value * published_presente
        if len(df) < minimum:
            raise ValueError(
                f"O Catálogo veio com {len(df)} escolas, menos de "
                f"{constants.MIN_CATALOG_SHARE.value:.0%} das "
                f"{published_presente} escolas Presente no diretório "
                "publicado. O download provavelmente veio incompleto."
            )

    # Rename columns
    df = df.rename(columns=constants.RENAME.value)

    # id_municipio — three-pass resolution:
    #   1. Manual corrections for known name divergences
    #      (constants.MUNICIPIO_NAME_FIXES)
    #   2. Accent-normalized lookup against BD+ municipio directory
    #   3. NULL for the few municipalities not found in the directory
    if municipio_lookup:

        def _resolve_id_municipio(nome: str, uf: str) -> str | None:
            """Resolve id_municipio from a municipality name and state.

            Args:
                nome: Municipality name as published by OBIEE.
                uf: State abbreviation.

            Returns:
                The 7-digit IBGE code, or None when the name is unknown.
            """
            # pass 1: manual fix
            fix = constants.MUNICIPIO_NAME_FIXES.value.get((nome, uf))
            if fix:
                return fix
            # pass 2: normalized lookup
            return municipio_lookup.get((_norm(nome), uf))

        nomes = df["nome_municipio"].astype(str).str.strip()
        ufs = df["sigla_uf"].astype(str).str.strip()
        df["id_municipio"] = [
            _resolve_id_municipio(nome, uf)
            for nome, uf in zip(nomes, ufs, strict=True)
        ]
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

    # Keep the schools the source no longer carries, flagged as absent
    df[constants.SITUACAO_CATALOGO.value] = constants.PRESENTE.value
    if diretorio_publicado is not None:
        no_catalogo = df["id_escola"].astype(str).str.strip()
        publicado = diretorio_publicado.copy()
        publicado["id_escola"] = publicado["id_escola"].astype(str).str.strip()
        ausentes = publicado[~publicado["id_escola"].isin(no_catalogo)]
        ausentes = ausentes.assign(
            **{constants.SITUACAO_CATALOGO.value: constants.AUSENTE.value}
        )
        log.info(
            "situacao_catalogo: %d Presente, %d Ausente",
            len(df),
            len(ausentes),
        )
        df = pd.concat([df, ausentes], ignore_index=True)
    else:
        log.warning(
            "diretorio_publicado not provided — schools removed from the INEP "
            "catalog will be dropped from the directory. Pass the DataFrame "
            "from fetch_diretorio_publicado to keep them."
        )

    if censo_escolar is not None:
        in_directory = df["id_escola"].astype(str).str.strip()
        censo = censo_escolar.copy()
        censo["id_escola"] = censo["id_escola"].astype(str).str.strip()
        censo_only = censo[~censo["id_escola"].isin(in_directory)].copy()
        censo_only[constants.SITUACAO_CATALOGO.value] = constants.AUSENTE.value
        log.info(
            "situacao_catalogo: %d Ausente from censo escolar only",
            len(censo_only),
        )
        df = pd.concat([df, censo_only], ignore_index=True)
    else:
        log.warning(
            "censo_escolar not provided — schools that appear only in the "
            "censo escolar will be missing from the directory. Pass the "
            "DataFrame from fetch_censo_escolar to add them."
        )

    # Ensure all staging columns exist (fill missing with None)
    for col in constants.COLUMNS.value:
        if col not in df.columns:
            df[col] = None

    df = df[constants.COLUMNS.value]

    # OBIEE pads missing latitude/longitude with spaces instead of leaving the
    # field empty, so na_values=[""] does not catch them and the column ends
    # up claiming a coordinate that is only whitespace.
    for col in constants.COLUMNS.value:
        if df[col].dtype == object:
            stripped = df[col].str.strip()
            df[col] = stripped.mask(stripped == "", None)

    # Cast to all-STRING PyArrow table (staging convention — dbt safe_casts later)
    schema = pa.schema([(col, pa.string()) for col in constants.COLUMNS.value])
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)

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
