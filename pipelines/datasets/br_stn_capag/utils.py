"""
Acesso ao CKAN do Tesouro Transparente para br_stn_capag.
"""

import csv
import re
import shutil
import time
import unicodedata
from collections.abc import Iterable
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import openpyxl
import pandas as pd

from pipelines.datasets.br_stn_capag.constants import (
    CHAVE_UNICA,
    CKAN_PACKAGES,
    COLUNAS_FONTE,
    COLUNAS_NUMERICAS,
    COLUNAS_OBRIGATORIAS,
    COLUNAS_SAIDA,
    DATA_FORMATS,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_MAX_ATTEMPTS,
    DOWNLOAD_PAUSE_SECONDS,
    DOWNLOAD_TIMEOUT,
    HEADER_SCAN_ROWS,
    HTTP_TIMEOUT,
    MIN_ROWS,
    MUNICIPIOS_TABLE_ID,
    NULL_TOKENS,
    PACKAGE_SHOW_URL,
    PREFERRED_SHEETS,
    RETRY_STATUS_CODES,
)
from pipelines.utils.utils import log

_NAME_DATE_RE = re.compile(r"(\d{2})/(\d{2})/(\d{4})\s*$")
_NAME_YEAR_RE = re.compile(r"\b(20\d{2})\b")


def _parse_datetime(value: str | None) -> datetime | None:
    """Lê um timestamp ISO do CKAN, que pode vir nulo ou vazio."""
    return datetime.fromisoformat(value) if value else None


def _slugify(text: str) -> str:
    """Normaliza o nome do recurso ou uma string qualquer."""
    ascii_text = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    )
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


def _resource_reference_date(name: str, created: datetime | None) -> date:
    """Data de referência de um recurso, quando presente no nome do arquivo
    (Ex.: `CAPAG Municípios 2024 - 14/05/2024`) ou data de criação no CKAN.
    """
    match = _NAME_DATE_RE.search(name)
    if match:
        day, month, year = map(int, match.groups())
        return date(year, month, day)
    if created is None:
        raise ValueError(
            f"O recurso {name!r} não tem data no nome nem `created`."
        )
    return created.date()


def _retry_wait(response: httpx.Response, attempt: int) -> int:
    """Segundos até a próxima tentativa: o `Retry-After` ou espera crescente."""
    retry_after = response.headers.get("Retry-After", "")
    if retry_after.isdigit():
        return int(retry_after)
    return DOWNLOAD_BACKOFF_SECONDS * attempt


def list_resources(table_id: str) -> list[dict[str, Any]]:
    """Lista os recursos de dados do pacote CKAN de uma tabela.
    Args:
        table_id: `municipios` ou `estados`.

    Returns:
        Um dicionário por recurso, com `id`, `name`, `format`, `url`,
        `created` e `last_modified`, `size`, `year` (o ano da CAPAG,
        lido do nome) e `reference_date`.

    Raises:
        KeyError: Quando `table_id` não tem pacote em `CKAN_PACKAGES`.
        ValueError: Quando o pacote não tem nenhum recurso de dados.
    """
    package_id = CKAN_PACKAGES[table_id]
    response = httpx.get(
        PACKAGE_SHOW_URL,
        params={"id": package_id},
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
    )
    response.raise_for_status()

    resources = []
    for raw in response.json()["result"]["resources"]:
        if (raw.get("format") or "").upper() not in DATA_FORMATS:
            continue
        name = raw["name"].strip()
        created = _parse_datetime(raw.get("created"))
        year = _NAME_YEAR_RE.search(name)
        if year is None:
            raise ValueError(f"O recurso {name!r} não tem ano no nome.")
        resources.append(
            {
                "id": raw["id"],
                "name": name,
                "format": raw["format"].upper(),
                "url": raw["url"],
                "created": created,
                "last_modified": _parse_datetime(raw.get("last_modified")),
                "size": int(raw.get("size") or 0),
                "year": int(year.group(1)),
                "reference_date": _resource_reference_date(name, created),
            }
        )
    if not resources:
        raise ValueError(f"O pacote {package_id} não tem recursos de dados.")

    resources.sort(key=lambda resource: resource["reference_date"])
    log(f"{package_id}: {len(resources)} recursos de dados")
    return resources


def get_source_last_modified(table_id: str) -> date:
    """Lê a data da última publicação ou republicação da tabela na fonte.

    Args:
        table_id: `municipios` ou `estados`.

    Returns:
        O maior `last_modified` entre os recursos de dados, usando `created`
        nos que não têm `last_modified`.
    """
    latest = max(
        resource["last_modified"] or resource["created"]
        for resource in list_resources(table_id)
    )
    log(f"{table_id}: última modificação na fonte em {latest}")
    return latest.date()


def download_resource(resource: dict[str, Any], input_dir: Path) -> Path:
    """Baixa um recurso para `input_dir`.

    Args:
        resource: Um item de `list_resources`.
        input_dir: Diretório de destino, criado se não existir.

    Returns:
        O caminho do arquivo baixado.

    Raises:
        ValueError: Quando o arquivo baixado está vazio.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    path = (
        input_dir
        / f"{_slugify(resource['name'])}.{resource['format'].lower()}"
    )

    for attempt in range(1, DOWNLOAD_MAX_ATTEMPTS + 1):
        with httpx.stream(
            method="GET",
            url=resource["url"],
            timeout=DOWNLOAD_TIMEOUT,
            follow_redirects=True,
        ) as response:
            if (
                response.status_code in RETRY_STATUS_CODES
                and attempt < DOWNLOAD_MAX_ATTEMPTS
            ):
                wait = _retry_wait(response, attempt)
                log(
                    f"HTTP {response.status_code} em {path.name}; "
                    f"tentativa {attempt}, nova tentativa em {wait}s"
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            with open(path, "wb") as file:
                for chunk in response.iter_bytes(
                    chunk_size=DOWNLOAD_CHUNK_SIZE
                ):
                    file.write(chunk)
            break

    size = path.stat().st_size
    if size == 0:
        raise ValueError(f"O download de {resource['url']} veio vazio.")

    log(f"Baixado {path.name}: {size} bytes")
    return path


def select_resources(
    resources: list[dict[str, Any]], reference_date: date | None = None
) -> list[dict[str, Any]]:
    """Filtra os recursos pela data de referência.

    Args:
        resources: Itens de `list_resources`.
        reference_date: Data de referência do recurso. `None` mantém todos

    Returns:
        Os recursos selecionados. Mais de um, se vários têm a mesma data.

    Raises:
        ValueError: Quando nenhum recurso tem essa data de referência.
    """
    if reference_date is None:
        return resources
    selected = [
        resource
        for resource in resources
        if resource["reference_date"] == reference_date
    ]
    if not selected:
        available = sorted({str(r["reference_date"]) for r in resources})
        raise ValueError(
            f"Nenhum recurso com data de referência {reference_date}. "
            f"Disponíveis: {', '.join(available)}"
        )
    return selected


def download_table(
    table_id: str,
    reference_date: date | None = None,
) -> list[tuple[dict[str, Any], Path]]:
    """Baixa os recursos de dados de uma tabela para `tmp/<table_id>/input`.

    Args:
        table_id: `municipios` ou `estados`.
        reference_date: Data de referência do recurso a baixar. `None`
            baixa todos os recursos (histórico).

    Returns:
        Pares `(recurso, caminho do arquivo)`, em ordem de data de
        referência.
    """
    input_dir = Path("tmp") / table_id / "input"
    shutil.rmtree(input_dir, ignore_errors=True)

    resources = select_resources(list_resources(table_id), reference_date)
    downloads = []
    for resource in resources:
        if downloads:
            time.sleep(DOWNLOAD_PAUSE_SECONDS)
        downloads.append((resource, download_resource(resource, input_dir)))
    return downloads


def _is_null(value: Any) -> bool:
    """`True` pro vazio (inclusive `NaN`) e pros marcadores de "sem dado"."""
    if value is None or value != value:
        return True
    if isinstance(value, str):
        return value.strip().lower() in NULL_TOKENS
    return False


def _to_number(value: Any) -> float | None:
    """Converte número, `82,07%` ou `0,8287` (vírgula decimal) pra float.

    Percentuais viram proporção (`82,07%` -> `0.8207`), a mesma escala dos
    demais arquivos.
    """
    if _is_null(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    is_percent = text.endswith("%")
    try:
        number = float(text.rstrip("%").strip().replace(",", "."))
    except ValueError:
        return None
    return round(number / 100, 8) if is_percent else number


def _clean_text(value: Any) -> str | None:
    """Texto sem espaços nas pontas, ou `None` pros marcadores de vazio."""
    return None if _is_null(value) else str(value).strip()


def _extract_records(
    rows: Iterable[tuple[Any, ...]], table_id: str
) -> list[dict[str, Any]] | None:
    """Acha o cabeçalho nas primeiras linhas e lê os dados abaixo dele.

    O cabeçalho é a linha que mapeia, via `COLUNAS_FONTE`, todas as `COLUNAS_OBRIGATORIAS`.

    Returns:
        Um dicionário por linha de dados, com as colunas já renomeadas;
        `None` quando nenhum cabeçalho foi achado.
    """
    aliases = COLUNAS_FONTE[table_id]
    required = set(COLUNAS_OBRIGATORIAS[table_id])
    header: dict[int, str] | None = None
    records = []
    for index, row in enumerate(rows):
        if header is None:
            if index >= HEADER_SCAN_ROWS:
                return None
            mapped: dict[int, str] = {}
            for position, cell in enumerate(row):
                name = aliases.get(_slugify(str(cell))) if cell else None
                if name and name not in mapped.values():
                    mapped[position] = name
            if required <= set(mapped.values()):
                header = mapped
            continue
        records.append(
            {
                name: row[position] if position < len(row) else None
                for position, name in header.items()
            }
        )
    return records if header is not None else None


def _read_records(path: Path, table_id: str) -> list[dict[str, Any]]:
    """Lê os registros de um arquivo baixado, xlsx ou csv.

    No xlsx procura a aba certa: `PREFERRED_SHEETS` primeiro, depois as
    demais na ordem do arquivo.

    Raises:
        ValueError: Quando nenhum cabeçalho esperado é achado.
    """
    if path.suffix == ".csv":
        with open(path, encoding="utf-8-sig", newline="") as file:
            rows = (tuple(row) for row in csv.reader(file, delimiter=";"))
            records = _extract_records(rows, table_id)
        if records is None:
            raise ValueError(f"{path.name}: cabeçalho não encontrado.")
        return records

    # `data_only` pra ler o valor calculado das fórmulas, não o texto delas.
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheets = sorted(
            workbook.worksheets,
            key=lambda sheet: _slugify(sheet.title) not in PREFERRED_SHEETS,
        )
        for sheet in sheets:
            # Alguns arquivos declaram a aba com 1 linha só (`A1`), e o
            # modo de leitura obedeceria isso.
            sheet.reset_dimensions()
            records = _extract_records(
                sheet.iter_rows(values_only=True), table_id
            )
            if records is not None:
                log(f"{path.name}: aba {sheet.title!r}, {len(records)} linhas")
                return records
        raise ValueError(
            f"{path.name}: cabeçalho não encontrado em nenhuma aba "
            f"({', '.join(workbook.sheetnames)})."
        )
    finally:
        workbook.close()


def process_records(
    path: Path, resource: dict[str, Any], table_id: str
) -> pd.DataFrame:
    """Lê um arquivo da fonte e devolve só as colunas de `COLUNAS_SAIDA`.

    Args:
        path: Arquivo baixado de `resource`.
        resource: Item de `list_resources`.
        table_id: `municipios` ou `estados`.

    Returns:
        DataFrame com `COLUNAS_SAIDA[table_id]` mais `ano` (partição). Colunas
        que o arquivo não tem (`populacao` de 2024 em diante, `nota_*` de
        estados em 2018) ficam nulas.

    Raises:
        ValueError: Quando faltam linhas ou há chaves repetidas.
    """
    df = pd.DataFrame(_read_records(path, table_id)).reindex(
        columns=COLUNAS_SAIDA[table_id]
    )

    df["sigla_uf"] = df["sigla_uf"].map(_clean_text).str.upper()
    if table_id == MUNICIPIOS_TABLE_ID:
        # Linhas de rodapé/notas não têm código IBGE.
        df["id_municipio"] = df["id_municipio"].map(_to_number)
        df = df.dropna(subset=["id_municipio"])
        df["id_municipio"] = df["id_municipio"].astype(int).astype(str)
        df["populacao"] = (
            pd.to_numeric(df["populacao"].map(_to_number))
            .round()
            .astype("Int64")
        )
    else:
        df = df[df["sigla_uf"].str.fullmatch(r"[A-Z]{2}", na=False)]

    for column in COLUNAS_NUMERICAS:
        df[column] = df[column].map(_to_number)
    for column in df.columns:
        if column.startswith("nota_"):
            df[column] = df[column].map(_clean_text)

    df["data_referencia"] = resource["reference_date"]
    df["ano"] = resource["year"]

    key = list(CHAVE_UNICA[table_id])
    if len(df) < MIN_ROWS[table_id]:
        raise ValueError(
            f"{path.name}: {len(df)} linhas, esperava pelo menos "
            f"{MIN_ROWS[table_id]}."
        )
    if df.duplicated(key).any():
        raise ValueError(f"{path.name}: chaves {key} repetidas.")
    log(f"{path.name}: {len(df)} linhas tratadas")
    return df


def write_partitioned(
    df: pd.DataFrame, path: Path, resource: dict[str, Any]
) -> Path:
    """Escreve `df` em `<path>/ano=<ano>/<recurso>.csv`

    Returns:
        O caminho do CSV escrito.
    """
    year_dir = path / f"ano={resource['year']}"
    year_dir.mkdir(parents=True, exist_ok=True)
    file = year_dir / f"{_slugify(resource['name'])}.csv"
    df.drop(columns="ano").to_csv(file, index=False)
    return file
