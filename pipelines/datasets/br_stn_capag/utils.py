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
    CHAVE_UNICA_BASE_CALCULO,
    CKAN_PACKAGES,
    COLUNAS_FONTE,
    COLUNAS_NUMERICAS,
    COLUNAS_OBRIGATORIAS,
    COLUNAS_SAIDA,
    COLUNAS_SAIDA_BASE_CALCULO,
    CONTAS_DATALAKE,
    DATA_FORMATS,
    DATALAKE_ID_COLUMN,
    DATALAKE_SHEET,
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
# Cabeçalho da DCA: `<ano><DO|RI|RO><código><conta><código> - <detalhe>`, com
# o código repetido (às vezes sem o último nível) depois do nome. Ex.:
# `2021RI7.0.0.0.00.0.0Deduções - FUNDEB7.0.0.0.00.0.0 - Receitas Correntes`
_DCA_HEADER_RE = re.compile(
    r"^(\d{4})(DO|RI|RO)(\d[\d.]*)([^\d.].*?)\d[\d.]* - (.+)$"
)
# Os prefixos mais longos primeiro, pra `DespesasCorrentesIntra` não cair em
# `DespesasCorrentes`.
_CONTAS_DATALAKE_PREFIXES = sorted(CONTAS_DATALAKE, key=len, reverse=True)


def _parse_datetime(value: str | None) -> datetime | None:
    """Lê um timestamp ISO do CKAN, que pode vir nulo ou vazio."""
    return datetime.fromisoformat(value) if value else None


def _slugify(text: str) -> str:
    """Normaliza o nome do recurso ou uma string qualquer."""
    ascii_text = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    )
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


def _resource_reference_date(name: str) -> date:
    """Data de referência de um recurso: a data no fim do nome
    (Ex.: `CAPAG Municípios 2024 - 14/05/2024`) ou, nos recursos só com o ano
    no nome, 1º de janeiro desse ano.
    """
    match_date = _NAME_DATE_RE.search(name)
    if match_date:
        day, month, year = map(int, match_date.groups())
        return date(year, month, day)
    match_year = _NAME_YEAR_RE.search(name)
    if match_year:
        return date(int(match_year.group(1)), 1, 1)
    raise ValueError(f"O recurso {name!r} não tem data nem ano no nome.")


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
        lido do nome), `reference_date` e `revisao` (se o nome indica um
        arquivo de revisão).

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
                "reference_date": _resource_reference_date(name),
                "revisao": "revisao" in _slugify(name),
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

    Num ano com arquivo de revisão, ele substitui o recurso que só traz o ano
    no nome: o segundo sai da seleção.

    Args:
        resources: Itens de `list_resources`.
        reference_date: Data de referência do recurso. `None` mantém todos

    Returns:
        Os recursos selecionados. Mais de um, se vários têm a mesma data.

    Raises:
        ValueError: Quando nenhum recurso tem essa data de referência.
    """
    revised_years = {r["year"] for r in resources if r["revisao"]}
    resources = [
        r for r in resources if r["revisao"] or r["year"] not in revised_years
    ]
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
    input_dir = Path("tmp") / "br_stn_capag" / table_id / "input"
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


def _to_year(value: Any) -> int | None:
    """Ano de `2022` ou de `CAPAG Ano Base 2023`; `None` se não houver."""
    if _is_null(value):
        return None
    match = _NAME_YEAR_RE.search(str(value))
    return int(match.group(1)) if match else None


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
        if (df["id_municipio"] % 1 != 0).any():
            raise ValueError(f"{path.name}: código IBGE com parte decimal.")
        df["id_municipio"] = df["id_municipio"].astype(int).astype(str)
        df["ano_base"] = df["ano_base"].map(_to_year).astype("Int64")
        df["populacao"] = (
            pd.to_numeric(df["populacao"].map(_to_number))
            .round()
            .astype("Int64")
        )
    else:
        df = df[df["sigla_uf"].str.fullmatch(r"[A-Z]{2}", na=False)]

    for column in COLUNAS_NUMERICAS:
        if column in df.columns:
            df[column] = df[column].map(_to_number)
    for column in df.columns:
        if column.startswith("nota_"):
            df[column] = df[column].map(_clean_text)
    df["data_referencia"] = resource["reference_date"]
    df["data_criacao"] = resource["created"]
    df["data_modificacao"] = resource["last_modified"]
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


def _parse_datalake_header(header: str) -> dict[str, Any]:
    """Separa o cabeçalho de uma coluna da aba Datalake em seus componentes.

    Args:
        header: cabeçalho do demonstrativo
            Ex.: `2021DO3.0.00.00.00.00Despesas Empenhadas3.0.00.00.00 -
            Despesas Correntes` (DCA) ou `2025DividaConsolidadaÚltimo
            PeríodoDÍVIDA CONSOLIDADA - DC (I)` (RGF/RREO).

    Returns:
        `ano_base`, `demonstrativo` (`dca`, `rgf` ou `rreo`), `tipo` (`DO`,
        `RI` ou `RO`, só na DCA), `codigo_conta` (só na DCA), `conta` e
        `detalhe`.

    Raises:
        ValueError: Quando o cabeçalho não segue nenhum formato conhecido.
    """
    match = _DCA_HEADER_RE.match(header)
    if match:
        ano_base, tipo, codigo_conta, conta, detalhe = match.groups()
        return {
            "ano_base": int(ano_base),
            "demonstrativo": "dca",
            "tipo": tipo,
            "codigo_conta": codigo_conta,
            "conta": conta.strip(),
            "detalhe": detalhe.strip(),
        }

    ano_base, rest = header[:4], header[4:]
    for prefix in _CONTAS_DATALAKE_PREFIXES:
        if not (ano_base.isdigit() and rest.startswith(prefix)):
            continue
        demonstrativo, conta, coluna = CONTAS_DATALAKE[prefix]
        detalhe = rest[len(prefix) :]
        if detalhe.startswith(coluna):
            return {
                "ano_base": int(ano_base),
                "demonstrativo": demonstrativo,
                "tipo": None,
                "codigo_conta": None,
                "conta": conta,
                "detalhe": detalhe[len(coluna) :].strip(),
            }
    raise ValueError(f"Cabeçalho do Datalake não reconhecido: {header!r}")


def _read_datalake(path: Path) -> tuple[tuple[Any, ...], list[tuple]]:
    """Lê a aba Datalake de um arquivo.

    Raises:
        ValueError: Quando o arquivo não tem a aba ou o cabeçalho não começa
            pelo código do município.
    """
    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        sheets = [
            sheet
            for sheet in workbook.worksheets
            if _slugify(sheet.title) == DATALAKE_SHEET
        ]
        if not sheets:
            raise ValueError(
                f"{path.name}: sem aba Datalake ({', '.join(workbook.sheetnames)})."
            )
        # Mesmo motivo de `_read_records`: a dimensão declarada pode ser `A1`.
        sheets[0].reset_dimensions()
        rows = sheets[0].iter_rows(values_only=True)
        header = next(rows)
        if _slugify(str(header[0])) != DATALAKE_ID_COLUMN:
            raise ValueError(
                f"{path.name}: a aba Datalake começa em {header[0]!r}, "
                f"esperava {DATALAKE_ID_COLUMN!r}."
            )
        return header, list(rows)
    finally:
        workbook.close()


def process_base_calculo(path: Path, resource: dict[str, Any]) -> pd.DataFrame:
    """Lê a aba Datalake e devolve os valores-base em formato longo.
    Args:
        path: Arquivo xlsx baixado de `resource`.
        resource: Item de `list_resources`.

    Returns:
        DataFrame com `COLUNAS_SAIDA_BASE_CALCULO` mais `ano` (partição).

    Raises:
        ValueError: Quando faltam municípios, o cabeçalho não é reconhecido
            ou há chaves repetidas.
    """
    header, rows = _read_datalake(path)
    columns = {
        position: _parse_datalake_header(str(cell))
        for position, cell in enumerate(header)
        if position > 0 and cell
    }

    records = []
    for row in rows:
        id_municipio = _to_number(row[0]) if row else None
        if id_municipio is None:
            continue
        if id_municipio % 1 != 0:
            raise ValueError(f"{path.name}: código IBGE com parte decimal.")
        for position, parsed in columns.items():
            valor = _to_number(row[position]) if position < len(row) else None
            if valor is not None:
                records.append(
                    {
                        "id_municipio": str(int(id_municipio)),
                        **parsed,
                        "valor": valor,
                    }
                )

    df = pd.DataFrame(records)
    df["data_referencia"] = resource["reference_date"]
    df["ano"] = resource["year"]
    df = df[[*COLUNAS_SAIDA_BASE_CALCULO, "ano"]]

    n_municipios = df["id_municipio"].nunique()
    if n_municipios < MIN_ROWS[MUNICIPIOS_TABLE_ID]:
        raise ValueError(
            f"{path.name}: {n_municipios} municípios, esperava pelo menos "
            f"{MIN_ROWS[MUNICIPIOS_TABLE_ID]}."
        )
    key = list(CHAVE_UNICA_BASE_CALCULO)
    if df.duplicated(key).any():
        raise ValueError(f"{path.name}: chaves {key} repetidas.")
    log(f"{path.name}: {len(df)} valores de {n_municipios} municípios")
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
