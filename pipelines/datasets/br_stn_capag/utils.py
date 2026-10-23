"""
Acesso ao CKAN do Tesouro Transparente para br_stn_capag.
"""

import re
import shutil
import time
import unicodedata
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx

from pipelines.datasets.br_stn_capag.constants import (
    CKAN_PACKAGES,
    DATA_FORMATS,
    DOWNLOAD_BACKOFF_SECONDS,
    DOWNLOAD_CHUNK_SIZE,
    DOWNLOAD_MAX_ATTEMPTS,
    DOWNLOAD_PAUSE_SECONDS,
    DOWNLOAD_TIMEOUT,
    HTTP_TIMEOUT,
    PACKAGE_SHOW_URL,
    RETRY_STATUS_CODES,
)
from pipelines.utils.utils import log


def _parse_datetime(value: str | None) -> datetime | None:
    """Lê um timestamp ISO do CKAN, que pode vir nulo ou vazio."""
    return datetime.fromisoformat(value) if value else None


def _slugify(text: str) -> str:
    """Converte o nome do recurso num trecho seguro de nome de arquivo."""
    ascii_text = (
        unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    )
    return re.sub(r"[^a-z0-9]+", "_", ascii_text.lower()).strip("_")


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
        `created` e `last_modified` (`datetime`, `last_modified` pode ser
        `None`) e `size` (bytes, `0` quando a fonte não informa).

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

    resources = [
        {
            "id": raw["id"],
            "name": raw["name"].strip(),
            "format": raw["format"].upper(),
            "url": raw["url"],
            "created": _parse_datetime(raw.get("created")),
            "last_modified": _parse_datetime(raw.get("last_modified")),
            "size": int(raw.get("size") or 0),
        }
        for raw in response.json()["result"]["resources"]
        if (raw.get("format") or "").upper() in DATA_FORMATS
    ]
    if not resources:
        raise ValueError(f"O pacote {package_id} não tem recursos de dados.")

    resources.sort(key=lambda resource: resource["created"])
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


def download_resource(resource: dict[str, Any], output_dir: Path) -> Path:
    """Baixa um recurso para `output_dir`.

    O arquivo se chama `<nome do recurso>.<formato>`, com o nome normalizado
    (Ex.: `capag_municipios_2024_14_05_2024.xlsx`)

    Args:
        resource: Um item de `list_resources`.
        output_dir: Diretório de destino, criado se não existir.

    Returns:
        O caminho do arquivo baixado.

    Raises:
        ValueError: Quando o arquivo baixado está vazio.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    path = (
        output_dir
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


def download_table(
    table_id: str,
    resources: list[dict[str, Any]] | None = None,
) -> list[Path]:
    """Baixa os recursos de dados de uma tabela para `PATH/<table_id>/input`.

    Args:
        table_id: `municipios` ou `estados`.
        resources: Recursos a baixar.

    Returns:
        Os caminhos dos arquivos baixados, na ordem de `resources`.
    """
    input_dir = Path("tmp") / table_id / "input"
    shutil.rmtree(input_dir, ignore_errors=True)

    if resources is None:
        resources = list_resources(table_id)
    paths = []
    for resource in resources:
        if paths:
            time.sleep(DOWNLOAD_PAUSE_SECONDS)
        paths.append(download_resource(resource, input_dir))
    return paths
