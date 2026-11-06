"""
Coleta e tratamento de br_tse_filiacao_partidaria.

O módulo não importa Prefect. As funções são chamadas pelas tasks de
`tasks.py` e também podem ser executadas diretamente, o que permite conferir a
contagem de linhas antes do upload.
"""

import csv
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from curl_cffi import requests as cffi_requests
from curl_cffi.requests.exceptions import RequestException

from pipelines.datasets.br_tse_filiacao_partidaria.constants import constants

_thread_local = threading.local()


@dataclass(frozen=True)
class Group:
    """Combinação de UF, município, zona e partido que a API consulta.

    Attributes:
        uf: Sigla da UF.
        municipio: Código do município no TSE.
        zona: Código da zona eleitoral no TSE.
        partido: Identificador do partido no TSE.
    """

    uf: str
    municipio: str
    zona: str
    partido: str


def get_source_max_date() -> date:
    """Data de referência da fonte: o dia 1º do mês corrente.

    A API devolve a lista de filiados do momento da consulta, sem data de
    publicação.

    Returns:
        O dia 1º do mês corrente.
    """
    return date.today().replace(day=1)


def get_session() -> cffi_requests.Session:
    """Sessão HTTP da thread atual, com a assinatura TLS do Chrome.

    Returns:
        A sessão da thread, criada na primeira chamada.
    """
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = cffi_requests.Session(
            impersonate=constants.IMPERSONATE.value,
            timeout=constants.REQUEST_TIMEOUT.value,
        )
        _thread_local.session = session
    return session


def fetch_json(url: str) -> Any:
    """Faz um GET na API e devolve o JSON, tentando de novo em caso de falha.

    Args:
        url: Endereço completo da consulta.

    Returns:
        O corpo da resposta, decodificado.

    Raises:
        RuntimeError: A API respondeu sem JSON, ou falhou em todas as
            tentativas.
    """
    last_error: Exception | None = None
    for attempt in range(1, constants.MAX_ATTEMPTS.value + 1):
        try:
            response = get_session().get(url)
        except RequestException as error:
            last_error = error
        else:
            if response.status_code == 200:
                try:
                    return response.json()
                except ValueError as error:
                    raise RuntimeError(
                        f"A API do TSE respondeu sem JSON em {url}. O TSE "
                        "tira o sistema do ar durante as eleições."
                    ) from error
            last_error = RuntimeError(f"HTTP {response.status_code} em {url}")
        time.sleep(constants.RETRY_DELAY_SECONDS.value * attempt)
    raise RuntimeError(
        f"A API do TSE falhou {constants.MAX_ATTEMPTS.value} vezes em {url}"
    ) from last_error


def list_partidos() -> list[str]:
    """Lista os identificadores de todos os partidos.

    Returns:
        Os identificadores, como texto.
    """
    partidos = fetch_json(constants.PARTIDOS_URL.value)
    return [str(partido["id"]) for partido in partidos]


def list_municipios() -> list[tuple[str, str]]:
    """Lista os municípios de todas as UFs.

    Returns:
        Pares de sigla da UF e código do município no TSE.
    """
    municipios = []
    for uf_id in constants.UF_IDS.value:
        url = constants.MUNICIPIOS_URL.value.format(uf_id=uf_id)
        for municipio in fetch_json(url):
            municipios.append(
                (municipio["uf"]["sglUf"], str(municipio["codObjeto"]))
            )
    return municipios


def list_zonas(municipio: tuple[str, str]) -> list[tuple[str, str, str]]:
    """Lista as zonas eleitorais de um município.

    Args:
        municipio: Par de sigla da UF e código do município no TSE.

    Returns:
        Trincas de sigla da UF, código do município e código da zona.
    """
    uf, municipio_id = municipio
    url = constants.ZONAS_URL.value.format(municipio=municipio_id)
    return [
        (uf, municipio_id, str(zona["codObjeto"])) for zona in fetch_json(url)
    ]


def list_groups() -> list[Group]:
    """Monta todos os grupos que a coleta consulta.

    Returns:
        Um grupo por UF, município, zona e partido, ordenados por UF.
    """
    partidos = list_partidos()
    municipios = list_municipios()
    with ThreadPoolExecutor(
        max_workers=constants.MAX_WORKERS.value
    ) as executor:
        zonas_by_municipio = list(executor.map(list_zonas, municipios))

    groups = [
        Group(uf=uf, municipio=municipio, zona=zona, partido=partido)
        for zonas in zonas_by_municipio
        for uf, municipio, zona in zonas
        for partido in partidos
    ]
    return sorted(groups, key=lambda group: group.uf)


def fetch_group(group: Group) -> list[dict]:
    """Baixa todos os filiados de um grupo, página a página.

    Args:
        group: Grupo a consultar.

    Returns:
        Os registros do grupo, na ordem da API.

    Raises:
        RuntimeError: O número de registros baixados difere do
            `totalElements` informado pela API.
    """
    records: list[dict] = []
    page = 0
    while True:
        url = constants.FILIADOS_URL.value.format(
            uf=group.uf,
            municipio=group.municipio,
            zona=group.zona,
            partido=group.partido,
            page=page,
            page_size=constants.PAGE_SIZE.value,
        )
        payload = fetch_json(url)
        page_records = payload["entitys"] or []
        total = payload["totalElements"] or 0
        records.extend(page_records)
        if len(records) >= total or not page_records:
            break
        page += 1

    if len(records) != total:
        raise RuntimeError(
            f"{group}: {len(records)} registros baixados, a API informa "
            f"{total}"
        )
    return records


def parse_date(value: list[int] | None) -> str | None:
    """Converte a data da API, uma lista `[ano, mes, dia, ...]`, para ISO.

    Usa só os três primeiros números, então lê data e data com hora. Data
    inválida, anterior a `MIN_DATE` ou futura vira nulo.

    Args:
        value: A data como a API devolve.

    Returns:
        A data em `AAAA-MM-DD`, ou None.

    Raises:
        ValueError: A data não veio como lista de números.
    """
    if value is None:
        return None
    if not isinstance(value, list) or len(value) < 3:
        raise ValueError(
            f"Data em formato inesperado na API: {type(value).__name__}"
        )
    try:
        parsed = date(value[0], value[1], value[2])
    except ValueError:
        return None
    if not constants.MIN_DATE.value <= parsed <= date.today():
        return None
    return parsed.isoformat()


def build_row(record: dict) -> list[str | None]:
    """Trata um registro da API e o põe na ordem das colunas da staging.

    Args:
        record: Registro de uma filiação, como a API devolve.

    Returns:
        Os valores na ordem de `API_COLUMNS`, seguidos de `EMPTY_COLUMNS`
        vazias.

    Raises:
        KeyError: A API deixou de trazer algum campo de `API_COLUMNS`.
    """
    missing = [
        column
        for column in constants.API_COLUMNS.value
        if column not in record
    ]
    if missing:
        raise KeyError(f"A API deixou de trazer os campos {missing}")

    row: list[str | None] = []
    for column in constants.API_COLUMNS.value:
        value = record[column]
        if value is None:
            row.append(None)
        elif column in constants.DATE_COLUMNS.value:
            row.append(parse_date(value))
        elif column == "tpSexo":
            row.append(constants.SEXO_LABELS.value.get(str(value)))
        elif column in constants.TITLE_CASE_COLUMNS.value:
            row.append(str(value).title())
        else:
            row.append(str(value))
    row.extend(None for _ in constants.EMPTY_COLUMNS.value)
    return row


def build_output_dir() -> Path:
    """Recria o diretório de saída vazio.

    Returns:
        O diretório de saída.
    """
    output_dir = Path(constants.PATH.value) / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    output_dir.mkdir(parents=True)
    return output_dir


def download_table(data_extracao: date) -> Path:
    """Baixa a lista inteira de filiados e grava a staging em CSV.

    Grava à medida que baixa, um bloco de grupos por vez, sem manter a lista
    inteira na memória. Cada UF vira um arquivo em
    `data_extracao=<data_extracao>/sigla_uf=<UF>/data.csv`.

    Args:
        data_extracao: Data da coleta, que nomeia a partição.

    Returns:
        O diretório raiz das partições.
    """
    output_dir = build_output_dir()
    groups = list_groups()
    print(f"{len(groups)} grupos a consultar")

    header = constants.API_COLUMNS.value + constants.EMPTY_COLUMNS.value
    writers: dict[str, Any] = {}
    written = 0
    chunk_size = constants.CHUNK_SIZE.value

    with (
        ExitStack() as files,
        ThreadPoolExecutor(
            max_workers=constants.MAX_WORKERS.value
        ) as executor,
    ):
        for start in range(0, len(groups), chunk_size):
            chunk = groups[start : start + chunk_size]
            for group, records in zip(
                chunk, executor.map(fetch_group, chunk), strict=True
            ):
                writer = writers.get(group.uf)
                if writer is None:
                    partition = (
                        output_dir
                        / f"data_extracao={data_extracao.isoformat()}"
                        / f"sigla_uf={group.uf}"
                    )
                    partition.mkdir(parents=True)
                    file = files.enter_context(
                        open(
                            partition / "data.csv",
                            "w",
                            newline="",
                            encoding="utf-8",
                        )
                    )
                    writer = csv.writer(file)
                    writer.writerow(header)
                    writers[group.uf] = writer
                writer.writerows(build_row(record) for record in records)
                written += len(records)
            print(
                f"{start + len(chunk)}/{len(groups)} grupos, "
                f"{written} filiações gravadas"
            )

    return output_dir
