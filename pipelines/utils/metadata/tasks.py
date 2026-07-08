"""
Tasks Prefect da camada de metadata — a borda que os flows consomem.

Cada `@task` aqui é uma interface pronta para uso em flows: constrói o
`MetadataClient`/`BigQueryReader` a partir do `env`/`bq_project` informados e
delega para as funções puras de orquestração de `register.py`. O autor do flow
só precisa escolher um `CoverageSpec` e os identificadores da tabela.
"""

from __future__ import annotations

from datetime import date, datetime

import basedosdados as bd
from prefect import task
from redis_pal import RedisPal

from pipelines.constants import constants
from pipelines.utils.metadata.bq import BigQueryReader
from pipelines.utils.metadata.client import MetadataClient
from pipelines.utils.metadata.constants import constants as metadata_constants
from pipelines.utils.metadata.domain import CoverageSpec
from pipelines.utils.metadata.register import (
    commit_source_size_update,
    commit_source_update,
    poll_source_for_update,
    poll_source_size_for_update,
    register_source_poll,
    register_source_poll_by_size,
    register_table_materialization,
)
from pipelines.utils.metadata.utils import get_api_most_recent_date, get_url


def __get_redis_client(
    host: str = "redis.redis.svc.cluster.local",
    port: int = 6379,
    db: int = 0,
    password: str | None = None,
) -> RedisPal:
    return RedisPal(host=host, port=port, db=db, password=password)


@task
def get_today_date():
    """Devolve a data de hoje como string `YYYY-MM-DD`.

    Sem parâmetros. Útil em flows que precisam carimbar a execução com a data
    corrente (ex.: nomear partições, registrar a data de extração).
    """
    d = datetime.today()
    return d.strftime("%Y-%m-%d")


@task
def task_get_api_most_recent_date(
    dataset_id,
    table_id,
    date_format,
    api_mode: str = "prod",
):
    """Lê do backend a data de cobertura mais recente já registrada para a tabela.

    Consulta os `DateTimeRange` de todas as coberturas da tabela na API e devolve
    a maior data de fim. Útil para decidir, num flow, se a fonte traz dados mais
    novos do que o que já está publicado.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery (ex.: `br_bcb_estban`).
        table_id: ID da tabela no GCP/BigQuery.
        date_format: granularidade da cobertura. Um de `"%Y-%m-%d"`, `"%Y-%m"`
            ou `"%Y"`; deve corresponder à granularidade real da coluna de
            cobertura da tabela — um formato incompatível levanta `ValueError`.
        api_mode: backend a consultar — `"prod"` (padrão) ou `"staging"`.

    Returns:
        datetime.date — a maior data de fim entre todas as coberturas (free e
        pro) da tabela.
    """
    backend = bd.Backend(graphql_url=get_url(api_mode))
    return get_api_most_recent_date(
        dataset_id=dataset_id,
        table_id=table_id,
        date_format=date_format,
        backend=backend,
    )


def _coerce_to_date(value: object, date_format: str) -> datetime.date | None:
    """Normaliza a data-da-fonte vinda do flow para `datetime.date`.

    As funções puras a jusante exigem `date`; aqui, no limite flow→domínio, uma
    string crua (ex.: `get_latest_file` devolve `"2026-04"`), um `datetime` ou um
    `pd.Timestamp` (subclasse de `datetime.datetime`, coberta pelo ramo de
    datetime) viram `date`. `None` passa adiante como `None`.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return datetime.strptime(value, date_format).date()
    raise TypeError(f"source_max_date inesperado: {type(value).__name__}")


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def register_source_poll_task(
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date | str | None = None,
    env: str = "dev",
    date_format: str = "%Y-%m-%d",
) -> bool:
    """Registra que a fonte original foi consultada hoje ("poll por data").

    Sempre grava um `Poll` na fonte (data de hoje). Se `source_max_date` indica
    dados mais novos do que o último `Update` registrado, grava também esse
    Update e devolve True; caso contrário devolve False.

    Use esta task quando a fonte EXPÕE uma data máxima (a maioria dos casos).
    Para fontes que só permitem detectar mudança por tamanho de arquivo, use
    `register_source_poll_by_size_task`.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: data máxima observada na fonte. Aceita `date`,
            `datetime`, `pd.Timestamp` ou `str` no formato `date_format`. Passe
            `None` (padrão) para registrar "só polei, sem novidade" — grava o
            Poll e devolve False sem mexer no Update.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        date_format: formato usado para parsear `source_max_date` quando vier
            como string. Padrão `"%Y-%m-%d"`; use `"%Y-%m"` ou `"%Y"` conforme a
            granularidade da string.

    Returns:
        bool — True se um novo `Update` foi gravado (fonte trouxe novidade),
        False caso contrário.
    """
    client = MetadataClient(env=env)
    return register_source_poll(
        client,
        dataset_id,
        table_id,
        _coerce_to_date(source_max_date, date_format),
    )


def _get_redis_client(local_execution: bool = False):
    """Cliente Redis para o poll por tamanho. `local_execution=True` conecta em
    localhost (exige proxy ativa p/ o pod do Redis no k8s); caso contrário usa o
    DNS do serviço no cluster."""
    from redis_pal import RedisPal

    host = "localhost" if local_execution else "redis.redis.svc.cluster.local"
    return RedisPal(host=host, port=6379, db=0, password=None)


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def register_source_poll_by_size_task(
    dataset_id: str,
    table_id: str,
    byte_length: int,
    env: str = "dev",
    local_execution: bool = False,
) -> bool:
    """Registra que a fonte foi consultada hoje, detectando mudança por TAMANHO.

    Para fontes que não expõem data máxima (ex.: arquivos em massa). Compara o
    `byte_length` informado com o último tamanho registrado no Redis:

    - tamanho MAIOR  → grava Poll + Update(hoje), devolve True;
    - tamanho IGUAL  → grava só Poll, devolve False;
    - tamanho MENOR  → levanta `ValueError` (a fonte encolheu).

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        byte_length: tamanho atual da fonte em bytes.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        local_execution: se True, conecta no Redis via `localhost` (exige proxy
            ativo para o pod do Redis); se False (padrão), usa o DNS do serviço
            no cluster.

    Returns:
        bool — True se a fonte trouxe novidade (tamanho maior), False se igual.
    """
    client = MetadataClient(env=env)
    redis = _get_redis_client(local_execution=local_execution)
    return register_source_poll_by_size(
        client, redis, dataset_id, table_id, byte_length
    )


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def register_table_materialization_task(
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    env: str = "dev",
    bq_project: str = "basedosdados",
    prefect_mode: str = "prod",
) -> None:
    """Registra a materialização de uma tabela: atualiza cobertura e atualização.

    Lê o BigQuery, atualiza o(s) `Coverage.DateTimeRange` (free e/ou pro conforme
    o tier), aplica Row Access Policies quando aplicável e atualiza
    `Table.Update.latest`. É a task chamada ao final de um flow de
    materialização.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        coverage: especificação da cobertura (`CoverageSpec`), união discriminada
            pelo campo `tier`:
            - `AllFree`       — toda a série é pública (free);
            - `AllBdpro`      — toda a série é BD pro;
            - `PartBdpro`     — série mista; a parte recente é BD pro e a antiga
              é liberada após `free_lag` (padrão 6 meses);
            - `NonHistorical` — cobertura única derivada do `last_modified` da
              tabela no BQ (sem coluna de data).
            Os tiers com data exigem `date_column` + `date_format` compatíveis
            (validados pelo Pydantic).
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
            Gravar em `prod` com dados de um `bq_project` não-produtivo exige que
            a tabela esteja `under_review`.
        bq_project: projeto BigQuery onde a tabela vive (padrão `"basedosdados"`).
        prefect_mode: define o projeto de billing — `"prod"` (padrão) usa
            `basedosdados`, `"dev"` usa `basedosdados-dev`.

    Returns:
        None.
    """
    billing = metadata_constants.MODE_PROJECT.value[prefect_mode]
    client = MetadataClient(env=env, billing_project=billing)
    bq = BigQueryReader(billing_project_id=billing, bq_project=bq_project)
    register_table_materialization(
        client,
        bq,
        dataset_id,
        table_id,
        coverage,
        env=env,
        bq_project=bq_project,
    )


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def poll_source_for_update_task(
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date | str | None = None,
    env: str = "dev",
    date_format: str = "%Y-%m-%d",
) -> bool:
    """Detecta se a fonte original tem novidade hoje, sem gravar o Update.

    Sempre grava um `Poll` na fonte (data de hoje) e devolve se `source_max_date`
    indica dados mais novos do que o último `Update` registrado — mas, ao
    contrário de `register_source_poll_task`, **não grava** o Update. A gravação
    fica a cargo de `commit_source_update_task`, chamada ao fim do flow, após a
    materialização. Use as duas em par quando a gravação do Update precisa ser
    adiada para não travar runs futuras se o flow falhar no meio.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: data máxima observada na fonte. Aceita `date`,
            `datetime`, `pd.Timestamp` ou `str` no formato `date_format`. Passe
            `None` (padrão) para registrar "só polei, sem novidade" — grava o
            Poll e devolve False.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        date_format: formato usado para parsear `source_max_date` quando vier
            como string. Padrão `"%Y-%m-%d"`; use `"%Y-%m"` ou `"%Y"` conforme a
            granularidade da string.

    Returns:
        bool — True se a fonte trouxe novidade (Update ainda não gravado),
        False caso contrário.
    """

    client = MetadataClient(env=env)
    return poll_source_for_update(
        client,
        dataset_id,
        table_id,
        _coerce_to_date(source_max_date, date_format),
    )


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def commit_source_update_task(
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date | str,
    env: str = "dev",
    date_format: str = "%Y-%m-%d",
) -> None:
    """Grava o `RawDataSource.Update` da fonte original.

    Contraparte de `poll_source_for_update_task`: registra `source_max_date`
    como o novo `Update.latest`. Deve ser chamada **só ao fim do flow**, depois
    da materialização bem-sucedida, para que o Update só avance quando o dado de
    fato chegou ao destino — evitando que uma falha no meio deixe o Update
    adiantado e trave as runs seguintes.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: data máxima observada na fonte, gravada como o novo
            `Update.latest`. Aceita `date`, `datetime`, `pd.Timestamp` ou `str`
            no formato `date_format`. Obrigatória — só se commita quando há data.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        date_format: formato usado para parsear `source_max_date` quando vier
            como string. Padrão `"%Y-%m-%d"`; use `"%Y-%m"` ou `"%Y"` conforme a
            granularidade da string.

    Returns:
        None.
    """

    client = MetadataClient(env=env)
    commit_source_update(
        client,
        dataset_id,
        table_id,
        _coerce_to_date(source_max_date, date_format),
    )


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def poll_source_size_for_update_task(
    dataset_id: str,
    table_id: str,
    byte_length: int,
    env: str = "dev",
    local_execution: bool = False,
) -> bool:
    """Detecta novidade por TAMANHO hoje, sem gravar histórico nem Update.

    Variante por bytes de `poll_source_for_update_task`. Sempre grava um `Poll`
    na fonte (data de hoje) e compara `byte_length` com o último tamanho no
    Redis — mas, ao contrário de `register_source_poll_by_size_task`, **não**
    grava o novo tamanho no histórico nem o Update. A gravação fica a cargo de
    `commit_source_size_update_task`, chamada ao fim do flow, após a
    materialização. Use as duas em par para não travar runs futuras se o flow
    falhar no meio.

    - tamanho MAIOR (ou primeira vez) → grava só Poll, devolve True;
    - tamanho IGUAL  → grava só Poll, devolve False;
    - tamanho MENOR  → levanta `ValueError` (a fonte encolheu).

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        byte_length: tamanho atual da fonte em bytes.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        local_execution: se True, conecta no Redis via `localhost` (exige proxy
            ativo para o pod do Redis); se False (padrão), usa o DNS do serviço
            no cluster.

    Returns:
        bool — True se a fonte trouxe novidade (tamanho maior), False se igual.
    """
    client = MetadataClient(env=env)
    redis = _get_redis_client(local_execution=local_execution)
    return poll_source_size_for_update(
        client, redis, dataset_id, table_id, byte_length
    )


@task(
    retries=constants.TASK_MAX_RETRIES.value,
    retry_delay_seconds=constants.TASK_RETRY_DELAY.value,
)
def commit_source_size_update_task(
    dataset_id: str,
    table_id: str,
    byte_length: int,
    env: str = "dev",
    local_execution: bool = False,
) -> None:
    """Grava o histórico de tamanho (Redis) e o `RawDataSource.Update`.

    Contraparte de `poll_source_size_for_update_task`: registra `byte_length` no
    histórico do Redis (mantendo os últimos 10) e grava o `Update.latest` com a
    data de hoje. Deve ser chamada **só ao fim do flow**, depois da
    materialização bem-sucedida, para que histórico e Update só avancem quando o
    dado de fato chegou ao destino — evitando que uma falha no meio deixe a
    detecção adiantada e trave as runs seguintes.

    Args:
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        byte_length: tamanho atual da fonte em bytes, gravado no histórico.
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        local_execution: se True, conecta no Redis via `localhost`; se False
            (padrão), usa o DNS do serviço no cluster.

    Returns:
        None.
    """
    client = MetadataClient(env=env)
    redis = _get_redis_client(local_execution=local_execution)
    commit_source_size_update(client, redis, dataset_id, table_id, byte_length)
