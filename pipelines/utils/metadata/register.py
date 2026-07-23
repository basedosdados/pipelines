"""
Orquestradores de alto nível da escrita de metadados.

Funções puras que coordenam as duas operações que as pipelines registram no
backend:

- `register_source_poll` / `register_source_poll_by_size` — "olhei a fonte
  original hoje" (por data máxima ou por tamanho em bytes).
- `register_table_materialization` — "materializei a tabela hoje".

A lógica vive aqui como funções puras: recebem o `client` (MetadataClient) e o
`bq` (adapter de BigQuery) por injeção, de modo a serem testáveis com fakes, sem
rede. Os wrappers Prefect `@task` que os flows consomem (e que constroem o client
a partir do `env`) vivem em `tasks.py`.
"""

from __future__ import annotations

import datetime
from typing import Protocol

from pipelines.utils.metadata import policy
from pipelines.utils.metadata.domain import (
    CoverageSpec,
    NonHistorical,
    PartBdpro,
)
from pipelines.utils.utils import log

DEFAULT_BQ_PROJECT = "basedosdados"


class BQReader(Protocol):
    """Superfície de BigQuery que os orquestradores consomem (injetável)."""

    def read_max_date(
        self, dataset_id: str, table_id: str, coverage: CoverageSpec
    ) -> datetime.date: ...
    def last_modified(
        self, dataset_id: str, table_id: str
    ) -> datetime.datetime: ...
    def can_read_metadata(self, bq_project: str) -> bool: ...
    def apply_row_access_policies(
        self,
        coverage: PartBdpro,
        free_end: datetime.date,
        dataset_id: str,
        table_id: str,
    ) -> None: ...


def register_source_poll(
    client,
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date | None = None,
) -> bool:
    """'Olhei a fonte original hoje' — versão eager (detecta e grava de uma vez).

    Compõe `poll_source_for_update` (registra o `RawDataSource.Poll` de hoje e
    detecta se a fonte tem dados mais novos que o `Table.Update.latest`
    atual) com `commit_source_update` (grava esse Update). Se há novidade, grava
    o Update e devolve True; caso contrário, devolve False sem gravar.
    `source_max_date=None` é a forma explícita de "só polei, sem novidade".

    Mantém o comportamento original de gravar o Update no mesmo passo do poll.
    Flows que precisam adiar a gravação para depois da materialização devem
    chamar `poll_source_for_update` e `commit_source_update` separadamente.
    """

    has_update = poll_source_for_update(
        client=client,
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=source_max_date,
    )

    if has_update:
        commit_source_update(
            client=client,
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=source_max_date,
        )

    return has_update


def register_source_poll_by_size(
    client,
    redis,
    dataset_id: str,
    table_id: str,
    byte_length: int,
) -> bool:
    """'Olhei a fonte original hoje' — variante por TAMANHO (bytes).

    Para fontes que NÃO expõem data máxima (ex.: `br_bcb_sicor`; arquivos em
    massa), onde a detecção de mudança é por byte-length. As escritas de backend
    passam pelo `MetadataClient` (Poll sempre; `RawDataSource.Update` quando há
    novidade) e o histórico de tamanhos é mantido no Redis no formato
    `dataset_id -> {table_id: {date: size}}`.

    - tamanho MAIOR  → novidade: grava Poll + Update(latest=hoje), devolve True.
    - tamanho IGUAL  → sem novidade: grava só Poll, devolve False.
    - tamanho MENOR  → `ValueError` (a fonte encolheu — possível quebra de schema).

    Mantém o comportamento original de gravar o histórico de tamanho e o Update
    no mesmo passo do poll. Flows que precisam adiar essas escritas para depois
    da materialização devem chamar `poll_source_size_for_update` e
    `commit_source_size_update` separadamente.
    """
    has_update = poll_source_size_for_update(
        client=client,
        redis=redis,
        dataset_id=dataset_id,
        table_id=table_id,
        byte_length=byte_length,
    )

    if has_update:
        commit_source_size_update(
            client=client,
            redis=redis,
            dataset_id=dataset_id,
            table_id=table_id,
            byte_length=byte_length,
        )

    return has_update


def poll_source_size_for_update(
    client, redis, dataset_id: str, table_id: str, byte_length: int
) -> bool:
    """Detecta novidade por TAMANHO, sem gravar histórico nem Update.

    Contraparte por bytes de `poll_source_for_update`. Sempre grava um
    `RawDataSource.Poll` (data de hoje) e compara `byte_length` com o último
    tamanho registrado no Redis, mas **não** grava o novo tamanho no histórico
    nem o `RawDataSource.Update` — essas escritas ficam a cargo de
    `commit_source_size_update`, chamada só após a materialização. Assim, se o
    flow falha no meio, nem o histórico de tamanho nem o Update avançam, e a run
    seguinte ainda detecta a novidade e retenta.

    - tamanho MAIOR (ou primeira vez) → novidade: grava só Poll, devolve True.
    - tamanho IGUAL  → sem novidade: grava só Poll, devolve False.
    - tamanho MENOR  → `ValueError` (a fonte encolheu — possível quebra de schema).

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        redis: cliente Redis com o histórico de tamanhos
            (`dataset_id -> {table_id: {date: size}}`).
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        byte_length: tamanho atual da fonte, em bytes.

    Returns:
        bool — True se a fonte trouxe novidade (tamanho maior ou primeira vez);
        False se o tamanho é igual ao último registrado.

    Raises:
        ValueError: se `byte_length` é menor que o último tamanho registrado.
    """
    client.upsert_raw_source_poll(
        dataset_id, table_id, latest=datetime.datetime.today()
    )  # Poll: sempre

    dataset_data = redis.get(dataset_id) or {}
    table_data = dataset_data.get(table_id, {})

    if table_data:
        latest_date = sorted(table_data.keys(), reverse=True)[0]
        latest_size = table_data[latest_date]
        log(
            f"Tamanho na fonte: {byte_length} | último ({latest_date}): {latest_size}"
        )
        if byte_length == latest_size:
            log("Não há novas atualizações na fonte original (tamanho igual)")
            return False
        if byte_length < latest_size:
            raise ValueError(
                f"Tamanho na fonte ({byte_length}) é MENOR que o último "
                f"registrado ({latest_size}) — possível alteração na tabela "
                f"original (coluna removida, recodificação, etc.)"
            )

    log("Há atualizações na fonte original (tamanho maior)")
    return True


def commit_source_size_update(
    client, redis, dataset_id: str, table_id: str, byte_length: int
) -> None:
    """Grava o histórico de tamanho (Redis) e o `RawDataSource.Update`.

    Contraparte por bytes de `commit_source_update`. Registra `byte_length` no
    histórico do Redis (mantendo os últimos 10 registros) e grava
    `RawDataSource.Update.latest` com a data de hoje. Deve ser chamada **só ao
    fim do flow**, depois de a materialização ter dado certo, de modo que o
    histórico e o Update só avancem quando o dado de fato chegou ao destino. É a
    contraparte de `poll_source_size_for_update`, que detecta a novidade sem
    gravar histórico nem Update.

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        redis: cliente Redis com o histórico de tamanhos.
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        byte_length: tamanho atual da fonte, em bytes, gravado no histórico.
    """
    today = datetime.datetime.today()
    today_key = today.strftime("%Y-%m-%d")

    dataset_data = redis.get(dataset_id) or {}
    table_data = dataset_data.get(table_id, {})

    table_data[today_key] = byte_length

    # mantém os últimos 10 registros
    if len(table_data) > 10:
        keep = sorted(table_data.keys(), reverse=True)[:10]
        table_data = {d: table_data[d] for d in keep}
    dataset_data[table_id] = table_data
    redis.set(dataset_id, dataset_data)

    client.upsert_raw_source_update(dataset_id, table_id, latest=today)
    log("Fonte atualizada (tamanho maior) — Update gravado")


def register_table_materialization(
    client,
    bq: BQReader,
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    *,
    env: str = "dev",
    bq_project: str | None = None,
) -> None:
    """'Materializei a tabela hoje.'

    Lê o BQ, atualiza `Coverage.DateTimeRange` (free e/ou pro), aplica Row Access
    Policies (só part_bdpro) e atualiza `Table.Update.latest`.
    """
    bq_project = bq_project or DEFAULT_BQ_PROJECT

    # Trava fail-fast antes de qualquer escrita.
    status = client.get_table_status(dataset_id, table_id)
    policy.assert_write_allowed(env, bq_project, status)

    # NonHistorical: cobertura única vinda da metadata do BQ; sem coverage ranges.
    if isinstance(coverage, NonHistorical):
        client.upsert_table_update(
            dataset_id, table_id, latest=bq.last_modified(dataset_id, table_id)
        )
        return

    source_end = bq.read_max_date(dataset_id, table_id, coverage)

    # A topologia das coberturas existentes deve bater com o tier pedido.
    coverage_ids = client.get_coverage_ids(dataset_id, table_id)
    policy.assert_coverage_topology(coverage, coverage_ids)

    # Cálculo puro dos ranges de cobertura.
    ranges = policy.compute_coverage_ranges(coverage, source_end, coverage_ids)
    for dtr in ranges.to_list():
        client.upsert_coverage_datetime_range(dtr)

    # Row Access Policies só para part_bdpro.
    if policy.needs_row_access_policy(coverage):
        bq.apply_row_access_policies(
            coverage, ranges.free_end, dataset_id, table_id
        )

    # Table.Update, com decisão explícita de pular quando billing != bq_project.
    if bq.can_read_metadata(bq_project):
        client.upsert_table_update(
            dataset_id, table_id, latest=bq.last_modified(dataset_id, table_id)
        )
    else:
        log("Pulando Table.Update: billing != bq_project", "warning")


def poll_source_for_update(
    client,
    dataset_id,
    table_id,
    source_max_date: datetime.date | None = None,
    use_raw_source_update: bool = False,
) -> bool:
    """Detecta se a fonte original tem novidade, sem gravar o Update.

    Sempre registra um `RawDataSource.Poll` (data de hoje) e devolve se a fonte
    traz dados mais novos que o Update de referência — por padrão o
    `Table.Update.latest` (wall clock da última materialização), ou o
    `RawDataSource.Update.latest` (última data de cobertura comitada) quando
    `use_raw_source_update=True`. Ao contrário de `register_source_poll`, **não
    grava** o Update — essa escrita fica a cargo de `commit_source_update`,
    chamada só após a materialização. Assim, se o flow falha no meio, o Update
    não avança e a run seguinte ainda detecta a novidade e retenta.

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        dataset_id: ID do dataset no GCP/BigQuery (ex.: `br_ibge_ipca`).
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: data máxima observada na fonte. `None` (padrão) é a
            forma explícita de "só polei, sem novidade" — grava o Poll e devolve
            `False`.
        use_raw_source_update: registro comparado com `source_max_date`. `False`
            (padrão) usa `Table.Update.latest`; `True` usa
            `RawDataSource.Update.latest`. Passe `True` quando a fonte é 1:1 com
            a tabela e o wall clock da materialização travaria o poll.

    Returns:
        bool — `True` se a fonte tem dados mais novos que o Update de
        referência; `False` caso contrário.
    """

    client.upsert_raw_source_poll(
        dataset_id, table_id, latest=datetime.datetime.today()
    )

    if source_max_date is None:
        return False

    api_latest = (
        client.get_raw_source_update_latest
        if use_raw_source_update
        else client.get_table_update_latest
    )(dataset_id, table_id)
    if not policy.should_update_raw_source(api_latest, source_max_date):
        log("Não há novas atualizações na fonte original")
        return False

    log("Há atualizações na fonte original")
    return True


def commit_source_update(
    client,
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date,
) -> None:
    """Grava o `RawDataSource.Update` da fonte original.

    Registra `source_max_date` como o novo `RawDataSource.Update.latest`. É a
    contraparte de `poll_source_for_update`: deve ser chamada **só ao fim do
    flow**, depois de a materialização ter dado certo, de modo que o Update só
    avance quando o dado de fato chegou ao destino. Separar a detecção (poll) da
    gravação (este commit) é o que evita que uma falha no meio do flow deixe o
    Update adiantado e trave as runs seguintes.

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        dataset_id: ID do dataset no GCP/BigQuery (ex.: `br_ibge_ipca`).
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: data máxima observada na fonte, gravada como o novo
            `RawDataSource.Update.latest`.
    """

    client.upsert_raw_source_update(
        dataset_id, table_id, latest=source_max_date
    )

    log("Data de atualização da fonte original modificada")
