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
    """'Olhei a fonte original hoje.'

    Sempre registra um `RawDataSource.Poll` (data de hoje). Se `source_max_date`
    é mais novo que o `RawDataSource.Update.latest` atual, grava também esse
    Update e devolve True. `source_max_date=None` é a forma explícita de "só
    polei, sem novidade".
    """
    client.upsert_raw_source_poll(
        dataset_id, table_id, latest=datetime.datetime.today()
    )  # Poll: sempre

    if source_max_date is None:
        return False

    api_latest = client.get_raw_source_update_latest(dataset_id, table_id)
    if not policy.should_update_raw_source(api_latest, source_max_date):
        log("Não há novas atualizações na fonte original")
        return False

    client.upsert_raw_source_update(
        dataset_id, table_id, latest=source_max_date
    )
    log("Data de atualização da fonte original modificada")
    return True


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
    """
    today = datetime.datetime.today()
    today_key = today.strftime("%Y-%m-%d")

    client.upsert_raw_source_poll(
        dataset_id, table_id, latest=today
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

    table_data[today_key] = byte_length

    # mantém os últimos 10 registros
    if len(table_data) > 10:
        keep = sorted(table_data.keys(), reverse=True)[:10]
        table_data = {d: table_data[d] for d in keep}
    dataset_data[table_id] = table_data
    redis.set(dataset_id, dataset_data)

    client.upsert_raw_source_update(dataset_id, table_id, latest=today)
    log("Fonte atualizada (tamanho maior) — Update gravado")
    return True


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
