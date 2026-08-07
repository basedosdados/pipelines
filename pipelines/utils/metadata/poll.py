"""
Processo de poll (modelo novo) — reconcilia a cobertura da fonte com a da tabela.

Compara a cobertura publicada pela fonte com a cobertura já materializada na
tabela e decide quando materializar. Três passos, um por registro de metadado:

- `register_source_coverage` — registra até onde a fonte publicou.
- `check_source_is_ahead_of_table` — a fonte passou a tabela? (o gate)
- `sync_table_coverage` — registra até onde a tabela materializou (faixas de
  cobertura + Row Access Policies) e carimba o `Table.Update` com a hora da
  materialização.

A cobertura já materializada vive nas faixas de cobertura, não no
`Table.Update.latest`: o gate compara cobertura com cobertura, e o
`Table.Update` continua sendo o relógio que o site exibe como "última
atualização na Base dos Dados".

Lógica pura: recebe o `client` (MetadataClient) e o `bq` (adapter de BigQuery)
por injeção, de modo a ser testável com fakes, sem rede. Os wrappers Prefect
`@task` que os flows consomem vivem em `tasks.py`. Primeiro adotante: CNES.
"""

from __future__ import annotations

import datetime

from pipelines.utils.metadata import policy
from pipelines.utils.metadata.client import MetadataClient
from pipelines.utils.metadata.domain import CoverageSpec, NonHistorical
from pipelines.utils.metadata.register import BQReader
from pipelines.utils.utils import log

DEFAULT_BQ_PROJECT = "basedosdados"


def register_source_coverage(
    client: MetadataClient,
    dataset_id: str,
    table_id: str,
    source_max_date: datetime.date | None = None,
) -> bool:
    """Registra até onde a fonte publicou e marca o poll de hoje.

    Sempre grava um `RawDataSource.Poll` (data de hoje). Se `source_max_date` for
    mais recente que o `RawDataSource.Update.latest` atual, avança esse Update
    para `source_max_date` e devolve `True`; caso contrário devolve `False` sem
    mexer no Update.

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        source_max_date: cobertura máxima observada na fonte. `None` (padrão)
            registra "só polei, sem novidade".

    Returns:
        bool — `True` se a cobertura publicada da fonte avançou; `False` caso
        contrário.
    """

    client.upsert_raw_source_poll(
        dataset_id=dataset_id,
        table_id=table_id,
        latest=datetime.datetime.today(),
    )

    if source_max_date is None:
        return False

    most_recent_update = client.get_raw_source_update_latest(
        dataset_id=dataset_id, table_id=table_id
    )

    if most_recent_update is None or source_max_date > most_recent_update:
        client.upsert_raw_source_update(
            dataset_id=dataset_id, table_id=table_id, latest=source_max_date
        )
        log(f"Fonte publicou cobertura nova: {source_max_date}")
        return True

    log("Fonte sem cobertura nova")
    return False


def check_source_is_ahead_of_table(
    client: MetadataClient, dataset_id: str, table_id: str
) -> bool:
    """Diz se há o que materializar: a fonte está à frente da tabela.

    Compara o `RawDataSource.Update.latest` (cobertura publicada pela fonte) com
    o fim das faixas de cobertura da tabela (cobertura já materializada). As duas
    pontas são data de cobertura — o `Table.Update.latest` não entra na conta,
    porque é relógio. Leitura pura — não grava nada.

    Args:
        client: cliente de leitura do backend de metadados (`MetadataClient`).
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.

    Returns:
        bool — `True` se a cobertura da fonte é mais recente que a da tabela;
        `False` caso contrário, inclusive quando a fonte não tem Update.

    Note:
        Tabela sem faixa de cobertura devolve `True` — é a leitura de "nunca
        materializou". Uma tabela `NonHistorical` também não tem faixa, por
        definição, e devolveria `True` em toda run: esse tier ainda não tem
        gate no modelo novo e não deve ser ligado a ele sem um critério
        próprio.
    """
    source_last_update = client.get_raw_source_update_latest(
        dataset_id=dataset_id, table_id=table_id
    )

    table_coverage_end = client.get_table_coverage_end(
        dataset_id=dataset_id, table_id=table_id
    )

    if source_last_update is None:
        return False

    if table_coverage_end is None:
        return True

    return source_last_update > table_coverage_end


def sync_table_coverage(
    client: MetadataClient,
    bq: BQReader,
    dataset_id: str,
    table_id: str,
    coverage: CoverageSpec,
    *,
    env: str = "dev",
    bq_project: str | None = None,
) -> None:
    """Registra até onde a tabela materializou (o commit do modelo novo).

    Atualiza as faixas de cobertura (free/pro) e as Row Access Policies (só
    part_bdpro), e carimba o `Table.Update` com o `last_modified` da tabela no
    BigQuery — "a BD atualizou nesta hora", que é o que o site mostra. A
    cobertura materializada fica nas faixas, e é de lá que o
    `check_source_is_ahead_of_table` a lê na run seguinte.

    Args:
        client: cliente de escrita/leitura do backend de metadados
            (`MetadataClient`).
        bq: adapter de leitura do BigQuery (cobertura máxima, last_modified,
            Row Access Policies).
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.
        coverage: especificação de cobertura/tier da tabela (`CoverageSpec`).
        env: backend de destino — `"dev"` (padrão), `"staging"` ou `"prod"`.
        bq_project: projeto de billing/leitura no BQ; `None` usa
            `DEFAULT_BQ_PROJECT`.
    """
    bq_project = bq_project or DEFAULT_BQ_PROJECT

    table_status = client.get_table_status(
        dataset_id=dataset_id, table_id=table_id
    )

    policy.assert_write_allowed(
        env=env, bq_project=bq_project, table_status=table_status
    )

    if isinstance(coverage, NonHistorical):
        last_modified = bq.last_modified(
            dataset_id=dataset_id, table_id=table_id
        )

        client.upsert_table_update(dataset_id, table_id, latest=last_modified)

        return

    source_coverage = bq.read_max_date(
        dataset_id=dataset_id, table_id=table_id, coverage=coverage
    )

    coverage_ids = client.get_coverage_ids(
        dataset_id=dataset_id, table_id=table_id
    )

    policy.assert_coverage_topology(coverage, coverage_ids)

    ranges = policy.compute_coverage_ranges(
        coverage, source_coverage, coverage_ids
    )

    for coverage_range in ranges.to_list():
        client.upsert_coverage_datetime_range(coverage_range)

    if policy.needs_row_access_policy(coverage):
        bq.apply_row_access_policies(
            # pyrefly: ignore [bad-argument-type]
            coverage,
            # pyrefly: ignore [bad-argument-type]
            ranges.free_end,
            dataset_id,
            table_id,
        )

    if not bq.can_read_metadata(bq_project):
        log("Pulando Table.Update: billing != bq_project", "warning")
        return

    last_modified = bq.last_modified(dataset_id=dataset_id, table_id=table_id)
    client.upsert_table_update(dataset_id, table_id, latest=last_modified)
    log(f"Table.Update carimbado com a materialização de {last_modified}")
