"""
Flows for world_cricsheet — Prefect 3.

Cricsheet global cricket. The full-history bundle ``all_csv2.zip`` ships the
entire history on every (near-daily) release, so each run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append — this sidesteps the
duplicate-on-append problem the overlapping recent windows would otherwise cause.
Because each run re-ingests the whole 11.4M-row history, it is scheduled
**weekly** (not daily).

Migrated to check_update -> extract_and_load, SEM ``build_and_promote``: ao
contrário do shape padrão de um ``pipeline_factory``/``CheckThenExtractLoadPipeline``
(ver ``pipelines/utils/stage_dispatch.py``), este dataset tem duas restrições que o
building block genérico não resolve sozinho — o mesmo par de restrições de
``br_bcb_ifdata``/``au_rba_statistical_tables``.

1. Download compartilhado e não decomponível: ``clean_all()`` baixa o bundle
   ``all_csv2.zip`` uma única vez e produz as quatro tabelas juntas (``people``,
   ``match_players``, ``matches``, ``deliveries``) — não dá para baixar/limpar
   uma tabela de cada vez sem rebaixar o bundle inteiro 4x. Por isso há só UM
   ``check_update`` e UM ``extract_and_load`` para o dataset inteiro, não um
   par por tabela.

2. Ordem obrigatória no dbt: o teste ``relationships`` de
   ``world_cricsheet__match_players.person_id`` referencia
   ``ref('world_cricsheet__people')`` (ver ``models/world_cricsheet/schema.yml``)
   — então as quatro tabelas precisam ser ``run`` antes de QUALQUER uma ser
   ``test``ada (ver o docstring de ``_materialize`` abaixo). O
   ``dispatch_build_and_promote`` genérico não serve aqui: ele dispara um
   ``build_and_promote`` POR TABELA, cada um de forma assíncrona e independente,
   o que reabriria exatamente essa race (teste de ``match_players`` rodando
   antes de ``people`` existir). A solução é não usar
   ``dispatch_build_and_promote``: ``extract_and_load`` chama ``_materialize``
   diretamente, que sobe as 4 tabelas, roda ``dbt run`` de todas e só depois
   ``dbt test`` de todas.

``get_latest_update`` (tasks.py) também não tem sinal de frescor barato: a
única data de referência mora dentro do próprio bundle, então o check_update
baixa e reconstrói a fonte inteira só pra ler essa data — o mesmo trabalho que
``extract_and_load`` repete, de forma independente, pra baixar de verdade (ver
o banner em tasks.py). Custo duplicado aceito em troca de um único formato de
pipeline em todo o repo (check_update -> extract_and_load, mesmo quando o
check precisa baixar dado).

``check_update``/``commit_source_update_task`` miram explicitamente
``raw_source_url=BUNDLE_URL``: as quatro tabelas estão ligadas a três fontes
cruas (bundle + people + names), e o cliente de poll/commit lança exceção numa
fonte ambígua (2+) a menos que a URL exata seja informada. Por isso este flow
chama ``poll_source_for_update_task``/``commit_source_update_task`` diretamente
em vez de ``check_update_and_dispatch`` (que não expõe ``raw_source_url``).

``register_table_materialization_task`` também continua chamada diretamente
dentro de ``extract_and_load``, não via um ``build_and_promote`` genérico, pelo
mesmo motivo do item 2.

Deploy: ``.github/workflows/scripts/deploy_flows.py`` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it (deployed paused).
"""

import shutil
import tempfile

from prefect.deployments import run_deployment
from prefect.schedules import Cron

from pipelines.datasets.world_cricsheet.constants import constants
from pipelines.datasets.world_cricsheet.tasks import (
    clean_cricsheet,
    download_cricsheet,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    DateOnly,
    FreeLag,
    PartBdpro,
    YearOnly,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.stage_dispatch import Etapa, deploy_tags, deployment_name
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
POLL_TABLE = constants.POLL_TABLE.value
BUNDLE_URL = constants.BUNDLE_RAW_SOURCE_URL.value
ALL_TABLES = constants.ALL_TABLES.value

# Name of the `world_cricsheet_extract_and_load` flow below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`).
# Passed explicitly because `deployment_name`'s default already matches
# `str(Etapa.EXTRACT_AND_LOAD)` — kept explicit so it doesn't depend on that
# coincidence.
_EXTRACT_AND_LOAD_DEPLOYMENT = "world_cricsheet_extract_and_load"

# Coverage spec per table.
#
# deliveries and matches refresh near-daily, so they carry the BD Pro rolling
# window on their start_date DATE column: the most recent 6 months are pro-only,
# everything older is free. Each run recomputes free_end = source_end - free_lag,
# rewrites both DateTimeRanges, and re-issues the BigQuery Row Access Policies, so
# the window slides forward on its own. part_bdpro requires BOTH a free
# (is_closed=False) and a pro (is_closed=True) Coverage to already exist on the
# table, or assert_coverage_topology raises before anything is written.
#
# match_players has only an annual `year` column (no fine date), so it stays
# fully free. people is a non-temporal dimension and takes no coverage spec at
# all (it is uploaded/materialized, but not registered here).
_COVERAGE = {
    "deliveries": PartBdpro(
        date_column=DateOnly(col="start_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "matches": PartBdpro(
        date_column=DateOnly(col="start_date"),
        date_format=DateFormat.YEAR_MD,
        free_lag=FreeLag(unit="months", value=6),
    ),
    "match_players": AllFree(
        date_column=YearOnly(col="year"), date_format=DateFormat.YEAR
    ),
}

# The deliveries build streams 11.4M rows and the bundle extracts to several GB;
# give the worker headroom. check_update does the same download + rebuild (see
# tasks.get_latest_update), so it needs the same memory as extract_and_load.
_JOB_VARIABLES = {"memory": "12Gi"}


def _materialize(target: str, bucket: str, paths: dict) -> None:
    """Sobe o staging e materializa todas as tabelas, depois testa todas.

    A separação entre `run` e `test` é obrigatória aqui, não estilística: o
    teste `relationships` de `world_cricsheet__match_players.person_id`
    referencia `ref('world_cricsheet__people')`. Testando tabela a tabela, o
    teste de `match_players` rodaria antes de `people` existir e falharia com
    `Not found: Table ... world_cricsheet.people`. Num ambiente já povoado isso
    passa despercebido porque `people` sobrou da build anterior; só quebra num
    ambiente limpo, então a ordem precisa ser estrutural.
    """
    for table in ALL_TABLES:
        upload_to_gcs(
            data_path=paths[table],
            dataset_id=DATASET_ID,
            table_id=table,
            bucket_name=bucket,
            dump_mode="overwrite",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="run",
            target=target,
        )

    for table in ALL_TABLES:
        run_dbt(
            dataset_id=DATASET_ID,
            table_id=table,
            dbt_command="test",
            target=target,
        )


@flow(name=f"{Etapa.CHECK_UPDATE}: {DATASET_ID}", log_prints=True)
def world_cricsheet_check_update() -> bool:
    """Baixa e reconstrói o bundle pra ler a data da partida mais recente.

    Ver o banner de `tasks.get_latest_update` pra por que esse estágio já
    baixa e reconstrói o bundle inteiro, em vez de um poll barato.

    Returns:
        `True` se havia partida nova (e `extract_and_load` foi disparado);
        `False` caso contrário.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    result = get_latest_update()
    print(f"max start_date na fonte: {result.reference_date}")

    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=result.reference_date,
        env="prod",
        date_format="%Y-%m-%d",
        raw_source_url=BUNDLE_URL,
        compare_against=result.compare_against,
    )
    if not has_new_data:
        return False

    # Comita o Update da fonte já aqui, antes de disparar o extract_and_load:
    # se o flow seguinte falhar no meio, o metadado da fonte ainda reflete que
    # havia dado novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=result.reference_date,
        env="prod",
        date_format="%Y-%m-%d",
        raw_source_url=BUNDLE_URL,
    )

    run_deployment(
        name=deployment_name(
            DATASET_ID, Etapa.EXTRACT_AND_LOAD, _EXTRACT_AND_LOAD_DEPLOYMENT
        ),
        parameters={
            "download_params": {
                "reference_date": result.reference_date.isoformat()
            }
        },
        timeout=0,
        as_subflow=True,
    )
    return True


world_cricsheet_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# Cricsheet republishes near-daily as matches finish, but each run rebuilds the
# whole 11.4M-row dataset, so a daily cadence would re-ingest everything every
# day. Poll WEEKLY instead — Monday 06:00 BRT — which cuts the cost ~4x while
# keeping freshness fine. The source-poll guard still no-ops between real
# releases, and the full-replace dump means overlapping windows never duplicate.
world_cricsheet_check_update.deploy_schedules = [
    Cron("15 6 * * 1", timezone="America/Sao_Paulo")
]
world_cricsheet_check_update.job_variables = _JOB_VARIABLES


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def world_cricsheet_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Baixa o bundle de novo, reconstrói as 4 tabelas e materializa em lote.

    Args:
        download_params: dict recebido do `check_update` via `run_deployment()`
            — só carrega `reference_date` (a data que disparou esta run); não é
            usada para decidir o que baixar (a fonte não tem parâmetro de
            período, é sempre o bundle de história inteira), só para
            rastreabilidade.
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção. Disparar este flow
            manualmente com `materialize_to_prod=False` substitui o antigo
            parâmetro `force_run` do flow monolítico: como `extract_and_load`
            agora é disparável direto (sem depender do poll de `check_update`
            achar partida nova), rematerializar sob demanda não exige mais uma
            flag separada.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="world_cricsheet_")
    try:
        input_dir = download_cricsheet(work_dir=work_dir)
        result = clean_cricsheet(work_dir=work_dir, input_dir=input_dir)

        _materialize("dev", "basedosdados-dev", result)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", result)

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both the dev-only early return and any exception. A k8s work
        # pool gives each run a fresh pod, but a process/local worker reuses
        # its filesystem — the extracted bundle is several GB.
        shutil.rmtree(work_dir, ignore_errors=True)


world_cricsheet_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
world_cricsheet_extract_and_load.job_variables = _JOB_VARIABLES
