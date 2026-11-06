"""Flows for world_wb_wdi — Prefect 3.

World Bank World Development Indicators (WDI). The bulk WDI_CSV.zip carries the
full history on every release, so each run is a **full replace**
(``dump_mode="overwrite"``), not an incremental append.

Migrated to check_update -> extract_and_load, SEM `build_and_promote`: ao
contrário do shape padrão de um `pipeline_factory`/`CheckThenExtractLoadPipeline`
(ver `pipelines/utils/stage_dispatch.py`), este dataset tem duas restrições que o
building block genérico não resolve sozinho — o mesmo par de restrições de
`br_bcb_ifdata`/`au_rba_statistical_tables`.

1. Download compartilhado e não decomponível: um único ``WDI_CSV.zip`` de
   ~270MB, lido e derretido (melt) numa passada só por `clean_all()`, produz
   as seis tabelas juntas (``data``, ``indicators``, ``country_indicator``,
   ``footnote``, ``indicator_time``, ``dicionario``) — não dá para baixar/
   limpar uma tabela de cada vez sem reprocessar o zip inteiro 6x. Por isso há
   só UM ``check_update`` e UM ``extract_and_load`` para o dataset inteiro,
   não um par por tabela.

2. Ordem obrigatória no dbt: o teste ``relationships`` de ``data``,
   ``country_indicator`` e ``indicator_time`` referencia
   ``ref('world_wb_wdi__indicators')`` (ver
   ``models/world_wb_wdi/schema.yml``) — então as seis tabelas precisam ser
   ``run`` antes de QUALQUER uma ser ``test``ada (ver o docstring de
   `_materialize` abaixo). O `dispatch_build_and_promote` genérico não serve
   aqui: ele dispara um `build_and_promote` POR TABELA, cada um de forma
   assíncrona e independente, o que reabriria exatamente essa race (teste de
   uma tabela rodando antes de a outra existir). A solução é não usar
   `dispatch_build_and_promote`: `extract_and_load` chama `_materialize`
   diretamente, que sobe as 6 tabelas, roda `dbt run` de todas e só depois
   `dbt test` de todas.

`get_latest_update` (tasks.py) não tem sinal de frescor barato: o ano mais
recente só existe dentro do próprio zip, então o check_update baixa e faz o
melt completo da fonte só pra ler esse ano — o mesmo trabalho que
`extract_and_load` repete, de forma independente, pra baixar de verdade (ver o
banner em tasks.py). Custo duplicado aceito em troca de um único formato de
pipeline em todo o repo (check_update -> extract_and_load, mesmo quando o
check precisa baixar dado) — a variante "check_and_download" que reaproveitaria
o zip já baixado foi cancelada.

`register_table_materialization_task` também continua chamada diretamente
dentro de `extract_and_load`, não via um `build_and_promote` genérico, pelo
mesmo motivo do item 2.

WDI é CC BY 4.0 e totalmente aberto, então toda tabela é AllFree — sem paywall
BD Pro (o paywall de janela móvel só se aplica a tabelas mensais ou mais
frequentes). As três tabelas de referência sem data (``indicators``,
``country_indicator``, ``dicionario``) tomam NonHistorical (cobertura única de
last-modified).

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it.
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.world_wb_wdi.constants import constants
from pipelines.datasets.world_wb_wdi.tasks import (
    clean_wdi,
    download_wdi,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.stage_dispatch import (
    Etapa,
    check_update_and_dispatch,
    deploy_tags,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
POLL_TABLE = constants.POLL_TABLE.value

# Name of the `world_wb_wdi_extract_and_load` flow below (`deploy_flows.py`
# registers a deployment under the Python variable name of the `@flow`).
# Passed explicitly to `check_update_and_dispatch` because it's literally
# `str(Etapa.EXTRACT_AND_LOAD)` already — kept explicit so it doesn't depend
# on that coincidence.
_EXTRACT_AND_LOAD_DEPLOYMENT = "world_wb_wdi_extract_and_load"


def _materialize(target: str, bucket: str, paths: dict) -> None:
    """Sobe o staging e materializa todas as tabelas, depois testa todas.

    A separação entre `run` e `test` é obrigatória aqui, não estilística: o
    teste `relationships` de `data`, `country_indicator` e `indicator_time`
    referencia `ref('world_wb_wdi__indicators')`. Testando tabela a tabela, o
    teste da primeira roda antes de `indicators` existir e falha com
    `Not found: Table ... world_wb_wdi.indicators`. Num ambiente já povoado
    isso passa despercebido porque a tabela sobrou da build anterior; só
    quebra num ambiente limpo, então a ordem precisa ser estrutural.
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
def world_wb_wdi_check_update() -> bool:
    """Baixa e faz o melt da fonte pra ler o ano mais recente publicado.

    Ver o banner de `tasks.get_latest_update` pra por que esse estágio já
    baixa e processa o zip inteiro, em vez de um poll barato.

    Returns:
        `True` se havia ano novo (e `extract_and_load` foi disparado); `False`
        caso contrário.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    result = get_latest_update()
    print(f"max year na fonte: {result.reference_date.year}")

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y",
        compare_against=result.compare_against,
    )


world_wb_wdi_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# WDI data is annual; the World Bank refreshes the bulk archive roughly
# quarterly. Poll on the 15th of March, June, September and December at 16:00
# BRT — the source-poll guard no-ops until a new year of data actually
# appears, so this ingests the annual update whenever the World Bank
# publishes it.
world_wb_wdi_check_update.deploy_schedules = [
    Cron("20 16 15 3,6,9,12 *", timezone="America/Sao_Paulo")
]
# check_update downloads and melts the full archive too (see tasks.py),
# holding the same ~26M-row long frame in memory.
world_wb_wdi_check_update.job_variables = {"memory": "16Gi"}


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def world_wb_wdi_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Baixa o zip de novo, reconstrói as 6 tabelas e materializa em lote.

    Args:
        download_params: dict recebido do `check_update` via `run_deployment()`
            — só carrega `reference_date` (o ano que disparou esta run); não é
            usado para decidir o que baixar (a fonte não tem parâmetro de
            período, o zip sempre traz a série inteira), só para
            rastreabilidade.
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção. Disparar este flow
            manualmente com `materialize_to_prod=False` substitui o antigo
            parâmetro `force_run` do flow monolítico: como `extract_and_load`
            agora é disparável direto (sem depender do poll de `check_update`
            achar ano novo), rematerializar sob demanda não exige mais uma
            flag separada.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="world_wb_wdi_")
    try:
        input_dir = download_wdi(work_dir=work_dir)
        result = clean_wdi(work_dir=work_dir, input_dir=input_dir)

        _materialize("dev", "basedosdados-dev", result)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", result)

        if update_metadata:
            for table in ALL_TABLES:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Covers both the dev-only early return and any exception. The k8s
        # work pool gives each run a fresh pod, but a process/local worker
        # reuses its filesystem — the download is ~270MB and the melt holds
        # tens of millions of rows.
        shutil.rmtree(work_dir, ignore_errors=True)


world_wb_wdi_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# The clean step melts the wide file into ~26M rows in pandas; give the
# worker headroom.
world_wb_wdi_extract_and_load.job_variables = {"memory": "16Gi"}
