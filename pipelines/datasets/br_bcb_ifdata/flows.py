"""Flows do br_bcb_ifdata (IF.data — Banco Central) — Prefect 3.

Migrado para check_update -> extract_and_load, SEM `build_and_promote`: ao
contrário do shape padrão de um `pipeline_factory`/`CheckThenExtractLoadPipeline`
(ver `pipelines/utils/stage_dispatch.py`), este dataset tem duas restrições que o
building block genérico não resolve sozinho.

1. Download compartilhado e não decomponível: `clean_all()` varre as ~105
   competências da fonte numa passada só e produz as quatro tabelas juntas
   (`dicionario`, `instituicao`, `coluna`, `relatorio`) — não dá para baixar/
   limpar uma tabela de cada vez sem reprocessar a série inteira 4x. Por isso
   há só UM `check_update` e UM `extract_and_load` para o dataset inteiro
   (mesma ideia de `au_geoscape_gnaf`), não um par por tabela.

2. Ordem obrigatória no dbt: `custom_dictionary_coverage` de `instituicao` e
   `coluna` referencia `ref('br_bcb_ifdata__dicionario')`, e `relationships` de
   `relatorio` referencia `ref('br_bcb_ifdata__instituicao')` — então as
   quatro tabelas precisam ser `run` antes de QUALQUER uma ser `test`ada (ver o
   docstring de `_materialize` abaixo). O `dispatch_build_and_promote`
   genérico não serve aqui mesmo com um único `extract_and_load`: ele dispara
   um `build_and_promote` POR TABELA, cada um de forma assíncrona e
   independente (cada deployment roda `run` + `test` só da sua própria
   tabela, sem esperar as irmãs) — isso reabriria exatamente a race que este
   dataset já bateu (teste de uma tabela rodando antes de a outra existir).
   A solução adotada é não usar `dispatch_build_and_promote`: `extract_and_load`
   chama `_materialize` diretamente, que sobe as 4 tabelas, roda `dbt run` de
   todas e só depois `dbt test` de todas — exatamente a lógica do antigo flow
   monolítico, preservada aqui, só reparticionada para depois do poll de
   `check_update` em vez de antes dele.

`register_table_materialization_task` (atualização de cobertura/Update no
backend) também continua chamada diretamente dentro de `extract_and_load`, não
via um `build_and_promote` genérico, pelo mesmo motivo.
"""

from __future__ import annotations

import shutil
import tempfile
from datetime import date

from prefect.schedules import Cron

from pipelines.datasets.br_bcb_ifdata.constants import constants
from pipelines.datasets.br_bcb_ifdata.tasks import (
    clean_all,
    get_source_max_period,
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

# Nome do deployment de `br_bcb_ifdata_extract_and_load` abaixo (`deploy_flows.py`
# registra um deployment com o nome da variável Python do `@flow`). Passado
# explicitamente para `check_update_and_dispatch` porque `Etapa.deployment_name()`
# default já bateria com `str(Etapa.EXTRACT_AND_LOAD)` — mantido explícito para
# não depender de coincidência com a convenção.
_EXTRACT_AND_LOAD_DEPLOYMENT = "br_bcb_ifdata_extract_and_load"

# Tabelas registradas com a metadata de cobertura do backend. `dicionario`
# não tem coluna de data e por isso não recebe spec de cobertura.
_COVERED_TABLES = ("instituicao", "coluna", "relatorio")


def _materialize(target: str, bucket: str, paths: dict) -> None:
    """Sobe o staging e materializa todas as tabelas, depois testa todas.

    A separação entre `run` e `test` é obrigatória aqui, não estilística: o
    `custom_dictionary_coverage` de `instituicao` e `coluna` referencia
    `ref('br_bcb_ifdata__dicionario')`, e o `relationships` de `relatorio`
    referencia `ref('br_bcb_ifdata__instituicao')`. Testando tabela a tabela, o
    teste da primeira roda antes de a irmã existir e falha com
    `Not found: Table ... br_bcb_ifdata.dicionario`. Num ambiente já povoado
    isso passa despercebido porque a irmã sobrou da build anterior; só quebra
    num ambiente limpo, então a ordem precisa ser estrutural.
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
def br_bcb_ifdata_check_update() -> bool:
    """Poll da competência mais recente na fonte, dispara extract_and_load.

    Returns:
        `True` se havia competência nova (e `extract_and_load` foi
        disparado); `False` caso contrário.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    max_ym = get_source_max_period()
    print(f"competência mais recente na fonte: {max_ym}")
    year, month = (int(part) for part in max_ym.split("-"))

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        reference_date=date(year, month, 1),
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y-%m",
    )


br_bcb_ifdata_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# O IF.data publica cerca de 90 dias após o fim do trimestre (a competência
# 2026-03 saiu em 2026-06-01). Poller nos primeiros dias de cada mês às 16:00
# BRT; a trava do poll no-opa até uma competência nova aparecer de fato.
br_bcb_ifdata_check_update.deploy_schedules = [
    Cron("50 16 1,2,3,4,5 * *", timezone="America/Sao_Paulo")
]


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def br_bcb_ifdata_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Reconstrói o IF.data inteiro (4 tabelas) e materializa em lote.

    A reconstrução é completa a cada run, não incremental: na data-base de
    dezembro o BCB republica os dados contábeis dos últimos quatro trimestres,
    então competências já carregadas mudam depois. Por isso `dump_mode` é
    `overwrite` dentro de `_materialize`.

    Args:
        download_params: dict recebido do `check_update` via `run_deployment()`
            — só carrega `reference_date` (a competência que disparou esta
            run); `clean_all` sempre refaz a série inteira, então o valor não
            é usado para decidir o que baixar, só para rastreabilidade.
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção. Disparar este flow
            manualmente com `materialize_to_prod=False` substitui o antigo
            parâmetro `force_run` do flow monolítico: como `extract_and_load`
            agora é disparável direto (sem depender do poll de
            `check_update` achar competência nova), rematerializar sob
            demanda não exige mais uma flag separada.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ",
        dataset_id=DATASET_ID,
        table_id="br_bcb_ifdata",
    )

    work_dir = tempfile.mkdtemp(prefix="br_bcb_ifdata_")
    try:
        paths = clean_all(work_dir=work_dir)
        _materialize("dev", "basedosdados-dev", paths)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", paths)

        if update_metadata:
            for table in _COVERED_TABLES:
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    env="prod",
                    bq_project="basedosdados",
                )
    finally:
        # Cobre a saída antecipada (dev-only) e qualquer exceção.
        shutil.rmtree(work_dir, ignore_errors=True)


br_bcb_ifdata_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# A limpeza percorre 105 competências, mantendo no máximo uma na memória
# (~1M células), mas o crosswalk do IBGE e o índice ficam residentes.
br_bcb_ifdata_extract_and_load.job_variables = {"memory": "4Gi"}
