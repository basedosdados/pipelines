"""
Flows for au_rba_statistical_tables — Prefect 3.

Reserve Bank of Australia statistical tables. Every CSV carries the full history
of its series, so each run is a **full replace** (``dump_mode="overwrite"``),
not an incremental append.

Migrated to check_update -> extract_and_load, SEM `build_and_promote`: ao
contrário do shape padrão de um `pipeline_factory`/`CheckThenExtractLoadPipeline`
(ver `pipelines/utils/stage_dispatch.py`), este dataset tem duas restrições que o
building block genérico não resolve sozinho — o mesmo par de restrições de
`br_bcb_ifdata`/`us_sec_edgar`.

1. Download compartilhado e não decomponível: uma única varredura dos ~220 CSVs
   (`download_all` + `clean_all`) produz as quatro tabelas juntas (`data`,
   `series`, `series_break`, `dicionario`) — não dá para baixar/limpar uma
   tabela de cada vez sem reprocessar a varredura inteira 4x. Por isso há só UM
   `check_update` e UM `extract_and_load` para o dataset inteiro, não um par
   por tabela.

2. Ordem obrigatória no dbt: o teste `dbt_utils.expression_is_true` de `data`
   referencia `ref('au_rba_statistical_tables__series')` (chave estrangeira
   composta), e o `custom_dictionary_coverage` de `series_break` referencia
   `ref('au_rba_statistical_tables__dicionario')` (ver
   `models/au_rba_statistical_tables/schema.yml`) — então as quatro tabelas
   precisam ser `run` antes de QUALQUER uma ser `test`ada (ver o docstring de
   `_materialize` abaixo). O `dispatch_build_and_promote` genérico não serve
   aqui: ele dispara um `build_and_promote` POR TABELA, cada um de forma
   assíncrona e independente, o que reabriria exatamente essa race (teste de
   uma tabela rodando antes de a outra existir). A solução é não usar
   `dispatch_build_and_promote`: `extract_and_load` chama `_materialize`
   diretamente, que sobe as 4 tabelas, roda `dbt run` de todas e só depois
   `dbt test` de todas.

`get_latest_update` (tasks.py) não tem sinal de frescor barato: a data de
publicação só existe dentro de cada CSV, então o check_update baixa e faz o
parse completo da fonte só pra ler esse carimbo — o mesmo trabalho que
`extract_and_load` repete, de forma independente, pra baixar de verdade (ver o
banner em tasks.py). Custo duplicado aceito em troca de um único formato de
pipeline em todo o repo (check_update -> extract_and_load, mesmo quando o
check precisa baixar dado).

`register_table_materialization_task` também continua chamada diretamente
dentro de `extract_and_load`, não via um `build_and_promote` genérico, pelo
mesmo motivo do item 2.

Deploy: `.github/workflows/scripts/deploy_flows.py` auto-discovers both flows
below; the dev pool ignores the check_update schedule, the prod pool activates
it (deployed paused).
"""

import shutil
import tempfile

from prefect.schedules import Cron

from pipelines.datasets.au_rba_statistical_tables.constants import constants
from pipelines.datasets.au_rba_statistical_tables.tasks import (
    clean_rba,
    download_rba,
    get_latest_update,
)
from pipelines.utils.flow import flow
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
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
POLL_TABLE = constants.POLL_TABLE.value
ALL_TABLES = constants.ALL_TABLES.value

# Name of the `au_rba_statistical_tables_extract_and_load` flow below
# (`deploy_flows.py` registers a deployment under the Python variable name of
# the `@flow`). Passed explicitly to `check_update_and_dispatch` because it's
# literally `str(Etapa.EXTRACT_AND_LOAD)` already — kept explicit so it
# doesn't depend on that coincidence.
_EXTRACT_AND_LOAD_DEPLOYMENT = "au_rba_statistical_tables_extract_and_load"

# Coverage spec per table.
#
# Every table is AllFree — no BD Pro rolling window — even though the dataset
# refreshes daily. The RBA copyright notice (Section 4) lists, as prohibited
# "improper commercial exploitation", charging for the Cash Rate "without
# informing customers that [it is] published on this website without a fee being
# charged by the RBA". An all-free dataset cannot engage that condition at all.
# See models/au_rba_statistical_tables/LICENCE.md.
#
# `dicionario` has no date column, so it takes no coverage spec.
_COVERAGE = {
    "data": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
    "series": AllFree(
        date_column=DateOnly(col="observation_end"),
        date_format=DateFormat.YEAR_MD,
    ),
    "series_break": AllFree(
        date_column=DateOnly(col="date"), date_format=DateFormat.YEAR_MD
    ),
}


def _materialize(target: str, bucket: str, paths: dict) -> None:
    """Sobe o staging e materializa todas as tabelas, depois testa todas.

    A separação entre `run` e `test` é obrigatória aqui, não estilística: o
    `dbt_utils.expression_is_true` de `data` referencia
    `ref('au_rba_statistical_tables__series')`, e o `custom_dictionary_coverage`
    de `series_break` referencia `ref('au_rba_statistical_tables__dicionario')`.
    Testando tabela a tabela, o teste da primeira roda antes de a irmã existir e
    falha com `Not found: Table ... au_rba_statistical_tables.series`. Num
    ambiente já povoado isso passa despercebido porque a irmã sobrou da build
    anterior; só quebra num ambiente limpo, então a ordem precisa ser
    estrutural.
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
def au_rba_statistical_tables_check_update() -> bool:
    """Baixa e faz o parse da fonte pra ler a data de publicação mais recente.

    Ver o banner de `tasks.get_latest_update` pra por que esse estágio já baixa
    e faz o parse completo dos ~220 CSVs, em vez de um poll barato.

    Returns:
        `True` se havia publicação nova (e `extract_and_load` foi disparado);
        `False` caso contrário.
    """
    rename_flow_run_dataset_table(
        prefix="Check Update: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    result = get_latest_update()
    print(f"max publication_date na fonte: {result.reference_date}")

    return check_update_and_dispatch(
        prefect_dataset_id=DATASET_ID,
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        reference_date=result.reference_date,
        next_deployment=_EXTRACT_AND_LOAD_DEPLOYMENT,
        date_format="%Y-%m-%d",
        compare_against=result.compare_against,
    )


au_rba_statistical_tables_check_update.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.CHECK_UPDATE
)
# The RBA publishes on Sydney business days, typically late morning AEST — which
# is the small hours in São Paulo. Run daily at 09:00 BRT, comfortably after.
au_rba_statistical_tables_check_update.deploy_schedules = [
    Cron("40 9 * * *", timezone="America/Sao_Paulo")
]
# check_update downloads and parses every CSV too (see tasks.py), holding the
# same ~1.5M parsed observations in memory.
au_rba_statistical_tables_check_update.job_variables = {"memory": "4Gi"}


@flow(name=f"{Etapa.EXTRACT_AND_LOAD}: {DATASET_ID}", log_prints=True)
def au_rba_statistical_tables_extract_and_load(
    download_params: dict,
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
) -> None:
    """Baixa os ~220 CSVs de novo, reconstrói as 4 tabelas e materializa em lote.

    Args:
        download_params: dict recebido do `check_update` via `run_deployment()`
            — só carrega `reference_date` (a publicação que disparou esta run);
            não é usado para decidir o que baixar (a fonte não tem parâmetro de
            período, é sempre a varredura inteira), só para rastreabilidade.
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção. Disparar este flow
            manualmente com `materialize_to_prod=False` substitui o antigo
            parâmetro `force_run` do flow monolítico: como `extract_and_load`
            agora é disparável direto (sem depender do poll de `check_update`
            achar publicação nova), rematerializar sob demanda não exige mais
            uma flag separada.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
    """
    rename_flow_run_dataset_table(
        prefix="Extract and Load: ", dataset_id=DATASET_ID, table_id=POLL_TABLE
    )

    work_dir = tempfile.mkdtemp(prefix="au_rba_statistical_tables_")
    try:
        input_dir = download_rba(work_dir=work_dir)
        result = clean_rba(work_dir=work_dir, input_dir=input_dir)

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
        # Covers both the dev-only early return and any exception. The k8s
        # work pool gives each run a fresh pod, but a process/local worker
        # reuses its filesystem.
        shutil.rmtree(work_dir, ignore_errors=True)


au_rba_statistical_tables_extract_and_load.deploy_tags = deploy_tags(
    DATASET_ID, Etapa.EXTRACT_AND_LOAD
)
# The clean step holds ~1.5M parsed observations in memory before writing.
au_rba_statistical_tables_extract_and_load.job_variables = {"memory": "4Gi"}
