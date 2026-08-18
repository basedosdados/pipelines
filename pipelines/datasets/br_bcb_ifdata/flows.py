"""Flow trimestral do br_bcb_ifdata (IF.data — Banco Central)."""

from __future__ import annotations

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_bcb_ifdata.constants import constants
from pipelines.datasets.br_bcb_ifdata.tasks import (
    clean_all,
    get_source_max_period,
)
from pipelines.utils.metadata.domain import (
    AllFree,
    DateFormat,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

DATASET_ID = constants.DATASET_ID.value
ALL_TABLES = constants.ALL_TABLES.value
POLL_TABLE = constants.POLL_TABLE.value

# O IF.data é trimestral, portanto menos frequente que mensal — pela regra de
# negócio da BD, não entra no paywall rotativo. Todas as tabelas são AllFree, e
# nenhuma Row Access Policy é emitida. `dicionario` não tem coluna de data e
# por isso não recebe spec de cobertura.
_COVERAGE = {
    table: AllFree(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )
    for table in ("instituicao", "coluna", "relatorio")
}


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


@flow(name="br_bcb_ifdata", log_prints=True)
def br_bcb_ifdata_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Reconstrói o IF.data inteiro e materializa.

    A reconstrução é completa a cada run, não incremental: na data-base de
    dezembro o BCB republica os dados contábeis dos últimos quatro trimestres,
    então competências já carregadas mudam depois. Por isso `dump_mode` é
    `overwrite`. O poll da fonte transforma uma run agendada em no-op barato
    entre publicações.

    Args:
        materialize_to_prod: segue além da materialização em dev para escrever
            o bucket de prod e rodar o dbt com `target="prod"`. Use False para
            exercitar só a metade dev — necessário para uma run de teste
            segura, já que o padrão escreve produção.
        update_metadata: registra cobertura das tabelas após a materialização
            em prod. Sem efeito quando `materialize_to_prod` é False.
        force_run: materializa mesmo quando o poll não vê competência nova.
    """
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id="br_bcb_ifdata"
    )

    max_ym = get_source_max_period()
    print(f"competência mais recente na fonte: {max_ym}")

    has_new_data = poll_source_for_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=max_ym,
        env="prod",
        date_format="%Y-%m",
        compare_against="coverage",
    )
    if not has_new_data and not force_run:
        print("Não há novas competências na fonte.")
        return

    # Grava o Update da fonte antes de baixar: se o flow falhar no meio, o
    # metadado ainda registra que havia competência nova publicada.
    commit_source_update_task(
        dataset_id=DATASET_ID,
        table_id=POLL_TABLE,
        source_max_date=max_ym,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_to_prod,
    )

    work_dir = tempfile.mkdtemp(prefix="br_bcb_ifdata_")
    try:
        paths = clean_all(work_dir=work_dir)
        _materialize("dev", "basedosdados-dev", paths)

        if not materialize_to_prod:
            return

        _materialize("prod", "basedosdados", paths)

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
        # Cobre a saída antecipada (dev-only) e qualquer exceção.
        shutil.rmtree(work_dir, ignore_errors=True)


# O IF.data publica cerca de 90 dias após o fim do trimestre (a competência
# 2026-03 saiu em 2026-06-01). Poller nos primeiros dias de cada mês às 16:00
# BRT; a trava do poll no-opa até uma competência nova aparecer de fato.
# pyrefly: ignore [missing-attribute]
br_bcb_ifdata_flow.deploy_schedules = [
    {"cron": "0 16 1,2,3,4,5 * *", "timezone": "America/Sao_Paulo"}
]
# A limpeza percorre 105 competências, mantendo no máximo uma na memória
# (~1M células), mas o crosswalk do IBGE e o índice ficam residentes.
# pyrefly: ignore [missing-attribute]
br_bcb_ifdata_flow.job_variables = {"memory": "4Gi"}
