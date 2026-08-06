"""Flows do br_sedec_desastres — Prefect 3.

Relatório "Reconhecimentos vigentes" do S2ID (SEDEC/MIDR): os reconhecimentos
federais de situação de emergência e estado de calamidade pública em vigor.

A tabela é uma série de retratos — cada execução grava o conjunto vigente na data
da extração. As decisões de desenho e o que ainda está aberto estão no README do
diretório.

Deploy: `.github/scripts/deploy_flows.py` descobre `br_sedec_desastres_flow`
automaticamente, desde que o flow esteja definido neste arquivo (o script filtra
por `obj.fn.__code__.co_filename`). O pool de dev ignora o schedule; o de prod o
ativa, mas entra pausado.
"""

import shutil
import tempfile

from prefect import flow

from pipelines.datasets.br_sedec_desastres.constants import constants
from pipelines.datasets.br_sedec_desastres.tasks import (
    clean_reconhecimentos,
    download_reconhecimentos,
)
from pipelines.utils.metadata.domain import AllFree, DateFormat, DateOnly
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
TABLE_ID = constants.TABLE_ID.value

DATE_FORMAT = "%Y-%m-%d"

_COVERAGE = {
    TABLE_ID: AllFree(
        date_column=DateOnly(col="data_extracao"),
        date_format=DateFormat.YEAR_MD,
    ),
}


@flow(name="br_sedec_desastres", log_prints=True)
def br_sedec_desastres_flow(
    materialize_to_prod: bool = True,
    update_metadata: bool = True,
    force_run: bool = False,
) -> None:
    """Baixa o relatório do S2ID, remonta a tabela e materializa.

    Args:
        materialize_to_prod: Seguir além da materialização em dev, escrevendo no
            bucket de staging de prod e rodando dbt com ``target="prod"``. Passar
            False para exercitar só a metade de dev — necessário para um teste
            seguro, já que o padrão escreve em produção.
        update_metadata: Depois de materializar prod com sucesso, registrar a
            cobertura da tabela e gravar o update da fonte. Não tem efeito quando
            ``materialize_to_prod`` é False.
        force_run: Materializar mesmo quando o poll não achar dado novo.
    """
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=DATASET_ID, table_id=TABLE_ID
    )

    work_dir = tempfile.mkdtemp(prefix="br_sedec_desastres_")
    try:
        input_dir = download_reconhecimentos(work_dir=work_dir)
        result = clean_reconhecimentos(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]

        has_new_data = poll_source_for_update_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            source_max_date=max_date,
            env="prod",
            date_format=DATE_FORMAT,
        )
        if not has_new_data and not force_run:
            return

        tables = constants.ALL_TABLES.value

        dump_mode = "append"

        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados-dev",
                dump_mode=dump_mode,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="dev",
            )

        if not materialize_to_prod:
            return

        for table in tables:
            upload_to_gcs(
                data_path=result[table],
                dataset_id=DATASET_ID,
                table_id=table,
                bucket_name="basedosdados",
                dump_mode=dump_mode,
                source_format="parquet",
            )
            run_dbt(
                dataset_id=DATASET_ID,
                table_id=table,
                dbt_command="run/test",
                target="prod",
            )

        if update_metadata:
            for table, coverage in _COVERAGE.items():
                register_table_materialization_task(
                    dataset_id=DATASET_ID,
                    table_id=table,
                    coverage=coverage,
                    env="prod",
                    bq_project="basedosdados",
                )

            commit_source_update_task(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Um retrato por mês. A fonte é contínua, então não há janela de publicação a
# perseguir; mensal basta porque a vigência é de 180 dias.
br_sedec_desastres_flow.deploy_schedules = [
    {"cron": "0 9 1 * *", "timezone": "America/Sao_Paulo"}
]


br_sedec_desastres_flow.job_variables = {"memory": "4Gi"}
