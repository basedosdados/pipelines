"""Flows do br_sedec_desastres — Prefect 3.

Relatório "Reconhecimentos vigentes" do S2ID (SEDEC/MIDR): os reconhecimentos
federais de situação de emergência e estado de calamidade pública em vigor. A
tabela é uma série de retratos, um por execução.

**Este flow não roda hoje.** O S2ID barra o IP de saída do cluster por
geolocalização e a raspagem morre no download, então o retrato é gerado pelo
`run_local.py` deste diretório e promovido por PR com a label `table-approve`. O
flow fica aqui porque o código é o mesmo — se o IP for liberado, basta devolver o
`deploy_schedules`.

As decisões de desenho, a receita mensal e o que ainda está aberto estão no
README do diretório.
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

DATE_FORMAT = constants.DATE_FORMAT.value

_COVERAGE = AllFree(
    date_column=DateOnly(col="data_extracao"),
    date_format=DateFormat.YEAR_MD,
)


@flow(name="br_sedec_desastres__reconhecimentos_vigentes", log_prints=True)
def br_sedec_desastres__reconhecimentos_vigentes(
    dataset_id: str = constants.DATASET_ID.value,
    table_id: str = constants.TABLE_ID.value,
    materialize_after_dump: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
) -> None:
    """Baixa o relatório do S2ID, remonta a tabela e materializa."""
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    work_dir = tempfile.mkdtemp(prefix="br_sedec_desastres_")
    try:
        input_dir = download_reconhecimentos(work_dir=work_dir)
        result = clean_reconhecimentos(work_dir=work_dir, input_dir=input_dir)
        max_date = result["max_date"]
        data_path = result[constants.TABLE_ID.value]

        if not force_run:
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=max_date,
                env="prod",
                date_format=DATE_FORMAT,
                compare_against="coverage",
            )
            if not has_new_data:
                return

        # Comita o Update da fonte já aqui, antes de materializar: se o flow
        # falhar no meio, o metadado da fonte ainda reflete que havia dado novo
        # publicado, mesmo que a tabela não tenha sido atualizada.
        commit_source_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=max_date,
            env="prod",
            date_format=DATE_FORMAT,
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

        # The dev materialization is the pre-arm validation path, not part of a
        # production run: it rebuilds and re-tests every table in
        # basedosdados-dev, which nothing downstream reads. Running it on an
        # armed run doubled the BigQuery bytes billed for no signal — prod
        # runs the same models and the same tests seconds later.
        if not materialize_after_dump:
            upload_to_gcs(
                data_path=data_path,
                dataset_id=dataset_id,
                table_id=table_id,
                bucket_name="basedosdados-dev",
                dump_mode="append",
                source_format="parquet",
            )
            run_dbt(
                dataset_id=dataset_id,
                table_id=table_id,
                dbt_command="run/test",
                target="dev",
            )
            return

        upload_to_gcs(
            data_path=data_path,
            dataset_id=dataset_id,
            table_id=table_id,
            bucket_name="basedosdados",
            dump_mode="append",
            source_format="parquet",
        )
        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            target=target,
        )

        if update_metadata:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=_COVERAGE,
                env="prod",
                bq_project="basedosdados",
            )
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


# Sem schedule de propósito: o pod não alcança a fonte. A cadência mensal é
# executada à mão pelo run_local.py. Se o IP for liberado, descomentar:
#
# br_sedec_desastres__reconhecimentos_vigentes.deploy_schedules = [
#     {"cron": "10 9 1 * *", "timezone": "America/Sao_Paulo"}
# ]


# pyrefly: ignore [missing-attribute]
br_sedec_desastres__reconhecimentos_vigentes.job_variables = {"memory": "4Gi"}
