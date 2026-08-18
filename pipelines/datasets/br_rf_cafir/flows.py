"""
Flows for br_rf_cafir — Prefect 3.
"""

import datetime

from prefect import flow

from pipelines.datasets.br_rf_cafir.constants import (
    constants as br_rf_cafir_constants,
)
from pipelines.datasets.br_rf_cafir.tasks import (
    build_paths,
    decide_files_to_download,
    download_files,
    get_api_metadata,
    get_last_reference_date,
    process_files,
)
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
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


@flow(
    name="br_rf_cafir__imoveis_rurais",
    log_prints=True,
)
def br_rf_cafir__imoveis_rurais(
    dataset_id: str = "br_rf_cafir",
    table_id: str = "imoveis_rurais",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = True,
    target: str = "prod",
    force_run: bool = False,
    data_referencia: str | None = None,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    input_folder, output_folder = build_paths()
    df_metadata = get_api_metadata(url=br_rf_cafir_constants.URL.value)

    if data_referencia is None:
        reference_date = get_last_reference_date(df_metadata)
    else:
        reference_date = datetime.datetime.strptime(
            data_referencia, "%Y-%m-%d"
        ).date()

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=reference_date,
            env="prod",
            date_format="%Y-%m-%d",
            compare_against="coverage",
        )
        if not has_new_data:
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=reference_date,
        env="prod",
        date_format="%Y-%m-%d",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    filtered_df = decide_files_to_download(
        df_metadata=df_metadata, reference_date=reference_date
    )

    # pyrefly: ignore [no-matching-overload]
    download_files(
        url=br_rf_cafir_constants.URL.value,
        input_folder=input_folder,
        df_metadata=filtered_df,
    )

    output_path = process_files(
        input_folder=input_folder,
        output_folder=output_folder,
        df_metadata=filtered_df,
    )

    upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados-dev",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target="dev",
    )

    if not materialize_after_dump:
        return

    upload_to_gcs(
        data_path=output_path,
        dataset_id=dataset_id,
        table_id=table_id,
        bucket_name="basedosdados",
        dump_mode="append",
    )

    run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        target=target,
    )

    if update_metadata:
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=PartBdpro(
                date_column=DateOnly(col="data_referencia"),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )


# pyrefly: ignore [missing-attribute]
br_rf_cafir__imoveis_rurais.deploy_schedules = [
    {"cron": "0 0 * * *", "timezone": "America/Sao_Paulo"}
]
