"""
Shared run logic for br_cvm_fi — Prefect 3.
"""

from pipelines.crawler.cvm.tasks import (
    clean_cvm_data,
    download_unzip,
    extract_links_and_dates,
    generate_links_to_download,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
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


def _run_cvm_fi(
    dataset_id: str,
    table_id: str,
    date_column_name: dict,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
    url: str | None = None,
) -> None:
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    df, max_date = extract_links_and_dates(table_id=table_id, url=url)
    print(f"Links e datas: {df}")

    if not force_run:
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=max_date,
            env="prod",
            date_format="%Y-%m-%d",
        )
        if not has_new_data:
            print(
                "Sem atualizações na fonte — aguardando próxima execução agendada"
            )
            return

    arquivos = generate_links_to_download(df=df, max_date=max_date)
    print(f"Arquivos: {arquivos}")

    input_filepath = download_unzip(table_id=table_id, files=arquivos, url=url)
    output_filepath = clean_cvm_data(
        input_dir=input_filepath, table_id=table_id, config_key=table_id
    )

    upload_to_gcs(
        data_path=output_filepath,
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
        data_path=output_filepath,
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
        # date_column_name é parametrizado pelos flows de dataset; todos os
        # callers passam {"date": <col>} (ver br_cvm_fi), logo DateOnly/YEAR_MD.
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=AllBdpro(
                date_column=DateOnly(col=date_column_name["date"]),
                date_format=DateFormat.YEAR_MD,
            ),
            env="prod",
            bq_project="basedosdados",
        )

        if max_date is not None:
            commit_source_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=max_date,
                env="prod",
                date_format="%Y-%m-%d",
            )
