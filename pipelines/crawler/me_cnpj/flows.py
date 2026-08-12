"""
Shared run logic for br_me_cnpj — Prefect 3.
"""

from pipelines.crawler.me_cnpj.constants import constants as constants_cnpj
from pipelines.crawler.me_cnpj.tasks import get_data_source_max_date, main
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
    YearMonth,
)
from pipelines.utils.metadata.tasks import (
    commit_source_update_task,
    poll_source_for_update_task,
    register_table_materialization_task,
)
from pipelines.utils.tasks import (
    download_data_to_gcs,
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)

_TABELAS_IDX = {
    "empresas": 0,
    "socios": 1,
    "estabelecimentos": 2,
    "simples": 3,
}


def _run_me_cnpj(
    dataset_id: str,
    table_id: str,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
    force_run: bool,
) -> None:
    # pyrefly: ignore [unused-coroutine]
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    idx = _TABELAS_IDX[table_id]
    tabelas = constants_cnpj.TABELAS.value[idx : idx + 1]

    max_folder_date, max_last_modified_date = get_data_source_max_date()

    if not force_run:
        # simples é NonHistorical (linha ~109): register_table_materialization
        # não grava Coverage.DateTimeRange para essa cobertura, só Table.Update
        # (a partir de bq.last_modified) — não há baseline de coverage pra comparar.
        compare_against = (
            "table_update" if table_id == "simples" else "coverage"
        )
        has_new_data = poll_source_for_update_task(
            dataset_id=dataset_id,
            table_id=table_id,
            source_max_date=max_folder_date,
            env="prod",
            date_format="%Y-%m",
            compare_against=compare_against,
        )
        if not has_new_data:
            print(f"Não há atualizações para a tabela {tabelas}!")
            return

    # Comita o Update da fonte já aqui, antes de baixar/materializar: se o
    # flow falhar no meio, o metadado da fonte ainda reflete que havia dado
    # novo publicado, mesmo que a tabela não tenha sido atualizada.
    commit_source_update_task(
        dataset_id=dataset_id,
        table_id=table_id,
        source_max_date=max_folder_date,
        env="prod",
        date_format="%Y-%m",
        update_metadata=update_metadata,
        materialize_after_dump=materialize_after_dump,
    )

    output_filepath = main(
        tabelas=tabelas,
        max_folder_date=max_folder_date,
        max_last_modified_date=max_last_modified_date,
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
        if table_id == "simples":
            # historical_database=False (sem coluna de data confiável) → NonHistorical
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=NonHistorical(),
                env="prod",
                bq_project="basedosdados",
            )
        else:
            register_table_materialization_task(
                dataset_id=dataset_id,
                table_id=table_id,
                coverage=PartBdpro(
                    date_column=YearMonth(year="ano", month="mes"),
                    date_format=DateFormat.YEAR_MONTH,
                ),
                env="prod",
                bq_project="basedosdados",
            )

    # estabelecimentos: atualiza diretório de empresas + download p/ GCS
    if table_id == "estabelecimentos":
        run_dbt(
            dataset_id="br_bd_diretorios_brasil",
            table_id="empresa",
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target=target,
        )
        download_data_to_gcs(
            dataset_id=dataset_id,
            table_id=table_id,
        )
        if update_metadata:
            register_table_materialization_task(
                dataset_id="br_bd_diretorios_brasil",
                table_id="empresa",
                coverage=AllBdpro(
                    date_column=DateOnly(col="data"),
                    date_format=DateFormat.YEAR_MD,
                ),
                env="prod",
                bq_project="basedosdados",
            )
