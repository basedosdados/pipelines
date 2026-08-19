"""
Flows for br_rf_cnpj — Prefect 3.
"""

from prefect import flow

from pipelines.datasets.br_rf_cnpj.constants import constants as constants_cnpj
from pipelines.datasets.br_rf_cnpj.tasks import get_data_source_max_date
from pipelines.utils.metadata.domain import (
    AllBdpro,
    DateFormat,
    DateOnly,
    NonHistorical,
    PartBdpro,
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
)


def _rf_cnpj_flow(table_id: str, cron: str):
    @flow(
        name=f"br_rf_cnpj__{table_id}",
        log_prints=True,
    )
    def _flow(
        dataset_id: str = "br_rf_cnpj",
        table_id: str = table_id,
        materialize_after_dump: bool = True,
        dbt_alias: bool = True,
        update_metadata: bool = True,
        target: str = "prod",
        force_run: bool = False,
        chunk_size: int = 100000,
        folder_date: str | None = None,
        download_chunk_size: int = 15 * 1024 * 1024,
        download_max_retries: int = 5,
        download_max_parallel: int = 15,
        download_timeout: int = 5 * 60,
    ) -> None:
        """Run the download/clean/upload/dbt/metadata cycle for one br_rf_cnpj table.

        Polls the source for a newer release (unless `force_run`), downloads and
        cleans the table's raw files, uploads the result to the dev GCS bucket and
        runs dbt against `dev`. If `materialize_after_dump` is set, also uploads to
        the prod bucket, runs dbt against `target`, and — if `update_metadata` is
        set — registers the table materialization and commits the source update.
        The `estabelecimentos` table additionally refreshes the
        `br_bd_diretorios_brasil.empresa` directory table.

        Args:
            dataset_id: BD dataset slug (e.g. "br_rf_cnpj").
            table_id: Table slug within the dataset (e.g. "empresas",
                "estabelecimentos", "socios", "simples", "dicionario").
            materialize_after_dump: If True, also upload to the prod bucket, run
                dbt against `target`, and register metadata (subject to
                `update_metadata`). If False, stop after the dev dump and dbt run.
            dbt_alias: Passed through to `run_dbt`'s `dbt_alias` argument.
            update_metadata: If True (and `materialize_after_dump` is True),
                register the table's coverage/materialization and commit the
                source's max date after a successful prod run.
            target: dbt target used for the prod run (e.g. "prod").
            force_run: If True, skip the `poll_source_for_update_task` check and
                run unconditionally, even if the source has no new data.
            chunk_size: Row chunk size used when writing the cleaned output file.
                Defaults to 100000.
            folder_date: Source folder date to process, formatted "%Y-%m". If
                None, resolved automatically via `get_data_source_max_date` to the
                source's latest available folder.
            download_chunk_size: Byte size of each HTTP range chunk when
                downloading source files. Defaults to 15 MiB (15 * 1024 * 1024).
            download_max_retries: Maximum retry attempts per failed download
                chunk. Defaults to 5.
            download_max_parallel: Maximum number of download chunks fetched
                concurrently. Defaults to 15.
            download_timeout: Per-request download timeout in seconds. Defaults
                to 5 minutes (5 * 60).

        Returns:
            None. Returns early (without uploading/running dbt) if `force_run` is
            False and the source has no new data since the last committed update.
        """
        # pyrefly: ignore [unused-coroutine]
        rename_flow_run_dataset_table(
            prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
        )

        tabelas = constants_cnpj.TABLES.value[table_id]

        folder_date, last_modified_date = get_data_source_max_date(folder_date)

        if not force_run:
            # simples/dicionario são NonHistorical (linha ~152): register_table_
            # materialization não grava Coverage.DateTimeRange para essa cobertura,
            # só Table.Update (a partir de bq.last_modified) — não há baseline de
            # coverage pra comparar.
            compare_against = (
                "table_update"
                if table_id in ("simples", "dicionario")
                else "coverage"
            )
            has_new_data = poll_source_for_update_task(
                dataset_id=dataset_id,
                table_id=table_id,
                source_max_date=folder_date,
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
            source_max_date=last_modified_date,
            env="prod",
            date_format="%Y-%m-%d",
            update_metadata=update_metadata,
            materialize_after_dump=materialize_after_dump,
        )

        # output_filepath = main(
        #     tables=tabelas,
        #     folder_date=folder_date,
        #     last_modified_date=last_modified_date,
        #     chunk_size=chunk_size,
        #     download_chunk_size=download_chunk_size,
        #     download_max_retries=download_max_retries,
        #     download_max_parallel=download_max_parallel,
        #     download_timeout=download_timeout,
        # )

        # upload_to_gcs(
        #     data_path=output_filepath,
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     bucket_name="basedosdados-dev",
        #     dump_mode="append",
        # )

        run_dbt(
            dataset_id=dataset_id,
            table_id=table_id,
            dbt_command="run/test",
            dbt_alias=dbt_alias,
            target="dev",
        )

        if not materialize_after_dump:
            return

        # upload_to_gcs(
        #     data_path=output_filepath,
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     bucket_name="basedosdados",
        #     dump_mode="append",
        # )

        # run_dbt(
        #     dataset_id=dataset_id,
        #     table_id=table_id,
        #     dbt_command="run/test",
        #     dbt_alias=dbt_alias,
        #     target=target,
        # )

        if update_metadata:
            if table_id == "simples" or table_id == "dicionario":
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
                        date_column=DateOnly(col="data_referencia"),
                        date_format=DateFormat.YEAR_MONTH,
                    ),
                    env="prod",
                    bq_project="basedosdados",
                )

        # estabelecimentos: atualiza diretório de empresas
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
                        date_column=DateOnly(col="data_referencia"),
                        date_format=DateFormat.YEAR_MONTH,
                    ),
                    env="prod",
                    bq_project="basedosdados",
                )

    # pyrefly: ignore [missing-attribute]
    _flow.deploy_schedules = [{"cron": cron, "timezone": "America/Sao_Paulo"}]
    return _flow


br_rf_cnpj__dicionario = _rf_cnpj_flow(table_id="dicionario", cron="0 5 * * *")
br_rf_cnpj__empresas = _rf_cnpj_flow(table_id="empresas", cron="0 6 * * *")
br_rf_cnpj__socios = _rf_cnpj_flow(table_id="socios", cron="0 7 * * *")
br_rf_cnpj__simples = _rf_cnpj_flow(table_id="simples", cron="0 8 * * *")
br_rf_cnpj__estabelecimentos = _rf_cnpj_flow(
    table_id="estabelecimentos", cron="0 9 * * *"
)
