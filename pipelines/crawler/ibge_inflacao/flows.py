"""
Flow compartilhado para índices de inflação do IBGE (IPCA, INPC, ...).
Prefect 3 — use os flows dos datasets (br_ibge_ipca, br_ibge_inpc) para deploy.
"""

from prefect import flow

from pipelines.crawler.ibge_inflacao.tasks import (
    check_for_updates,
    collect_data_utils,
    json_to_csv,
)
from pipelines.utils.metadata.tasks import (
    check_if_data_is_outdated,
    update_django_metadata,
)
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _run_ibge_inflacao(
    dataset_id: str,
    table_id: str,
    periodo: str | None,
    materialize_after_dump: bool,
    dbt_alias: bool,
    update_metadata: bool,
    target: str,
) -> None:
    """Lógica completa do flow de inflação IBGE. Chamada pelos flows de cada dataset."""
    rename_flow_run_dataset_table(
        prefix="Dump: ", dataset_id=dataset_id, table_id=table_id
    )

    collect_data_utils(
        dataset_id=dataset_id, table_id=table_id, periodo=periodo
    )

    max_date = check_for_updates(dataset_id=dataset_id, table_id=table_id)

    is_outdated = check_if_data_is_outdated(
        dataset_id=dataset_id,
        table_id=table_id,
        data_source_max_date=max_date,
        date_format="%Y-%m",
    )

    if not is_outdated:
        return

    filepath = json_to_csv(table_id=table_id, dataset_id=dataset_id)

    upload_to_gcs(
        data_path=filepath,
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
        data_path=filepath,
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
        update_django_metadata(
            dataset_id=dataset_id,
            table_id=table_id,
            date_column_name={"year": "ano", "month": "mes"},
            date_format="%Y-%m",
            coverage_type="part_bdpro",
            time_delta={"months": 6},
            bq_project="basedosdados",
        )


@flow(name="ibge-inflacao", log_prints=True)
def ibge_inflacao_flow(
    dataset_id: str = "br_ibge_ipca",
    table_id: str = "mes_brasil",
    periodo: str | None = None,
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    update_metadata: bool = False,
    target: str = "prod",
) -> None:
    """Flow genérico — para acionar manualmente sem schedule."""
    _run_ibge_inflacao(
        dataset_id=dataset_id,
        table_id=table_id,
        periodo=periodo,
        materialize_after_dump=materialize_after_dump,
        dbt_alias=dbt_alias,
        update_metadata=update_metadata,
        target=target,
    )


ibge_inflacao_flow.deploy_schedules = []
