"""
Flow de transferência de arquivos do bucket basedosdados-dev para basedosdados
(produção) e materialização da tabela em produção — Prefect 3.
"""

from __future__ import annotations

from prefect import flow

from pipelines.utils.materialize_prod.tasks import (
    download_files_from_bucket_folders,
)
from pipelines.utils.metadata.domain import (
    AllBdpro,
    AllFree,
    CoverageSpec,
    DateFormat,
    DateOnly,
    FreeLag,
    NonHistorical,
    PartBdpro,
    YearMonth,
    YearOnly,
    YearQuarter,
)
from pipelines.utils.metadata.tasks import register_table_materialization_task
from pipelines.utils.tasks import (
    rename_flow_run_dataset_table,
    run_dbt,
    upload_to_gcs,
)


def _build_coverage(
    coverage_tier: str,
    date_column_kind: str,
    date_col: str | None,
    year_col: str,
    month_col: str,
    quarter_col: str | None,
    free_lag_unit: str,
    free_lag_value: int,
) -> CoverageSpec:
    """Constrói um `CoverageSpec` a partir de parâmetros primitivos do flow.

    Espelha `_sicor_coverage` (crawler/bcb): traduz a combinação
    (tier, tipo de coluna de data) numa das variantes da união discriminada.
    Os validadores Pydantic garantem a compatibilidade coluna/formato.
    """
    if coverage_tier == "non_historical":
        return NonHistorical()

    if date_column_kind == "date":
        date_column = DateOnly(col=date_col)
        date_format = DateFormat.YEAR_MD
    elif date_column_kind == "year":
        date_column = YearOnly(col=date_col)
        date_format = DateFormat.YEAR
    elif date_column_kind == "year_month":
        date_column = YearMonth(year=year_col, month=month_col)
        date_format = DateFormat.YEAR_MONTH
    elif date_column_kind == "year_quarter":
        date_column = YearQuarter(year=year_col, quarter=quarter_col)
        date_format = DateFormat.YEAR_MONTH
    else:
        raise ValueError(
            f"date_column_kind não suportado: {date_column_kind!r}"
        )

    if coverage_tier == "all_free":
        return AllFree(date_column=date_column, date_format=date_format)
    if coverage_tier == "all_bdpro":
        return AllBdpro(date_column=date_column, date_format=date_format)
    if coverage_tier == "part_bdpro":
        return PartBdpro(
            date_column=date_column,
            date_format=date_format,
            free_lag=FreeLag(unit=free_lag_unit, value=free_lag_value),
        )
    raise ValueError(f"coverage_tier não suportado: {coverage_tier!r}")


@flow(
    name="BD Utils: Transfere arquivos do bucket basedosdados-dev para basedosdados",
    log_prints=True,
)
def transfer_files_to_prod_flow(
    dataset_id: str = "br_cgu_beneficios_cidadao",
    table_id: str = "novo_bolsa_familia",
    folders: list[str] | None = None,
    source_bucket: str = "basedosdados-dev",
    download_billing_project: str = "basedosdados",
    materialize_after_dump: bool = True,
    dbt_alias: bool = True,
    # Interface de metadados — opt-in (desligada por padrão).
    update_metadata: bool = False,
    coverage_tier: str = "all_free",  # all_free|all_bdpro|part_bdpro|non_historical
    date_column_kind: str = "year_month",  # date|year|year_month|year_quarter
    date_col: str | None = None,  # usado em date/year
    year_col: str = "ano",
    month_col: str = "mes",
    quarter_col: str | None = None,
    free_lag_unit: str = "months",
    free_lag_value: int = 6,
    env: str = "prod",
    bq_project: str = "basedosdados",
) -> None:
    if folders is None:
        folders = ["mes_competencia=202306", "mes_competencia=202305"]

    rename_flow_run_dataset_table(
        prefix="Materialização Prod: ",
        dataset_id=dataset_id,
        table_id=table_id,
    )

    output_filepath = download_files_from_bucket_folders(
        dataset_id=dataset_id,
        table_id=table_id,
        folders=folders,
        source_bucket=source_bucket,
        billing_project=download_billing_project,
    )

    if not materialize_after_dump:
        return

    # Chamadas diretas (síncronas e bloqueantes): garantem a ordem
    # upload → dbt → register e propagam falhas para o flow run. Usar
    # .submit() aqui faria o flow retornar antes das tasks terminarem e o
    # task runner cancelaria as pendentes ("cancelled by the runtime").
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
        target="prod",
    )

    if update_metadata:
        coverage = _build_coverage(
            coverage_tier=coverage_tier,
            date_column_kind=date_column_kind,
            date_col=date_col,
            year_col=year_col,
            month_col=month_col,
            quarter_col=quarter_col,
            free_lag_unit=free_lag_unit,
            free_lag_value=free_lag_value,
        )
        register_table_materialization_task(
            dataset_id=dataset_id,
            table_id=table_id,
            coverage=coverage,
            env=env,
            bq_project=bq_project,
        )


transfer_files_to_prod_flow.deploy_schedules = []  # utilitário, disparo manual
