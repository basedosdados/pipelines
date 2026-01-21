"""
Flows for br_fgv_igp
"""

from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_fgv_igp.tasks import clean_fgv_df, crawler_fgv
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

ROOT = Path("tmp/data")

with Flow(
    name="IGP-DI mensal",
    code_owners=[],
) as fgv_igpdi_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=True)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp", required=True)
    table_id = Parameter("table_id", default="igp_di_mes", required=True)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano", "mes"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[wait_upload_table],
    # )


fgv_igpdi_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpdi_mes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpdi_mes_flow.schedule = igp_di_mes


with Flow(
    name="IGP-DI anual",
    code_owners=[],
) as fgv_igpdi_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_di_ano")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_dev_and_upload_to_gcs(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpdi_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpdi_ano_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpdi_ano_flow.schedule = igp_di_ano


with Flow(
    name="IGP-M mensal",
    code_owners=[],
) as fgv_igpm_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_mes")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano", "mes"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpm_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpm_mes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpm_mes_flow.schedule = igp_m_mes


with Flow(
    name="IGP-M anual",
    code_owners=[],
) as fgv_igpm_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_ano")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpm_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpm_ano_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpm_ano_flow.schedule = igp_m_ano


with Flow(
    name="IGP-OG mensal",
    code_owners=[],
) as fgv_igpog_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_mes")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano", "mes"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpog_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpog_mes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpog_mes_flow.schedule = igp_og_mes


with Flow(
    name="IGP-OG anual",
    code_owners=[],
) as fgv_igpog_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_ano")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igpog_ano_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igpog_ano_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igpog_ano_flow.schedule = igp_og_ano


with Flow(
    name="IGP-10 mensal",
    code_owners=[],
) as fgv_igp10_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGP10", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_10_mes")

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )
    df_indice = crawler_fgv(INDICE, PERIODO)
    filepath = clean_fgv_df(
        df_indice,
        upstream_tasks=[
            df_indice,
        ],
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="append",
        upstream_tasks=[filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        create_table_prod_gcs_and_run_dbt(
            data_path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="append",
            upstream_tasks=[wait_for_materialization],
        )

    # temporal_coverage = get_temporal_coverage(
    #     filepath=filepath,
    #     date_cols=["ano", "mes"],
    #     time_unit="month",
    #     interval="1",
    #     upstream_tasks=[filepath],
    # )

    # wait_update_metadata = update_metadata(
    #     dataset_id=dataset_id,
    #     table_id=table_id,
    #     fields_to_update=[
    #         {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
    #         {"temporal_coverage": [temporal_coverage]},
    #     ],
    #     upstream_tasks=[filepath],
    # )


fgv_igp10_mes_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
fgv_igp10_mes_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# fgv_igp10_mes_flow.schedule = igp_10_mes
