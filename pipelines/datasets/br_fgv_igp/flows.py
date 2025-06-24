# -*- coding: utf-8 -*-
"""
Flows for br_fgv_igp
"""

# pylint: disable=invalid-name
from pathlib import Path

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.datasets.br_fgv_igp.tasks import (
    clean_fgv_df,
    crawler_fgv,
    get_output,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    get_temporal_coverage,
    rename_current_flow_run_dataset_table,
)
from pipelines.utils.template_flows.tasks import (
    template_upload_to_gcs_and_materialization,
)

ROOT = Path("tmp/data")

with Flow(
    name="IGP-DI mensal",
    code_owners=["equipe_pipelines"],
) as fgv_igpdi_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=True)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp", required=True)
    table_id = Parameter("table_id", default="igp_di_mes", required=True)
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igpdi_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPDI", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_di_ano")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igpm_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_mes")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igpm_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPM", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_m_ano")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igpog_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_mes")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igpog_ano_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGPOG", required=False)
    PERIODO = Parameter("periodo", default="ano", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_og_ano")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
    code_owners=["equipe_pipelines"],
) as fgv_igp10_mes_flow:
    # Parameters
    INDICE = Parameter("indice", default="IGP10", required=False)
    PERIODO = Parameter("periodo", default="mes", required=False)
    dataset_id = Parameter("dataset_id", default="br_fgv_igp")
    table_id = Parameter("table_id", default="igp_10_mes")
    target = Parameter("target", default="prod", required=False)

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

    get_output(df_indice, upstream_tasks=[filepath])

    upload_and_materialization_dev = (
        template_upload_to_gcs_and_materialization(
            dataset_id=dataset_id,
            table_id=table_id,
            data_path=get_output,
            target="dev",
            bucket_name=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            labels=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            billing_project=constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            dbt_alias=dbt_alias,
            dump_mode="append",
            run_model="run/test",
            upstream_tasks=[get_output],
        )
    )

    with case(target, "prod"):
        upload_and_materialization_prod = (
            template_upload_to_gcs_and_materialization(
                dataset_id=dataset_id,
                table_id=table_id,
                data_path=get_output,
                target="prod",
                bucket_name=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                labels=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                billing_project=constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
                dbt_alias=dbt_alias,
                dump_mode="append",
                run_model="run/test",
                upstream_tasks=[upload_and_materialization_dev],
            )
        )

    temporal_coverage = get_temporal_coverage(
        filepath=filepath,
        date_cols=["ano", "mes"],
        time_unit="month",
        interval="1",
        upstream_tasks=[filepath],
    )

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
