# -*- coding: utf-8 -*-
"""
Flows for botdosdados
"""
from prefect import case, Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.botdosdados.tasks import (
    was_table_updated,
    get_credentials,
    message_last_tables,
    message_inflation_plot,
    send_media,
    send_thread,
    echo,
)
from pipelines.datasets.botdosdados.schedules import every_day
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    rename_current_flow_run,
    get_date_time_str,
    download_storage_files,
)

with Flow(
    name="botdosdados.message_inflation", code_owners=["lucas_cr"]
) as inflation_flow:
    dataset_id = Parameter("dataset_id", default="br_ibge_ipca", required=True)
    table_id = Parameter("table_id", default="mes_brasil", required=True)
    # pylint: disable=C0103
    now = get_date_time_str()
    rename_flow_run = rename_current_flow_run(msg=f"botdosdados - {now}", wait=now)

    cond = was_table_updated(
        page_size=100, hours=24, subset="inflation", wait=rename_flow_run
    )

    with case(cond, False):
        echo("No table updated")

    with case(cond, True):
        (
            access_token_secret,
            access_token,
            consumer_key,
            consumer_secret,
            bearer_token,
        ) = get_credentials(
            secret_path="botdosdados_credentials", upstream_tasks=[cond]
        )

        download_storage_files(
            project_id="basedosdados-dev",
            bucket_id="basedosdados-dev",
            objects=[
                "auxiliary_files/Ubuntu-Bold.ttf",
                "auxiliary_files/Ubuntu-Regular.ttf",
            ],
            upstream_tasks=[access_token],
        )

        text = message_inflation_plot(
            dataset_id=dataset_id,
            table_id=table_id,
            upstream_tasks=[download_storage_files],
        )

        twt_media_id = send_media(
            access_token=access_token,
            access_token_secret=access_token_secret,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            text=text,
            image="/tmp/plots/inflation.jpeg",
            upstream_tasks=[text],
        )

        texts = message_last_tables(upstream_tasks=[twt_media_id])

        id_last_twt = send_thread(
            access_token=access_token,
            access_token_secret=access_token_secret,
            consumer_key=consumer_key,
            consumer_secret=consumer_secret,
            bearer_token=bearer_token,
            texts=texts,
            is_reply=True,
            reply_id=twt_media_id,
            upstream_tasks=[texts],
        )  # pylint: disable=C0103


inflation_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
inflation_flow.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
inflation_flow.schedule = every_day
