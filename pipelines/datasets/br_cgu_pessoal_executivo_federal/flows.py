# -*- coding: utf-8 -*-
"""
Flows for br_cgu_terceirizados
"""
from datetime import datetime

from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.datasets.br_cgu_pessoal_executivo_federal.tasks import (
    crawl,
    clean_save_table,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    update_metadata,
    publish_table,
)
from pipelines.datasets.br_cgu_pessoal_executivo_federal.schedules import (
    every_four_months,
)


ROOT = "/tmp/data"
URL = "https://www.gov.br/cgu/pt-br/acesso-a-informacao/dados-abertos/arquivos/terceirizados"


# pylint: disable=C0103
with Flow(
    "br_cgu_pessoal_executivo_federal.terceirizados"
) as br_cgu_pess_exec_fed_terc:
    dataset_id = "br_cgu_pessoal_executivo_federal"
    table_id = "terceirizados"
    crawl_urls, temporal_coverage = crawl(URL)
    filepath = clean_save_table(ROOT, crawl_urls)

    wait_upload_table = create_table_and_upload_to_gcs(
        data_path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_type="overwrite",
        wait=filepath,
    )

    wait_update_metadata = update_metadata(
        dataset_id=dataset_id,
        table_id=table_id,
        fields_to_update=[
            {"last_updated": {"data": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}},
            {"temporal_coverage": [temporal_coverage]},
        ],
        upstream_tasks=[temporal_coverage],
    )

    publish_table(
        path=filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        if_exists="replace",
        wait=wait_update_metadata,
    )


br_cgu_pess_exec_fed_terc.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_cgu_pess_exec_fed_terc.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
br_cgu_pess_exec_fed_terc.schedule = every_four_months
