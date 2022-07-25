"""
Flows for br_ms_vacinacao_covid19
"""

from prefect import Flow
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from pipelines.constants import constants
from pipelines.tasks import upload_to_gcs, create_bd_table, dump_header_to_csv
from prefect.tasks.shell import ShellTask
import basedosdados as bd
from pipelines.datasets.br_ms_vacinacao_covid19.tasks import download_ufs, build_data
from pipelines.datasets.br_ms_vacinacao_covid19.schedules import every_day

UFS = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "SC",
    "SE",
    "RS",
    "TO",
    "SP",
]

UFS=["AC", "RR"]

MUNICIPIO = bd.read_sql(
    """
                          SELECT *
                          FROM basedosdados.br_bd_diretorios_brasil.municipio
                          """,
    billing_project_id="basedosdados-dev",
)

create_dirs = ShellTask(command="bash bash_scripts/br_ms_vacinacao_covid19/create_dirs.sh", stream_output=True)
append_partitions = ShellTask(command="bash bash_scripts/br_ms_vacinacao_covid19/append_partitions.sh", stream_output=True)
append_states = ShellTask(command="bash bash_scripts/br_ms_vacinacao_covid19/append_states.sh microdados", stream_output=True)
test=ShellTask(command="ls", stream_output=True)

with Flow("download_data") as download_data:
    create = create_dirs()
    download = download_ufs(ufs=UFS, method="multiprocess", upstream_tasks=[create])
    append = append_partitions(upstream_tasks=[download])    

download_data.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
download_data.schedule = every_day

with Flow("br_ms_vacinacao_covid19.microdados") as br_ms_vacinacao_covid19_microdados:
    filepaths = build_data(ufs=UFS, municipio=MUNICIPIO, table="microdados")
    # filepath = append_states(upstream_tasks=[build])
    
    dataset_id = "br_ms_vacinacao_covid19"
    table_id = "microdados"

    for filepath in filepaths:
        wait_header_path = dump_header_to_csv(data_path=filepath)
        # Create table in BigQuery
        wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
            path=wait_header_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="overwrite",
            wait=wait_header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=wait_create_bd_table,
        )

br_ms_vacinacao_covid19_microdados.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_vacinacao_covid19_microdados.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ms_vacinacao_covid19_microdados.schedule = every_day

with Flow("br_ms_vacinacao_covid19.microdados_estabelecimento") as br_ms_vacinacao_covid19_microdados_estabelecimento:
    filepaths = build_data(ufs=UFS, municipio=MUNICIPIO, table="estabelecimento")
    # filepath = append_states(upstream_tasks=[build])
    
    dataset_id = "br_ms_vacinacao_covid19"
    table_id = "microdados_estabelecimento"

    for filepath in filepaths:
        wait_header_path = dump_header_to_csv(data_path=filepath)
        # Create table in BigQuery
        wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
            path=wait_header_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="overwrite",
            wait=wait_header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=wait_create_bd_table,
        )

br_ms_vacinacao_covid19_microdados_estabelecimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_vacinacao_covid19_microdados_estabelecimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ms_vacinacao_covid19_microdados_estabelecimento.schedule = every_day

with Flow("br_ms_vacinacao_covid19.microdados_paciente") as br_ms_vacinacao_covid19_microdados_paciente:
    filepaths = build_data(ufs=UFS, municipio=MUNICIPIO, table="paciente")
    # filepath = append_states(upstream_tasks=[build])
    
    dataset_id = "br_ms_vacinacao_covid19"
    table_id = "microdados_paciente"

    for filepath in filepaths:
        wait_header_path = dump_header_to_csv(data_path=filepath)
        # Create table in BigQuery
        wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
            path=wait_header_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="overwrite",
            wait=wait_header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=wait_create_bd_table,
        )

br_ms_vacinacao_covid19_microdados_paciente.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_vacinacao_covid19_microdados_paciente.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ms_vacinacao_covid19_microdados_paciente.schedule = every_day

with Flow("br_ms_vacinacao_covid19.microdados_vacinacao") as br_ms_vacinacao_covid19_microdados_vacinacao:
    filepaths = build_data(ufs=UFS, municipio=MUNICIPIO, table="vacinacao")
    # filepath = append_states(upstream_tasks=[build])
    
    dataset_id = "br_ms_vacinacao_covid19"
    table_id = "microdados_vacinacao"

    for filepath in filepaths:
        wait_header_path = dump_header_to_csv(data_path=filepath)
        # Create table in BigQuery
        wait_create_bd_table = create_bd_table(  # pylint: disable=invalid-name
            path=wait_header_path,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_type="overwrite",
            wait=wait_header_path,
        )
        # Upload to GCS
        upload_to_gcs(
            path=filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            wait=wait_create_bd_table,
        )

br_ms_vacinacao_covid19_microdados_vacinacao.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_ms_vacinacao_covid19_microdados_vacinacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_ms_vacinacao_covid19_microdados_vacinacao.schedule = every_day


