"""
Flows for br_bcb_sicor

"""

from copy import deepcopy

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.constants import constants
from pipelines.crawler.bcb.flows import br_bcb_sicor_template
from pipelines.crawler.bcb.tasks import (
    create_load_dictionary,
)
from pipelines.datasets.br_bcb_sicor.schedules import (
    every_day_empreendimento,
    every_day_microdados_liberacao,
    every_day_microdados_operacao,
    every_day_microdados_recurso_publico_complemento_operacao,
    every_day_microdados_recurso_publico_cooperado,
    every_day_microdados_recurso_publico_gleba,
    every_day_microdados_recurso_publico_mutuario,
    every_day_microdados_recurso_publico_propriedade,
    every_day_microdados_saldo,
    # every_day_operacoes_desclassificadas,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_dev_and_upload_to_gcs,
    create_table_prod_gcs_and_run_dbt,
    rename_current_flow_run_dataset_table,
    run_dbt,
)

# br_bcb_sicor__microdados_operacao
br_bcb_sicor__microdados_operacao = deepcopy(br_bcb_sicor_template)
br_bcb_sicor__microdados_operacao.name = "br_bcb_sicor__microdados_operacao"
br_bcb_sicor__microdados_operacao.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__microdados_operacao.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_operacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_operacao.schedule = every_day_microdados_operacao

# br_bcb_sicor__microdados_saldo
br_bcb_sicor__microdados_saldo = deepcopy(br_bcb_sicor_template)
br_bcb_sicor__microdados_saldo.name = "br_bcb_sicor__microdados_saldo"
br_bcb_sicor__microdados_saldo.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__microdados_saldo.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor__microdados_saldo.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_saldo.schedule = every_day_microdados_saldo

# br_bcb_sicor__microdados_liberacao
br_bcb_sicor__microdados_liberacao = deepcopy(br_bcb_sicor_template)
br_bcb_sicor__microdados_liberacao.name = "br_bcb_sicor__microdados_liberacao"
br_bcb_sicor__microdados_liberacao.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__microdados_liberacao.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_liberacao.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_liberacao.schedule = every_day_microdados_liberacao

# br_bcb_sicor__microdados_recurso_publico_complemento_operacao
br_bcb_sicor__microdados_recurso_publico_complemento_operacao = deepcopy(
    br_bcb_sicor_template
)
br_bcb_sicor__microdados_recurso_publico_complemento_operacao.name = (
    "br_bcb_sicor__microdados_recurso_publico_complemento_operacao"
)
br_bcb_sicor__microdados_recurso_publico_complemento_operacao.code_owners = [
    "Gabriel Pisa"
]
br_bcb_sicor__microdados_recurso_publico_complemento_operacao.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_recurso_publico_complemento_operacao.run_config = (
    KubernetesRun(image=constants.DOCKER_IMAGE.value)
)
br_bcb_sicor__microdados_recurso_publico_complemento_operacao.schedule = (
    every_day_microdados_recurso_publico_complemento_operacao
)

# br_bcb_sicor__microdados_recurso_publico_cooperado
br_bcb_sicor__microdados_recurso_publico_cooperado = deepcopy(
    br_bcb_sicor_template
)
br_bcb_sicor__microdados_recurso_publico_cooperado.name = (
    "br_bcb_sicor__microdados_recurso_publico_cooperado"
)
br_bcb_sicor__microdados_recurso_publico_cooperado.code_owners = [
    "Gabriel Pisa"
]
br_bcb_sicor__microdados_recurso_publico_cooperado.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_recurso_publico_cooperado.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_recurso_publico_cooperado.schedule = (
    every_day_microdados_recurso_publico_cooperado
)

# br_bcb_sicor__microdados_recurso_publico_gleba
br_bcb_sicor__microdados_recurso_publico_gleba = deepcopy(
    br_bcb_sicor_template
)
br_bcb_sicor__microdados_recurso_publico_gleba.name = (
    "br_bcb_sicor__microdados_recurso_publico_gleba"
)
br_bcb_sicor__microdados_recurso_publico_gleba.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__microdados_recurso_publico_gleba.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_recurso_publico_gleba.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_recurso_publico_gleba.schedule = (
    every_day_microdados_recurso_publico_gleba
)

# br_bcb_sicor__microdados_recurso_publico_mutuario
br_bcb_sicor__microdados_recurso_publico_mutuario = deepcopy(
    br_bcb_sicor_template
)
br_bcb_sicor__microdados_recurso_publico_mutuario.name = (
    "br_bcb_sicor__microdados_recurso_publico_mutuario"
)
br_bcb_sicor__microdados_recurso_publico_mutuario.code_owners = [
    "Gabriel Pisa"
]
br_bcb_sicor__microdados_recurso_publico_mutuario.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_recurso_publico_mutuario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__microdados_recurso_publico_mutuario.schedule = (
    every_day_microdados_recurso_publico_mutuario
)

# br_bcb_sicor__microdados_recurso_publico_propriedade
br_bcb_sicor__microdados_recurso_publico_propriedade = deepcopy(
    br_bcb_sicor_template
)
br_bcb_sicor__microdados_recurso_publico_propriedade.name = (
    "br_bcb_sicor__microdados_recurso_publico_propriedade"
)
br_bcb_sicor__microdados_recurso_publico_propriedade.code_owners = [
    "Gabriel Pisa"
]
br_bcb_sicor__microdados_recurso_publico_propriedade.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__microdados_recurso_publico_propriedade.run_config = (
    KubernetesRun(image=constants.DOCKER_IMAGE.value)
)
br_bcb_sicor__microdados_recurso_publico_propriedade.schedule = (
    every_day_microdados_recurso_publico_propriedade
)

# br_bcb_sicor__operacoes_desclassificadas
br_bcb_sicor__operacoes_desclassificadas = deepcopy(br_bcb_sicor_template)
br_bcb_sicor__operacoes_desclassificadas.name = (
    "br_bcb_sicor__operacoes_desclassificadas"
)
br_bcb_sicor__operacoes_desclassificadas.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__operacoes_desclassificadas.storage = GCS(
    constants.GCS_FLOWS_BUCKET.value
)
br_bcb_sicor__operacoes_desclassificadas.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
# br_bcb_sicor__operacoes_desclassificadas.schedule = (
#     every_day_operacoes_desclassificadas
# )

# br_bcb_sicor__empreendimento
br_bcb_sicor__empreendimento = deepcopy(br_bcb_sicor_template)
br_bcb_sicor__empreendimento.name = "br_bcb_sicor__empreendimento"
br_bcb_sicor__empreendimento.code_owners = ["Gabriel Pisa"]
br_bcb_sicor__empreendimento.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor__empreendimento.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
br_bcb_sicor__empreendimento.schedule = every_day_empreendimento


with Flow(
    name="br_bcb_sicor.dicionario",
    code_owners=[
        "Gabriel Pisa",
    ],
) as br_bcb_sicor_dicionario:
    dataset_id = Parameter("dataset_id", default="br_bcb_sicor", required=True)
    table_id = Parameter("table_id", default="dicionario", required=True)
    update_metadata = Parameter(
        "update_metadata", default=False, required=False
    )
    dbt_alias = Parameter("dbt_alias", default=False, required=False)

    materialize_after_dump = Parameter(
        "materialize_after_dump", default=True, required=False
    )

    dicionario_filepath = create_load_dictionary()

    rename_flow_run = rename_current_flow_run_dataset_table(
        prefix="Dump: ",
        dataset_id=dataset_id,
        table_id=table_id,
        wait=table_id,
    )

    wait_upload_table = create_table_dev_and_upload_to_gcs(
        data_path=dicionario_filepath,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode="overwrite",
        upstream_tasks=[dicionario_filepath],
    )

    wait_for_materialization = run_dbt(
        dataset_id=dataset_id,
        table_id=table_id,
        dbt_command="run/test",
        dbt_alias=dbt_alias,
        upstream_tasks=[wait_upload_table],
    )

    with case(materialize_after_dump, True):
        wait_upload_prod = create_table_prod_gcs_and_run_dbt(
            data_path=dicionario_filepath,
            dataset_id=dataset_id,
            table_id=table_id,
            dump_mode="overwrite",
            upstream_tasks=[wait_for_materialization],
        )


br_bcb_sicor_template.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_bcb_sicor_template.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)
