# -*- coding: utf-8 -*-
"""
Flows for br_jota
"""

from datetime import timedelta

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.execute_dbt_model.constants import constants as dump_db_constants
from pipelines.utils.tasks import get_current_flow_labels

from pipelines.datasets.br_jota.tasks import (
    materialization
)

with Flow(
    name="BD template - br_jota", code_owners=["luiz"]
) as br_jota_2024:

    dataset_id = Parameter("dataset_id", default="mundo_transfermarkt_competicoes", required=True)

    table_id = Parameter("table_id", default="copa_brasil", required=False)

    materialization_mode = Parameter(
        "materialization_mode", default="dev", required=False
    )
    dbt_alias = Parameter("dbt_alias", default=True, required=False)

    current_flow_labels = get_current_flow_labels()

    tabela1 = materialization(1, dataset_id, table_id, materialization_mode,
                    dbt_alias, current_flow_labels, upstream_tasks=[current_flow_labels])

    tabela2 = materialization(2, dataset_id, table_id, materialization_mode,
                    dbt_alias, current_flow_labels, upstream_tasks=[tabela1])

    tabela3 = materialization(3, dataset_id, table_id, materialization_mode,
                    dbt_alias, current_flow_labels, upstream_tasks=[tabela2])

    tabela4 = materialization(4, dataset_id, table_id, materialization_mode,
                    dbt_alias, current_flow_labels, upstream_tasks=[tabela3])


br_jota_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
br_jota_2024.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value
)


# from prefect.run_configs import KubernetesRun
# from prefect.storage import GCS

# from pipelines.constants import constants

# from pipelines.utils.template_br_jota.flows import br_jota_2024
# from pipelines.datasets.br_jota.schedules import (
#     schedule_eleicao_perfil_candidato,
#     schedule_eleicao_prestacao_contas_candidato,
#     schedule_contas_candidato_origem,
#     schedule_prestacao_contas_partido
# )
# from copy import deepcopy


# # Tabela: eleicao_perfil_candidato_2024

# eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
# eleicao_perfil_candidato_2024.name = "br_jota.eleicao_perfil_candidato_2024"
# eleicao_perfil_candidato_2024.code_owners = ["luiz"]
# eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# eleicao_perfil_candidato_2024.schedule = schedule_eleicao_perfil_candidato

# # Tabela: eleicao_prestacao_contas_candidato_2024

# eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
# eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_candidato_2024"
# eleicao_perfil_candidato_2024.code_owners = ["luiz"]
# eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# eleicao_perfil_candidato_2024.schedule = schedule_eleicao_prestacao_contas_candidato

# # Tabela: eleicao_prestacao_contas_candidato_origem_2024

# eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
# eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_candidato_origem_2024"
# eleicao_perfil_candidato_2024.code_owners = ["luiz"]
# eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# eleicao_perfil_candidato_2024.schedule = schedule_contas_candidato_origem

# # Tabela: eleicao_prestacao_contas_partido_2024

# eleicao_perfil_candidato_2024 = deepcopy(br_jota_2024)
# eleicao_perfil_candidato_2024.name = "br_jota.eleicao_prestacao_contas_partido_2024"
# eleicao_perfil_candidato_2024.code_owners = ["luiz"]
# eleicao_perfil_candidato_2024.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# eleicao_perfil_candidato_2024.run_config = KubernetesRun(image=constants.DOCKER_IMAGE.value)
# eleicao_perfil_candidato_2024.schedule = schedule_prestacao_contas_partido

# -*- coding: utf-8 -*-
