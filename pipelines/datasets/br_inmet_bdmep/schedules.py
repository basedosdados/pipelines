# -*- coding: utf-8 -*-
"""
Schedules for br_inmet_bdmep
"""

###############################################################################
#
# Aqui é onde devem ser definidos os schedules para os flows do projeto.
# Cada schedule indica o intervalo de tempo entre as execuções.
# Um schedule pode ser definido para um ou mais flows.
# Mais informações sobre schedules podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/schedules.html
#
# De modo a manter consistência na codebase, todo o código escrito passará
# pelo pylint. Todos os warnings e erros devem ser corrigidos.
#
# Os schedules devem ser definidos de acordo com a sintaxe do Prefect, como,
# por exemplo, o seguinte (para executar a cada 1 minuto):
#
# -----------------------------------------------------------------------------
# from datetime import timedelta, datetime
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import IntervalClock
# from pipelines.constants import constants
#
# minute_schedule = Schedule(
#     clocks=[
#         IntervalClock(
#             interval=timedelta(minutes=1),
#             start_date=datetime(2021, 1, 1),
#             labels=[
#                 constants.DATASETS_AGENT_LABEL.value,
#             ]
#         ),
#     ]
# )
# -----------------------------------------------------------------------------
#
# Vale notar que o parâmetro `labels` é obrigatório e deve ser uma lista com
# apenas um elemento, correspondendo ao label do agente que será executado.
# O label do agente é definido em `constants.py` e deve ter o formato
# `DATASETS_AGENT_LABEL`.
#
# Outro exemplo, para executar todos os dias à meia noite, segue abaixo:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from datetime import timedelta
# import pendulum
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import IntervalClock
# from pipelines.constants import constants
#
# every_day_at_midnight = Schedule(
#     clocks=[
#         IntervalClock(
#             interval=timedelta(days=1),
#             start_date=pendulum.datetime(
#                 2021, 1, 1, 0, 0, 0, tz="America/Sao_Paulo"),
#             labels=[
#                 constants.K8S_AGENT_LABEL.value,
#             ]
#         )
#     ]
# )
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################


from datetime import timedelta, datetime
from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock
from pipelines.constants import constants


every_month_inmet = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(days=30),
            start_date=datetime(2021, 3, 31, 17, 11),
            labels=[
                constants.BASEDOSDADOS_DEV_AGENT_LABEL.value,
            ],
            parameter_defaults={
                "dataset_id": "br_inmet_bdmep",
                "table_id": "microdados",
                "materialization_mode": "dev",
                "materialize_after_dump": True,
                "dbt_alias": True,
            },
        ),
    ],
)
