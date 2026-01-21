"""
Schedules for {{cookiecutter.pipeline_name}}
"""

###############################################################################
#
# Aqui é onde devem ser definidos os schedules para os flows da pipeline.
# Cada schedule indica o intervalo de tempo entre as execuções.
# Um schedule pode ser definido para um ou mais flows.
# Mais informações sobre schedules podem ser encontradas na documentação do
# Prefect: https://docs.prefect.io/core/concepts/schedules.html
#
# Os schedules devem ser definidos de acordo com a sintaxe do Prefect, como,
# por exemplo, o seguinte (para executar a cada 1 minuto):
#
# -----------------------------------------------------------------------------
# from datetime import datetime
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import CronClock
# from pipelines.constants import constants
#
# minute_schedule = Schedule(
#     clocks=[
#         CronClock(
#             cron="* * * * *", # At every minute
#             start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
#             labels=[
#                 constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
#             ]
#         ),
#     ]
# )
# -----------------------------------------------------------------------------
#
# Vale notar que o parâmetro `labels` é obrigatório e deve ser uma lista com
# apenas um elemento, correspondendo ao label do agente que será executado.
# O label do agente é definido em `constants.py` e deve ter o formato
# `constants.BASEDOSDADOS_{PROD,DEV}.value,`.
#
# Outro exemplo, para executar todos os dias à meia noite, segue abaixo:
#
# -----------------------------------------------------------------------------
# from prefect import task
# from datetime import datetime
# from prefect.schedules import Schedule
# from prefect.schedules.clocks import CronClock
# from pipelines.constants import constants
#
# every_day_at_midnight = Schedule(
#     clocks=[
#         CronClock(
#             cron="0 0 * * *", # At 00:00
#             start_date=datetime(2021, 1, 1, 0, 0, 0, tz="America/Sao_Paulo"),
#             labels=[
#                 constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
#             ]
#         )
#     ]
# )
# -----------------------------------------------------------------------------
#
# Abaixo segue um código para exemplificação, que pode ser removido.
#
###############################################################################


from datetime import datetime

from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock

from pipelines.constants import constants

schedule = Schedule(
    clocks=[
        CronClock(
            # See https://crontab.guru/
            cron="0 14 * * 1", # At 14:00 on Monday.
            start_date=datetime(2025, 1, 1, tz="America/Sao_Paulo"),
            labels=[
                constants.BASEDOSDADOS_PROD_AGENT_LABEL.value,
            ]
        ),
    ]
)
