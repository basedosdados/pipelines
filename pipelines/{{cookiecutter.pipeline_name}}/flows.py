"""
Flows for {{cookiecutter.pipeline_name}} — Prefect 3.
"""

###############################################################################
#
# Aqui é onde devem ser definidos os flows da pipeline (Prefect 3).
#
# Cada flow é uma função decorada com `@flow`. Os passos são chamadas de
# tasks (funções decoradas com `@task`, definidas em `tasks.py`) executadas
# na ordem do corpo da função.
#
# O deploy é feito por `.github/scripts/deploy_flows.py`, que descobre os
# objetos `Flow` deste arquivo automaticamente — não é preciso registrar
# storage nem run_config manualmente.
#
# O agendamento é definido inline no próprio objeto do flow, via
# `<flow>.deploy_schedules`, uma lista de dicts `{"cron": ..., "timezone": ...}`.
# Em `dev` os schedules são ignorados; em `prod` são ativados na sincronização
# com o backend. Veja https://crontab.guru/ para montar a expressão cron.
#
# Documentação: https://docs.prefect.io/v3/
#
# Abaixo segue um código de exemplo, que pode ser removido.
#
###############################################################################

from prefect import flow

from pipelines.datasets.{{cookiecutter.pipeline_name}}.tasks import say_hello


@flow(name="{{cookiecutter.pipeline_name}}", log_prints=True)
def {{cookiecutter.pipeline_name}}_flow(name: str = "World") -> None:
    say_hello(name=name)


# Agendamento (opcional). Remova se o flow não deve rodar em intervalos fixos.
{{cookiecutter.pipeline_name}}_flow.deploy_schedules = [
    {"cron": "0 14 * * 1", "timezone": "America/Sao_Paulo"}  # segundas, 14:00
]
