"""Testes do decorator `flow` da BD (`pipelines.utils.flow`)."""

from prefect import Flow as PrefectFlow

from pipelines.utils.flow import DeploySchedule, Flow, flow


@flow(name="flow_de_teste", log_prints=True)
def _flow_com_opcoes(x: int = 1) -> int:
    return x


@flow
def _flow_sem_parenteses() -> str:
    return "ok"


def test_e_um_flow_do_prefect():
    """O deploy e o próprio Prefect fazem `isinstance(obj, prefect.Flow)`."""
    assert isinstance(_flow_com_opcoes, Flow)
    assert isinstance(_flow_com_opcoes, PrefectFlow)
    assert isinstance(_flow_sem_parenteses, PrefectFlow)


def test_repassa_as_opcoes_do_prefect():
    assert _flow_com_opcoes.name == "flow_de_teste"
    assert _flow_com_opcoes.log_prints is True
    # Sem `name=`, o Prefect infere o nome a partir da função.
    assert _flow_sem_parenteses.name == "-flow-sem-parenteses"


def test_atributos_de_deploy_comecam_vazios():
    assert _flow_com_opcoes.deploy_schedules == []
    assert _flow_com_opcoes.job_variables == {}


def test_atributos_de_deploy_nao_sao_compartilhados():
    """Cada flow tem os seus — nada de default mutável de classe."""
    assert (
        _flow_com_opcoes.deploy_schedules
        is not _flow_sem_parenteses.deploy_schedules
    )
    assert (
        _flow_com_opcoes.job_variables
        is not _flow_sem_parenteses.job_variables
    )


def test_aceita_atribuicao_dos_atributos_de_deploy():
    schedules: list[DeploySchedule] = [
        {"cron": "0 16 10 * *", "timezone": "America/Sao_Paulo"}
    ]

    _flow_com_opcoes.deploy_schedules = schedules
    _flow_com_opcoes.job_variables = {"memory": "8Gi"}

    assert _flow_com_opcoes.deploy_schedules == schedules
    assert _flow_com_opcoes.job_variables == {"memory": "8Gi"}
