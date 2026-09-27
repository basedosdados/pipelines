"""Testes do decorator `flow` da BD (`pipelines.utils.flow`).

Os flows são criados dentro de fixtures, e não no nível do módulo: o
`.github/scripts/deploy_flows.py` registra todo objeto `Flow` de nível de
módulo que encontra em `pipelines/`, então um flow de teste declarado aqui
seria deployado em produção junto com os demais.
"""

import pytest
from prefect import Flow as PrefectFlow
from prefect.schedules import Cron

from pipelines.utils.flow import Flow, flow


@pytest.fixture
def flow_com_opcoes() -> Flow:
    """Flow criado com `@flow(...)`, repassando opções do Prefect.

    Returns:
        Um `Flow` novo a cada teste, com `name` e `log_prints` definidos.
    """

    @flow(name="flow_de_teste", log_prints=True)
    def _flow_com_opcoes(x: int = 1) -> int:
        return x

    return _flow_com_opcoes


@pytest.fixture
def flow_sem_parenteses() -> Flow:
    """Flow criado com `@flow`, sem parênteses nem opções.

    Returns:
        Um `Flow` novo a cada teste, com o nome inferido da função.
    """

    @flow
    def _flow_sem_parenteses() -> str:
        return "ok"

    return _flow_sem_parenteses


def test_e_um_flow_do_prefect(
    flow_com_opcoes: Flow, flow_sem_parenteses: Flow
) -> None:
    """O deploy e o próprio Prefect fazem `isinstance(obj, prefect.Flow)`.

    Args:
        flow_com_opcoes: Fixture de flow criado com `@flow(...)`.
        flow_sem_parenteses: Fixture de flow criado com `@flow`.
    """
    assert isinstance(flow_com_opcoes, Flow)
    assert isinstance(flow_com_opcoes, PrefectFlow)
    assert isinstance(flow_sem_parenteses, PrefectFlow)


def test_repassa_as_opcoes_do_prefect(
    flow_com_opcoes: Flow, flow_sem_parenteses: Flow
) -> None:
    """As opções de `@flow(...)` chegam ao construtor do `prefect.Flow`.

    Args:
        flow_com_opcoes: Fixture de flow criado com `@flow(...)`.
        flow_sem_parenteses: Fixture de flow criado com `@flow`.
    """
    assert flow_com_opcoes.name == "flow_de_teste"
    assert flow_com_opcoes.log_prints is True
    # Sem `name=`, o Prefect infere o nome a partir da função.
    assert flow_sem_parenteses.name == "-flow-sem-parenteses"


def test_atributos_de_deploy_comecam_vazios(flow_com_opcoes: Flow) -> None:
    """`None` é o que o Prefect entende como "não informado" no deploy.

    Args:
        flow_com_opcoes: Fixture de flow criado com `@flow(...)`.
    """
    assert flow_com_opcoes.deploy_schedules is None
    assert flow_com_opcoes.job_variables is None


def test_atributos_de_deploy_nao_sao_compartilhados(
    flow_com_opcoes: Flow, flow_sem_parenteses: Flow
) -> None:
    """Cada flow tem os seus — nada de default mutável de classe.

    Args:
        flow_com_opcoes: Fixture de flow criado com `@flow(...)`.
        flow_sem_parenteses: Fixture de flow criado com `@flow`.
    """
    flow_com_opcoes.deploy_schedules = [Cron("0 16 10 * *")]
    flow_com_opcoes.job_variables = {"memory": "8Gi"}

    assert flow_sem_parenteses.deploy_schedules is None
    assert flow_sem_parenteses.job_variables is None


def test_aceita_atribuicao_dos_atributos_de_deploy(
    flow_com_opcoes: Flow,
) -> None:
    """`deploy_schedules` e `job_variables` guardam o que se atribui a eles.

    Args:
        flow_com_opcoes: Fixture de flow criado com `@flow(...)`.
    """
    schedules = [Cron("0 16 10 * *", timezone="America/Sao_Paulo")]

    flow_com_opcoes.deploy_schedules = schedules
    flow_com_opcoes.job_variables = {"memory": "8Gi"}

    assert flow_com_opcoes.deploy_schedules == schedules
    assert flow_com_opcoes.deploy_schedules[0].cron == "0 16 10 * *"
    assert flow_com_opcoes.deploy_schedules[0].timezone == "America/Sao_Paulo"
    assert flow_com_opcoes.job_variables == {"memory": "8Gi"}
