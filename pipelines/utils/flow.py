"""
`Flow` da Base dos Dados — o `Flow` do Prefect 3 mais os atributos de deploy.

`.github/workflows/scripts/deploy_flows.py` lê três atributos do objeto flow que o
`prefect.Flow` não declara:

- `deploy_schedules`: lista de agendamentos (`prefect.schedules.Cron`), usada no
  deploy de produção (no pool de dev os schedules são descartados);
- `job_variables`: overrides da configuração de infraestrutura do work pool
  (memória, CPU...);
- `deploy_tags`: tags extras de deploy (além de `"automated-deploy"`, sempre
  adicionada), usadas pra achar deployments relacionados no Prefect UI/CI sem
  abrir cada `flows.py` (ex. `deploy_tags(dataset_id, etapa)` em
  `pipelines/utils/stage_dispatch.py`).

Atribuí-los a um `prefect.Flow` funciona em runtime, mas o Pyrefly acusa
`missing-attribute`, já que a classe do Prefect não os declara. Este módulo
declara os três numa subclasse de `prefect.Flow` e expõe um decorator `flow`
que instancia essa subclasse. Use sempre este `flow` em `flows.py`, nunca o
`prefect.flow`:

```python
from prefect.schedules import Cron

from pipelines.utils.flow import flow


@flow(name="meu_dataset", log_prints=True)
def meu_dataset_flow() -> None: ...


meu_dataset_flow.deploy_schedules = [
    Cron("0 16 10 * *", timezone="America/Sao_Paulo")
]
meu_dataset_flow.job_variables = {"memory": "8Gi"}
meu_dataset_flow.deploy_tags = ["minha-tag"]
```

Como `Flow` herda de `prefect.Flow`, as checagens `isinstance(obj, Flow)` do
script de deploy e do próprio Prefect continuam valendo.
"""

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar, overload

from prefect import Flow as PrefectFlow
from prefect.futures import PrefectFuture
from prefect.schedules import Schedule
from prefect.task_runners import TaskRunner

P = ParamSpec("P")
R = TypeVar("R")


class Flow(PrefectFlow[P, R]):
    """Flow do Prefect 3 com os atributos que o deploy da BD lê.

    Attributes:
        deploy_schedules: Agendamentos do flow, construídos com
            `prefect.schedules.Cron` (que já recebe `timezone`). Vazio (o
            padrão) significa deployment sem schedule — execução apenas manual.
        job_variables: Overrides da configuração de infraestrutura do work
            pool, por exemplo `{"memory": "8Gi"}`. Vazio usa o padrão do pool.
        deploy_tags: Tags extras de deploy, além de `"automated-deploy"`
            (sempre adicionada por `deploy_flow()`). Vazio não adiciona
            nenhuma tag extra.
    """

    deploy_schedules: list[Schedule] | None
    job_variables: dict[str, Any] | None
    deploy_tags: list[str] | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Constrói o flow e inicializa os atributos de deploy como `None`.

        Os atributos são de instância, e não de classe, para que cada flow
        tenha os seus — atribuir `deploy_schedules` a um flow não afeta outro.

        Args:
            *args: Argumentos posicionais do construtor do `prefect.Flow`.
            **kwargs: Argumentos nomeados do construtor do `prefect.Flow`
                (`fn`, `name`, `log_prints`, ...).
        """
        super().__init__(*args, **kwargs)
        self.deploy_schedules = None
        self.job_variables = None
        self.deploy_tags = None


# Uso sem parênteses (`@flow`). A assinatura da função decorada não é
# preservada aqui — usar `Callable[P, R]` tornaria esta sobrecarga genérica, o
# que exigiria a sintaxe de type parameters do Python 3.12 (ruff UP047).
@overload
def flow(fn: Callable[..., Any], /) -> Flow[..., Any]: ...


@overload
def flow(
    fn: None = None,
    /,
    *,
    name: str | None = None,
    version: str | None = None,
    flow_run_name: Callable[[], str] | str | None = None,
    retries: int | None = None,
    retry_delay_seconds: int | float | None = None,
    task_runner: TaskRunner[PrefectFuture[Any]] | None = None,
    description: str | None = None,
    timeout_seconds: int | float | None = None,
    validate_parameters: bool = True,
    persist_result: bool | None = None,
    cache_result_in_memory: bool = True,
    log_prints: bool | None = None,
    **kwargs: Any,
) -> Callable[[Callable[P, R]], Flow[P, R]]: ...


def flow(
    fn: Callable[..., Any] | None = None, /, **kwargs: Any
) -> Flow[..., Any] | Callable[[Callable[..., Any]], Flow[..., Any]]:
    """Decorator equivalente ao `prefect.flow`, mas devolvendo um `Flow` da BD.

    Aceita os mesmos argumentos nomeados do `prefect.flow` (`name`,
    `log_prints`, `retries`, `flow_run_name`, ...), repassados ao construtor do
    `prefect.Flow`.

    Args:
        fn: A função decorada, quando o decorator é usado sem parênteses.
        **kwargs: Argumentos do `prefect.flow`.

    Returns:
        O `Flow` construído, ou o decorator que o constrói.
    """
    if fn is not None:
        return Flow(fn=fn, **kwargs)

    def decorator(fn: Callable[..., Any]) -> Flow[..., Any]:
        """Constrói o `Flow` da BD a partir da função decorada.

        Args:
            fn: A função decorada.

        Returns:
            O `Flow` construído com os `kwargs` passados a `flow(...)`.
        """
        return Flow(fn=fn, **kwargs)

    return decorator
