"""
Customizing stuff for the pipelines package.
"""

from collections.abc import Callable, Iterable
from functools import partial

from prefect.core.edge import Edge
from prefect.core.flow import Flow
from prefect.core.task import Task
from prefect.engine.result import Result
from prefect.engine.state import State
from prefect.environments import Environment
from prefect.executors import Executor
from prefect.run_configs import RunConfig
from prefect.schedules import Schedule
from prefect.storage import Storage

from pipelines.constants import constants
from pipelines.utils.utils import notify_discord_on_failure


class CustomFlow(Flow):
    """
    A custom Flow class that implements code ownership in order to make it easier to
    notify people when a FlowRun fails.
    """

    def __init__(
        self,
        name: str,
        schedule: Schedule | None = None,
        executor: Executor | None = None,
        environment: Environment | None = None,
        run_config: RunConfig | None = None,
        storage: Storage | None = None,
        tasks: Iterable[Task] | None = None,
        edges: Iterable[Edge] | None = None,
        reference_tasks: Iterable[Task] | None = None,
        state_handlers: list[Callable] | None = None,
        validate: bool | None = None,
        result: Result | None = None,
        terminal_state_handler: Callable[
            ["Flow", State, set[State]], State | None
        ]
        | None = None,
        code_owners: list[str] | None = None,
    ):
        super().__init__(
            name=name,
            schedule=schedule,
            executor=executor,
            environment=environment,
            run_config=run_config,
            storage=storage,
            tasks=tasks,
            edges=edges,
            reference_tasks=reference_tasks,
            state_handlers=state_handlers,
            on_failure=partial(
                notify_discord_on_failure,
                secret_path=constants.BD_DISCORD_WEBHOOK_SECRET_PATH.value,
                code_owners=code_owners,
            ),
            validate=validate,
            result=result,
            terminal_state_handler=terminal_state_handler,
        )
