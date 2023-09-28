# -*- coding: utf-8 -*-
"""
Customizing stuff for the pipelines package.
"""

from functools import partial
from typing import Callable, Iterable, List, Optional, Set

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
# from pipelines.utils.utils import notify_discord_on_failure



class CustomFlow(Flow):
    """
    A custom Flow class that implements code ownership in order to make it easier to
    notify people when a FlowRun fails.
    """

    def __init__(  # pylint: disable=too-many-arguments
        self,
        name: str,
        schedule: Schedule = None,
        executor: Executor = None,
        environment: Environment = None,
        run_config: RunConfig = None,
        storage: Storage = None,
        tasks: Iterable[Task] = None,
        edges: Iterable[Edge] = None,
        reference_tasks: Iterable[Task] = None,
        state_handlers: List[Callable] = None,
        validate: bool = None,
        result: Optional[Result] = None,
        terminal_state_handler: Optional[
            Callable[["Flow", State, Set[State]], Optional[State]]
        ] = None,
        code_owners: Optional[List[str]] = None,
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
            # on_failure=partial(
            #     notify_discord_on_failure,
            #     secret_path=constants.BD_DISCORD_WEBHOOK_SECRET_PATH.value,
            #     code_owners=code_owners,
            # ),
            validate=validate,
            result=result,
            terminal_state_handler=terminal_state_handler,
        )
