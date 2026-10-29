"""Peças do pipeline em estágios, que liga os flows por `run_deployment()`.

- `contracts`: tipos que passam entre as etapas.
- `naming`: convenção de nome dos flows e deployments.
- `dispatch`: passagem de uma etapa para a seguinte.
- `derived_tables`: materialização de uma tabela junto com as derivadas.
- `pipeline`: interface que os `flows.py` usam.

Os nomes públicos de todos os módulos são reexportados aqui, então os
`flows.py` importam direto de `pipelines.utils.stage_dispatch`.
"""

from pipelines.utils.stage_dispatch.contracts import (
    Etapa,
    ExtractAndLoad,
    SourceInspection,
)
from pipelines.utils.stage_dispatch.derived_tables import (
    promote_primary_and_derived_tables,
)
from pipelines.utils.stage_dispatch.dispatch import (
    check_update_and_dispatch,
    dispatch_build_and_promote,
)
from pipelines.utils.stage_dispatch.naming import (
    deploy_tags,
    deployment_name,
)
from pipelines.utils.stage_dispatch.pipeline import (
    CheckThenExtractLoadPipeline,
    discover_partition_folders,
    pipeline_factory,
)

__all__ = [
    "CheckThenExtractLoadPipeline",
    "Etapa",
    "ExtractAndLoad",
    "SourceInspection",
    "check_update_and_dispatch",
    "deploy_tags",
    "deployment_name",
    "discover_partition_folders",
    "dispatch_build_and_promote",
    "pipeline_factory",
    "promote_primary_and_derived_tables",
]
