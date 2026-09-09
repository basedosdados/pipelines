"""
Constant values for the utils projects
"""

from enum import Enum
from typing import ClassVar


class constants(Enum):
    """
    Constant values for the metadata project
    """

    # pyrefly: ignore [invalid-annotation]
    MODE_PROJECT: ClassVar[dict[str, str]] = {
        "dev": "basedosdados-dev",
        "prod": "basedosdados",
    }


# Recursos de pod pro mat_test_flow (issue #1867) — único deployment
# compartilhado por todos os datasets, então o tier dele mora aqui, não no
# constants.py de um dataset específico. 2Gi é provisório — o piloto
# sintético (CSV, tabela pequena, pipelines/datasets/test_dataset/ (event_pipeline_*))
# mediu ~1GB de uso real em 2026-09-01, bem abaixo do default de 4Gi do
# work pool. O upload/download em si não escala com o tamanho do arquivo
# (streaming pro disco, não pra memória — ver
# `download_files_from_bucket_folders`/`basedosdados.Storage.upload`), mas
# `dump_header()` em formato Parquet carrega um row group inteiro na
# memória pra inferir o schema (CSV usa `nrows=1`, sempre seguro) —
# reavaliar quando um dataset real com `source_format="parquet"` passar
# por aqui. Ver tutoriais/iac/prefect3-override-de-recursos-por-flow.md no
# ftwca pra mais contexto sobre o mecanismo de override.
MAT_TEST_JOB_VARIABLES = {"memory_limit": "2Gi"}
