"""
Constant values for the utils projects
"""

from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING, ClassVar

from pipelines.utils.metadata.domain import DateFormat

if TYPE_CHECKING:
    from pipelines.utils.metadata.coverage import DateColumnKind


class constants(Enum):
    """
    Constant values for the metadata project
    """

    # pyrefly: ignore [invalid-annotation]
    MODE_PROJECT: ClassVar[dict[str, str]] = {
        "dev": "basedosdados-dev",
        "prod": "basedosdados",
    }


# `DateFormat` (strptime) correspondente a cada `date_column_kind` vindo do
# backend — fixo e não-ambíguo, não é dado que o backend manda (seria
# redundante): dado o `date_column_kind`, só existe um `DateFormat`
# compatível. Usado por `coverage.date_format_for`.
DATE_FORMAT_BY_KIND: dict[DateColumnKind, DateFormat] = {
    "date": DateFormat.YEAR_MD,
    "year": DateFormat.YEAR,
    "year_month": DateFormat.YEAR_MONTH,
    "year_quarter": DateFormat.YEAR_MONTH,
    "year_bimester": DateFormat.YEAR_MONTH,
}


# Recursos de pod pro build_and_promote (issue #1867) — único deployment
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
BUILD_AND_PROMOTE_JOB_VARIABLES = {"memory_limit": "2Gi"}
