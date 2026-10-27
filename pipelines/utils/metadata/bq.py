"""
BigQueryReader — adapter de BigQuery que `register_table_materialization` consome.

Faz as leituras de BigQuery necessárias para atualizar a cobertura temporal de
uma tabela: data máxima da coluna de cobertura, `last_modified` da tabela,
permissão de leitura da metadata e aplicação das Row Access Policies. Traduz
entre `CoverageSpec` (`metadata/coverage.py`) e as funções de BigQuery de
`utils.py`.

Faz I/O (BigQuery). Implementa a superfície `register.BQReader`; nos testes é
substituído por um fake.
"""

from __future__ import annotations

import datetime

from pipelines.utils.metadata.coverage import (
    CoverageSpec,
    date_format_for,
    legacy_date_columns,
)
from pipelines.utils.metadata.utils import (
    able_to_query_bigquery_metadata,
    extract_last_date_from_bq,
    update_date_from_bq_metadata,
    update_row_access_policy,
)


def _end_parameters(d: datetime.date, date_format: str) -> dict:
    """Constrói o dict {endYear, endMonth?, endDay?} que `update_row_access_policy`
    (via `format_date_parameters`) consome, conforme a granularidade do formato."""
    out = {"endYear": d.year}
    if date_format in ("%Y-%m", "%Y-%m-%d"):
        out["endMonth"] = d.month
    if date_format == "%Y-%m-%d":
        out["endDay"] = d.day
    return out


class BigQueryReader:
    def __init__(
        self,
        billing_project_id: str,
        bq_project: str = "basedosdados",
        historical_database: bool = True,
    ):
        self.billing_project_id = billing_project_id
        self.bq_project = bq_project
        self.historical_database = historical_database

    def read_max_date(
        self, dataset_id: str, table_id: str, coverage: CoverageSpec
    ) -> datetime.date:
        fmt = date_format_for(coverage.date_column_kind)
        last_date = extract_last_date_from_bq(
            dataset_id,
            table_id,
            fmt.value,
            legacy_date_columns(coverage),
            self.billing_project_id,
            self.bq_project,
            self.historical_database,
        )
        return datetime.datetime.strptime(last_date, fmt.value).date()

    def last_modified(
        self, dataset_id: str, table_id: str
    ) -> datetime.datetime:
        # pyrefly: ignore [bad-return]
        return update_date_from_bq_metadata(
            dataset_id, table_id, self.billing_project_id, self.bq_project
        )

    def can_read_metadata(self, bq_project: str) -> bool:
        return able_to_query_bigquery_metadata(
            self.billing_project_id, bq_project
        )

    def apply_row_access_policies(
        self,
        coverage: CoverageSpec,
        free_end: datetime.date,
        dataset_id: str,
        table_id: str,
    ) -> None:
        fmt = date_format_for(coverage.date_column_kind)
        update_row_access_policy(
            self.bq_project,
            dataset_id,
            table_id,
            self.billing_project_id,
            legacy_date_columns(coverage),
            fmt.value,
            _end_parameters(free_end, fmt.value),
        )
