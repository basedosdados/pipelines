"""
BigQueryReader — adapter de BigQuery que `register_table_materialization` consome.

Faz as leituras de BigQuery necessárias para atualizar a cobertura temporal de
uma tabela: data máxima da coluna de cobertura, `last_modified` da tabela,
permissão de leitura da metadata e aplicação das Row Access Policies. Traduz
entre os tipos de domínio (`CoverageSpec`/`DateColumn`) e as funções de BigQuery
de `utils.py`.

Faz I/O (BigQuery). Implementa a superfície `register.BQReader`; nos testes é
substituído por um fake.
"""

from __future__ import annotations

import datetime

from pipelines.utils.metadata.domain import (
    CoverageSpec,
    PartBdpro,
    date_column_to_legacy_dict,
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
        last_date = extract_last_date_from_bq(
            dataset_id,
            table_id,
            # pyrefly: ignore [missing-attribute]
            coverage.date_format.value,
            # pyrefly: ignore [missing-attribute]
            date_column_to_legacy_dict(coverage.date_column),
            self.billing_project_id,
            self.bq_project,
            self.historical_database,
        )
        return datetime.datetime.strptime(
            # pyrefly: ignore [missing-attribute]
            last_date,
            # pyrefly: ignore [missing-attribute]
            coverage.date_format.value,
        ).date()

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
        coverage: PartBdpro,
        free_end: datetime.date,
        dataset_id: str,
        table_id: str,
    ) -> None:
        update_row_access_policy(
            self.bq_project,
            dataset_id,
            table_id,
            self.billing_project_id,
            date_column_to_legacy_dict(coverage.date_column),
            coverage.date_format.value,
            _end_parameters(free_end, coverage.date_format.value),
        )
