"""
Testes do adapter `BigQueryReader` (`pipelines/utils/metadata/bq.py`).

Mocka as funções de `utils.py` e asserta a tradução domain→legacy-dict e a
delegação correta. Sem BigQuery real.
"""

import datetime
from unittest.mock import patch

from pipelines.utils.metadata.bq import BigQueryReader
from pipelines.utils.metadata.domain import (
    DateFormat,
    DateOnly,
    PartBdpro,
    YearMonth,
)


def _monthly():
    return PartBdpro(
        date_column=YearMonth(year="ano", month="mes"),
        date_format=DateFormat.YEAR_MONTH,
    )


def _daily():
    return PartBdpro(
        date_column=DateOnly(col="data"), date_format=DateFormat.YEAR_MD
    )


@patch("pipelines.utils.metadata.bq.extract_last_date_from_bq")
def test_read_max_date_translates_and_parses(mock_extract):
    mock_extract.return_value = "2026-04"
    bq = BigQueryReader(billing_project_id="proj", bq_project="basedosdados")

    out = bq.read_max_date("br_x", "tab", _monthly())

    assert out == datetime.date(2026, 4, 1)
    # tradução: YearMonth → {"year":"ano","month":"mes"} e formato "%Y-%m"
    args = mock_extract.call_args.args
    assert args[2] == "%Y-%m"
    assert args[3] == {"year": "ano", "month": "mes"}
    assert args[4] == "proj"  # billing_project_id


@patch("pipelines.utils.metadata.bq.update_date_from_bq_metadata")
def test_last_modified_delegates(mock_lm):
    mock_lm.return_value = datetime.datetime(2026, 6, 2, 10, 0)
    bq = BigQueryReader(billing_project_id="proj")
    assert bq.last_modified("br_x", "tab") == datetime.datetime(
        2026, 6, 2, 10, 0
    )
    mock_lm.assert_called_once_with("br_x", "tab", "proj", "basedosdados")


def test_can_read_metadata_is_billing_eq_project():
    assert BigQueryReader(billing_project_id="basedosdados").can_read_metadata(
        "basedosdados"
    )
    assert not BigQueryReader(
        billing_project_id="basedosdados-dev"
    ).can_read_metadata("basedosdados")


@patch("pipelines.utils.metadata.bq.update_row_access_policy")
def test_apply_rap_builds_end_parameters_daily(mock_rap):
    bq = BigQueryReader(billing_project_id="proj", bq_project="basedosdados")
    bq.apply_row_access_policies(
        _daily(), datetime.date(2025, 12, 31), "br_x", "tab"
    )
    args = mock_rap.call_args.args
    assert args[0] == "basedosdados"  # bq_project
    assert args[4] == {"date": "data"}  # date_column_name traduzido
    assert args[5] == "%Y-%m-%d"  # date_format
    assert args[6] == {"endYear": 2025, "endMonth": 12, "endDay": 31}


@patch("pipelines.utils.metadata.bq.update_row_access_policy")
def test_apply_rap_end_parameters_monthly_has_no_day(mock_rap):
    bq = BigQueryReader(billing_project_id="proj")
    bq.apply_row_access_policies(
        _monthly(), datetime.date(2025, 12, 1), "br_x", "tab"
    )
    assert mock_rap.call_args.args[6] == {"endYear": 2025, "endMonth": 12}
