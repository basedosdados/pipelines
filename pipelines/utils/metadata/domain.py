"""
Tipos de domínio da interação com o backend da BD.

Aqui estão registradas as regras de negócio de preenchimento de metadados e políticas de materialização das pipelines.
"""

from __future__ import annotations

from enum import Enum
from typing import Annotated, Literal

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel, Field, model_validator


class DateFormat(str, Enum):
    """Formatos de data aceitos pelo backend."""

    YEAR = "%Y"
    YEAR_MONTH = "%Y-%m"
    YEAR_MD = "%Y-%m-%d"


class DateOnly(BaseModel):
    kind: Literal["date"] = "date"
    col: str


class YearOnly(BaseModel):
    kind: Literal["year"] = "year"
    col: str


class YearMonth(BaseModel):
    kind: Literal["year_month"] = "year_month"
    year: str
    month: str


class YearQuarter(BaseModel):
    kind: Literal["year_quarter"] = "year_quarter"
    year: str
    quarter: str


DateColumn = Annotated[
    DateOnly | YearOnly | YearMonth | YearQuarter,
    Field(discriminator="kind"),
]


def date_column_to_legacy_dict(dc) -> dict:
    """Traduz um `DateColumn` para o dict que as funções de BigQuery em
    `utils.py` (`extract_last_date_from_bq`, `update_row_access_policy`)
    esperam."""
    if isinstance(dc, DateOnly):
        return {"date": dc.col}
    if isinstance(dc, YearOnly):
        return {"year": dc.col}
    if isinstance(dc, YearMonth):
        return {"year": dc.year, "month": dc.month}
    if isinstance(dc, YearQuarter):
        return {"year": dc.year, "quarter": dc.quarter}
    raise TypeError(f"DateColumn desconhecido: {type(dc).__name__}")


class FreeLag(BaseModel):
    """Defasagem entre a cobertura temporal BD pro e a free."""

    unit: Literal["years", "months", "weeks", "days"]
    value: int = Field(ge=1)

    def as_relativedelta(self) -> relativedelta:
        # pyrefly: ignore [bad-argument-type]
        return relativedelta(**{self.unit: self.value})


class _CoverageBase(BaseModel):
    date_column: DateColumn
    date_format: DateFormat

    @model_validator(mode="after")
    def _column_matches_format(self) -> _CoverageBase:
        # a coluna de data declarada precisa casar com o date_format declarado.
        allowed = {
            ("date", DateFormat.YEAR_MD),
            ("year_month", DateFormat.YEAR_MONTH),
            ("year_quarter", DateFormat.YEAR_MONTH),
            ("year", DateFormat.YEAR),
        }
        if (self.date_column.kind, self.date_format) not in allowed:
            raise ValueError(
                f"date_column {self.date_column.kind!r} incompatível "
                f"com date_format {self.date_format.value!r}"
            )
        return self


class AllFree(_CoverageBase):
    tier: Literal["all_free"] = "all_free"


class AllBdpro(_CoverageBase):
    tier: Literal["all_bdpro"] = "all_bdpro"


class PartBdpro(_CoverageBase):
    tier: Literal["part_bdpro"] = "part_bdpro"
    free_lag: FreeLag = Field(
        default_factory=lambda: FreeLag(unit="months", value=6)
    )
    # part_bdpro implica série histórica: não há tier 'parcial' sem histórico.


class NonHistorical(BaseModel):
    """Cobertura única baseada em `__TABLES__.last_modified_time`.

    Sem `date_column`/`date_format`: a data vem da metadata do BQ.
    """

    tier: Literal["non_historical"] = "non_historical"

    model_config = {"frozen": True}


CoverageSpec = Annotated[
    AllFree | AllBdpro | PartBdpro | NonHistorical,
    Field(discriminator="tier"),
]
