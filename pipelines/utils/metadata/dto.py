"""
DTOs Pydantic — contrato com a API GraphQL do backend da BD.

Um modelo por entidade do Django, espelhando os campos de cada mutation. Este é
o único módulo que conhece os nomes de campo do backend: tudo que sai pelo fio é
validado aqui (formas de data consistentes, intervalos numéricos, UUIDs
obrigatórios), de modo que nenhum `dict` solto chegue à camada de transporte.

As datas são componentes `int | None` validados por intervalo (nunca strings de
formato livre), e o campo `latest` é normalizado para um datetime ISO 8601
completo, pois o scalar GraphQL `DateTime` do backend recusa strings date-only.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Annotated

from pydantic import BaseModel, BeforeValidator, Field, model_validator

UUIDStr = Annotated[str, Field(pattern=r"^[0-9a-f-]{36}$")]
Year = Annotated[int, Field(ge=1900, le=2100)]
Month = Annotated[int, Field(ge=1, le=12)]
Day = Annotated[int, Field(ge=1, le=31)]


def _to_iso8601(value: object) -> str:
    """Normaliza `latest` para um datetime ISO 8601 completo.

    O campo `latest` de `Poll`/`Update` no backend é um scalar GraphQL `DateTime`
    e recusa strings date-only (`"2026-06-01"`). Por isso uma `date` (ou string
    date-only) é combinada com meia-noite. Strings não-parseáveis levantam
    `ValueError`.
    """
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return datetime.combine(value, datetime.min.time()).isoformat()
    if isinstance(value, str):
        # aceita date-only ou datetime ISO; normaliza para datetime completo
        return datetime.fromisoformat(value).isoformat()
    raise ValueError("latest deve ser ISO 8601 (str) ou date/datetime")


IsoDateStr = Annotated[str, BeforeValidator(_to_iso8601)]


def _compose_end_date(datetime_range: dict) -> date | None:
    """Recompõe o fim de um `DateTimeRange` (endYear/endMonth/endDay) numa `date`.

    Inverso de `policy._components`, e usa a mesma convenção de
    `bq.read_max_date`, que faz `strptime(valor, date_format)` — com `"%Y"` o
    Python já completa mês/dia com 1. Por isso mês/dia ausentes viram 1 aqui: os
    dois lados da comparação ficam na mesma escala e uma cobertura anual que
    termina em 2025 vira `2025-01-01`, exatamente o que `read_max_date` devolve
    para aquela tabela. Completar com o fim do período (2025-12-31) faria toda
    tabela anual parecer defasada e disparar ingestão a cada run.

    Devolve ``None`` se não há nem ano.
    """
    year = datetime_range.get("endYear")
    if year is None:
        return None
    return date(
        year,
        datetime_range.get("endMonth") or 1,
        datetime_range.get("endDay") or 1,
    )


class DateTimeRangeInput(BaseModel):
    """Payload de `CreateUpdateDateTimeRange` (Coverage.DateTimeRange)."""

    coverage: UUIDStr
    startYear: Year | None = None
    startMonth: Month | None = None
    startDay: Day | None = None
    endYear: Year | None = None
    endMonth: Month | None = None
    endDay: Day | None = None

    @model_validator(mode="after")
    def _shape_consistent(self) -> DateTimeRangeInput:
        # forma consistente: dia exige mês; mês exige ano — em ambos os lados.
        for side in ("start", "end"):
            y, m, d = (
                getattr(self, f"{side}{x}") for x in ("Year", "Month", "Day")
            )
            if d is not None and m is None:
                raise ValueError(f"{side}Day preenchido sem {side}Month")
            if m is not None and y is None:
                raise ValueError(f"{side}Month preenchido sem {side}Year")
        return self


class PollInput(BaseModel):
    """Payload de `CreateUpdatePoll` (RawDataSource.Poll)."""

    rawDataSource: UUIDStr
    latest: IsoDateStr
    frequency: int = 1
    entity: UUIDStr  # resolvido pelo Client, nunca hardcoded


class TableUpdateInput(BaseModel):
    """Payload de `CreateUpdateUpdate` para Update FK=Table."""

    table: UUIDStr
    latest: IsoDateStr
    frequency: int | None = None
    entity: UUIDStr | None = None


class RawSourceUpdateInput(BaseModel):
    """Payload de `CreateUpdateUpdate` para Update FK=RawDataSource."""

    rawDataSource: UUIDStr
    latest: IsoDateStr
    frequency: int | None = None
    entity: UUIDStr | None = None
