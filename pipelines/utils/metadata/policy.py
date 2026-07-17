"""
Camada de Policy — regras de negócio como funções puras.

Sem I/O: sem `bd.Backend`, sem `bd.read_sql`, sem BigQuery. Recebe dados já lidos
(coberturas existentes, data máxima da fonte, status da tabela) e devolve
decisões e valores a gravar. É onde vivem as regras de preenchimento de cobertura
temporal e as travas de escrita; por não tocar a rede, é testável com `pytest`
puro.
"""

from __future__ import annotations

from datetime import date

from pydantic import BaseModel

from pipelines.utils.metadata.domain import (
    AllBdpro,
    AllFree,
    CoverageSpec,
    DateFormat,
    NonHistorical,
    PartBdpro,
)
from pipelines.utils.metadata.dto import DateTimeRangeInput


class CoverageIds(BaseModel):
    """Coberturas encontradas no backend para uma tabela (resultado de leitura)."""

    free: str | None = None
    pro: str | None = None


class CoverageRanges(BaseModel):
    """Resultado de `compute_coverage_ranges`: os DateTimeRange a gravar + a data
    de fim da cobertura free (para a Row Access Policy)."""

    free: DateTimeRangeInput | None = None
    pro: DateTimeRangeInput | None = None
    free_end: date | None = None

    def to_list(self) -> list[DateTimeRangeInput]:
        return [r for r in (self.free, self.pro) if r is not None]


# ----------------------------------------------- atualização de RawDataSource
def should_update_raw_source(
    api_latest: date | None, source_max: date | None
) -> bool:
    """Só atualiza `RawDataSource.Update` se a fonte tem data nova."""
    if source_max is None:
        return False
    return api_latest is None or source_max > api_latest


# --------------------------------------------------- trava de escrita em prod
def assert_write_allowed(
    env: str, bq_project: str, table_status: str | None
) -> None:
    """Escrever em prod com dados não-produtivos só é permitido se a tabela está
    `under_review`. Trava fail-fast antes de qualquer escrita."""
    if (
        env == "prod"
        and bq_project != "basedosdados"
        and table_status != "under_review"
    ):
        raise ValueError(
            "Para gravar metadados em prod com dados não-produtivos "
            f"({bq_project}), a tabela precisa estar 'under_review' "
            f"(status atual: {table_status!r})."
        )


# ------------------------------------------------------- Row Access Policies
def needs_row_access_policy(spec: CoverageSpec) -> bool:
    """Row Access Policies só se aplicam a `part_bdpro`."""
    return isinstance(spec, PartBdpro)


# ------------------------------------------- topologia de coberturas no backend
def assert_coverage_topology(spec: CoverageSpec, found: CoverageIds) -> None:
    """A topologia de coberturas existentes no backend deve bater com o tier
    pedido."""
    if isinstance(spec, PartBdpro) and not (found.free and found.pro):
        raise ValueError(
            f"part_bdpro exige Coverage free + pro; encontrado: {found!r}"
        )
    if isinstance(spec, AllFree) and (not found.free or found.pro):
        raise ValueError(
            f"all_free exige Coverage free e ausência de pro; encontrado: {found!r}"
        )
    if isinstance(spec, AllBdpro) and (not found.pro or found.free):
        raise ValueError(
            f"all_bdpro exige Coverage pro e ausência de free; encontrado: {found!r}"
        )


# ----------------------------------------------- cálculo dos DateTimeRange
def _components(d: date, fmt: DateFormat, position: str) -> dict:
    """Decompõe uma data nos campos do DTO conforme a granularidade do formato."""
    out = {f"{position}Year": d.year}
    if fmt in (DateFormat.YEAR_MONTH, DateFormat.YEAR_MD):
        out[f"{position}Month"] = d.month
    if fmt == DateFormat.YEAR_MD:
        out[f"{position}Day"] = d.day
    return out


def compute_coverage_ranges(
    spec: CoverageSpec, source_end: date, coverage_ids: CoverageIds
) -> CoverageRanges:
    """Calcula os DateTimeRange free e/ou pro.

    - all_free  → range free terminando em `source_end`.
    - all_bdpro → range pro terminando em `source_end`.
    - part_bdpro→ pro termina em `source_end`; free termina em
      `source_end - free_lag`; pro começa onde a free termina.
    """
    if isinstance(spec, NonHistorical):
        raise ValueError("NonHistorical não usa compute_coverage_ranges")

    fmt = spec.date_format

    if isinstance(spec, AllFree):
        return CoverageRanges(
            free=DateTimeRangeInput(
                coverage=coverage_ids.free,
                **_components(source_end, fmt, "end"),
            ),
            free_end=source_end,
        )

    if isinstance(spec, AllBdpro):
        return CoverageRanges(
            pro=DateTimeRangeInput(
                coverage=coverage_ids.pro,
                **_components(source_end, fmt, "end"),
            )
        )

    # part_bdpro
    free_end = source_end - spec.free_lag.as_relativedelta()
    free = DateTimeRangeInput(
        coverage=coverage_ids.free, **_components(free_end, fmt, "end")
    )
    pro = DateTimeRangeInput(
        coverage=coverage_ids.pro,
        **_components(source_end, fmt, "end"),
        **_components(
            free_end, fmt, "start"
        ),  # sincronização: pro começa onde a free termina
    )
    return CoverageRanges(free=free, pro=pro, free_end=free_end)
