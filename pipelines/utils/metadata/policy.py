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

from dateutil.relativedelta import relativedelta
from pydantic import BaseModel

from pipelines.utils.metadata.coverage import CoverageSpec, date_format_for
from pipelines.utils.metadata.domain import DateFormat
from pipelines.utils.metadata.dto import DateTimeRangeInput

# Generalizado pra todos os datasets, por decisão explícita do time — não vem
# mais do backend nem é configurável por tabela (ver pipelines#2130). Alguns
# datasets ativos tinham um `free_lag` diferente (`br_anp_precos_combustiveis`:
# 6 semanas; `br_cgu_emendas_parlamentares`: 1 ano) — comunicar à equipe antes
# desta regra valer pra eles.
PART_BDPRO_FREE_LAG = relativedelta(months=6)


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
    return spec.tier == "part_bdpro"


# ------------------------------------------- topologia de coberturas no backend
def assert_coverage_topology(spec: CoverageSpec, found: CoverageIds) -> None:
    """A topologia de coberturas existentes no backend deve bater com o tier
    pedido."""
    match spec.tier:
        case "part_bdpro":
            if not (found.free and found.pro):
                raise ValueError(
                    f"part_bdpro exige Coverage free + pro; encontrado: {found!r}"
                )
        case "all_free":
            if not found.free or found.pro:
                raise ValueError(
                    "all_free exige Coverage free e ausência de pro; "
                    f"encontrado: {found!r}"
                )
        case "all_bdpro":
            if not found.pro or found.free:
                raise ValueError(
                    "all_bdpro exige Coverage pro e ausência de free; "
                    f"encontrado: {found!r}"
                )
        case "non_historical":
            pass


# ----------------------------------------------- cálculo dos DateTimeRange
def _components(d: date, fmt: DateFormat, position: str) -> dict:
    """Decompõe uma data nos campos do DTO conforme a granularidade do formato."""
    out = {f"{position}Year": d.year}
    if fmt in (DateFormat.YEAR_MONTH, DateFormat.YEAR_MD):
        out[f"{position}Month"] = d.month
    if fmt == DateFormat.YEAR_MD:
        out[f"{position}Day"] = d.day
    return out


def _next_period(d: date, fmt: DateFormat) -> date:
    """Avança `d` em um período, na granularidade do formato.

    A cobertura free termina em `free_end` **inclusive** — a Row Access Policy
    concede `allUsers` FILTER USING (date <= free_end). A cobertura pro portanto
    começa no período seguinte; começar no próprio `free_end` faria as duas
    coberturas reivindicarem o mesmo período e exageraria o paywall.
    """
    step = {
        DateFormat.YEAR: relativedelta(years=1),
        DateFormat.YEAR_MONTH: relativedelta(months=1),
        DateFormat.YEAR_MD: relativedelta(days=1),
    }[fmt]
    return d + step


def compute_coverage_ranges(
    spec: CoverageSpec, source_end: date, coverage_ids: CoverageIds
) -> CoverageRanges:
    """Calcula os DateTimeRange free e/ou pro.

    - all_free  → range free terminando em `source_end`.
    - all_bdpro → range pro terminando em `source_end`.
    - part_bdpro→ pro termina em `source_end`; free termina em
      `source_end - PART_BDPRO_FREE_LAG`; pro começa onde a free termina.
    """
    if spec.tier == "non_historical":
        raise ValueError("NonHistorical não usa compute_coverage_ranges")

    fmt = date_format_for(spec.date_column_kind)

    if spec.tier == "all_free":
        return CoverageRanges(
            free=DateTimeRangeInput(
                # pyrefly: ignore [bad-argument-type]
                coverage=coverage_ids.free,
                **_components(source_end, fmt, "end"),
            ),
            free_end=source_end,
        )

    if spec.tier == "all_bdpro":
        return CoverageRanges(
            pro=DateTimeRangeInput(
                # pyrefly: ignore [bad-argument-type]
                coverage=coverage_ids.pro,
                **_components(source_end, fmt, "end"),
            )
        )

    # part_bdpro
    free_end = source_end - PART_BDPRO_FREE_LAG
    free = DateTimeRangeInput(
        # pyrefly: ignore [bad-argument-type]
        coverage=coverage_ids.free,
        **_components(free_end, fmt, "end"),
    )
    pro = DateTimeRangeInput(
        # pyrefly: ignore [bad-argument-type]
        coverage=coverage_ids.pro,
        **_components(source_end, fmt, "end"),
        # free termina em free_end inclusive, então pro começa no período
        # seguinte: as coberturas são mutuamente exclusivas.
        **_components(_next_period(free_end, fmt), fmt, "start"),
    )
    return CoverageRanges(free=free, pro=pro, free_end=free_end)
