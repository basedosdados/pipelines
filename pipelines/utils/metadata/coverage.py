"""
CoverageSpec — vindo do backend, não mais hardcoded em constants.py.

Protótipo em avaliação (basedosdados/backend#1110, basedosdados/pipelines#2130):
substitui as 4 classes antigas (`AllFree`/`AllBdpro`/`PartBdpro`/`NonHistorical`
+ `DateColumn`/`YearMonth`/etc., em `domain.py`) por um modelo achatado só,
porque a validação de que a combinação faz sentido agora é feita pelo próprio
backend (`Table.clean()`, olhando `Column.coverage_entity`) — pipelines não
precisa mais revalidar isso, só consumir.

`tier`/`date_column_kind` são `Literal`, não `str` solto: `match` sobre eles
continua coberto por exaustividade pelo `pyrefly`, mesmo sem a hierarquia de
classes antiga.

`bq.py`/`policy.py`/`register.py`/`poll.py` já leem só este formato — `domain.py`
não é mais consumido em nenhum ponto da cadeia de materialização. Ele continua
existindo só porque os datasets ainda não migrados constroem suas constantes
`COVERAGE` hardcoded a partir dele; some quando o último desses migrar (ver
backlog no ftwca).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

from pydantic import BaseModel

from pipelines.utils.metadata.constants import DATE_FORMAT_BY_KIND
from pipelines.utils.metadata.domain import DateFormat

if TYPE_CHECKING:
    from pipelines.utils.metadata.client import MetadataClient

Tier = Literal["all_free", "all_bdpro", "part_bdpro", "non_historical"]
DateColumnKind = Literal[
    "date", "year", "year_month", "year_quarter", "year_bimester", ""
]
CoverageEntity = Literal["date", "year", "month", "quarter", "bimester"]


class CoverageColumn(BaseModel):
    name: str
    entity: CoverageEntity


class CoverageSpec(BaseModel):
    tier: Tier
    date_column_kind: DateColumnKind
    coverage_columns: list[CoverageColumn] = []

    def column(self, entity: CoverageEntity) -> str:
        """Nome da coluna que carrega o papel pedido (ex.: column("year"))."""
        return next(
            c.name for c in self.coverage_columns if c.entity == entity
        )


def coverage_from_backend(
    client: MetadataClient, dataset_id: str, table_id: str
) -> CoverageSpec:
    """Busca o `CoverageSpec` de uma tabela no backend, pronto pra usar.

    Levanta se a tabela não tiver `coverageSpec` cadastrado no backend —
    ausência/inconsistência no metadado é tratada como um problema real a
    corrigir lá, não um estado a contornar aqui (ver `tier=""` pra tabela com
    `coverage_columns` mas sem `Coverage` ainda, que também é repassado como
    está, não escondido).

    Args:
        client: `MetadataClient` já autenticado/configurado pro ambiente certo.
        dataset_id: ID do dataset no GCP/BigQuery.
        table_id: ID da tabela no GCP/BigQuery.

    Returns:
        `CoverageSpec` pronto, com `tier`, `date_column_kind` e
        `coverage_columns` já resolvidos.
    """
    data = client.get_coverage_spec_raw(dataset_id, table_id)
    if data is None:
        raise ValueError(
            f"Backend não tem coverageSpec pra {dataset_id}.{table_id} — "
            "confira se a tabela tem um CloudTable registrado com esse "
            "gcp_dataset_id/gcp_table_id."
        )
    return CoverageSpec(**data)


def date_format_for(kind: DateColumnKind) -> DateFormat:
    """Formato de data (strptime) correspondente a um `date_column_kind`.

    Fixo e não-ambíguo — não vem do backend (seria redundante): dado o
    `date_column_kind`, só existe um `DateFormat` compatível.

    Raises:
        ValueError: `kind == ""`, válido só pra `tier="non_historical"`
            (que não tem `date_column_kind`/`date_format` nenhum).
    """
    if kind == "":
        raise ValueError(
            "date_column_kind vazio não tem date_format correspondente "
            "(só é válido pra tier='non_historical')"
        )
    return DATE_FORMAT_BY_KIND[kind]


def legacy_date_columns(spec: CoverageSpec) -> dict[str, str]:
    """Traduz `coverage_columns` pro dict (`{"date"|"year"/"month"/...: nome}`)
    que as funções de BigQuery em `metadata/utils.py` esperam
    (`extract_last_date_from_bq`, `update_row_access_policy`).

    `date_column_kind` já é os nomes das entities separados por `_`
    (`"year_month"` -> `"year"`, `"month"`) — mesmas chaves que `legacy dict`
    espera e que `CoverageColumn.entity` aceita, daí dar pra montar o dict
    sem repetir um `case` por combinação.

    Raises:
        ValueError: `date_column_kind == ""` (só válido pra
            `tier="non_historical"`, que não chama isto).
    """
    if spec.date_column_kind == "":
        raise ValueError(
            "date_column_kind vazio não tem colunas de data "
            "(só é válido pra tier='non_historical')"
        )
    return {
        # pyrefly: ignore [bad-argument-type]
        entity: spec.column(entity)
        for entity in spec.date_column_kind.split("_")
    }
