"""
Checagem do schema de um upload contra a tabela externa de staging.

Compara as colunas dos arquivos locais com o schema da staging no BigQuery e
classifica cada divergência por formato: `warning` segue e avisa, `error`
aborta o upload (`SchemaMismatchError`). Usado pelo `upload_to_gcs`.

Organização, de baixo para cima: modelo (`Column`, `TableSchema`, `Finding`),
`SchemaDiff` (fatos), regras (severidade), `SchemaCheckResult` (resultado),
leitura dos schemas (arquivos locais e staging) e `check`, o ponto de entrada.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import NamedTuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import bigquery

from pipelines.utils.utils import bq_safe_column_name

MAX_MESSAGE_CHARS = 1900  # 2000 caracteres do Discord
MAX_COLUMNS_LISTED = 15

_TYPE_ALIASES = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}

_OVERWRITE_NOTE = (
    "A staging foi recriada com o schema do arquivo. Revisar os modelos dbt "
    "dependentes."
)

_ARROW_TO_BQ: tuple[tuple[Callable[[pa.DataType], bool], str], ...] = (
    (pa.types.is_boolean, "BOOL"),
    (pa.types.is_integer, "INT64"),
    (pa.types.is_floating, "FLOAT64"),
    (pa.types.is_decimal, "NUMERIC"),
    (pa.types.is_string, "STRING"),
    (pa.types.is_large_string, "STRING"),
    (pa.types.is_binary, "BYTES"),
    (pa.types.is_large_binary, "BYTES"),
    (pa.types.is_date, "DATE"),
    (pa.types.is_time, "TIME"),
)


class SchemaMismatchError(Exception):
    """Classe que define divergência de schema que impede o upload.

    Tipo próprio para que a task possa excluí-lo do retry e a Automation do
    Prefect possa filtrá-lo.
    """


class SourceFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


# ── Modelo ────────────────────────────────────────────────────────────────────


class Severity(StrEnum):
    WARNING = "warning"
    ERROR = "error"


@dataclass(frozen=True)
class Finding:
    severity: Severity
    message: str


def _warning(message: str) -> Finding:
    return Finding(Severity.WARNING, message)


def _error(message: str) -> Finding:
    return Finding(Severity.ERROR, message)


@dataclass(frozen=True)
class Column:
    """Coluna com nome e tipo já normalizados.

    O tipo perde os aliases (`INTEGER` vira `INT64`).
    """

    name: str
    data_type: str | None = None

    @classmethod
    def of(cls, name: str, data_type: str | None = None) -> "Column":
        normalized = (
            _TYPE_ALIASES.get(data_type.upper(), data_type.upper())
            if data_type
            else None
        )
        return cls(bq_safe_column_name(name), normalized)


@dataclass(frozen=True)
class TableSchema:
    """Schema de um lado da comparação: a staging ou o upload.

    Attributes:
        columns: colunas sem as chaves de partição, que o BigQuery inclui
            no fim de `table.schema` mas que não existem dentro dos arquivos.
        partition_keys: chaves Hive, em ordem; `()` se não há partição e
            `None` se não foi possível ler.
    """

    columns: tuple[Column, ...]
    partition_keys: tuple[str, ...] | None = ()


# ── Leitura dos schemas ───────────────────────────────────────────────────────
def _arrow_to_bq_type(arrow_type: pa.DataType) -> str | None:
    """Mapeia um tipo do pyarrow para o tipo do BigQuery.

    Tipos sem correspondência clara (listas, structs, dicionários) devolvem
    `None` e ficam fora da checagem de tipo, em vez de gerar falso positivo.
    """
    if pa.types.is_timestamp(arrow_type):
        return "TIMESTAMP" if arrow_type.tz else "DATETIME"
    for is_type, bq_type in _ARROW_TO_BQ:
        if is_type(arrow_type):
            return bq_type
    return None


def _data_dir(data_path: str | Path) -> Path:
    base = Path(data_path)
    return base if base.is_dir() else base.parent


def _first_data_file(data_path: str | Path, fmt: SourceFormat) -> Path:
    """Primeiro arquivo do formato em `data_path`, na ordem do `dump_header`."""
    base = _data_dir(data_path)
    for subdir, _, filenames in os.walk(base):
        for fname in filenames:
            if fname.endswith(f".{fmt.value}"):
                return Path(subdir) / fname
    raise FileNotFoundError(f"Nenhum arquivo {fmt.value} encontrado em {base}")


@dataclass(frozen=True)
class UploadSchema(TableSchema):
    """Schema dos arquivos locais que serão carregados."""

    @classmethod
    def from_files(
        cls, data_path: str | Path, source_format: str | SourceFormat
    ) -> "UploadSchema":
        """Lê o header e as chaves de partição do primeiro arquivo.

        As chaves vêm só das pastas `chave=valor` relativas a `data_path`
        (`to_partitions` as remove do arquivo). Em Parquet, o tipo é o do
        arquivo; em CSV não há tipo (a tabela é toda `STRING`).
        """
        fmt = SourceFormat(source_format)
        file = _first_data_file(data_path, fmt)
        keys = tuple(
            bq_safe_column_name(part.split("=")[0])
            for part in file.relative_to(_data_dir(data_path)).parts[:-1]
            if "=" in part
        )
        if fmt is SourceFormat.PARQUET:
            columns = tuple(
                Column.of(f.name, _arrow_to_bq_type(f.type))
                for f in pq.read_schema(file)
            )
        else:
            names = pd.read_csv(file, nrows=0, dtype=str).columns
            columns = tuple(Column.of(name) for name in names)
        return cls(columns, keys)


@dataclass(frozen=True)
class StagingSchema(TableSchema):
    """Schema atual da tabela externa de staging."""

    @classmethod
    def from_table(cls, table: bigquery.Table) -> "StagingSchema":
        """Lê o schema e as chaves de partição de um `bigquery.Table`.

        `table.schema` inclui as chaves de partição no fim; elas saem de
        `columns`. As chaves vêm de `partitionDefinition`, que o client não
        expõe como atributo público (daí o `_properties`). Ausente, a tabela
        não é particionada, a menos que `hive_partitioning` (público) diga o
        contrário: aí as chaves são `None` (desconhecidas), e não `()`.
        """
        definition = table._properties.get("partitionDefinition")
        if definition:
            keys = tuple(
                bq_safe_column_name(c["field"])
                for c in definition.get("partitionedColumn", [])
            )
        else:
            external = table.external_data_configuration
            hive = getattr(external, "hive_partitioning", None)
            keys = None if hive is not None else ()
        columns = tuple(Column.of(f.name, f.field_type) for f in table.schema)
        return cls(
            tuple(c for c in columns if c.name not in (keys or ())), keys
        )


class TypeMismatch(NamedTuple):
    name: str
    staging: str
    upload: str


@dataclass(frozen=True)
class SchemaDiff:
    """Comparação entre o upload e a staging, sem juízo de severidade.

    Attributes:
        upload: nomes das colunas dos arquivos locais, na ordem do arquivo.
        staging: nomes das colunas da staging, na ordem da tabela.
        added: colunas que o upload traz e a staging não tem.
        missing: colunas que a staging tem e o upload não traz.
        reordered: as colunas em comum aparecem em ordem diferente.
        type_mismatches: colunas em comum com tipo diferente (só quando o
            tipo é conhecido dos dois lados).
        upload_keys: chaves de partição do upload (`None`: não lidas).
        staging_keys: chaves de partição da staging (`None`: não lidas).
    """

    upload: tuple[str, ...]
    staging: tuple[str, ...]
    added: tuple[str, ...]
    missing: tuple[str, ...]
    reordered: bool
    type_mismatches: tuple[TypeMismatch, ...]
    upload_keys: tuple[str, ...] | None
    staging_keys: tuple[str, ...] | None

    @classmethod
    def between(
        cls, staging: StagingSchema, upload: UploadSchema
    ) -> "SchemaDiff":
        up = [c.name for c in upload.columns]
        st = [c.name for c in staging.columns]
        up_set, st_set = set(up), set(st)
        up_types = {c.name: c.data_type for c in upload.columns if c.data_type}
        st_types = {
            c.name: c.data_type for c in staging.columns if c.data_type
        }

        common_in_up = [name for name in up if name in st_set]
        common_in_st = [name for name in st if name in up_set]
        return cls(
            upload=tuple(up),
            staging=tuple(st),
            added=tuple(name for name in up if name not in st_set),
            missing=tuple(name for name in st if name not in up_set),
            reordered=common_in_up != common_in_st,
            type_mismatches=tuple(
                TypeMismatch(name, st_types[name], up_types[name])
                for name in common_in_up
                if name in st_types
                and name in up_types
                and st_types[name] != up_types[name]
            ),
            upload_keys=upload.partition_keys,
            staging_keys=staging.partition_keys,
        )

    @property
    def final(self) -> tuple[str, ...]:
        """Schema esperado depois do sync: a staging mais as colunas novas."""
        return self.staging + self.added

    @property
    def layout_unknown(self) -> bool:
        """`None` é "não deu para ler", não "igual": o layout não é verificado."""
        return self.upload_keys is None or self.staging_keys is None

    @property
    def layout_changed(self) -> bool:
        return (
            not self.layout_unknown and self.upload_keys != self.staging_keys
        )

    @property
    def upload_is_prefix_of_final(self) -> bool:
        """Em CSV, coluna `i` do arquivo vai para a coluna `i` da tabela."""
        return self.upload == self.final[: len(self.upload)]


# ── Regras de severidade ──────────────────────────────────────────────────────
def _csv_findings(diff: SchemaDiff) -> list[Finding]:
    """CSV é posicional: o header precisa ser um prefixo do schema final."""
    if not diff.upload_is_prefix_of_final:
        if diff.reordered:
            return [
                _error(
                    "a ordem das colunas do CSV difere da staging (a tabela "
                    "é posicional: os dados cairiam na coluna errada). "
                    f"Staging: {_listed(diff.staging)}. "
                    f"Arquivo: {_listed(diff.upload)}."
                )
            ]
        return [
            _error(
                "coluna nova ou faltando fora do fim do schema desloca as "
                "colunas seguintes do CSV (tabela posicional). "
                f"Novas: {_listed(diff.added) or '-'}. "
                f"Faltando: {_listed(diff.missing) or '-'}."
            )
        ]
    findings = []
    if diff.added:
        findings.append(_warning(_added_message(diff)))
    if diff.missing:
        findings.append(
            _error(
                _missing_message(
                    diff,
                    " O BigQuery recusaria a leitura das linhas curtas "
                    "(allow_jagged_rows=False).",
                )
            )
        )
    return findings


def _parquet_findings(diff: SchemaDiff) -> list[Finding]:
    """Parquet casa por nome: a ordem não afeta os dados, o tipo sim."""
    findings = []
    if diff.added:
        findings.append(_warning(_added_message(diff)))
    if diff.missing:
        findings.append(_error(_missing_message(diff)))
    if diff.reordered:
        findings.append(
            _warning(
                "a ordem das colunas mudou (Parquet casa por nome, sem "
                "impacto nos dados)."
            )
        )
    if diff.type_mismatches:
        findings.append(_error(_type_message(diff)))
    return findings


def _layout_findings(diff: SchemaDiff) -> list[Finding]:
    """Layout de partição: o BigQuery exige as mesmas chaves, na mesma ordem.

    Chaves `None` (não deu para ler) não são verificadas, mas o aviso deixa
    isso visível.
    """
    if diff.layout_unknown:
        return [
            _warning(
                "não foi possível ler as chaves de partição da staging ou do "
                "upload; layout de partição não verificado."
            )
        ]
    if diff.layout_changed:
        staging = ", ".join(diff.staging_keys or ()) or "sem partição"
        upload = ", ".join(diff.upload_keys or ()) or "sem partição"
        return [
            _error(
                f"layout de partição diferente: staging [{staging}] x upload "
                f"[{upload}]. O BigQuery exige as mesmas chaves, na mesma "
                "ordem, em todos os arquivos."
            )
        ]
    return []


def _for_overwrite(findings: list[Finding]) -> list[Finding]:
    """No overwrite a staging é recriada, então nada bloqueia."""
    demoted = [
        _warning(f"overwrite, staging será recriada: {f.message}")
        for f in findings
        if f.severity is Severity.ERROR
    ]
    kept = [f for f in findings if f.severity is Severity.WARNING]
    result = demoted + kept
    return [*result, _warning(_OVERWRITE_NOTE)] if result else result


def _listed(columns: Sequence[str]) -> str:
    shown = ", ".join(columns[:MAX_COLUMNS_LISTED])
    extra = len(columns) - MAX_COLUMNS_LISTED
    return f"{shown} (+{extra})" if extra > 0 else shown


def _added_message(diff: SchemaDiff) -> str:
    return f"colunas novas acrescentadas à staging: {_listed(diff.added)}."


def _missing_message(diff: SchemaDiff, hint: str = "") -> str:
    return f"colunas ausentes no arquivo: {_listed(diff.missing)}.{hint}"


def _type_message(diff: SchemaDiff) -> str:
    detail = ", ".join(
        f"{m.name} ({m.staging} -> {m.upload})" for m in diff.type_mismatches
    )
    return f"tipo diferente da staging: {detail}."


_RULES: dict[SourceFormat, Callable[[SchemaDiff], list[Finding]]] = {
    SourceFormat.CSV: _csv_findings,
    SourceFormat.PARQUET: _parquet_findings,
}


# ── Resultado ─────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class SchemaCheckResult:
    """O que a checagem achou e as colunas que o sync deve acrescentar.

    Attributes:
        findings: avisos e erros, na ordem em que as regras os geraram.
        added_columns: colunas do upload ausentes na staging, já normalizadas
            e com o tipo do arquivo (`None` em CSV).
    """

    findings: tuple[Finding, ...] = ()
    added_columns: tuple[Column, ...] = ()

    @property
    def warnings(self) -> list[str]:
        return [
            f.message for f in self.findings if f.severity is Severity.WARNING
        ]

    @property
    def errors(self) -> list[str]:
        return [
            f.message for f in self.findings if f.severity is Severity.ERROR
        ]

    @property
    def has_changes(self) -> bool:
        return bool(self.warnings or self.errors)

    def render(self, title: str | None = None) -> str:
        """Mensagem única, truncada para caber em um webhook."""
        lines = [title or "Mudança de schema em staging"]
        lines += [f"ERRO: {msg}" for msg in self.errors]
        lines += [f"AVISO: {msg}" for msg in self.warnings]
        text = "\n".join(lines)
        if len(text) > MAX_MESSAGE_CHARS:
            text = text[: MAX_MESSAGE_CHARS - 3] + "..."
        return text

    def raise_if_errors(self, title: str | None = None) -> None:
        if self.errors:
            raise SchemaMismatchError(self.render(title))


# ── Ponto de entrada ──────────────────────────────────────────────────────────


def check(
    staging: StagingSchema,
    upload: UploadSchema,
    source_format: str | SourceFormat,
    overwrite: bool = False,
) -> SchemaCheckResult:
    """Compara o upload com a staging atual.

    O schema final esperado é a staging mais as colunas novas no fim, que é
    como o sync as acrescenta.

    - CSV é posicional: o header precisa ser prefixo do schema final. Coluna
      nova no fim é aviso; faltando, ordem trocada ou nova no meio é erro.
    - Parquet casa por nome: coluna nova e ordem trocada são avisos; coluna
      faltando e tipo diferente (quando informado dos dois lados) são erros.
    - Layout de partição diferente é erro.
    - `overwrite=True`: a staging é recriada a partir do arquivo, então nada
      bloqueia; os erros viram avisos.

    Args:
        staging: schema atual da staging.
        upload: schema dos arquivos que serão carregados.
        source_format: `"csv"` ou `"parquet"`.
        overwrite: se o upload recria a staging.

    Returns:
        O resultado, sem levantar exceção. Use `raise_if_errors()`.

    Raises:
        ValueError: `source_format` inválido.
    """
    fmt = SourceFormat(source_format)
    diff = SchemaDiff.between(staging, upload)
    findings = _layout_findings(diff) + _RULES[fmt](diff)
    if overwrite:
        findings = _for_overwrite(findings)
    added = set(diff.added)
    return SchemaCheckResult(
        tuple(findings),
        tuple(c for c in upload.columns if c.name in added),
    )


def schema_title(
    dataset_id: str, table_id: str, source_format: str, dump_mode: str
) -> str:
    return (
        f"Schema da staging {dataset_id}.{table_id} "
        f"({source_format}, {dump_mode})"
    )
