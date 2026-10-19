"""
Checagem do schema de um upload contra a tabela externa de staging.

Compara as colunas dos arquivos locais com o schema da staging no BigQuery e
classifica cada divergência por formato: `warning` segue e avisa, `error`
aborta o upload (`SchemaMismatchError`). Usado pelo `upload_to_gcs`.

Organização, de baixo para cima: `Column` e `SourceFormat` (modelo),
`SchemaDiff` (fatos), regras (severidade), `SchemaCheckResult` (resultado),
`LocalDataSchema` (leitura dos arquivos) e os pontos de entrada.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any, NamedTuple, Protocol

import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.utils.utils import bq_safe_column_name

MAX_MESSAGE_CHARS = 1900  # 2000 caracteres do Discord
MAX_COLUMNS_LISTED = 15

_TYPE_ALIASES = {"INTEGER": "INT64", "FLOAT": "FLOAT64", "BOOLEAN": "BOOL"}

_OVERWRITE_NOTE = (
    "A staging foi recriada com o schema do arquivo. Revisar os modelos dbt "
    "dependentes."
)


class SchemaMismatchError(Exception):
    """Classe que define divergência de schema que impede o upload.

    Tipo próprio para que a task possa excluí-lo do retry e a Automation do
    Prefect possa filtrá-lo.
    """


class SourceFormat(StrEnum):
    CSV = "csv"
    PARQUET = "parquet"


### MODELO
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

    @classmethod
    def coerce(cls, value: "ColumnLike") -> "Column":
        if isinstance(value, cls):
            return value
        if isinstance(value, str):
            # pyrefly: ignore [bad-argument-type]
            return cls.of(value)
        # pyrefly: ignore [bad-argument-type]
        # pyrefly: ignore [not-iterable]
        return cls.of(*value)


ColumnLike = Column | str | tuple[str, str | None]


class TypeMismatch(NamedTuple):
    name: str
    expected: str
    source: str


@dataclass(frozen=True)
class SchemaDiff:
    """Fatos da comparação, sem juízo de severidade.

    `final` é o schema esperado depois do sync: a staging atual mais as
    colunas novas, acrescentadas no fim.
    """

    source: tuple[str, ...]
    expected: tuple[str, ...]
    added: tuple[str, ...]
    missing: tuple[str, ...]
    reordered: bool
    type_mismatches: tuple[TypeMismatch, ...]

    @classmethod
    def between(
        cls, source: Sequence[Column], expected: Sequence[Column]
    ) -> "SchemaDiff":
        src = [c.name for c in source]
        exp = [c.name for c in expected]
        src_set, exp_set = set(src), set(exp)
        src_types = {c.name: c.data_type for c in source if c.data_type}
        exp_types = {c.name: c.data_type for c in expected if c.data_type}

        common_in_src = [n for n in src if n in exp_set]
        common_in_exp = [n for n in exp if n in src_set]
        return cls(
            source=tuple(src),
            expected=tuple(exp),
            added=tuple(n for n in src if n not in exp_set),
            missing=tuple(n for n in exp if n not in src_set),
            reordered=common_in_src != common_in_exp,
            type_mismatches=tuple(
                TypeMismatch(n, exp_types[n], src_types[n])
                for n in common_in_src
                if n in exp_types
                and n in src_types
                and exp_types[n] != src_types[n]
            ),
        )

    @property
    def final(self) -> tuple[str, ...]:
        return self.expected + self.added

    @property
    def source_is_prefix_of_final(self) -> bool:
        """Em CSV, coluna `i` do arquivo vai para a coluna `i` da tabela."""
        return self.source == self.final[: len(self.source)]


# ── Regras de severidade ──────────────────────────────────────────────────────


@dataclass
class _Findings:
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def for_overwrite(self) -> "_Findings":
        """No overwrite a staging é recriada, então nada bloqueia."""
        warnings = [
            f"overwrite, staging será recriada: {e}" for e in self.errors
        ] + self.warnings
        if warnings:
            warnings.append(_OVERWRITE_NOTE)
        return _Findings(warnings=warnings)


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
        f"{m.name} ({m.expected} -> {m.source})" for m in diff.type_mismatches
    )
    return f"tipo diferente da staging: {detail}."


def _csv_findings(diff: SchemaDiff) -> _Findings:
    """CSV é posicional: o header precisa ser um prefixo do schema final."""
    findings = _Findings()
    if not diff.source_is_prefix_of_final:
        if diff.reordered:
            findings.errors.append(
                "a ordem das colunas do CSV difere da staging (a tabela é "
                "posicional: os dados cairiam na coluna errada). "
                f"Staging: {_listed(diff.expected)}. "
                f"Arquivo: {_listed(diff.source)}."
            )
        else:
            findings.errors.append(
                "coluna nova ou faltando fora do fim do schema desloca as "
                "colunas seguintes do CSV (tabela posicional). "
                f"Novas: {_listed(diff.added) or '-'}. "
                f"Faltando: {_listed(diff.missing) or '-'}."
            )
        return findings
    if diff.added:
        findings.warnings.append(_added_message(diff))
    if diff.missing:
        findings.errors.append(
            _missing_message(
                diff,
                " O BigQuery recusaria a leitura das linhas curtas "
                "(allow_jagged_rows=False).",
            )
        )
    return findings


def _parquet_findings(diff: SchemaDiff) -> _Findings:
    """Parquet casa por nome: a ordem não afeta os dados, o tipo sim."""
    findings = _Findings()
    if diff.added:
        findings.warnings.append(_added_message(diff))
    if diff.missing:
        findings.errors.append(_missing_message(diff))
    if diff.reordered:
        findings.warnings.append(
            "a ordem das colunas mudou (Parquet casa por nome, sem impacto "
            "nos dados)."
        )
    if diff.type_mismatches:
        findings.errors.append(_type_message(diff))
    return findings


_RULES: dict[SourceFormat, Callable[[SchemaDiff], _Findings]] = {
    SourceFormat.CSV: _csv_findings,
    SourceFormat.PARQUET: _parquet_findings,
}


def _layout_findings(
    staging_keys: tuple[str, ...] | None, local_keys: tuple[str, ...]
) -> _Findings:
    """Layout de partição: o BigQuery exige as mesmas chaves, na mesma ordem.

    `staging_keys=None` significa que não deu para ler as chaves da staging;
    o layout não é verificado, mas o aviso deixa isso visível.
    """
    if staging_keys is None:
        return _Findings(
            warnings=[
                "não foi possível ler as chaves de partição da staging; "
                "layout de partição não verificado."
            ]
        )
    if staging_keys == local_keys:
        return _Findings()
    return _Findings(
        errors=[
            "layout de partição diferente: staging "
            f"[{', '.join(staging_keys) or 'sem partição'}] x upload "
            f"[{', '.join(local_keys) or 'sem partição'}]. O BigQuery exige "
            "as mesmas chaves, na mesma ordem, em todos os arquivos."
        ]
    )


# ── Resultado ─────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class SchemaCheckResult:
    """Fatos (`diff`) e o que fazer com eles (`warnings` e `errors`)."""

    diff: SchemaDiff
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

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


# ── Arquivos locais ───────────────────────────────────────────────────────────

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
class LocalDataSchema:
    """O que os arquivos locais dizem sobre o schema, lido uma única vez.

    Attributes:
        source_format: formato dos arquivos.
        partition_keys: chaves Hive (`chave=valor`) das pastas do upload, na
            ordem em que aparecem e já normalizadas. Vêm só do caminho
            relativo a `data_path`: um `=` numa pasta pai não é chave. As
            chaves não estão no arquivo (`to_partitions` as remove).
        types: tipo BigQuery por coluna normalizada, lido do schema do
            Parquet. O header que a lib usa vira tudo `STRING`, então o tipo
            real só está no arquivo. Vazio em CSV. Acesse por `type_of`.
    """

    source_format: SourceFormat
    partition_keys: tuple[str, ...] = ()
    types: dict[str, str | None] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "source_format", SourceFormat(self.source_format)
        )

    def type_of(self, name: str) -> str | None:
        """Tipo BigQuery da coluna, dado o nome como veio do arquivo."""
        return self.types.get(bq_safe_column_name(name))

    @classmethod
    def read(
        cls, data_path: str | Path, source_format: str | SourceFormat
    ) -> "LocalDataSchema":
        fmt = SourceFormat(source_format)
        file = _first_data_file(data_path, fmt)
        relative = file.relative_to(_data_dir(data_path))
        keys = tuple(
            bq_safe_column_name(part.split("=")[0])
            for part in relative.parts[:-1]
            if "=" in part
        )
        types: dict[str, str | None] = {}
        if fmt is SourceFormat.PARQUET:
            types = {
                bq_safe_column_name(f.name): _arrow_to_bq_type(f.data_type)
                for f in pq.read_schema(file)
            }
        return cls(fmt, keys, types)


# ── Pontos de entrada ─────────────────────────────────────────────────────────


class SchemaFieldLike(Protocol):
    """O que a checagem precisa de um `bigquery.SchemaField`."""

    name: str
    field_type: str


@dataclass(frozen=True)
class StagingSchema:
    """O schema atual da tabela externa de staging.

    Attributes:
        fields: campos de `table.schema`. Em tabelas particionadas, o
            BigQuery inclui as chaves de partição no fim da lista.
        partition_keys: chaves de partição, na ordem do BigQuery (que segue a
            das pastas); `()` se a tabela não é particionada e `None` se não
            foi possível ler.
    """

    fields: tuple[SchemaFieldLike, ...]
    partition_keys: tuple[str, ...] | None = ()

    @classmethod
    def from_table(cls, table: Any) -> "StagingSchema":
        """Lê o schema e as chaves de partição de um `bigquery.Table`.

        As chaves vêm de `partitionDefinition`, que o client Python não
        expõe como atributo público (por isso o `_properties`, o JSON cru da
        API). Ausente, a tabela não é particionada, a menos que
        `hive_partitioning` (público) diga o contrário: aí as chaves são
        desconhecidas, e não vazias.
        """
        properties = getattr(table, "_properties", None) or {}
        definition = properties.get("partitionDefinition")
        if definition:
            keys: tuple[str, ...] | None = tuple(
                bq_safe_column_name(column["field"])
                for column in definition.get("partitionedColumn", [])
            )
        else:
            external = getattr(table, "external_data_configuration", None)
            hive = getattr(external, "hive_partitioning", None)
            keys = None if hive is not None else ()
        return cls(tuple(table.schema), keys)


def _compare(
    source: Sequence[Column],
    expected: Sequence[Column],
    fmt: SourceFormat,
    overwrite: bool,
    ignore: frozenset[str] = frozenset(),
    layout: _Findings | None = None,
) -> SchemaCheckResult:
    diff = SchemaDiff.between(
        [c for c in source if c.name not in ignore],
        [c for c in expected if c.name not in ignore],
    )
    findings = _RULES[fmt](diff)
    if layout:
        findings = _Findings(
            warnings=layout.warnings + findings.warnings,
            errors=layout.errors + findings.errors,
        )
    if overwrite:
        findings = findings.for_overwrite()
    return SchemaCheckResult(diff, findings.warnings, findings.errors)


def check_schema(
    source: Sequence[ColumnLike],
    expected: Sequence[ColumnLike],
    source_format: str | SourceFormat = SourceFormat.CSV,
    overwrite: bool = False,
) -> SchemaCheckResult:
    """Compara as colunas do arquivo com o schema da staging.

    O schema final esperado é a staging atual mais as colunas novas no fim,
    que é como `_sync_staging_schema` as acrescenta.

    - CSV é posicional: o header do arquivo precisa ser um prefixo do schema
      final. Coluna nova só é aceita (aviso) no fim; coluna faltando, ordem
      trocada ou coluna nova no meio é erro.
    - Parquet casa por nome: coluna nova e ordem trocada são avisos. Coluna
      faltando e tipo diferente (quando informado dos dois lados) são erros.
    - `overwrite=True`: a staging é recriada a partir do arquivo, então
      nenhuma divergência bloqueia; os erros viram avisos.

    Args:
        source: colunas do arquivo, como `nome`, `(nome, tipo)` ou `Column`.
        expected: colunas da staging atual, no mesmo formato.
        source_format: `"csv"` ou `"parquet"`.
        overwrite: se o upload recria a staging.

    Returns:
        O resultado, sem levantar exceção. Use `raise_if_errors()`.

    Raises:
        ValueError: `source_format` inválido.
    """
    return _compare(
        [Column.coerce(c) for c in source],
        [Column.coerce(c) for c in expected],
        SourceFormat(source_format),
        overwrite,
    )


def check_staging_schema(
    staging: StagingSchema,
    incoming: Sequence[SchemaFieldLike],
    local: LocalDataSchema,
    overwrite: bool = False,
) -> SchemaCheckResult:
    """Compara as colunas do upload com o schema atual da staging.

    Além das regras de `check_schema`, compara o layout de partição do
    upload com o da staging. As chaves de partição ficam fora da comparação
    de colunas, dos dois lados, e os tipos só entram em Parquet.

    Args:
        staging: schema atual da staging (`StagingSchema.from_table`).
        incoming: campos inferidos do header do arquivo.
        local: o que os arquivos locais dizem sobre o schema.
        overwrite: se o upload recria a staging.
    """
    parquet = local.source_format is SourceFormat.PARQUET
    return _compare(
        [
            Column.of(f.name, local.type_of(f.name) if parquet else None)
            for f in incoming
        ],
        [
            Column.of(f.name, f.field_type if parquet else None)
            for f in staging.fields
        ],
        local.source_format,
        overwrite,
        ignore=frozenset(local.partition_keys)
        | frozenset(staging.partition_keys or ()),
        layout=_layout_findings(staging.partition_keys, local.partition_keys),
    )


def schema_title(
    dataset_id: str, table_id: str, source_format: str, dump_mode: str
) -> str:
    return (
        f"Schema da staging {dataset_id}.{table_id} "
        f"({source_format}, {dump_mode})"
    )
