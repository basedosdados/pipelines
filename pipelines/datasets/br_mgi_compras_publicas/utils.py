"""Pure download and cleaning functions for br_mgi_compras_publicas.

No Prefect imports: the one-shot onboarding driver under
``models/br_mgi_compras_publicas/code/`` and the recurring flow both import from
here, so the transform exists in exactly one place.
"""

from __future__ import annotations

import csv
import datetime as dt
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from pipelines.datasets.br_mgi_compras_publicas.constants import (
    ARCHITECTURE_DIR,
    WindowKind,
    constants,
)

logger = logging.getLogger(__name__)

ARROW_TYPES = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "BOOLEAN": pa.bool_(),
    "DATE": pa.date32(),
    "DATETIME": pa.timestamp("us"),
}


# --------------------------------------------------------------------------
# Harvest specs
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class TableSpec:
    """How one table is harvested and how its partition column is derived."""

    table: str
    path: str
    window: WindowKind
    #: names of the two date parameters, for date-windowed endpoints
    date_params: tuple[str, str] | None = None
    #: name of the single year parameter, for YEAR endpoints
    year_param: str | None = None
    #: source field holding the date the partition year comes from
    year_field: str | None = None
    #: fixed parameters sent on every request
    params: dict[str, Any] = field(default_factory=dict)
    #: values iterated over `codigoModalidade` (14.133) or `modalidade` (legado)
    modalidades: tuple[int, ...] = ()
    modalidade_param: str = "codigoModalidade"
    #: name of the orgao parameter, for YEAR_ORGAO endpoints
    orgao_param: str = "co_orgao"
    #: snapshot endpoints iterate one parameter over a fixed set of values
    snapshot_param: str | None = None
    snapshot_values: tuple[str, ...] = ()
    #: days per window; never above the API's 365-day cap
    step_days: int = 30
    first_year: int | None = None
    last_year: int | None = None

    @property
    def module(self) -> str:
        return self.path.strip("/").split("/", 1)[0]


C = constants
_A = C.ANO_INICIO_14133.value
_L0 = C.ANO_INICIO_LEGADO.value
_L1 = C.ANO_FIM_LEGADO.value

TABLE_SPECS: dict[str, TableSpec] = {
    # ---- Lei 14.133/2021 ------------------------------------------------
    "contratacao": TableSpec(
        table="contratacao",
        path="/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
        window=WindowKind.HALF_OPEN,
        date_params=("dataPublicacaoPncpInicial", "dataPublicacaoPncpFinal"),
        year_field="dataPublicacaoPncp",
        modalidades=C.MODALIDADES_14133.value,
        step_days=30,
        first_year=_A,
    ),
    "contratacao_item": TableSpec(
        table="contratacao_item",
        path="/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133",
        window=WindowKind.HALF_OPEN,
        date_params=("dataInclusaoPncpInicial", "dataInclusaoPncpFinal"),
        year_field="dataInclusaoPncp",
        step_days=3,
        first_year=_A,
    ),
    "contratacao_item_resultado": TableSpec(
        table="contratacao_item_resultado",
        path="/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133",
        window=WindowKind.HALF_OPEN,
        date_params=("dataResultadoPncpInicial", "dataResultadoPncpFinal"),
        year_field="dataResultadoPncp",
        step_days=3,
        first_year=_A,
    ),
    "ata_registro_preco": TableSpec(
        table="ata_registro_preco",
        path="/modulo-arp/1_consultarARP",
        window=WindowKind.HALF_OPEN,
        date_params=("dataVigenciaInicialMin", "dataVigenciaInicialMax"),
        year_field="dataVigenciaInicial",
        step_days=15,
        first_year=2023,
        last_year=2027,
    ),
    "ata_registro_preco_item": TableSpec(
        table="ata_registro_preco_item",
        path="/modulo-arp/2_consultarARPItem",
        window=WindowKind.HALF_OPEN,
        date_params=("dataVigenciaInicialMin", "dataVigenciaInicialMax"),
        year_field="dataVigenciaInicial",
        step_days=5,
        first_year=2023,
        last_year=2027,
    ),
    # ---- Contratos (per-orgao; heavily rate limited) ---------------------
    "contrato": TableSpec(
        table="contrato",
        path="/modulo-contratos/1_consultarContratos",
        window=WindowKind.ORGAO,
        date_params=("dataVigenciaInicialMin", "dataVigenciaInicialMax"),
        year_field="dataVigenciaInicial",
        first_year=2010,
    ),
    "contrato_item": TableSpec(
        table="contrato_item",
        path="/modulo-contratos/2_consultarContratosItem",
        window=WindowKind.ORGAO,
        date_params=("dataVigenciaInicialMin", "dataVigenciaInicialMax"),
        year_field="dataVigenciaInicial",
        first_year=2010,
    ),
    # ---- Legado (Lei 8.666) ---------------------------------------------
    "licitacao": TableSpec(
        table="licitacao",
        path="/modulo-legado/1_consultarLicitacao",
        window=WindowKind.CLOSED,
        date_params=("data_publicacao_inicial", "data_publicacao_final"),
        year_field="data_publicacao",
        step_days=30,
        first_year=_L0,
        last_year=_L1,
    ),
    "licitacao_pregao": TableSpec(
        table="licitacao_pregao",
        path="/modulo-legado/3_consultarPregoes",
        window=WindowKind.CLOSED,
        date_params=("dt_data_edital_inicial", "dt_data_edital_final"),
        year_field="dt_data_edital",
        step_days=30,
        first_year=2000,
        last_year=_L1,
    ),
    "licitacao_item_pregao": TableSpec(
        table="licitacao_item_pregao",
        path="/modulo-legado/4_consultarItensPregoes",
        window=WindowKind.CLOSED,
        date_params=("dt_hom_inicial", "dt_hom_final"),
        year_field="dt_hom",
        step_days=5,
        first_year=2000,
        last_year=_L1,
    ),
    "licitacao_item": TableSpec(
        table="licitacao_item",
        # No date filter of any kind exists on this endpoint: the only partition
        # key is `modalidade`. Tier A takes the six modalidades absent from the
        # date-partitioned siblings; 5, 6 and 7 are tier B. See PLAN.md.
        path="/modulo-legado/2_consultarItemLicitacao",
        window=WindowKind.MODALIDADE,
        modalidades=C.MODALIDADES_LEGADO_TIER_A.value,
        modalidade_param="modalidade",
        first_year=_L0,
        last_year=_L1,
    ),
    "compra_sem_licitacao": TableSpec(
        table="compra_sem_licitacao",
        path="/modulo-legado/5_consultarComprasSemLicitacao",
        window=WindowKind.YEAR,
        year_param="dt_ano_aviso",
        first_year=_L0,
        last_year=_L1,
    ),
    "compra_sem_licitacao_item": TableSpec(
        table="compra_sem_licitacao_item",
        # Partitioned by year AND orgao, unlike its parent. A year-only query
        # paginates far too deep to be affordable: this endpoint rescans on
        # OFFSET, so page 1 costs 3s and page 2000 costs 37s, and 2002 alone is
        # 2,541 pages. Splitting by the (year, orgao) pairs already present in
        # the harvested parent keeps 10,209 of 10,242 partitions under 100
        # pages, where latency is flat. Measured: 81.7h of serial work becomes
        # about 34.8h. See PLAN.md.
        path="/modulo-legado/6_consultarCompraItensSemLicitacao",
        window=WindowKind.YEAR_ORGAO,
        year_param="dt_ano_aviso_licitacao",
        orgao_param="co_orgao",
        first_year=_L0,
        last_year=_L1,
    ),
    # ---- Registries and catalogues --------------------------------------
    "orgao": TableSpec(
        table="orgao",
        path="/modulo-uasg/2_consultarOrgao",
        window=WindowKind.SNAPSHOT,
        snapshot_param="statusOrgao",
        snapshot_values=("true", "false"),
    ),
    "unidade_administrativa": TableSpec(
        table="unidade_administrativa",
        path="/modulo-uasg/1_consultarUasg",
        window=WindowKind.SNAPSHOT,
        snapshot_param="statusUasg",
        snapshot_values=("true", "false"),
    ),
    "fornecedor": TableSpec(
        table="fornecedor",
        path="/modulo-fornecedor/1_consultarFornecedor",
        window=WindowKind.SNAPSHOT,
        snapshot_param="ativo",
        snapshot_values=("true", "false"),
    ),
    "catalogo_material": TableSpec(
        table="catalogo_material",
        path="/modulo-material/4_consultarItemMaterial",
        window=WindowKind.SNAPSHOT,
    ),
    "catalogo_servico": TableSpec(
        table="catalogo_servico",
        path="/modulo-servico/6_consultarItemServico",
        window=WindowKind.SNAPSHOT,
    ),
}


# --------------------------------------------------------------------------
# Architecture
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Column:
    name: str
    bigquery_type: str
    original_name: str


def load_architecture(
    table: str, architecture_dir: Path | None = None
) -> list[Column]:
    """Read a table's architecture CSV: the authority on column order and type."""
    path = (architecture_dir or ARCHITECTURE_DIR) / f"{table}.csv"
    with path.open(encoding="utf-8") as fh:
        return [
            Column(r["name"], r["bigquery_type"], r["original_name"])
            for r in csv.DictReader(fh)
        ]


# --------------------------------------------------------------------------
# Value coercion
# --------------------------------------------------------------------------

_TRUE = {"true", "t", "1", "sim", "s", "yes", "y"}
_FALSE = {"false", "f", "0", "nao", "não", "n", "no"}
_ID_YEAR = re.compile(r"(\d{4})$")


def _blank(value: Any) -> bool:
    return value is None or (isinstance(value, str) and not value.strip())


def to_int(value: Any) -> int | None:
    if _blank(value):
        return None
    if isinstance(value, bool):
        return int(value)
    try:
        return int(float(str(value).strip().replace(",", ".")))
    except (TypeError, ValueError):
        return None


def to_float(value: Any) -> float | None:
    if _blank(value):
        return None
    try:
        return float(str(value).strip().replace(",", "."))
    except (TypeError, ValueError):
        return None


def to_bool(value: Any) -> bool | None:
    if _blank(value):
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in _TRUE:
        return True
    if token in _FALSE:
        return False
    return None


def to_date(value: Any) -> dt.date | None:
    stamp = to_datetime(value)
    return stamp.date() if stamp else None


def to_datetime(value: Any) -> dt.datetime | None:
    """Parse the several timestamp shapes this API emits.

    Seen in the wild: ``2025-06-01T10:54:43``, ``2025-06-03``,
    ``2025-06-01 00:00:00.0000000`` (seven fractional digits, which
    ``fromisoformat`` rejects) and ``1999-07-15T03:00:00.000+00:00``.
    """
    if _blank(value):
        return None
    if isinstance(value, dt.datetime):
        return value
    if isinstance(value, dt.date):
        return dt.datetime(value.year, value.month, value.day)

    text = str(value).strip().replace(" ", "T", 1)
    # Python accepts at most 6 fractional digits.
    text = re.sub(r"\.(\d{6})\d+", r".\1", text)
    text = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        for pattern in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                parsed = dt.datetime.strptime(text, pattern)
                break
            except ValueError:
                continue
        else:
            return None
    return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed


def to_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        # Codes arrive as JSON numbers; "05" -> 5.0 must not become "5.0".
        return str(int(value))
    text = str(value).strip()
    return text or None


COERCERS = {
    "STRING": to_string,
    "INT64": to_int,
    "FLOAT64": to_float,
    "BOOLEAN": to_bool,
    "DATE": to_date,
    "DATETIME": to_datetime,
}


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------


def partition_year(
    spec: TableSpec, raw: dict[str, Any], fallback: int | None = None
) -> int | None:
    """Derive the `ano` partition value for one record.

    Most tables read it from a date field. `licitacao_item` has no date at all,
    so the year is taken from the trailing four digits of `id_compra`
    (``15830305000062015`` -> 2015), which is how SIASG composes that key.
    """
    if spec.year_field:
        stamp = to_datetime(raw.get(spec.year_field))
        if stamp:
            return stamp.year
    if spec.table == "licitacao_item":
        match = _ID_YEAR.search(str(raw.get("id_compra") or ""))
        if match:
            year = int(match.group(1))
            if 1990 <= year <= 2100:
                return year
    return fallback


def clean_records(
    spec: TableSpec,
    raw_rows: list[dict[str, Any]],
    columns: list[Column],
    *,
    year_fallback: int | None = None,
    extraction_date: dt.date | None = None,
) -> list[dict[str, Any]]:
    """Map raw API records onto the architecture's columns, coercing each type."""
    cleaned: list[dict[str, Any]] = []
    for raw in raw_rows:
        row: dict[str, Any] = {}
        for column in columns:
            if column.name == "ano" and not column.original_name:
                row["ano"] = partition_year(spec, raw, year_fallback)
                continue
            if column.name == "data_extracao" and not column.original_name:
                row["data_extracao"] = extraction_date or dt.date.today()
                continue
            row[column.name] = COERCERS[column.bigquery_type](
                raw.get(column.original_name)
            )
        cleaned.append(row)
    return cleaned


# --------------------------------------------------------------------------
# Parquet
# --------------------------------------------------------------------------


def string_schema(columns: list[Column]) -> pa.Schema:
    """All-STRING schema, in architecture order.

    Staging is all-STRING by house convention and `gcs.py::dump_header`
    stringifies the parquet header anyway, so a typed staging table would
    collide with the pipeline's later overwrite. The dbt model `safe_cast`s each
    column back to its real type.
    """
    return pa.schema([(column.name, pa.string()) for column in columns])


def to_string_table(
    rows: list[dict[str, Any]], columns: list[Column]
) -> pa.Table:
    """Build an all-STRING arrow table, casting through each column's real type.

    Casting via arrow rather than ``astype(str)`` matters twice over: ``astype``
    renders NULL as the literal ``"nan"``, which ``safe_cast`` will not turn back
    into NULL, and going through the real type first keeps ``1959`` from
    serialising as ``"1959.0"``.
    """
    arrays = []
    for column in columns:
        values = [row.get(column.name) for row in rows]
        typed = pa.array(values, type=ARROW_TYPES[column.bigquery_type])
        arrays.append(pc.cast(typed, pa.string()))
    return pa.Table.from_arrays(arrays, schema=string_schema(columns))


def write_chunk(
    rows: list[dict[str, Any]], columns: list[Column], path: Path
) -> int:
    """Write one harvest chunk atomically; returns the row count.

    Atomic rename is what makes a run resumable: a chunk file exists only when
    it is complete, so an interrupted run never leaves a half-written partition
    that a later run would trust.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    table = to_string_table(rows, columns)
    tmp = path.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp, compression="snappy")
    tmp.replace(path)
    return len(rows)
