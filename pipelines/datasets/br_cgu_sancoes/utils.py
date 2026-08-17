"""Download + cleaning transform for br_cgu_sancoes.

Pure functions (no Prefect) shared by the recurring pipeline (wrapped in @task
in tasks.py) and the one-shot bootstrap in ``models/br_cgu_sancoes/code/clean.py``
(which imports the column specs and transform from here). This module is the
schema source of truth for the dataset — there are no architecture CSVs; the
per-column ``(name, kind)`` specs below define the output column order and types,
and the dbt models ``safe_cast`` to those types.

The registries are cumulative on-demand snapshots (each snapshot contains the
full past + active history), so the pipeline keeps a single current snapshot per
table (``dump_mode="overwrite"``) with ``data_extracao`` as the freshness stamp.
"""

from __future__ import annotations

import io
import logging
import time
import zipfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_cgu_sancoes.constants import constants

log = logging.getLogger("br_cgu_sancoes")

# ── column specs (schema source of truth; kind ∈ {"str", "date", "float"}) ────
CEIS_COLS = [
    ("cadastro", "str"),
    ("codigo_sancao", "str"),
    ("tipo_pessoa", "str"),
    ("cpf_cnpj_sancionado", "str"),
    ("nome_sancionado", "str"),
    ("nome_informado_orgao", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("numero_processo", "str"),
    ("categoria_sancao", "str"),
    ("data_inicio_sancao", "date"),
    ("data_final_sancao", "date"),
    ("data_publicacao", "date"),
    ("publicacao", "str"),
    ("detalhamento_meio_publicacao", "str"),
    ("data_transito_julgado", "date"),
    ("abrangencia_sancao", "str"),
    ("orgao_sancionador", "str"),
    ("sigla_uf_orgao", "str"),
    ("esfera_orgao", "str"),
    ("fundamentacao_legal", "str"),
    ("data_origem_informacao", "date"),
    ("origem_informacao", "str"),
    ("observacoes", "str"),
]

CNEP_COLS = [
    ("cadastro", "str"),
    ("codigo_sancao", "str"),
    ("tipo_pessoa", "str"),
    ("cpf_cnpj_sancionado", "str"),
    ("nome_sancionado", "str"),
    ("nome_informado_orgao", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("numero_processo", "str"),
    ("categoria_sancao", "str"),
    ("valor_multa", "float"),
    ("data_inicio_sancao", "date"),
    ("data_final_sancao", "date"),
    ("data_publicacao", "date"),
    ("publicacao", "str"),
    ("detalhamento_meio_publicacao", "str"),
    ("data_transito_julgado", "date"),
    ("abrangencia_sancao", "str"),
    ("orgao_sancionador", "str"),
    ("sigla_uf_orgao", "str"),
    ("esfera_orgao", "str"),
    ("fundamentacao_legal", "str"),
    ("data_origem_informacao", "date"),
    ("origem_informacao", "str"),
    ("observacoes", "str"),
]

CEPIM_COLS = [
    ("cnpj_entidade", "str"),
    ("nome_entidade", "str"),
    ("numero_convenio", "str"),
    ("orgao_concedente", "str"),
    ("motivo_impedimento", "str"),
]

ACORDOS_COLS = [
    ("id_acordo", "str"),
    ("cnpj_sancionado", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("data_inicio_acordo", "date"),
    ("data_fim_acordo", "date"),
    ("situacao_acordo", "str"),
    ("data_informacao", "date"),
    ("numero_processo", "str"),
    ("termos_acordo", "str"),
    ("orgao_sancionador", "str"),
]

EFEITOS_COLS = [
    ("id_acordo", "str"),
    ("efeito", "str"),
    ("complemento", "str"),
]

# Table slug -> its column spec. Single source consumed by both the pipeline and
# the bootstrap.
TABLE_COLS: dict[str, list[tuple[str, str]]] = {
    "ceis": CEIS_COLS,
    "cnep": CNEP_COLS,
    "cepim": CEPIM_COLS,
    "acordos_leniencia": ACORDOS_COLS,
    "acordos_leniencia_efeitos": EFEITOS_COLS,
}

# Static dictionary decoding the coded `tipo_pessoa` column (F/J) in ceis/cnep.
# Authored here (not sourced from a CSV) so the staging `dicionario` table is
# reproducible from this module.
DICIONARIO_COLS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]
DICIONARIO_ROWS: list[tuple[str, str, str, str, str]] = [
    ("ceis", "tipo_pessoa", "F", "", "Pessoa física"),
    ("ceis", "tipo_pessoa", "J", "", "Pessoa jurídica"),
    ("cnep", "tipo_pessoa", "F", "", "Pessoa física"),
    ("cnep", "tipo_pessoa", "J", "", "Pessoa jurídica"),
]


# ── cleaning helpers ─────────────────────────────────────────────────────────
def clean_string(s: pd.Series) -> pd.Series:
    """Trim whitespace; empty string -> NA. 'Sem Informação' is preserved."""
    out = s.astype(str).str.strip()
    # pyrefly: ignore [bad-argument-type]
    return out.replace({"": pd.NA})


def clean_date(s: pd.Series) -> pd.Series:
    """DD/MM/YYYY -> pandas datetime; invalid/empty -> NaT."""
    # pyrefly: ignore [bad-argument-type]
    stripped = s.astype(str).str.strip().replace({"": pd.NA})
    return pd.to_datetime(stripped, format="%d/%m/%Y", errors="coerce")


def clean_float_brl(s: pd.Series) -> pd.Series:
    """'1.234,56' -> 1234.56; empty -> NaN."""
    # pyrefly: ignore [bad-argument-type]
    stripped = s.astype(str).str.strip().replace({"": pd.NA})
    normalized = stripped.str.replace(".", "", regex=False).str.replace(
        ",", ".", regex=False
    )
    return pd.to_numeric(normalized, errors="coerce")


def build_schema(cols: list[tuple[str, str]]) -> pa.Schema:
    """Build the typed Arrow schema (date32/float64/string) for one table.

    ``data_extracao`` (the hive partition column) is included first, matching the
    logical in-memory table; it is dropped from the written file by
    :func:`write_partitioned_string`.

    Args:
        cols: The table's ordered ``(output_column, kind)`` pairs.

    Returns:
        The Arrow schema, ``data_extracao`` first then the architecture columns.
    """
    fields = [pa.field("data_extracao", pa.date32())]
    for name, kind in cols:
        if kind == "date":
            fields.append(pa.field(name, pa.date32()))
        elif kind == "float":
            fields.append(pa.field(name, pa.float64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


# ── transform (ported from the validated bootstrap) ──────────────────────────
def process_table(
    csv_path: Path, cols: list[tuple[str, str]], snapshot: date, name: str = ""
) -> pd.DataFrame:
    """Read one raw CGU CSV, map columns positionally, clean, prepend snapshot.

    The raw files are ISO-8859-1, ``;``-delimited, ``"``-quoted with embedded
    newlines inside quoted fields; the pandas C parser handles those. Columns are
    mapped positionally against the raw header (verified to match the spec),
    which insulates the transform from header-label drift.

    Args:
        csv_path: Path to the raw CSV.
        cols: The table's ordered ``(output_column, kind)`` pairs.
        snapshot: Extraction date used as the leading ``data_extracao`` column.
        name: Table slug, for logging and error messages.

    Returns:
        A frame with ``data_extracao`` first, then the architecture columns in
        order, each cleaned per its ``kind``.

    Raises:
        ValueError: If the raw file's column count does not match the spec.
    """
    raw = pd.read_csv(
        csv_path,
        sep=";",
        encoding="latin-1",
        quotechar='"',
        dtype=str,
        keep_default_na=False,
    )

    if raw.shape[1] != len(cols):
        raise ValueError(
            f"{name}: raw file has {raw.shape[1]} columns, "
            f"architecture expects {len(cols)}"
        )

    rename = {raw.columns[i]: cols[i][0] for i in range(len(cols))}
    df = raw.rename(columns=rename)

    for out_name, kind in cols:
        if kind == "date":
            df[out_name] = clean_date(df[out_name])
        elif kind == "float":
            df[out_name] = clean_float_brl(df[out_name])
        else:
            df[out_name] = clean_string(df[out_name])

    df.insert(0, "data_extracao", snapshot)
    ordered = ["data_extracao"] + [c[0] for c in cols]
    df = df[ordered]
    log.info(f"{name}: parsed {len(df):,} rows from {csv_path.name}")
    return df


def to_arrow(df: pd.DataFrame, cols: list[tuple[str, str]]) -> pa.Table:
    """Build a typed Arrow table (date32/float64/string) from a cleaned frame.

    Args:
        df: Cleaned frame from :func:`process_table`.
        cols: The table's ordered ``(output_column, kind)`` pairs.

    Returns:
        An Arrow table whose schema matches the architecture types.
    """
    schema = build_schema(cols)
    arrays: dict = {}
    arrays["data_extracao"] = pa.array(df["data_extracao"], type=pa.date32())
    for name, kind in cols:
        if kind == "date":
            arrays[name] = pa.array(df[name].dt.date, type=pa.date32())
        elif kind == "float":
            arrays[name] = pa.array(
                df[name].astype("float64"), type=pa.float64()
            )
        else:
            arrays[name] = pa.array(
                # pyrefly: ignore [bad-argument-type]
                df[name].where(df[name].notna(), None).astype(object),
                type=pa.string(),
            )
    return pa.Table.from_pydict(
        {f.name: arrays[f.name] for f in schema}, schema=schema
    )


# ── download (on-demand snapshot behind AWS WAF) ─────────────────────────────
def _portal_url(registry: str, snapshot: date) -> str:
    return constants.PORTAL_URL.value.format(
        registry=registry, date=snapshot.strftime("%Y%m%d")
    )


def snapshot_ready(url: str) -> bool:
    """Probe whether the on-demand zip is generated and ready to download.

    The portal prepares the file asynchronously: 202 while generating, 200 (via
    a 302 to the S3 layer) when ready, and a bare 403/404 for a stale/wrong date.
    A browser User-Agent is mandatory (405 without one). Mirrors the readiness
    poll in ``pipelines/crawler/cgu/utils.py::source_url_is_available``.

    Args:
        url: The portal ``/download-de-dados/<registry>/<YYYYMMDD>`` URL.

    Returns:
        True if the zip is ready (HTTP 200 after redirects); False otherwise.
    """
    headers = {"User-Agent": constants.USER_AGENT.value}
    for attempt in range(1, constants.POLL_MAX_RETRIES.value + 1):
        with requests.get(
            url, headers=headers, stream=True, timeout=60, allow_redirects=True
        ) as r:
            if r.status_code == 200:
                return True
            if r.status_code == 202:
                log.info(f"snapshot preparing (202), attempt {attempt}: {url}")
                time.sleep(constants.POLL_WAIT_SECONDS.value)
                continue
            return False
    log.warning(f"snapshot never became ready: {url}")
    return False


def download_registry(
    registry: str, input_dir: Path, max_lookback: int | None = None
) -> date:
    """Download and extract the latest ready snapshot for one registry.

    There is no historical archive, so this walks back from today until a date
    yields a ready zip (200), downloads it and extracts its CSVs into
    ``input_dir``. The extracted filenames carry the snapshot date as their
    ``YYYYMMDD`` prefix.

    Args:
        registry: Portal path segment (e.g. ``"ceis"``, ``"acordos-leniencia"``).
        input_dir: Directory to extract CSVs into; created if absent.
        max_lookback: How many days back to probe; defaults to the constant.

    Returns:
        The snapshot date that was downloaded.

    Raises:
        RuntimeError: If no ready snapshot is found within the lookback window.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    lookback = max_lookback or constants.MAX_LOOKBACK_DAYS.value
    headers = {"User-Agent": constants.USER_AGENT.value}
    for delta in range(lookback):
        snapshot = date.today() - timedelta(days=delta)
        url = _portal_url(registry, snapshot)
        if not snapshot_ready(url):
            continue
        r = requests.get(
            url, headers=headers, timeout=300, allow_redirects=True
        )
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            zf.extractall(path=input_dir)
        log.info(f"{registry}: downloaded snapshot {snapshot.isoformat()}")
        return snapshot
    raise RuntimeError(
        f"{registry}: no ready snapshot in the last {lookback} days"
    )


def download_all(input_dir: Path) -> dict[str, date]:
    """Download every registry's latest snapshot into ``input_dir``.

    Args:
        input_dir: Directory to extract all registry CSVs into.

    Returns:
        Mapping of table slug to the snapshot date of the file that feeds it.
    """
    snapshots: dict[str, date] = {}
    for registry, table_slugs in constants.REGISTRIES.value.items():
        snap = download_registry(registry, input_dir)
        for slug in table_slugs:
            snapshots[slug] = snap
    return snapshots


def find_csv(input_dir: Path, slug: str) -> Path:
    """Find the extracted CSV that feeds a given table slug.

    Args:
        input_dir: Directory holding the extracted registry CSVs.
        slug: Table slug (looked up against ``FILE_SUFFIX``).

    Returns:
        Path to the matching ``YYYYMMDD_<SUFFIX>`` CSV.

    Raises:
        FileNotFoundError: If no matching CSV is present.
    """
    suffix = constants.FILE_SUFFIX.value[slug]
    matches = sorted(input_dir.glob(f"*_{suffix}"))
    if not matches:
        raise FileNotFoundError(f"{slug}: no *_{suffix} in {input_dir}")
    return matches[-1]


def snapshot_from_filename(csv_path: Path) -> date:
    """Parse the ``YYYYMMDD`` snapshot date from an extracted CSV filename.

    Args:
        csv_path: Path whose name starts with ``YYYYMMDD_``.

    Returns:
        The parsed snapshot date.
    """
    stamp = csv_path.name[:8]
    return date(int(stamp[:4]), int(stamp[4:6]), int(stamp[6:8]))


# ── write (all-STRING staging parquet) ───────────────────────────────────────
def write_partitioned_string(
    typed_table: pa.Table, slug: str, snapshot: date, output_dir: Path
) -> Path:
    """Write one table as all-STRING Snappy Parquet, hive-partitioned by snapshot.

    ``upload_to_gcs`` infers the staging schema from a header file that
    ``gcs.py::dump_header`` stringifies, so BigQuery reads every column as STRING;
    emitting typed parquet then fails on read. The values pass through the
    architecture's real types first (via :func:`to_arrow`), so a date serializes
    as ``"2026-08-13"`` and a float as ``"1234.56"`` rather than as a
    float-formatted date or ``"1959.0"``, and only then are cast to string **via
    arrow** — never ``astype(str)``, which would render a NULL as the literal
    ``"nan"`` and defeat the dbt ``safe_cast``. ``data_extracao`` is dropped from
    the file (it lives in the hive directory name).

    Args:
        typed_table: Typed Arrow table from :func:`to_arrow` (includes
            ``data_extracao``).
        slug: Table slug (output directory name).
        snapshot: Extraction date used as the hive partition value.
        output_dir: Root output directory.

    Returns:
        The table's directory,
        ``<output_dir>/<slug>/data_extracao=<YYYY-MM-DD>/data.parquet``.
    """
    file_table = typed_table.drop(["data_extracao"])
    string_schema = pa.schema(
        [pa.field(f.name, pa.string()) for f in file_table.schema]
    )
    file_table = file_table.cast(string_schema)
    pdir = output_dir / slug / f"data_extracao={snapshot.isoformat()}"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(file_table, pdir / "data.parquet", compression="snappy")
    log.info(f"{slug}: {file_table.num_rows:,} rows -> {pdir}")
    return output_dir / slug


def build_dicionario(output_dir: Path) -> Path:
    """Write the static ``dicionario`` table (decodes ``tipo_pessoa`` F/J).

    Already all-STRING, so no cast is needed. Unpartitioned.

    Args:
        output_dir: Root output directory.

    Returns:
        The dictionary's output directory,
        ``<output_dir>/dicionario/data.parquet``.
    """
    schema = pa.schema([(c, pa.string()) for c in DICIONARIO_COLS])
    table = pa.Table.from_pydict(
        {
            c: [row[i] for row in DICIONARIO_ROWS]
            for i, c in enumerate(DICIONARIO_COLS)
        },
        schema=schema,
    )
    tdir = output_dir / "dicionario"
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, tdir / "data.parquet", compression="snappy")
    log.info(f"dicionario: {len(DICIONARIO_ROWS)} rows -> {tdir}")
    return tdir


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build every table from the already-downloaded registry CSVs.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.br_cgu_sancoes.tasks.clean_sancoes`). Each table's
    ``data_extracao`` is read from its own CSV filename, since registries update
    on independent schedules and a run may mix snapshot dates.

    Args:
        input_dir: Directory holding the extracted registry CSVs.
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"snapshots"`` (a
        ``{slug: "YYYY-MM-DD"}`` map) and ``"max_date"`` — the latest extraction
        date across all tables, used to poll/commit the source update.
    """
    result: dict = {}
    snapshots: dict[str, str] = {}
    for slug, cols in TABLE_COLS.items():
        csv_path = find_csv(input_dir, slug)
        snapshot = snapshot_from_filename(csv_path)
        df = process_table(csv_path, cols, snapshot, name=slug)
        typed = to_arrow(df, cols)
        result[slug] = write_partitioned_string(
            typed, slug, snapshot, output_dir
        )
        snapshots[slug] = snapshot.isoformat()
    result["dicionario"] = build_dicionario(output_dir)
    result["snapshots"] = snapshots
    result["max_date"] = max(snapshots.values())
    return result
