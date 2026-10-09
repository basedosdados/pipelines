"""Pure download and cleaning helpers for cl_chilecompra_mercado_publico.

No Prefect imports here: the one-shot onboarding bootstrap under
``models/cl_chilecompra_mercado_publico/code/`` imports these same functions, so the
transform lives in exactly one place.

Source: ChileCompra bulk downloads, one ZIP per month per container.

    https://transparenciachc.blob.core.windows.net/oc-da/<year>-<month>.zip
    https://transparenciachc.blob.core.windows.net/lic-da/<year>-<month>.zip

The files are MONTHLY, not semestral -- ``<month>`` runs 1..12.
"""

from __future__ import annotations

import csv as csv_module
import functools
import io
import time
import unicodedata
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://transparenciachc.blob.core.windows.net"
CONTAINER = {"orden_compra": "oc-da", "licitacion": "lic-da"}

ARCHITECTURE_DIR = (
    Path(__file__).resolve().parents[3]
    / "models"
    / "cl_chilecompra_mercado_publico"
    / "code"
    / "architecture"
)

# The source is Windows-1252, not Latin-1: bytes 0x92-0x97 are smart quotes and dashes,
# which decode to control characters under Latin-1. A few files also carry 0x81, which
# is undefined in cp1252, hence errors="replace".
ENCODING = "cp1252"
ENCODING_ERRORS = "replace"

# Rows per parquet row group. See write_partitioned.
ROW_GROUP_SIZE = 50_000

# Source rows held in memory at once while parsing. See clean_month.
CHUNK_ROWS = 200_000

# Values the publisher uses to mean "absent".
NULL_TOKENS = {"", " ", "NA", "N/A", "NULL", "null", "-"}
NULL_DATE = "1900-01-01"

# Header spellings that changed across eras. ChileCompra stripped accents from the
# licitaciones header in 2015, but inconsistently: "Adquisición" lost its accent to a
# plain "o", while "genérico" had the "e" dropped entirely. Neither accent-folding nor
# accent-deletion reconciles both, so the mapping is explicit. Keys are the 2007-2014
# spellings; values are the canonical names used in the architecture CSVs.
HEADER_ALIASES = {
    "Tipo de Adquisición": "Tipo de Adquisicion",
    "Moneda Adquisición": "Moneda Adquisicion",
    "Nombre producto genérico": "Nombre producto genrico",
    "Nombre línea Adquisición": "Nombre linea Adquisicion",
    "Descripción línea Adquisición": "Descripcion linea Adquisicion",
}

# Deliberately excluded from every table.
#
# lic-da/2014-3 and lic-da/2014-4 -- and only those two of the 236 licitaciones files --
# carry nine extra columns naming individual public officials: their RUT, name, job
# title, email and telephone. They are fully populated there (about 4,000 distinct
# people across 1.5M rows each) and absent everywhere else, so including them would add
# personal data that is 99% null and that the procurement record does not otherwise
# publish. Chile's Ley 19.628 governs this kind of data and ChileCompra's own terms of
# use invoke it.
#
# This is an explicit decision, not an oversight: any source column that is neither in
# an architecture table nor listed here raises in _read_csv_from_zip, so a column the
# publisher adds later cannot be dropped silently.
EXCLUDED_COLUMNS = {
    "RutUsuario",
    "CodigoUsuario",
    "NombreUsuario",
    "CargoUsuario",
    "NombreResponsablePago",
    "EmailResponsablePago",
    "NombreResponsableContrato",
    "EmailResponsableContrato",
    "FonoResponsableContrato",
}

# Source months to skip because the publisher put another month's data in the slot.
#
# lic-da/2014-4.zip holds a file named 2014-4.csv whose 1,532,452 rows are ALL March
# 2014 -- same 20,511 tenders, same first tender, same row count as lic-da/2014-3.zip,
# and the two blobs are the same byte length uploaded 15 seconds apart. So April 2014
# licitaciones do not exist at source; that month is a genuine gap in the series, not a
# gap in this load.
#
# Left in, it would write a second copy of March into the March partition, because
# partition files are named for their source month. Before that naming change it instead
# silently overwrote March with an identical copy, which is how it went unnoticed.
DUPLICATE_SOURCE_MONTHS = {("licitacion", 2014, 4)}

# Which table each licitaciones column belongs to was determined empirically, by
# measuring how many distinct values each column takes within one tender and within one
# tender-item. See PLAN.md section 8.
PARTITION_SOURCE = {
    "orden_compra_item": "FechaEnvio",
    "licitacion_item": "FechaPublicacion",
    "licitacion_oferta": "FechaPublicacion",
}

TABLES_BY_KIND = {
    "orden_compra": ["orden_compra_item"],
    "licitacion": ["licitacion_item", "licitacion_oferta"],
}

CROSSWALK_PATH = ARCHITECTURE_DIR.parent / "geografia_crosswalk.csv"

# ChileCompra publishes región and comuna as free text, never as a código único
# territorial, so the link to br_bd_diretorios_cl is a name lookup. The mapping lives in
# a checked-in crosswalk rather than fuzzy matching at load time: fuzzy matching would
# quietly return a different answer as the data drifts, while a table returns the same
# answer or none at all. Columns here are (source name column, kind, derived id column).
GEOGRAPHY_LINKS = {
    "orden_compra_item": [
        ("region_unidad_compra", "region", "id_region_unidad_compra"),
        ("region_proveedor", "region", "id_region_proveedor"),
        ("comuna_proveedor", "comuna", "id_comuna_proveedor"),
    ],
    "licitacion_item": [
        ("region_unidad_compra", "region", "id_region_unidad_compra"),
        ("comuna_unidad_compra", "comuna", "id_comuna_unidad_compra"),
    ],
    "licitacion_oferta": [],
}

# Deliberately no "region de la " entry: several directory names begin with their
# article ("La Araucania", "Los Lagos"), so stripping it would turn "Region de la
# Araucania" into "araucania" and miss "la araucania".
REGION_PREFIXES = ("region del ", "region de ", "region ")


PRIMARY_KEYS = {
    "orden_compra_item": ["codigo_orden_compra", "id_item"],
    "licitacion_item": ["codigo_licitacion", "codigo_item"],
    "licitacion_oferta": [
        "codigo_licitacion",
        "codigo_item",
        "codigo_proveedor",
        "nombre_oferta",
    ],
}


@functools.lru_cache(maxsize=8)
def read_architecture(table: str) -> pd.DataFrame:
    """Return the architecture CSV for ``table``: the single source of truth for
    column order, types and the mapping back to the source header.

    Cached for two reasons. It is called once per table per chunk, so a licitaciones
    month re-read these files a dozen-odd times, and they sit on a Dropbox-synced path.
    More importantly it pins the schema for the whole run: editing an architecture while
    a load is in flight is exactly what produces partitions that disagree on their
    columns, which the upload step then has to refuse.
    """
    return pd.read_csv(ARCHITECTURE_DIR / f"{table}.csv", dtype=str).fillna("")


def month_url(kind: str, year: int, month: int) -> str:
    return f"{BASE_URL}/{CONTAINER[kind]}/{year}-{month}.zip"


def _session(total_retries: int = 5) -> requests.Session:
    """HTTP session that retries connection errors and 5xx with exponential backoff.

    The blob host intermittently refuses connections and stalls mid-transfer. Without
    this, such a blip turns a slow month into a failed one -- on the first full run it
    cost 467 of 472 files.
    """
    session = requests.Session()
    retry = Retry(
        total=total_retries,
        connect=total_retries,
        read=total_retries,
        backoff_factor=2,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def head_month(
    kind: str, year: int, month: int, timeout: int = 60
) -> dict | None:
    """HEAD one blob. Returns None when the month does not exist (404).

    ``last_modified`` and ``etag`` are what the recurring pipeline diffs against its
    stored state: ChileCompra rewrites *old* months retroactively, so "only refresh the
    current month" would miss real revisions.
    """
    r = _session().head(month_url(kind, year, month), timeout=timeout)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    return {
        "kind": kind,
        "year": year,
        "month": month,
        "bytes": int(r.headers.get("Content-Length", 0)),
        "last_modified": r.headers.get("Last-Modified", ""),
        "etag": r.headers.get("ETag", "").strip('"'),
    }


def download_month(
    kind: str,
    year: int,
    month: int,
    dest_dir: Path,
    attempts: int = 4,
    read_timeout: int = 120,
) -> Path:
    """Stream one monthly ZIP to disk, verifying the byte count. Returns the path.

    ``read_timeout`` applies per chunk, not per file: a stalled socket then fails in two
    minutes and is retried, instead of holding the run for the length of a file-wide
    timeout. The first full run lost four hours to single months this way.

    A truncated transfer is caught here by comparing against Content-Length. Left
    undetected it surfaces much later as an unhelpful zip or parser error.
    """
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"{kind}_{year}-{month:02d}.zip"
    tmp = out.with_suffix(".zip.part")
    url = month_url(kind, year, month)

    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            with _session().get(
                url, stream=True, timeout=(30, read_timeout)
            ) as response:
                response.raise_for_status()
                expected = int(response.headers.get("Content-Length", 0))
                written = 0
                with open(tmp, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        handle.write(chunk)
                        written += len(chunk)
            if expected and written != expected:
                raise OSError(
                    f"{url}: truncated download, got {written} of {expected} bytes"
                )
            tmp.rename(out)
            return out
        except Exception as exc:
            last_error = exc
            tmp.unlink(missing_ok=True)
            if attempt < attempts:
                time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    raise RuntimeError(
        f"{url}: failed after {attempts} attempts"
    ) from last_error


def _csv_names(zf: zipfile.ZipFile) -> list[str]:
    """CSV members of a monthly ZIP, in name order.

    Usually one. ``lic-da/2011-3`` is split into ``lic_2011-3a.csv`` and
    ``lic_2011-3b.csv`` -- 1.27 GB across the two, the largest month in the series --
    with byte-identical headers. Reading them in order and concatenating reproduces the
    single-file months exactly.
    """
    names = sorted(n for n in zf.namelist() if n.lower().endswith(".csv"))
    if not names:
        raise ValueError(f"no CSV inside {zf.filename}")
    return names


def _rename_columns(chunk: pd.DataFrame, filename: str, checked: bool) -> bool:
    chunk.columns = [
        HEADER_ALIASES.get(c.strip(), c.strip()) for c in chunk.columns
    ]
    if not checked:
        _assert_columns_known(chunk.columns, filename)
    return True


def _iter_csv_chunks(
    path: Path, chunk_rows: int = CHUNK_ROWS, strict: bool = False
):
    """Yield every CSV inside a monthly ZIP in row chunks, as all-string frames.

    Everything arrives as string; typing happens in the dbt model. The parser must be a
    real CSV parser -- quoted fields contain both embedded ``;`` and embedded newlines,
    so the file cannot be split on line boundaries.

    ``strict`` swaps pandas' C parser for Python's csv module and drops records whose
    field count does not match the header. That is slower, so it is only used as a
    fallback for a file the fast parser rejects -- see clean_month.
    """
    with zipfile.ZipFile(path) as zf:
        checked = False
        for name in _csv_names(zf):
            if strict:
                for chunk in _strict_chunks(zf, name, path, chunk_rows):
                    checked = _rename_columns(chunk, path.name, checked)
                    yield chunk
                continue
            with zf.open(name) as raw:
                text = io.TextIOWrapper(
                    raw, encoding=ENCODING, errors=ENCODING_ERRORS, newline=""
                )
                reader = pd.read_csv(
                    text,
                    sep=";",
                    quotechar='"',
                    dtype=str,
                    keep_default_na=False,
                    na_values=[],
                    engine="c",
                    low_memory=False,
                    chunksize=chunk_rows,
                )
                for chunk in reader:
                    checked = _rename_columns(chunk, path.name, checked)
                    yield chunk


def _strict_chunks(
    zf: zipfile.ZipFile, name: str, path: Path, chunk_rows: int
):
    """Parse one CSV with Python's csv module, dropping records of the wrong width.

    ``lic-da/2026-3`` carries an unescaped double quote that breaks field alignment for
    849 records, all on a single tender. Their field counts come out at 100, 111, 127,
    140 rather than 110. Dropping them is the only honest option -- the fields are
    misaligned, not merely missing -- but note that pandas would NOT have dropped the
    short ones: its C parser pads a too-short record with NaN, quietly writing shifted
    values into the right-hand columns. Hence the explicit width check, and the count
    printed so the loss is on the record rather than silent.
    """
    csv_module.field_size_limit(10**9)
    with zf.open(name) as raw:
        text = io.TextIOWrapper(
            raw, encoding=ENCODING, errors=ENCODING_ERRORS, newline=""
        )
        reader = csv_module.reader(text, delimiter=";", quotechar='"')
        header = next(reader)
        width = len(header)
        rows: list[list[str]] = []
        dropped = 0
        for record in reader:
            if len(record) != width:
                dropped += 1
                continue
            rows.append(record)
            if len(rows) >= chunk_rows:
                yield pd.DataFrame(rows, columns=header, dtype=str)
                rows = []
        if rows:
            yield pd.DataFrame(rows, columns=header, dtype=str)
    if dropped:
        print(
            f"    {path.name}/{name}: dropped {dropped:,} malformed record(s) "
            f"whose field count differed from the {width}-column header",
            flush=True,
        )


def _read_csv_from_zip(path: Path) -> pd.DataFrame:
    """Whole-file read. Kept for ad-hoc inspection; the loader uses _iter_csv_chunks."""
    return pd.concat(_iter_csv_chunks(path), ignore_index=True)


def _known_source_columns() -> set[str]:
    known: set[str] = set()
    for tables in TABLES_BY_KIND.values():
        for table in tables:
            arch = read_architecture(table)
            known |= {s for s in arch["original_name"] if s}
    return known | EXCLUDED_COLUMNS


def _assert_columns_known(columns, filename: str) -> None:
    """Refuse to silently drop a column the architecture does not know about.

    The source has four header signatures per table across 2007-2026, and two of the
    236 licitaciones files carry columns no other file has. A quiet projection onto the
    architecture would hide the next such change instead of surfacing it.
    """
    unknown = sorted(set(columns) - _known_source_columns())
    if unknown:
        raise ValueError(
            f"{filename}: source columns not present in any architecture table and not "
            f"in EXCLUDED_COLUMNS: {unknown}. Add them to the architecture or to "
            f"EXCLUDED_COLUMNS before loading this month."
        )


def _clean_strings(s: pd.Series) -> pd.Series:
    """Trim, collapse internal whitespace, and map the publisher's null tokens to NA.

    Source values carry trailing spaces (``"Región del Maule "``), which would otherwise
    split one region into two categories.
    """
    out = s.astype("string").str.replace(r"\s+", " ", regex=True).str.strip()
    return out.mask(out.isin(NULL_TOKENS))


def _to_numeric_text(s: pd.Series) -> pd.Series:
    """Normalise the decimal comma to a point, keeping the result as text.

    Staging is all-STRING by house convention and the dbt model does the ``safe_cast``;
    this only fixes the separator so that ``safe_cast`` can succeed. ``892,5`` must not
    become ``8925``.
    """
    out = _clean_strings(s)
    # Only the comma is translated. Verified across every numeric column of a full
    # month: the source never uses a dot as a thousands separator, so stripping dots
    # would silently corrupt any point-decimal that did appear. Scientific notation
    # ("1,5e+07") survives the swap and safe_cast reads it as a float.
    return out.str.replace(",", ".", regex=False)


def _to_date_text(s: pd.Series) -> pd.Series:
    """Return ISO date text, mapping the 1900-01-01 sentinel to NA."""
    out = _clean_strings(s)
    out = out.str.slice(0, 10)
    out = out.mask(out == NULL_DATE)
    return out.mask(~out.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False))


def _sort_semicolon_list(s: pd.Series) -> pd.Series:
    """Order the tokens of a ``;``-separated list.

    ChileCompra emits the same evaluation criteria for one tender in different orders
    across rows, which otherwise manufactures duplicate tender-item rows.
    """

    def _one(v):
        if pd.isna(v):
            return v
        parts = sorted(p.strip() for p in str(v).split(";") if p.strip())
        return "; ".join(parts) if parts else pd.NA

    return s.map(_one).astype("string")


def _fold(value: object) -> str:
    """Lower-case, drop accents, normalise the acute-accent apostrophe, collapse space."""
    text = unicodedata.normalize("NFD", str(value))
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().replace("\u00b4", "'").replace(".", "")
    return " ".join(text.split())


def _strip_region_prefix(folded: str) -> str:
    for prefix in REGION_PREFIXES:
        if folded.startswith(prefix):
            return folded[len(prefix) :].strip()
    return folded


@functools.lru_cache(maxsize=1)
def geography_crosswalk() -> dict[str, dict[str, str]]:
    """{kind: {normalised name: CUT id}} read from the checked-in crosswalk."""
    frame = pd.read_csv(CROSSWALK_PATH, dtype=str)
    out: dict[str, dict[str, str]] = {"region": {}, "comuna": {}}
    for row in frame.itertuples():
        out[str(row.tipo)][str(row.nombre_normalizado)] = str(row.id)
    return out


def resolve_geography(series: pd.Series, kind: str) -> pd.Series:
    """Map a free-text región or comuna name onto its CUT id, NA when unmatched.

    Folds each *distinct* value once rather than each row. A column of 350k rows holds
    only a few hundred distinct place names; folding per row cost millions of
    unicodedata.normalize calls per month and took one month from 52s to 419s.
    """
    table = geography_crosswalk()[kind]
    values = series.astype("string")
    mapping: dict[str, str | None] = {}
    for raw in values.dropna().unique():
        folded = _fold(raw)
        if kind == "region":
            folded = _strip_region_prefix(folded)
        mapping[str(raw)] = table.get(folded)
    return values.map(mapping).astype("string")


def build_table(raw: pd.DataFrame, table: str) -> pd.DataFrame:
    """Project one raw monthly frame onto one architecture table.

    Columns absent from this era's file are created empty, so every month yields the
    same schema regardless of which of the three header signatures it came from.
    """
    arch = read_architecture(table)
    out = pd.DataFrame(index=raw.index)

    for _, col in arch.iterrows():
        name, src, btype = (
            col["name"],
            col["original_name"],
            col["bigquery_type"],
        )
        if name in ("ano", "mes"):
            continue
        if not src or src not in raw.columns:
            out[name] = pd.Series(pd.NA, index=raw.index, dtype="string")
            continue
        s = raw[src]
        if btype == "DATE":
            out[name] = _to_date_text(s)
        elif btype in ("INT64", "FLOAT64"):
            out[name] = _to_numeric_text(s)
        elif name == "criterios_evaluacion":
            out[name] = _sort_semicolon_list(_clean_strings(s))
        else:
            out[name] = _clean_strings(s)

    for source_col, kind, id_col in GEOGRAPHY_LINKS.get(table, []):
        if source_col in out.columns:
            out[id_col] = resolve_geography(out[source_col], kind)

    part_src = PARTITION_SOURCE[table]
    part = (
        _to_date_text(raw[part_src])
        if part_src in raw.columns
        else pd.Series(pd.NA, index=raw.index, dtype="string")
    )
    out.insert(0, "mes", part.str.slice(5, 7))
    out.insert(0, "ano", part.str.slice(0, 4))

    keys = PRIMARY_KEYS[table]
    out = out.drop_duplicates(subset=keys)
    return out.reindex(columns=list(arch["name"]))


def clean_month(
    kind: str, zip_path: Path, chunk_rows: int = CHUNK_ROWS
) -> dict[str, pd.DataFrame]:
    """Turn one monthly ZIP into one frame per table it feeds.

    Read in chunks rather than whole. The largest month, lic-da/2014-3, is 1.5M rows
    across 111 columns; held at once as pandas object strings that is roughly 11 GB,
    which does not fit alongside everything else on a 16 GB machine. Projecting each
    chunk onto the output tables first keeps the peak to the chunk plus the accumulated
    (much narrower) results.
    """
    try:
        return _clean_month(kind, zip_path, chunk_rows, strict=False)
    except pd.errors.ParserError as exc:
        # The fast C parser refuses the whole file on a malformed record. Retry from
        # scratch with the strict reader, which drops just the bad records and reports
        # how many. Restarting rather than resuming matters: chunks already yielded
        # would otherwise be counted twice.
        print(
            f"    {Path(zip_path).name}: C parser rejected the file ({exc}); "
            f"re-reading with the strict parser",
            flush=True,
        )
        return _clean_month(kind, zip_path, chunk_rows, strict=True)


def _clean_month(
    kind: str, zip_path: Path, chunk_rows: int, strict: bool
) -> dict[str, pd.DataFrame]:
    tables = TABLES_BY_KIND[kind]
    parts: dict[str, list[pd.DataFrame]] = {t: [] for t in tables}

    for raw in _iter_csv_chunks(Path(zip_path), chunk_rows, strict=strict):
        for table in tables:
            parts[table].append(build_table(raw, table))

    out = {}
    for table in tables:
        frame = (
            pd.concat(parts[table], ignore_index=True)
            if parts[table]
            else build_table(pd.DataFrame(), table)
        )
        # De-duplicate again after concatenating: build_table only sees one chunk, so a
        # pair of duplicate rows split across a chunk boundary survives until here.
        out[table] = frame.drop_duplicates(
            subset=PRIMARY_KEYS[table]
        ).reset_index(drop=True)
    return out


def arrow_schema(table: str) -> pa.Schema:
    """All-STRING arrow schema in architecture order.

    Both upload paths -- the one-shot onboarding upload and the pipeline's
    ``upload_to_gcs`` -- must write all-STRING parquet. They share one staging dataset,
    so a typed external table left behind by one collides with the other's overwrite.
    The schema carries column order, not types; the dbt model does the casting.
    """
    arch = read_architecture(table)
    return pa.schema(
        [(n, pa.string()) for n in arch["name"] if n not in ("ano", "mes")]
    )


def write_partitioned(
    df: pd.DataFrame, table: str, output_dir: Path, source_tag: str
) -> list[Path]:
    """Write hive-partitioned parquet: ``<table>/ano=YYYY/mes=MM/data_<source>.parquet``.

    The file is named for the SOURCE month it came from, not the partition it lands in,
    so two source files that both contribute to one partition cannot destroy each other.
    That is not hypothetical: ``lic-da/2026-3`` yields one row whose FechaPublicacion
    reads June -- corruption residue from the malformed region in that file, since the
    tender's other 142 rows are March and the June file does not contain it at all. With
    a fixed ``data.parquet`` name, loading March after June would have replaced June's
    entire partition with that single row, silently.

    It matters more for the recurring pipeline than for the one-shot load, because the
    pipeline re-ingests an arbitrary rolling window of months in whatever order the
    publisher touched them.

    Naming by source also keeps re-runs idempotent: re-loading one month overwrites
    exactly that month's contribution to every partition it touches.

    Cast to string through arrow rather than ``astype(str)``: the latter renders NULL as
    the literal "nan", which ``safe_cast`` will not turn back into NULL.
    """
    output_dir = Path(output_dir)
    schema = arrow_schema(table)
    written = []
    df = df[df["ano"].notna() & df["mes"].notna()]
    for (ano, mes), part in df.groupby(["ano", "mes"], sort=True):
        target = output_dir / table / f"ano={ano}" / f"mes={mes}"
        target.mkdir(parents=True, exist_ok=True)
        body = part.drop(columns=["ano", "mes"])
        arrays = [
            pa.array(body[f.name].astype("string"), type=pa.string())
            for f in schema
        ]
        pq.write_table(
            pa.Table.from_arrays(arrays, schema=schema),
            target / f"data_{source_tag}.parquet",
            compression="snappy",
            # Bounded row groups keep gcs.dump_header cheap: it builds the staging
            # table by reading row group 0 of the first parquet it walks into, so one
            # giant row group there is an out-of-memory risk on the worker.
            row_group_size=ROW_GROUP_SIZE,
        )
        written.append(target / f"data_{source_tag}.parquet")
    return written
