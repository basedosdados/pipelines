"""Download + streaming cleaning transform for br_mf_divida_ativa (PGFN Dívida
Ativa da União).

Pure functions (no Prefect), importable and unit-testable; a later recurring
pipeline will reuse them. The source publishes one quarterly ZIP per system
(SIDA / PREV / FGTS); each ZIP holds several `;`-delimited, Latin-1 CSV parts.
The SIDA (nao_previdenciario) table is ~40-50M rows per quarter, so every part is
processed in row chunks and streamed to Parquet - the whole table is never held
in memory.

Schema and column order come from the architecture CSVs in ``architecture/`` (the
single source of truth). Staging Parquet is all-STRING by Data Basis convention;
the dbt model ``safe_cast``s each column to its real type.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
import time
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

log = logging.getLogger("br_mf_divida_ativa")

# ── constants ────────────────────────────────────────────────────────────────
BASE_URL = "https://dadosabertos.pgfn.gov.br"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) DataBasis/1.0"

# table slug -> source category file stem in Dados_abertos_<stem>.zip
CATEGORY = {
    "nao_previdenciario": "Nao_Previdenciario",
    "previdenciario": "Previdenciario",
    "fgts": "FGTS",
}
TABLES = tuple(CATEGORY)

# Known anomalous filenames on the server (the typo is the real object name).
URL_OVERRIDES = {
    (2022, 4, "nao_previdenciario"): (
        f"{BASE_URL}/2022_trimestre_04/Dados_abertos_Nao_Previdenciariozip.zip"
    ),
}

SENTINEL_DATES = {"01/01/1000"}
CHUNKSIZE = 400_000

# Source header drift: the debtor-state column was named UF_UNIDADE_RESPONSAVEL
# in 2020 Q1-2022 Q3 (the UF of the responsible PGFN unit) and UF_DEVEDOR from
# 2022 Q4 on (the debtor's UF). Both occupy the same position and are mapped to
# sigla_uf; map: canonical original_name -> older aliases to rename to it.
COLUMN_ALIASES = {"UF_DEVEDOR": ("UF_UNIDADE_RESPONSAVEL",)}

# The 27 Brazilian federative units. UF_DEVEDOR carries a directory FK, so values
# outside this set (e.g. "Si", tied to "SEM INFORMACAO" records) are mapped to
# NULL rather than kept as fake geography codes.
VALID_UFS = frozenset(
    [
        "AC",
        "AL",
        "AP",
        "AM",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MT",
        "MS",
        "MG",
        "PA",
        "PB",
        "PR",
        "PE",
        "PI",
        "RJ",
        "RN",
        "RS",
        "RO",
        "RR",
        "SC",
        "SP",
        "SE",
        "TO",
    ]
)

_HERE = Path(__file__).resolve().parent
ARCH_DIR = _HERE / "architecture"


def data_root() -> Path:
    """Root for downloaded input + cleaned output - under ~/Downloads.

    Override with ``PGFN_DATA_ROOT``. Tens of GB land here for the full backfill,
    so it lives outside Dropbox/the repo (never synced, never committed) and is
    deleted as the final onboarding step.
    """
    return Path(
        os.environ.get(
            "PGFN_DATA_ROOT",
            str(Path.home() / "Downloads" / "br_mf_divida_ativa_data"),
        )
    )


# ── schema ───────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[dict]:
    """Read a table's architecture CSV - column order + types + source mapping."""
    with open(ARCH_DIR / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _source_map(table: str) -> dict[str, str]:
    """original_name -> output name, for columns read from the source CSV.

    ``ano`` and ``trimestre`` are injected from the folder, not read, so they are
    excluded here.
    """
    return {
        a["original_name"]: a["name"]
        for a in read_arch(table)
        if a["name"] not in ("ano", "trimestre")
    }


def _string_schema(table: str) -> pa.Schema:
    return pa.schema(
        [pa.field(a["name"], pa.string()) for a in read_arch(table)]
    )


# ── download ─────────────────────────────────────────────────────────────────
def quarter_url(year: int, quarter: int, table: str) -> str:
    """Canonical source URL for a (year, quarter, table), with known overrides."""
    if (year, quarter, table) in URL_OVERRIDES:
        return URL_OVERRIDES[(year, quarter, table)]
    return f"{BASE_URL}/{year}_trimestre_{quarter:02d}/Dados_abertos_{CATEGORY[table]}.zip"


def source_exists(year: int, quarter: int, table: str, session=None) -> bool:
    """HEAD the source URL; True iff it returns 200 (some quarters lack FGTS)."""
    s = session or requests
    r = s.head(
        quarter_url(year, quarter, table),
        headers={"User-Agent": USER_AGENT},
        allow_redirects=True,
        timeout=60,
    )
    return r.status_code == 200


def download_quarter(
    year: int,
    quarter: int,
    table: str,
    input_dir: Path,
    session=None,
    retries: int = 4,
) -> Path:
    """Download one quarterly ZIP into ``input_dir``; return its path.

    Streams to disk (files reach 1.3 GB). A ``(connect, read)`` timeout means a
    stalled connection - e.g. after the laptop sleeps and the socket dies - fails
    within ~2 min instead of hanging, and is retried with backoff; on wake the
    retry succeeds. Raises the last error only after all retries, so a caller can
    distinguish a genuine failure from an absent quarter (use ``source_exists``).
    """
    s = session or requests
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / f"{table}_{year}_T{quarter}.zip"
    url = quarter_url(year, quarter, table)
    last: Exception | None = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            with s.get(
                url,
                headers={"User-Agent": USER_AGENT},
                stream=True,
                timeout=(30, 120),
            ) as r:
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for block in r.iter_content(chunk_size=1 << 20):
                        fh.write(block)
            log.info(
                "downloaded %s -> %s (%.1f MB)",
                url,
                dest,
                dest.stat().st_size / 1e6,
            )
            return dest
        except Exception as e:
            last = e
            log.warning(
                "download attempt %d/%d failed for %s: %s",
                attempt,
                retries,
                url,
                e,
            )
            dest.unlink(missing_ok=True)
            if attempt < retries:
                # exponential backoff (5s, 10s, 20s, ... capped at 60s) so a
                # connection killed by laptop sleep gets time to recover.
                time.sleep(min(60, 5 * 2 ** (attempt - 1)))
    # pyrefly: ignore [bad-raise]
    raise last


# ── transform ────────────────────────────────────────────────────────────────
def _clean_series(s: pd.Series) -> pd.Series:
    """Strip whitespace; map empty string to None."""
    s = s.astype("string").str.strip()
    return s.mask(s.eq(""), other=pd.NA)


def _clean_uf(s: pd.Series) -> pd.Series:
    """Uppercase-strip; map any value outside the 27 UFs to None (keeps the FK)."""
    s = _clean_series(s).str.upper()
    # pyrefly: ignore [no-matching-overload]
    return s.where(s.isin(VALID_UFS), other=pd.NA)


def _clean_valor(s: pd.Series) -> pd.Series:
    """Normalize the consolidated value to a plain float string or None.

    Source uses dot decimals; a leading-dot value like ``.00`` is normalized to
    ``0.00`` so a later ``safe_cast`` is unambiguous. Kept as string (all-STRING
    staging); the dbt model casts to FLOAT64.
    """
    s = _clean_series(s)
    s = s.str.replace(r"^\.", "0.", regex=True)
    return s


def _clean_date(s: pd.Series) -> pd.Series:
    """Parse dd/mm/yyyy to ISO ``YYYY-MM-DD``; sentinel/invalid -> None.

    ``01/01/1000`` (and any pre-1677 date) is out of pandas' datetime64[ns]
    range, so ``errors="coerce"`` maps it to NaT - exactly the sentinel handling
    we want. Real inscription dates are all modern.
    """
    raw = _clean_series(s)
    dt = pd.to_datetime(raw, format="%d/%m/%Y", errors="coerce")
    iso = dt.dt.strftime("%Y-%m-%d")
    # pyrefly: ignore [no-matching-overload]
    return iso.where(dt.notna(), other=pd.NA)


def _clean_chunk(
    df: pd.DataFrame, table: str, year: int, quarter: int
) -> pa.Table:
    """Map one raw chunk to the architecture schema as an all-STRING arrow Table."""
    # reset_index is load-bearing: pandas read_csv chunks carry an offset index
    # (chunk 2 is 400000..), while the injected ano/trimestre Series below are
    # 0-based. Building a DataFrame from misaligned indices would align on the
    # index and yield all-NULL rows past the first chunk. Reset so every column
    # shares a 0-based index.
    df = df.rename(columns=lambda c: c.strip()).reset_index(drop=True)
    # normalize older source header names to the canonical ones (schema drift)
    for canon, aliases in COLUMN_ALIASES.items():
        if canon not in df.columns:
            for a in aliases:
                if a in df.columns:
                    df = df.rename(columns={a: canon})
                    break
    smap = _source_map(table)
    missing = [src for src in smap if src not in df.columns]
    if missing:
        raise ValueError(
            f"{table} {year}Q{quarter}: source missing columns {missing}; "
            f"got {list(df.columns)}"
        )
    out = {}
    for src, name in smap.items():
        col = df[src]
        if name == "data_inscricao":
            out[name] = _clean_date(col)
        elif name == "valor_consolidado":
            out[name] = _clean_valor(col)
        elif name == "sigla_uf":
            out[name] = _clean_uf(col)
        else:
            out[name] = _clean_series(col)
    n = len(df)
    out["ano"] = pd.Series([str(year)] * n, dtype="string")
    out["trimestre"] = pd.Series([str(quarter)] * n, dtype="string")
    order = [a["name"] for a in read_arch(table)]
    frame = pd.DataFrame({k: out[k] for k in order})
    return pa.Table.from_pandas(
        frame, schema=_string_schema(table), preserve_index=False
    )


def clean_quarter_zip(
    zip_path: Path,
    table: str,
    year: int,
    quarter: int,
    output_dir: Path,
    chunksize: int = CHUNKSIZE,
) -> int:
    """Stream every CSV part in a quarterly ZIP to partitioned Parquet.

    Output: ``<output_dir>/<table>/ano=<Y>/trimestre=<Q>/part_<i>.parquet``, one
    file per source CSV part, written chunk-by-chunk so memory stays bounded.

    Returns the total row count written for the quarter.
    """
    ano_dir = output_dir / table / f"ano={year}"
    final = ano_dir / f"trimestre={quarter}"
    tmp = ano_dir / f".trimestre={quarter}.tmp"
    ano_dir.mkdir(parents=True, exist_ok=True)
    if tmp.exists():
        shutil.rmtree(tmp)
    tmp.mkdir()
    schema = _string_schema(table)
    total = 0
    # Atomic publish: write parts to a temp dir and rename into place only on
    # full success. A failure (e.g. schema drift) leaves NO partition behind, so
    # --skip-existing never mistakes a half-written partition for a complete one.
    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if m.lower().endswith(".csv")]
            if not members:
                raise ValueError(f"{zip_path} contains no .csv members")
            for i, member in enumerate(sorted(members)):
                writer = pq.ParquetWriter(
                    tmp / f"part_{i:03d}.parquet", schema, compression="snappy"
                )
                part_rows = 0
                try:
                    reader = pd.read_csv(
                        io.TextIOWrapper(zf.open(member), encoding="latin-1"),
                        sep=";",
                        dtype=str,
                        na_filter=False,
                        chunksize=chunksize,
                    )
                    for chunk in reader:
                        at = _clean_chunk(chunk, table, year, quarter)
                        writer.write_table(at)
                        part_rows += at.num_rows
                finally:
                    writer.close()
                total += part_rows
                log.info(
                    "%s %sQ%s %s: %s rows",
                    table,
                    year,
                    quarter,
                    member,
                    f"{part_rows:,}",
                )
        if final.exists():
            shutil.rmtree(final)
        tmp.rename(final)
    except BaseException:
        shutil.rmtree(tmp, ignore_errors=True)
        raise
    log.info(
        "%s %sQ%s TOTAL: %s rows -> %s",
        table,
        year,
        quarter,
        f"{total:,}",
        final,
    )
    return total
