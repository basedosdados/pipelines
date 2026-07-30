"""Download + cleaning transform for br_me_siconfi (shared by the pipeline).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py).

The cleaning transform itself is **not reimplemented here** — it is reused from
the validated one-shot bootstrap under ``models/br_me_siconfi/code/``:

- download primitives come from ``code/download_api.py``;
- the per-year builders run through ``code/tables_final`` (``_init_worker`` +
  ``process_year_task``), driven by the ``BUILDERS`` registry in ``code/build.py``;
- the hand-maintained crosswalk lives in ``code/crosswalk/*.xlsx``.

This module adds only the pieces a recurring pipeline needs on top of that:

1. ``download_window`` — download only the trailing window of years from the API;
2. ``to_staging_parquet`` — convert the builders' CSV output to all-STRING,
   hive-partitioned Snappy parquet for ``upload_to_gcs``;
3. ``pull_cache`` / ``push_cache`` — a GCS parquet cache of the out-of-window
   years, so the full tables can be rebuilt and fully overwritten every run
   without re-downloading every year;
4. ``clean_all`` — the single orchestration entry point.

Crosswalk gaps fail loud: if Tesouro emits account codes missing from the
crosswalk, ``clean_window`` raises with the offending keys (see the user
decision in the plan) rather than silently dropping or mislabeling rows.
"""

import importlib.util
import json
import os
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_me_siconfi.constants import constants

# Steps log via print() so Prefect's log_prints captures them (a module logger
# would be invisible in the flow-run logs), matching the reused build code.
CODE_DIR = str(constants.CODE_DIR.value)
PATH_QUERIES = str(constants.PATH_QUERIES.value)
CACHE_PREFIX = constants.CACHE_PREFIX.value
RAW_PREFIX = constants.RAW_PREFIX.value


# ── reused-code importers ────────────────────────────────────────────────────
def _ensure_code_on_path() -> None:
    """Put ``models/br_me_siconfi/code`` on sys.path (idempotent).

    Needed so ``import download_api`` and ``import tables_final.*`` resolve to
    the validated bootstrap, exactly as ``code/build.py`` does internally.
    """
    if CODE_DIR not in sys.path:
        sys.path.insert(0, CODE_DIR)


def _download_api():
    """Import the ``download_api`` module from the bootstrap code dir."""
    _ensure_code_on_path()
    import download_api

    return download_api


def _build_registry() -> dict:
    """Load ``BUILDERS`` from ``code/build.py`` by file path.

    Loaded by path (not ``import build``) to avoid colliding with the PEP 517
    ``build`` package that may be installed in the environment.
    """
    _ensure_code_on_path()
    spec = importlib.util.spec_from_file_location(
        "siconfi_build", os.path.join(CODE_DIR, "build.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.BUILDERS


def _shared():
    """Import the builders' shared worker helpers."""
    _ensure_code_on_path()
    from tables_final import shared

    return shared


def _level_of(table: str) -> str:
    """Return the government level prefix of a table slug."""
    return table.split("_", 1)[0]


def tables_for_levels(levels) -> list[str]:
    """All table slugs belonging to the requested government levels."""
    by_level = constants.TABLES_BY_LEVEL.value
    return [t for lvl in levels for t in by_level[lvl]]


# ── download (trailing window only) ──────────────────────────────────────────
def _download_entes_slice(da, api_dir, entes, years, force) -> None:
    """Download one worker's slice of entities across all window years.

    Own session per call so this is thread-safe; ``download_dca`` carries the
    per-call ~1.1s rate limit, so N threads make ~Nx the request rate.
    """
    session = da.make_session()
    try:
        for lvl, cod in entes:
            for ano in years:
                out_file = api_dir / lvl / f"dca_{ano}_{cod}.json"
                if out_file.exists() and not force:
                    continue
                data = da.download_dca(session, ano, cod)
                if data is None or not data.get("items"):
                    out_file.write_text(json.dumps({"data": {"items": []}}))
                    continue
                out_file.write_text(
                    json.dumps(
                        {
                            "metadata": {"exercicio": ano, "cod_ibge": cod},
                            "data": data,
                        },
                        ensure_ascii=False,
                    )
                )
    finally:
        session.close()


def download_window(
    work_dir: str,
    start_year: int,
    end_year: int,
    levels,
    workers: int = 1,
    force: bool = False,
) -> str:
    """Download the SICONFI DCA for the requested years and levels.

    One ``/dca`` call per (entity, year) via the bootstrap's ``download_dca``.
    The município level is ~5,570 of the ~5,598 entities, so runtime is
    dominated by it: a 5-year all-level window is ~28k calls ≈ 8.5h at
    ``workers=1`` (~2h at ``workers=4``, at the cost of a higher request rate
    against a .gov API — raise with care). Files land as
    ``<work_dir>/input/api/<level>/dca_<year>_<cod_ibge>.json`` in the exact
    shape ``load_year_data`` expects (``{"data": {"items": [...]}}``).

    Args:
        work_dir: Run scratch directory.
        start_year: First year to download (inclusive).
        end_year: Last year to download (inclusive).
        levels: Iterable subset of ``("brasil", "uf", "municipio")``.
        workers: Parallel download threads (each its own session). Default 1.
        force: Re-download even if a file already exists (resumable otherwise).

    Returns:
        The ``input/api`` directory path.
    """
    da = _download_api()
    api_dir = Path(work_dir) / "input" / "api"
    for lvl in levels:
        (api_dir / lvl).mkdir(parents=True, exist_ok=True)

    # The entity list itself needs a session (paginates /entes for municípios).
    listing = da.make_session()
    try:
        entes: list[tuple[str, str]] = []
        if "brasil" in levels:
            entes.append(("brasil", "1"))
        if "uf" in levels:
            entes += [("uf", str(c)) for c in da.UF_CODES]
        if "municipio" in levels:
            for m in da.get_municipios(listing):
                cod = m.get("cod_ibge")
                if cod:
                    entes.append(("municipio", str(cod)))
    finally:
        listing.close()

    years = list(range(start_year, end_year + 1))
    print(
        f"download_window: {len(entes)} entities x {len(years)} years "
        f"({start_year}-{end_year}), workers={workers}"
    )

    if workers <= 1:
        _download_entes_slice(da, api_dir, entes, years, force)
    else:
        chunks = [entes[i::workers] for i in range(workers)]
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(
                    _download_entes_slice, da, api_dir, chunk, years, force
                )
                for chunk in chunks
            ]
            for f in futures:
                f.result()
    return str(api_dir)


# ── clean (reuse the bootstrap builders) ─────────────────────────────────────
def clean_window(
    work_dir: str, api_dir: str, start_year: int, end_year: int, tables
) -> str:
    """Build the requested tables for the window years via the bootstrap.

    Runs ``tables_final.process_year_task`` per year (the same code path
    ``code/build.py`` uses), which writes the builders' CSV output under
    ``<work_dir>/output/<table>/…``. Only window years are built; the legacy
    Finbra path (≤2012) is never touched here — older years come from the cache.

    Crosswalk gaps fail loud: if any crosswalk-backed builder reports unmatched
    ``(ano, estagio, portaria, conta)`` keys, this raises with the offending
    keys grouped by crosswalk file, so a human can extend
    ``models/br_me_siconfi/code/crosswalk/<file>.xlsx`` and re-run.

    Args:
        work_dir: Run scratch directory (output written under ``output/``).
        api_dir: Directory of downloaded JSON, from :func:`download_window`.
        start_year: First window year (inclusive).
        end_year: Last window year (inclusive).
        tables: Table slugs to build.

    Returns:
        The ``<work_dir>/output`` directory path.

    Raises:
        RuntimeError: If any crosswalk gaps are found.
    """
    shared = _shared()
    builders = _build_registry()
    # ``_init_worker`` loads the crosswalk into shared._comp and fixes sys.path.
    shared._init_worker(CODE_DIR, PATH_QUERIES)

    table_configs = [
        (name, first, last, comp)
        for name, (first, last, comp) in builders.items()
        if name in tables
    ]
    path_dados = str(work_dir)

    unmatched: dict[str, list] = {}
    for ano in range(start_year, end_year + 1):
        _, ano_unmatched = shared.process_year_task(
            (ano, str(api_dir), path_dados, PATH_QUERIES, table_configs)
        )
        for comp, df in ano_unmatched.items():
            if comp and df is not None and not df.empty:
                unmatched.setdefault(comp, []).append(df)

    if unmatched:
        blocks = []
        for comp, dfs in sorted(unmatched.items()):
            combined = pd.concat(dfs, ignore_index=True).drop_duplicates()
            blocks.append(
                f"[{comp}.xlsx] {len(combined)} unmatched key(s); sample:\n"
                f"{combined.head(20).to_string(index=False)}"
            )
        raise RuntimeError(
            "SICONFI crosswalk gaps — Tesouro emitted account keys missing "
            "from the compatibilização tables. Add them to "
            "models/br_me_siconfi/code/crosswalk/<file>.xlsx (fill the *_bd "
            "columns) and re-run.\n\n" + "\n\n".join(blocks)
        )

    return os.path.join(path_dados, "output")


# ── CSV -> all-STRING staging parquet ────────────────────────────────────────
def to_staging_parquet(
    output_dir: str, table: str, staging_root: Path
) -> Path | None:
    """Convert one table's builder CSV output to all-STRING staging parquet.

    ``partition_and_save`` drops ``ano`` (and ``sigla_uf`` for non-Brasil
    tables) into the hive path; both are re-injected as columns so the parquet
    files are self-describing — the ``upload_to_gcs`` path reads the staging
    schema from the file (folders are cosmetic), matching ``us_bls_cpi``.

    Staging is all-STRING by house convention (the dbt model ``safe_cast``s
    every column). CSV values are already text, so ``ano`` from the path stays
    ``"2024"`` and a missing ``valor`` stays ``""`` (→ NULL after ``safe_cast``,
    never the literal ``"nan"``).

    Args:
        output_dir: The ``output`` dir from :func:`clean_window`.
        table: Table slug.
        staging_root: Root under which ``<table>/ano=…/…/data.parquet`` is written.

    Returns:
        The table's parquet directory, or ``None`` if the table had no rows.
    """
    src = Path(output_dir) / table
    if not src.exists():
        return None
    tdir = staging_root / table
    n_rows = 0
    for csv_path in sorted(src.glob("**/*.csv")):
        parts = {
            k: v
            for seg in csv_path.parts
            if "=" in seg
            for k, v in [seg.split("=", 1)]
        }
        ano = parts.get("ano")
        sigla_uf = parts.get("sigla_uf")

        df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        if df.empty:
            continue

        if ano is not None:
            df["ano"] = ano
        if sigla_uf is not None:
            df["sigla_uf"] = sigla_uf
        lead = [c for c in ("ano", "sigla_uf") if c in df.columns]
        df = df[lead + [c for c in df.columns if c not in lead]]

        rel = f"ano={ano}" + (f"/sigla_uf={sigla_uf}" if sigla_uf else "")
        pdir = tdir / rel
        pdir.mkdir(parents=True, exist_ok=True)
        schema = pa.schema([pa.field(c, pa.string()) for c in df.columns])
        at = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        pq.write_table(at, pdir / "data.parquet", compression="snappy")
        n_rows += len(df)

    if not n_rows:
        return None
    print(f"{table}: {n_rows:,} rows -> {tdir}")
    return tdir


# ── GCS parquet cache (out-of-window years) ──────────────────────────────────
def _bucket(bucket_name: str):
    """A requester-pays GCS bucket handle billed to the bucket's own project."""
    from google.cloud import storage

    client = storage.Client(project=bucket_name)
    return client.bucket(bucket_name, user_project=bucket_name)


def push_cache(bucket_name: str, table: str, staging_root: Path) -> None:
    """Upload a table's freshly built window-year parquet to the GCS cache.

    Overwrites the same year partitions in the cache so the cache always holds
    the latest cleaned parquet for every year built this run.
    """
    tdir = staging_root / table
    if not tdir.exists():
        return
    bucket = _bucket(bucket_name)
    for pq_path in tdir.glob("**/*.parquet"):
        rel = pq_path.relative_to(staging_root)
        bucket.blob(f"{CACHE_PREFIX}/{rel.as_posix()}").upload_from_filename(
            str(pq_path)
        )


def pull_cache(
    bucket_name: str, table: str, staging_root: Path, before_year: int
) -> None:
    """Download cached parquet for years < ``before_year`` into ``staging_root``.

    These out-of-window years complete the full table for a from-scratch
    overwrite without re-downloading them from the API. Window years are skipped
    (they were just rebuilt fresh and are already present under ``staging_root``).
    """
    bucket = _bucket(bucket_name)
    prefix = f"{CACHE_PREFIX}/{table}/"
    for blob in bucket.client.list_blobs(bucket, prefix=prefix):
        rel = blob.name[len(f"{CACHE_PREFIX}/") :]
        year = None
        for seg in rel.split("/"):
            if seg.startswith("ano="):
                year = int(seg[len("ano=") :])
                break
        if year is None or year >= before_year:
            continue
        dest = staging_root / rel
        dest.parent.mkdir(parents=True, exist_ok=True)
        blob.download_to_filename(str(dest))


def download_prefix(bucket_name: str, prefix: str, dest_dir: Path) -> int:
    """Download every blob under a GCS prefix into ``dest_dir`` (flattened).

    Files are placed by basename (the legacy Excel names ``quadro<year>_<n>.xlsx``
    are unique across the archive), so any nesting under the prefix collapses to
    the flat layout the legacy build globs (``input/municipio/quadro*``).

    Args:
        bucket_name: Source bucket (e.g. ``basedosdados``).
        prefix: Object prefix, e.g. ``raw/br_me_siconfi/1989-2012``.
        dest_dir: Local directory to download into; created if absent.

    Returns:
        Number of files downloaded.
    """
    bucket = _bucket(bucket_name)
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for blob in bucket.client.list_blobs(
        bucket, prefix=prefix.rstrip("/") + "/"
    ):
        fname = blob.name.rsplit("/", 1)[-1]
        if not fname:  # a "directory" placeholder blob
            continue
        blob.download_to_filename(str(dest_dir / fname))
        n += 1
    print(
        f"download_prefix: {n} files from gs://{bucket_name}/{prefix} -> {dest_dir}"
    )
    return n


# ── raw archival (provenance) ────────────────────────────────────────────────
def archive_raw(work_dir: str, bucket_name: str) -> int:
    """Archive the downloaded raw API JSON to ``gs://<bucket>/raw/br_me_siconfi/``.

    A provenance copy of the raw source files, one gzip tarball per year
    (``raw/br_me_siconfi/api/dca_<year>.tar.gz``) holding that year's per-entity
    JSON across all downloaded levels. One upload per year keeps this cheap even
    for the município-heavy window (vs. tens of thousands of per-file uploads).
    Idempotent: re-archiving a year overwrites its tarball.

    Only the freshly downloaded trailing window is archived here; the frozen
    1989-2012 Finbra raw files and out-of-window API years are archived once at
    seed time, alongside the parquet cache seed. Kept separate from the cleaned
    parquet cache, which is a derived artifact rather than raw source data.

    Args:
        work_dir: Run scratch directory (raw JSON under ``input/api``).
        bucket_name: Target bucket — matches the materialization bucket
            (``basedosdados-dev`` or ``basedosdados``).

    Returns:
        Number of year tarballs uploaded.
    """
    api_dir = Path(work_dir) / "input" / "api"
    if not api_dir.exists():
        return 0

    by_year: dict[str, list[Path]] = {}
    for jpath in api_dir.glob("**/*.json"):
        # filename is dca_<year>_<cod_ibge>.json
        parts = jpath.stem.split("_")
        if len(parts) < 3:
            continue
        by_year.setdefault(parts[1], []).append(jpath)

    bucket = _bucket(bucket_name)
    archive_dir = Path(work_dir) / "raw_archive"
    archive_dir.mkdir(parents=True, exist_ok=True)
    n = 0
    for year, files in sorted(by_year.items()):
        tar_path = archive_dir / f"dca_{year}.tar.gz"
        with tarfile.open(str(tar_path), "w:gz") as tar:
            for f in files:
                tar.add(str(f), arcname=str(f.relative_to(api_dir)))
        bucket.blob(
            f"{RAW_PREFIX}/api/dca_{year}.tar.gz"
        ).upload_from_filename(str(tar_path))
        n += 1
        print(
            f"archive_raw: dca_{year}.tar.gz ({len(files)} files) -> "
            f"gs://{bucket_name}/{RAW_PREFIX}/api/"
        )
    return n


def _max_year(staging_root: Path, tables) -> int | None:
    """Latest ``ano=`` partition present across the built tables."""
    years = []
    for table in tables:
        for seg_dir in (staging_root / table).glob("ano=*"):
            years.append(int(seg_dir.name[len("ano=") :]))
    return max(years) if years else None


# ── orchestration ────────────────────────────────────────────────────────────
def assemble(
    work_dir: str,
    api_dir: str,
    start_year: int,
    end_year: int,
    levels,
    use_cache: bool,
    cache_bucket: str,
) -> dict:
    """Build the window years and assemble full all-STRING parquet tables.

    Steps: build the downloaded window via the bootstrap → convert to staging
    parquet → (if ``use_cache``) refresh the cache with the new window years and
    pull the out-of-window years back in, so each table is complete for a
    from-scratch overwrite. Split from :func:`download_window` so a task retry
    on the download does not rebuild.

    Args:
        work_dir: Run scratch directory.
        api_dir: Directory of downloaded JSON, from :func:`download_window`.
        start_year: First window year (inclusive).
        end_year: Last window year (inclusive).
        levels: Government levels to build.
        use_cache: Union older years from the GCS cache (steady state). Set
            False for a bounded dev run that uploads only the window years.
        cache_bucket: Bucket holding the cache (matches the upload bucket).

    Returns:
        ``{table: parquet_dir}`` for every non-empty table, plus ``"max_year"``
        — the latest year present, used to advance the source ``Update``.
    """
    tables = tables_for_levels(levels)
    output_dir = clean_window(work_dir, api_dir, start_year, end_year, tables)

    staging_root = Path(work_dir) / "staging"
    result: dict = {}
    for table in tables:
        to_staging_parquet(output_dir, table, staging_root)
        if use_cache:
            push_cache(cache_bucket, table, staging_root)
            pull_cache(
                cache_bucket, table, staging_root, before_year=start_year
            )
        tdir = staging_root / table
        if tdir.exists() and any(tdir.glob("**/*.parquet")):
            result[table] = str(tdir)

    result["max_year"] = _max_year(staging_root, tables)
    return result


def clean_all(
    work_dir: str,
    start_year: int,
    end_year: int,
    levels,
    use_cache: bool,
    cache_bucket: str,
) -> dict:
    """Download the window then :func:`assemble` — the one-call local entry point.

    The recurring pipeline calls ``download_window`` and ``assemble`` as separate
    tasks (see tasks.py); this composition exists for local runs and tests.
    """
    api_dir = download_window(work_dir, start_year, end_year, levels)
    return assemble(
        work_dir,
        api_dir,
        start_year,
        end_year,
        levels,
        use_cache,
        cache_bucket,
    )


# ── one-time seed: 1989-2012 legacy from raw Excel -> parquet cache ───────────
def seed_legacy_cache(
    work_dir: str,
    raw_bucket: str,
    raw_prefix: str,
    cache_bucket: str,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """Seed the parquet cache with the frozen 1989-2012 legacy years.

    One-time bootstrap so the recurring flow can serve pre-window years from the
    cache without re-downloading. Downloads the raw Excel from
    ``gs://<raw_bucket>/<raw_prefix>/`` and reuses the validated legacy build
    (``_build_legacy`` in the four município builders, via :func:`clean_window`)
    to produce all-STRING parquet, then pushes it to the cache. Fails loud on any
    crosswalk gap, exactly like the recurring clean.

    Must run where the buckets are accessible (the deployed worker for prod).

    Args:
        work_dir: Run scratch directory.
        raw_bucket: Bucket holding the raw legacy Excel.
        raw_prefix: Prefix of the raw legacy Excel (e.g.
            ``raw/br_me_siconfi/1989-2012``).
        cache_bucket: Bucket whose parquet cache is seeded (matches the
            recurring flow's ``cache_bucket``).
        start_year: First legacy year (defaults to ``LEGACY_START_YEAR``).
        end_year: Last legacy year (defaults to ``LEGACY_END_YEAR``).

    Returns:
        ``{table: cache_dir}`` for each legacy table seeded.

    Raises:
        RuntimeError: If no raw files are found under the prefix.
    """
    start_year = start_year or constants.LEGACY_START_YEAR.value
    end_year = end_year or constants.LEGACY_END_YEAR.value
    tables = constants.LEGACY_TABLES.value

    input_municipio = Path(work_dir) / "input" / "municipio"
    if download_prefix(raw_bucket, raw_prefix, input_municipio) == 0:
        raise RuntimeError(
            f"No raw legacy files under gs://{raw_bucket}/{raw_prefix}"
        )
    # An (empty) api dir so the builders' load_year_data glob resolves cleanly;
    # legacy years (<=2012) dispatch to _build_legacy, which reads the Excel.
    api_dir = Path(work_dir) / "input" / "api"
    (api_dir / "municipio").mkdir(parents=True, exist_ok=True)

    output_dir = clean_window(
        work_dir, str(api_dir), start_year, end_year, tables
    )
    staging_root = Path(work_dir) / "staging"
    result: dict = {}
    for table in tables:
        tdir = to_staging_parquet(output_dir, table, staging_root)
        if tdir is not None:
            push_cache(cache_bucket, table, staging_root)
            result[table] = str(tdir)
    return result
