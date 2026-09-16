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
import re
import sys
import tarfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import NamedTuple

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

# GCP project ids are 6-30 chars: a leading lowercase letter, then lowercase
# letters / digits / hyphens, no trailing hyphen. Used to guard the project id
# interpolated into the seed_cache_from_bq query (no bind params for identifiers).
_GCP_PROJECT_ID_RE = re.compile(r"[a-z][a-z0-9-]{4,28}[a-z0-9]")


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
    # pyrefly: ignore [missing-import]
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
    # pyrefly: ignore [bad-argument-type]
    module = importlib.util.module_from_spec(spec)
    # pyrefly: ignore [missing-attribute]
    spec.loader.exec_module(module)
    return module.BUILDERS


def _shared():
    """Import the builders' shared worker helpers."""
    _ensure_code_on_path()
    # pyrefly: ignore [missing-import]
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
            # get_municipios() lists ALL entes (União + UFs + municípios). Keep
            # only 7-digit município codes: otherwise the 1-digit (Brasil) and
            # 2-digit (UF) entes are re-fetched into input/api/municipio/ and
            # load_year_data double-counts them into the brasil/uf tables on an
            # all-levels run (the município builders read only 7-digit codes).
            for m in da.get_municipios(listing):
                cod = str(m.get("cod_ibge") or "")
                if len(cod) == 7:
                    entes.append(("municipio", cod))
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


# ── crosswalk preflight ──────────────────────────────────────────────────────
# Tesouro adds account codes to the DCA layout without notice, and a key the
# builders join on that is absent from the compatibilização tables must fail the
# run rather than silently drop or mislabel rows. That check used to live at the
# *end* of the clean — i.e. ~18h into a run that spends almost all of it in the
# API download (flow run 01a059bc, 2026-09-02: download 17h18m, archive 13m,
# clean 24m, then the raise).
#
# The same check now runs in two cheaper places. Both read only the account
# keys; neither builds a row:
#
#   1. :func:`preflight_crosswalk` — *before* the download, against the raw JSON
#      the previous run archived to GCS. Measured on the 2022-2026 window:
#      ~370 MB fetched in ~2 min, 27,990 files scanned in 209 s, 46,291 distinct
#      keys — about 6 minutes against the download's 17h18m. Complete for that
#      snapshot (every entity, not a sample), but blind to keys Tesouro has
#      published since it was written.
#   2. the head of :func:`clean_window` — against this run's own fresh download,
#      before any builder runs. Complete and current, but only reachable once
#      the download has been paid for.
#
# A cheap live probe of Brasil + all 27 UFs (28 calls, ~2 min) was measured and
# rejected as the preflight source: none of the five keys that failed on
# 2026-09-02 appear at those levels — they are município-only — so such a probe
# would have passed and the run would still have died at hour 18.

# ``apply_conta_split``'s pattern: "CODE - NAME" -> (portaria, conta).
_CONTA_SPLIT_RE = re.compile(r"^(\d+(?:\.\d+)*)\s*-?\s*(.*)")

_ACCOUNT_KEY = ("ano", "estagio", "portaria", "conta")


class _CompSpec(NamedTuple):
    """How one crosswalk file is joined, mirrored from its builders.

    Attributes:
        comp_key: Key of this table in the dict ``load_crosswalk`` returns.
        join_cols: Columns the builder merges on (its ``chaves``).
        report_cols: Columns ``get_unmatched`` reports for a gap.
        eoy_only: Builder first filters to the ``31/12/<ano>`` snapshot.
        lstrip_zeros: Builder strips leading zeros from ``portaria`` after the
            conta split.
    """

    comp_key: str
    join_cols: tuple[str, ...]
    report_cols: tuple[str, ...]
    eoy_only: bool = False
    lstrip_zeros: bool = False


# Per crosswalk file — the ``comp_file`` in ``build.py``'s BUILDERS registry, and
# the name printed in the failure. This mirrors each builder's ``chaves``,
# ``get_unmatched`` call and post-split normalisation in code/tables_final/*.py;
# those live inside ``_build_api`` and cannot be imported, so the one thing to
# check when a builder changes is that this table still matches it. The
# ``clean_window`` backstop exists to catch it when it does not.
_COMP_SPEC = {
    "receitas_orcamentarias": _CompSpec(
        "receitas", _ACCOUNT_KEY, _ACCOUNT_KEY
    ),
    "despesas_orcamentarias": _CompSpec(
        "despesas", _ACCOUNT_KEY, _ACCOUNT_KEY
    ),
    # The three despesas_funcao builders alone do
    # ``portaria.str.lstrip("0")`` — the API sends "01.031", the crosswalk
    # holds "1.031". Without it every função code would read as a gap.
    "despesas_funcao": _CompSpec(
        "despesas_funcao", _ACCOUNT_KEY, _ACCOUNT_KEY, lstrip_zeros=True
    ),
    # municipio_balanco_patrimonial keeps only the 31/12 snapshot, merges on
    # (ano, portaria), and reports the gap with ``conta`` appended.
    "balanco_patrimonial": _CompSpec(
        "balanco",
        ("ano", "portaria"),
        ("ano", "portaria", "conta"),
        eoy_only=True,
    ),
}


def _crosswalk_targets(tables) -> dict[tuple[str, str], str]:
    """``{(level, anexo): comp_file}`` for the crosswalk-backed tables in scope.

    ``LEVEL`` / ``ANEXO`` are read from the builder modules and ``comp_file``
    from the BUILDERS registry, so the preflight checks exactly the (level,
    anexo) pairs the builders will join — no second copy of that mapping.
    Tables whose registry entry carries an empty ``comp_file`` do no crosswalk
    join and are skipped.

    Args:
        tables: Table slugs in scope for this run.

    Returns:
        Mapping from ``(level, anexo)`` to the crosswalk file name.
    """
    import importlib

    _ensure_code_on_path()
    builders = _build_registry()
    targets: dict[tuple[str, str], str] = {}
    for table in tables:
        spec = builders.get(table)
        if not spec:
            continue
        comp_file = spec[2]
        if not comp_file:
            continue
        mod = importlib.import_module(f"tables_final.{table}")
        targets[(mod.LEVEL, mod.ANEXO)] = comp_file
    return targets


def _account_keys(items, ano: int, level: str, anexos) -> list[tuple]:
    """Account keys of one entity-year payload, without building a DataFrame.

    Mirrors ``load_year_data`` (``estagio`` is the API's ``coluna``; ``anexo``
    gets the ``DCA-`` prefix) and ``apply_conta_split`` (split ``"CODE - NAME"``
    into portaria/conta, repair the two mojibake dashes). Items whose anexo is
    not checked against a crosswalk are dropped here — that is roughly half of
    them, and skipping them halves the scan.

    Args:
        items: The ``data.items`` list of one downloaded entity-year JSON.
        ano: The year of that payload.
        level: ``brasil`` / ``uf`` / ``municipio``.
        anexos: Anexos worth keeping (the keys of :func:`_crosswalk_targets`).

    Returns:
        ``(ano, level, anexo, estagio, portaria, conta)`` tuples.
    """
    out = []
    for it in items:
        anexo = str(it.get("anexo") or "")
        if not anexo.startswith("DCA-"):
            anexo = "DCA-" + anexo
        if anexo not in anexos:
            continue
        raw = str(it.get("conta") or "")
        m = _CONTA_SPLIT_RE.match(raw)
        if m:
            portaria, conta = m.group(1).strip(), m.group(2).strip()
        else:
            portaria, conta = "", raw.strip()
        conta = conta.replace("�", "-").replace("¿", "-")
        out.append((ano, level, anexo, str(it.get("coluna")), portaria, conta))
    return out


def _level_of_file(name: str, ano: int) -> str:
    """Government level of a ``dca_<year>_<cod_ibge>.json`` file.

    Derived from the entity-code length exactly as ``load_year_data`` does, so
    the preflight buckets a file the same way the builders will.
    """
    cod = name.rsplit("/", 1)[-1][len(f"dca_{ano}_") : -len(".json")]
    n = len(cod)
    return "brasil" if n == 1 else "uf" if n == 2 else "municipio"


def scan_keys_api_dir(api_dir: str, years, levels, anexos) -> set[tuple]:
    """Distinct account keys in a freshly downloaded ``input/api`` tree.

    Args:
        api_dir: Directory from :func:`download_window`.
        years: Window years to scan.
        levels: Government levels to scan.
        anexos: Anexos worth keeping.

    Returns:
        Set of ``(ano, level, anexo, estagio, portaria, conta)``.
    """
    root = Path(api_dir)
    keys: set[tuple] = set()
    for ano in years:
        for lvl in levels:
            for jpath in sorted((root / lvl).glob(f"dca_{ano}_*.json")):
                try:
                    payload = json.loads(jpath.read_text())
                except (json.JSONDecodeError, OSError):
                    continue
                items = payload.get("data", {}).get("items", [])
                if not items:
                    continue
                keys.update(
                    _account_keys(
                        items, ano, _level_of_file(jpath.name, ano), anexos
                    )
                )
    return keys


def scan_keys_archive(
    bucket_name: str, work_dir: str, years, levels, anexos
) -> tuple[set[tuple], list[tuple[int, object]]]:
    """Distinct account keys in the raw JSON a previous run archived to GCS.

    Reads ``gs://<bucket>/<RAW_PREFIX>/api/dca_<year>.tar.gz`` — written by
    :func:`archive_raw` on **every** run, including one that later fails, so the
    snapshot is at most one cycle old. Each tarball is streamed and deleted
    before the next, so peak disk is one year (~95 MB compressed).

    A year that is missing, or that cannot be fetched, is skipped rather than
    fatal, and the caller prints exactly which years were read: the preflight is
    an early-exit optimisation, and :func:`clean_window` still gates the run on
    this run's own download. Degrading to "checked fewer years" is strictly
    better than failing a monthly run on a transient GCS error.

    Args:
        bucket_name: Bucket holding the archive.
        work_dir: Run scratch directory.
        years: Window years to scan.
        levels: Government levels to scan.
        anexos: Anexos worth keeping.

    Returns:
        ``(keys, provenance)`` where provenance is ``(year, blob_updated)`` per
        tarball actually read.
    """
    bucket = _bucket(bucket_name)
    tmp = Path(work_dir) / "preflight"
    tmp.mkdir(parents=True, exist_ok=True)

    keys: set[tuple] = set()
    provenance: list[tuple[int, object]] = []
    wanted_levels = set(levels)
    for ano in years:
        local = tmp / f"dca_{ano}.tar.gz"
        year_keys: set[tuple] = set()
        n_files = 0
        try:
            blob = bucket.blob(f"{RAW_PREFIX}/api/dca_{ano}.tar.gz")
            if not blob.exists():
                print(f"preflight: no archive for {ano}, skipping that year")
                continue
            blob.reload()
            blob.download_to_filename(str(local))
            with tarfile.open(str(local), "r:gz") as tar:
                for member in tar:
                    if not member.isfile():
                        continue
                    level = _level_of_file(member.name, ano)
                    if level not in wanted_levels:
                        continue
                    handle = tar.extractfile(member)
                    if handle is None:
                        continue
                    try:
                        payload = json.loads(handle.read())
                    except json.JSONDecodeError:
                        continue
                    n_files += 1
                    items = payload.get("data", {}).get("items", [])
                    if items:
                        year_keys.update(
                            _account_keys(items, ano, level, anexos)
                        )
        except Exception as exc:
            # A GCS blip or a truncated tarball must not cost a monthly run.
            # The year is simply left unchecked and said so, out loud.
            print(
                f"preflight: could not read the {ano} archive ({exc}); skipping it"
            )
            continue
        finally:
            local.unlink(missing_ok=True)
        keys |= year_keys
        provenance.append((ano, blob.updated))
        print(
            f"preflight: dca_{ano}.tar.gz ({n_files} files, archived "
            f"{blob.updated:%Y-%m-%d}) -> {len(keys):,} keys so far"
        )
    return keys, provenance


def crosswalk_gaps(keys, tables) -> dict[str, pd.DataFrame]:
    """Account keys present in the source but absent from the crosswalk.

    The set-membership equivalent of the builders' left join plus
    ``get_unmatched``: a key that would not find a crosswalk row is a gap.

    Args:
        keys: Iterable of ``(ano, level, anexo, estagio, portaria, conta)``.
        tables: Table slugs in scope.

    Returns:
        ``{comp_file: DataFrame}`` of the distinct unmatched report keys. Empty
        when every key resolves.
    """
    targets = _crosswalk_targets(tables)
    if not targets:
        return {}

    comp = _shared().load_crosswalk(PATH_QUERIES)
    known: dict[str, set[tuple]] = {}
    for comp_file in set(targets.values()):
        spec = _COMP_SPEC[comp_file]
        known[comp_file] = set(
            comp[spec.comp_key][list(spec.join_cols)]
            .astype(str)
            .itertuples(index=False, name=None)
        )

    missing: dict[str, set[tuple]] = {cf: set() for cf in known}
    for ano, level, anexo, estagio, portaria, conta in keys:
        comp_file = targets.get((level, anexo))
        if comp_file is None:
            continue
        spec = _COMP_SPEC[comp_file]
        if spec.eoy_only and estagio != f"31/12/{ano}":
            continue
        row = {
            "ano": str(ano),
            "estagio": estagio,
            "portaria": portaria.lstrip("0")
            if spec.lstrip_zeros
            else portaria,
            "conta": conta,
        }
        if tuple(row[c] for c in spec.join_cols) in known[comp_file]:
            continue
        missing[comp_file].add(tuple(row[c] for c in spec.report_cols))

    return {
        comp_file: pd.DataFrame(
            sorted(rows), columns=list(_COMP_SPEC[comp_file].report_cols)
        ).reset_index(drop=True)
        for comp_file, rows in missing.items()
        if rows
    }


# Cap on how many gap keys are printed per crosswalk file. The operator has to
# add every one of them by hand, so the default is to print them all; the 2,228
# gaps of 2026-07-31 are the reason there is a cap at all.
_MAX_REPORTED_GAPS = 200


def raise_on_crosswalk_gaps(gaps: dict, source: str) -> None:
    """Raise the actionable crosswalk-gap error, or return if there are none.

    Args:
        gaps: ``{comp_file: DataFrame}`` from :func:`crosswalk_gaps`.
        source: What was checked, named in the message — the operator needs to
            know whether the gaps came from this run's download or from the
            previous run's archive.

    Raises:
        RuntimeError: If ``gaps`` is non-empty.
    """
    if not gaps:
        return
    blocks = []
    for comp_file, frame in sorted(gaps.items()):
        shown = frame.head(_MAX_REPORTED_GAPS)
        suffix = (
            f"\n… and {len(frame) - len(shown)} more"
            if len(frame) > len(shown)
            else ""
        )
        blocks.append(
            f"[{comp_file}.xlsx] {len(frame)} unmatched key(s):\n"
            f"{shown.to_string(index=False)}{suffix}"
        )
    raise RuntimeError(
        "SICONFI crosswalk gaps — Tesouro emitted account keys missing "
        "from the compatibilização tables. Add them to "
        "models/br_me_siconfi/code/crosswalk/<file>.xlsx (fill the *_bd "
        f"columns) and re-run.\nSource checked: {source}.\n\n"
        + "\n\n".join(blocks)
    )


def preflight_crosswalk(
    work_dir: str,
    start_year: int,
    end_year: int,
    levels,
    archive_bucket: str,
) -> int:
    """Check the crosswalk before the download, against the archived raw JSON.

    Turns the recurring failure from "18 hours, then a RuntimeError" into
    "~6 minutes, then the same RuntimeError with the same key list". It is an
    early exit, not a new gate: passing here does not prove this run will pass,
    because Tesouro may have published new codes since the archive was written —
    :func:`clean_window` re-checks against the fresh download.

    No archive (a first run, or a bucket that has never been written) is a
    no-op with a printed note, never a failure.

    Args:
        work_dir: Run scratch directory.
        start_year: First window year (inclusive).
        end_year: Last window year (inclusive).
        levels: Government levels this run will build.
        archive_bucket: Bucket holding the raw archive.

    Returns:
        Number of distinct account keys checked (0 when no archive was found).

    Raises:
        RuntimeError: If the archived keys reveal crosswalk gaps.
    """
    tables = tables_for_levels(levels)
    targets = _crosswalk_targets(tables)
    if not targets:
        print("preflight: no crosswalk-backed tables in scope, skipping")
        return 0

    years = list(range(start_year, end_year + 1))
    anexos = {anexo for _, anexo in targets}
    keys, provenance = scan_keys_archive(
        archive_bucket, work_dir, years, levels, anexos
    )
    if not provenance:
        print(
            f"preflight: no raw archive under gs://{archive_bucket}/"
            f"{RAW_PREFIX}/api/ for {start_year}-{end_year}; "
            "skipping (clean_window still checks this run's download)"
        )
        return 0

    checked = ", ".join(
        f"{ano} (archived {when:%Y-%m-%d})" for ano, when in provenance
    )
    print(
        f"preflight: checked {len(keys):,} distinct account keys from "
        f"{checked} against {len(set(targets.values()))} crosswalk file(s)"
    )
    raise_on_crosswalk_gaps(
        crosswalk_gaps(keys, tables),
        f"gs://{archive_bucket}/{RAW_PREFIX}/api/ — {checked}",
    )
    print("preflight: no crosswalk gaps in the archived window")
    return len(keys)


# ── clean (reuse the bootstrap builders) ─────────────────────────────────────
def clean_window(
    work_dir: str, api_dir: str, start_year: int, end_year: int, tables
) -> str:
    """Build the requested tables for the window years via the bootstrap.

    Runs ``tables_final.process_year_task`` per year (the same code path
    ``code/build.py`` uses), which writes the builders' CSV output under
    ``<work_dir>/output/<table>/…``. Only window years are built; the legacy
    Finbra path (≤2012) is never touched here — older years come from the cache.

    Crosswalk gaps fail loud, and they fail **before** any builder runs: the
    downloaded JSON is first scanned for its distinct account keys (a read-only
    pass, no rows built) and checked against the compatibilização tables. That
    turns a gap into a failure a couple of minutes into the clean instead of ~24
    minutes in, after 20M rows of CSV have been written and thrown away, and it
    reports every offending key up front rather than whatever the last year
    happened to surface. The builders' own ``get_unmatched`` result is still
    checked afterwards as the authoritative backstop — it is the code that
    actually performs the join.

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

    # Cheap key-only pre-check over the same JSON the builders are about to
    # read. Complete and current — unlike the archive-based preflight, which
    # cannot see codes published since the previous run.
    levels = sorted({_level_of(t) for t in tables})
    targets = _crosswalk_targets(tables)
    if targets:
        years = list(range(start_year, end_year + 1))
        keys = scan_keys_api_dir(
            api_dir, years, levels, {anexo for _, anexo in targets}
        )
        print(
            f"clean_window: pre-checking {len(keys):,} distinct account keys "
            f"from the fresh download against the crosswalk"
        )
        raise_on_crosswalk_gaps(
            crosswalk_gaps(keys, tables), f"this run's download ({api_dir})"
        )

    table_configs = [
        (name, first, last, comp)
        for name, (first, last, comp) in builders.items()
        if name in tables
    ]
    # pyrefly: ignore [unnecessary-type-conversion]
    path_dados = str(work_dir)

    unmatched: dict[str, list] = {}
    for ano in range(start_year, end_year + 1):
        _, ano_unmatched = shared.process_year_task(
            # pyrefly: ignore [unnecessary-type-conversion]
            (ano, str(api_dir), path_dados, PATH_QUERIES, table_configs)
        )
        for comp, df in ano_unmatched.items():
            if comp and df is not None and not df.empty:
                unmatched.setdefault(comp, []).append(df)

    # Backstop. The pre-check above should already have raised; reaching here
    # with gaps means the key scan drifted from what the builders actually join
    # on, which is worth knowing about rather than silently tolerating.
    raise_on_crosswalk_gaps(
        {
            comp: pd.concat(dfs, ignore_index=True).drop_duplicates()
            for comp, dfs in unmatched.items()
        },
        "the builders' own join (the key pre-check missed these)",
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
    """A requester-pays GCS bucket handle with the right service-account creds.

    Mirrors ``DBTArtifactUploader._init_gcs``: ``basedosdados-dev`` is
    requester-pays and the prod SA lacks ``serviceusage.services.use`` on that
    project, so dev/staging buckets use the staging SA and the prod bucket uses
    the prod SA. Default ADC 403s on the worker. Worker-only — needs the
    ``BASEDOSDADOS_CREDENTIALS_{PROD,STAGING}`` env the deployed pod carries.
    """
    from google.cloud import storage

    from pipelines.utils.gcs import get_credentials_from_env

    mode = "prod" if bucket_name == "basedosdados" else "staging"
    credentials = get_credentials_from_env(mode=mode)
    client = storage.Client(project=bucket_name, credentials=credentials)
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


def seed_cache_from_bq(
    cache_bucket: str,
    tables,
    start_year: int,
    end_year: int,
    bq_project: str = "basedosdados",
) -> dict:
    """Seed the parquet cache for API years from the existing prod BQ tables.

    Complements :func:`seed_legacy_cache`: the API-era cleaned data (2013+)
    already lives in ``<bq_project>.br_me_siconfi.*`` (built by the same code),
    so the out-of-window API years are cached by reading BigQuery and writing the
    **same** all-STRING, ``ano=…[/sigla_uf=…]`` partitioned parquet that
    :func:`to_staging_parquet` produces — far cheaper than re-downloading the
    paginated API. Reads prod BQ with the prod SA; the cache write picks its SA by
    bucket via :func:`_bucket`. Worker-only. One-time bootstrap so the first
    recurring run only has to download the trailing window.

    Args:
        cache_bucket: Bucket whose cache is seeded.
        tables: Table slugs to seed (typically all 19).
        start_year: First year to read (inclusive), e.g. ``API_FIRST_YEAR``.
        end_year: Last year to read (inclusive), i.e. ``window_start - 1``.
        bq_project: Project holding the prod tables.

    Returns:
        ``{table: cache_dir}`` for each table seeded.
    """
    import shutil
    import tempfile

    from google.cloud import bigquery

    from pipelines.utils.gcs import get_credentials_from_env

    # Both identifiers below are interpolated into the query (BigQuery has no
    # bind parameters for project/table names), so validate them before any
    # query runs: bq_project against the GCP id grammar, and every table against
    # the known SICONFI table set. year is a Python int from range(), so it is
    # already safe.
    if not _GCP_PROJECT_ID_RE.fullmatch(bq_project):
        raise ValueError(f"invalid BigQuery project id: {bq_project!r}")
    known_tables = {
        t for lvl in constants.TABLES_BY_LEVEL.value.values() for t in lvl
    }
    unknown = [t for t in tables if t not in known_tables]
    if unknown:
        raise ValueError(f"unknown br_me_siconfi table(s): {unknown}")

    client = bigquery.Client(
        project=bq_project, credentials=get_credentials_from_env(mode="prod")
    )
    work = Path(tempfile.mkdtemp(prefix="br_me_siconfi_bqseed_"))
    staging_root = work / "staging"
    result: dict = {}
    try:
        for table in tables:
            part_uf = _level_of(table) in ("municipio", "uf")
            wrote = False
            for year in range(start_year, end_year + 1):
                at = client.query(
                    f"SELECT * FROM `{bq_project}.br_me_siconfi.{table}` "
                    f"WHERE ano = {year}"
                ).to_arrow()
                if at.num_rows == 0:
                    continue
                # Cast every column to string, preserving NULLs (never "nan"):
                # arrow int64/float64 -> string is exact and null-safe.
                df = at.cast(
                    pa.schema(
                        [pa.field(f.name, pa.string()) for f in at.schema]
                    )
                ).to_pandas()
                lead = [c for c in ("ano", "sigla_uf") if c in df.columns]
                df = df[lead + [c for c in df.columns if c not in lead]]
                groups = df.groupby("sigla_uf") if part_uf else [(None, df)]
                for uf, g in groups:
                    rel = f"ano={year}" + (
                        f"/sigla_uf={uf}" if uf is not None else ""
                    )
                    pdir = staging_root / table / rel
                    pdir.mkdir(parents=True, exist_ok=True)
                    pq.write_table(
                        pa.Table.from_pandas(
                            g,
                            schema=pa.schema(
                                [pa.field(c, pa.string()) for c in g.columns]
                            ),
                            preserve_index=False,
                        ),
                        pdir / "data.parquet",
                        compression="snappy",
                    )
                wrote = True
                print(f"  bq_seed {table} {year}: {at.num_rows:,} rows")
            if wrote:
                push_cache(cache_bucket, table, staging_root)
                result[table] = str(staging_root / table)
    finally:
        shutil.rmtree(work, ignore_errors=True)
    return result
