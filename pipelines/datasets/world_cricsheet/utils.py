"""Download + cleaning transform for world_cricsheet (shared by the recurring
pipeline and the one-shot bootstrap in models/world_cricsheet/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
``clean_all`` directly. Column order / types come from the architecture CSVs (the
single source of truth) for ``deliveries`` and ``matches``; ``match_players`` and
``people`` were renamed in the dbt models via SELECT aliases, so their **staging**
column names (what the dbt ``from staging`` reads) differ from the architecture's
final names — see STAGING_COLUMNS below.

Staging parquet is written **all-STRING** (values pass through the real types
first, then cast via arrow): ``upload_to_gcs`` infers the staging schema from a
one-row header that ``gcs.dump_header`` stringifies, so typed parquet is rejected
by BigQuery. This differs from the onboarding upload, which used typed parquet.
See [[project_dump_header_parquet_bug]]. The dbt model ``safe_cast``s every column
back to its real type, so nothing downstream changes.
"""

from __future__ import annotations

import csv
import datetime as dt
import glob
import io
import logging
import os
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.world_cricsheet.constants import constants

log = logging.getLogger("world_cricsheet")

PA = {
    "STRING": pa.string(),
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "DATE": pa.date32(),
}
_ARCH = constants.ARCHITECTURE_DIR.value

# match_players and people were renamed in the dbt models via SELECT aliases
# (team->team_name, player->player_name, player_identifier->person_id;
# identifier->person_id). The staging parquet must keep the RAW names the dbt
# `from staging` references. deliveries and matches use their architecture names
# unchanged (None => read the architecture CSV).
_MATCH_PLAYERS_STAGING = [
    ("year", "INT64"),
    ("match_id", "STRING"),
    ("team", "STRING"),
    ("player", "STRING"),
    ("player_identifier", "STRING"),
]


# ── download ────────────────────────────────────────────────────────────────
def _get(url: str) -> bytes:
    """GET a URL with the browser-like User-Agent, raising on any HTTP error."""
    r = requests.get(
        url, headers={"User-Agent": constants.USER_AGENT.value}, timeout=600
    )
    r.raise_for_status()
    return r.content


def concat_ball_files(all_csv2_dir: Path) -> Path:
    """Concatenate the per-match ball files into a single all_matches.csv.

    The full bundle ships one ``<id>.csv`` (ball-by-ball) and one
    ``<id>_info.csv`` per match but NOT the pre-concatenated all_matches.csv, so
    it is rebuilt here. Every ball file carries the same 27-column header and its
    ``match_id`` in the first column, so the concatenation keeps the header once
    and streams the rest. The per-match ball files are deleted afterwards to free
    disk (the ``_info.csv`` files are kept — matches/match_players need them).

    Args:
        all_csv2_dir: Directory of extracted bundle files.

    Returns:
        Path to the written ``all_matches.csv``.
    """
    out_path = all_csv2_dir / "all_matches.csv"
    ball_files = [
        p
        for p in sorted(glob.glob(os.path.join(all_csv2_dir, "*.csv")))
        if not p.endswith("_info.csv")
        and os.path.basename(p) != "all_matches.csv"
    ]
    header_written = False
    with open(out_path, "wb") as out:
        for p in ball_files:
            with open(p, "rb") as fh:
                first = fh.readline()
                if not header_written:
                    out.write(first)
                    header_written = True
                # else: skip this file's header row
                while True:
                    buf = fh.read(1 << 20)
                    if not buf:
                        break
                    out.write(buf)
    for p in ball_files:
        os.remove(p)
    log.info(f"all_matches.csv: concatenated {len(ball_files):,} ball files")
    return out_path


def download_bundle(input_dir: Path) -> Path:
    """Download and stage the Cricsheet bundle + person registry.

    Fetches ``all_csv2.zip``, extracts it into ``input_dir/all_csv2/``, rebuilds
    all_matches.csv from the per-match ball files, and fetches ``people.csv`` into
    ``input_dir/people.csv``. After this, ``input_dir`` has exactly what
    :func:`clean_all` reads: ``all_csv2/all_matches.csv``,
    ``all_csv2/<id>_info.csv`` and ``people.csv``.

    Args:
        input_dir: Directory to download into; created if absent.

    Returns:
        The same ``input_dir``, for chaining.

    Raises:
        requests.HTTPError: If any file fails to download.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    all_csv2_dir = input_dir / "all_csv2"
    all_csv2_dir.mkdir(parents=True, exist_ok=True)

    log.info("downloading all_csv2.zip …")
    with zipfile.ZipFile(io.BytesIO(_get(constants.ALL_CSV2_URL.value))) as zf:
        zf.extractall(all_csv2_dir)
    concat_ball_files(all_csv2_dir)

    (input_dir / "people.csv").write_bytes(_get(constants.PEOPLE_URL.value))
    log.info(f"bundle staged -> {input_dir}")
    return input_dir


# ── schema ──────────────────────────────────────────────────────────────────
def read_arch(table: str) -> list[tuple[str, str]]:
    """Read a table's architecture CSV as ``[(name, bigquery_type), ...]``."""
    with open(_ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return [(r["name"], r["bigquery_type"]) for r in csv.DictReader(fh)]


def staging_columns(
    table: str, people_header: list[str] | None = None
) -> list[tuple[str, str]]:
    """Return the STAGING ``[(name, bigquery_type), ...]`` for a table.

    deliveries/matches use their architecture columns unchanged. match_players
    and people carry the raw names the dbt models read (aliased to the final
    names in SQL). people's columns come from the raw ``people.csv`` header (all
    STRING), so a new external-id column added upstream still uploads.

    Args:
        table: Table slug.
        people_header: Raw ``people.csv`` header, required only for ``people``.
    """
    if table in ("deliveries", "matches"):
        return read_arch(table)
    if table == "match_players":
        return _MATCH_PLAYERS_STAGING
    if table == "people":
        if people_header is None:
            raise ValueError("people staging needs the raw header")
        return [(c, "STRING") for c in people_header]
    raise ValueError(table)


# ── casting helpers (ported from the validated bootstrap) ───────────────────
def s_or_none(v: str | None) -> str | None:
    """Empty string / None -> None; else stripped string."""
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else None


def i_or_none(v: str | None) -> int | None:
    """Parse an int, tolerating a trailing ``.0``; blank/garbage -> None."""
    v = s_or_none(v)
    if v is None:
        return None
    try:
        return int(v)
    except ValueError:
        try:
            return int(float(v))
        except ValueError:
            return None


def parse_date_slash(v: str | None) -> dt.date | None:
    """Parse a Cricsheet info-file date ``YYYY/MM/DD`` -> ``datetime.date``."""
    v = s_or_none(v)
    if v is None:
        return None
    try:
        return dt.datetime.strptime(v, "%Y/%m/%d").date()
    except ValueError:
        return None


# ── typed -> all-string arrow writing ───────────────────────────────────────
def _typed_arrays(df: pd.DataFrame, cols: list[tuple[str, str]]) -> pa.Table:
    """Build a typed arrow table from a frame, per the staging column types."""
    arrays = []
    for name, bqt in cols:
        col = df[name]
        if bqt == "INT64":
            arrays.append(
                pa.array(
                    pd.to_numeric(col, errors="coerce").astype("Int64"),
                    type=pa.int64(),
                )
            )
        elif bqt == "DATE":
            arrays.append(pa.array(col.tolist(), type=pa.date32()))
        else:  # STRING
            s = col.astype("string").str.strip()
            s = s.mask(s == "", pd.NA)
            arrays.append(pa.array(s, type=pa.string()))
    return pa.Table.from_arrays(
        arrays, schema=pa.schema([(n, PA[t]) for n, t in cols])
    )


def _to_string_table(
    df: pd.DataFrame, cols: list[tuple[str, str]]
) -> pa.Table:
    """Typed arrow table cast to an all-STRING schema (NULL-preserving).

    Values pass through the real types first (so ``year`` serializes ``"2016"``
    not ``"2016.0"`` and a DATE becomes ``"2016-06-11"``), then arrow ``cast``
    to string keeps NULLs as NULL — never the literal ``"nan"`` ``astype(str)``
    would produce.
    """
    typed = _typed_arrays(df, cols)
    string_schema = pa.schema([(n, pa.string()) for n, _ in cols])
    return typed.cast(string_schema)


# ── table builders (ported from the validated bootstrap) ────────────────────
MATCH_SCALARS = {
    "gender",
    "season",
    "match_number",
    "event",
    "city",
    "venue",
    "toss_winner",
    "toss_decision",
    "winner",
    "method",
    "match_type",
    "team_type",
    "tv_umpire",
    "reserve_umpire",
    "match_referee",
    "outcome",
    "winner_innings",
    "neutral_venue",
}


def parse_info_file(path: str) -> tuple[dict, list[dict]]:
    """Parse one ``<id>_info.csv`` into a match dict + player rows.

    The match dict uses the ``matches`` architecture column names; the player
    rows use the ``match_players`` STAGING names (``team``, ``player``,
    ``player_identifier``). ``year`` is the year of the earliest date; multi-day
    Tests give ``start_date`` = min and ``end_date`` = max.

    Returns:
        ``(match_dict, [player_dict, ...])``. ``match_dict`` carries a transient
        ``_n_pom`` (count of player_of_match rows) the caller pops.
    """
    match_id = os.path.basename(path)[: -len("_info.csv")]

    teams: list[str] = []
    dates: list[dt.date] = []
    umpires: list[str] = []
    poms: list[str] = []
    players: list[tuple[str, str]] = []
    registry: dict[str, str] = {}
    scalars: dict[str, str] = {}
    ints: dict[str, int] = {}
    targets: dict[str, dict[str, int]] = {
        "target_runs": {},
        "target_overs": {},
    }

    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if not row or row[0] != "info" or len(row) < 2:
                continue
            field = row[1]
            val = row[2] if len(row) > 2 else None

            if field == "team":
                if val:
                    teams.append(val.strip())
            elif field == "date":
                d = parse_date_slash(val)
                if d is not None:
                    dates.append(d)
            elif field == "umpire":
                if val:
                    umpires.append(val.strip())
            elif field == "player_of_match":
                if val:
                    poms.append(val.strip())
            elif field in ("player", "players"):
                team = s_or_none(val)
                name = s_or_none(row[3] if len(row) > 3 else None)
                if name is not None:
                    # pyrefly: ignore [bad-argument-type]
                    players.append((team, name))
            elif field == "registry" and val == "people":
                nm = s_or_none(row[3] if len(row) > 3 else None)
                ident = s_or_none(row[4] if len(row) > 4 else None)
                if nm is not None:
                    # pyrefly: ignore [unsupported-operation]
                    registry[nm] = ident
            elif field in (
                "balls_per_over",
                "overs",
                "winner_runs",
                "winner_wickets",
            ):
                iv = i_or_none(val)
                if iv is not None:
                    ints[field] = iv
            elif field in ("target_runs", "target_overs"):
                innings = s_or_none(val)
                tv = i_or_none(row[3] if len(row) > 3 else None)
                if innings is not None and tv is not None:
                    targets[field][innings] = tv
            elif field in MATCH_SCALARS:
                sv = s_or_none(val)
                if sv is not None:
                    scalars[field] = sv  # last wins

    start_date = min(dates) if dates else None
    end_date = max(dates) if dates else None
    year = start_date.year if start_date is not None else None

    match = {
        "year": year,
        "match_id": match_id,
        "season": scalars.get("season"),
        "start_date": start_date,
        "end_date": end_date,
        "match_type": scalars.get("match_type"),
        "team_type": scalars.get("team_type"),
        "gender": scalars.get("gender"),
        "event": scalars.get("event"),
        "match_number": scalars.get("match_number"),
        "balls_per_over": ints.get("balls_per_over"),
        "overs": ints.get("overs"),
        "venue": scalars.get("venue"),
        "city": scalars.get("city"),
        "team1": teams[0] if len(teams) >= 1 else None,
        "team2": teams[1] if len(teams) >= 2 else None,
        "toss_winner": scalars.get("toss_winner"),
        "toss_decision": scalars.get("toss_decision"),
        "player_of_match": poms[0] if poms else None,
        "winner": scalars.get("winner"),
        "outcome": scalars.get("outcome"),
        "winner_runs": ints.get("winner_runs"),
        "winner_wickets": ints.get("winner_wickets"),
        "winner_innings": scalars.get("winner_innings"),
        "method": scalars.get("method"),
        "target_runs": targets["target_runs"].get("2"),
        "target_overs": targets["target_overs"].get("2"),
        "neutral_venue": scalars.get("neutral_venue"),
        "umpire1": umpires[0] if len(umpires) >= 1 else None,
        "umpire2": umpires[1] if len(umpires) >= 2 else None,
        "tv_umpire": scalars.get("tv_umpire"),
        "reserve_umpire": scalars.get("reserve_umpire"),
        "match_referee": scalars.get("match_referee"),
        "_n_pom": len(poms),
    }
    player_rows = [
        {
            "year": year,
            "match_id": match_id,
            "team": team,
            "player": name,
            "player_identifier": registry.get(name),
        }
        for team, name in players
    ]
    return match, player_rows


# ── deliveries (streaming, per-year all-string writer) ──────────────────────
def run_deliveries(all_matches_csv: Path, output_dir: Path) -> dict[int, int]:
    """Build the ``deliveries`` table, streaming all_matches.csv by year.

    ``year`` and a proper ``start_date`` are parsed from the ``YYYY-MM-DD``
    ``start_date`` column; rows are grouped by year and written as all-STRING
    Snappy Parquet, hive-partitioned. Returns rows-per-year.
    """
    cols = staging_columns("deliveries")
    string_schema = pa.schema([(n, pa.string()) for n, _ in cols])
    out_root = output_dir / "deliveries"
    out_root.mkdir(parents=True, exist_ok=True)

    writers: dict[int, pq.ParquetWriter] = {}
    counts: dict[int, int] = {}
    total = 0

    reader = pd.read_csv(
        all_matches_csv,
        dtype=str,
        keep_default_na=False,
        na_values=[],
        chunksize=500_000,
    )
    for chunk in reader:
        sd = pd.to_datetime(
            chunk["start_date"], format="%Y-%m-%d", errors="coerce"
        )
        chunk = chunk.copy()
        chunk["year"] = sd.dt.year.astype("Int64")
        chunk["start_date"] = sd.dt.date
        for yr, grp in chunk.groupby("year", dropna=False):
            if pd.isna(yr):
                raise ValueError(
                    f"delivery rows with unparseable year: {len(grp)}"
                )
            yr = int(yr)
            table = _to_string_table(grp, cols)
            if yr not in writers:
                ydir = out_root / f"year={yr}"
                ydir.mkdir(parents=True, exist_ok=True)
                writers[yr] = pq.ParquetWriter(
                    ydir / "data.parquet", string_schema, compression="snappy"
                )
            writers[yr].write_table(table)
            counts[yr] = counts.get(yr, 0) + len(grp)
            total += len(grp)
    for w in writers.values():
        w.close()
    log.info(f"deliveries: {total:,} rows across {len(counts)} years")
    return counts


# ── matches + match_players (loop over info files) ──────────────────────────
def _write_partitioned_rows(
    rows: list[dict], table: str, output_dir: Path
) -> dict[int, int]:
    """Write per-year all-STRING parquet for a small table of row dicts."""
    cols = staging_columns(table)
    out_root = output_dir / table
    out_root.mkdir(parents=True, exist_ok=True)
    by_year: dict[object, list[dict]] = {}
    for r in rows:
        by_year.setdefault(r["year"], []).append(r)
    counts: dict[int, int] = {}
    for yr, grp in by_year.items():
        if yr is None:
            raise ValueError(f"{table}: {len(grp)} rows with null year")
        # pyrefly: ignore [bad-argument-type]
        yr = int(yr)
        df = pd.DataFrame(grp, columns=[n for n, _ in cols])
        at = _to_string_table(df, cols)
        ydir = out_root / f"year={yr}"
        ydir.mkdir(parents=True, exist_ok=True)
        pq.write_table(at, ydir / "data.parquet", compression="snappy")
        counts[yr] = len(grp)
    return counts


def run_matches_and_players(
    all_csv2_dir: Path, output_dir: Path
) -> tuple[dict[int, int], dict[int, int], dt.date | None]:
    """Build ``matches`` and ``match_players`` from every ``<id>_info.csv``.

    Returns ``(matches_counts, players_counts, max_start_date)``; the last is the
    latest match start date across the bundle — the source's max coverage date,
    used to poll/commit the source Update.
    """
    info_files = sorted(glob.glob(os.path.join(all_csv2_dir, "*_info.csv")))
    match_rows: list[dict] = []
    player_rows: list[dict] = []
    max_start: dt.date | None = None

    for path in info_files:
        match, players = parse_info_file(path)
        match.pop("_n_pom", None)
        match_rows.append(match)
        player_rows.extend(players)
        sd = match["start_date"]
        if sd is not None and (max_start is None or sd > max_start):
            max_start = sd

    m_counts = _write_partitioned_rows(match_rows, "matches", output_dir)
    p_counts = _write_partitioned_rows(
        player_rows, "match_players", output_dir
    )
    return m_counts, p_counts, max_start


# ── people (single dimension file, all STRING) ──────────────────────────────
def run_people(people_csv: Path, output_dir: Path) -> int:
    """Build the ``people`` dimension table (single all-STRING parquet).

    Staging keeps the raw ``people.csv`` header (``identifier`` etc.); the dbt
    model aliases ``identifier`` -> ``person_id``.
    """
    df = pd.read_csv(
        people_csv, dtype=str, keep_default_na=False, na_values=[]
    )
    cols = staging_columns("people", people_header=list(df.columns))
    at = _to_string_table(df, cols)
    out_dir = output_dir / "people"
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(at, out_dir / "data.parquet", compression="snappy")
    return at.num_rows


# ── entry point ─────────────────────────────────────────────────────────────
def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Build all four tables from a staged ``input_dir``.

    The single entry point shared by the recurring pipeline (via
    :func:`pipelines.datasets.world_cricsheet.tasks.clean_cricsheet`) and the
    one-shot bootstrap in ``models/world_cricsheet/code/``. Reads
    ``input_dir/all_csv2/all_matches.csv``, ``input_dir/all_csv2/<id>_info.csv``
    and ``input_dir/people.csv``.

    Args:
        input_dir: Staged bundle root (see :func:`download_bundle`).
        output_dir: Root output directory.

    Returns:
        Mapping of table slug to output directory, plus ``"max_start_date"`` —
        the latest match ``start_date`` as ``"YYYY-MM-DD"`` (or None), which
        drives the source-update poll, and ``"counts"`` (rows per table).
    """
    input_dir = Path(input_dir)
    output_dir = Path(output_dir)
    all_csv2_dir = input_dir / "all_csv2"

    d_counts = run_deliveries(all_csv2_dir / "all_matches.csv", output_dir)
    m_counts, p_counts, max_start = run_matches_and_players(
        all_csv2_dir, output_dir
    )
    n_people = run_people(input_dir / "people.csv", output_dir)

    return {
        "deliveries": output_dir / "deliveries",
        "matches": output_dir / "matches",
        "match_players": output_dir / "match_players",
        "people": output_dir / "people",
        "max_start_date": max_start.strftime("%Y-%m-%d")
        if max_start
        else None,
        "counts": {
            "deliveries": sum(d_counts.values()),
            "matches": sum(m_counts.values()),
            "match_players": sum(p_counts.values()),
            "people": n_people,
        },
    }
