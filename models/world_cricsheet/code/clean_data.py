"""Clean the Cricsheet global cricket bundle into typed, partitioned Parquet.

Produces four tables, conforming to the architecture CSVs under
``architecture/``:

- ``deliveries``     (partitioned by ``year``) — ball-by-ball, from all_matches.csv
- ``matches``        (partitioned by ``year``) — one row per match, pivoted from
                     each ``<id>_info.csv``
- ``match_players``  (partitioned by ``year``) — one row per (match, team, player)
- ``people``         (single file, dimension)  — from people.csv, all STRING

Typed, one-shot onboarding path: explicit ``pa.Schema`` per table, Snappy Parquet.
Do NOT all-STRING cast (that is the recurring-pipeline path).

Usage:
    python clean_data.py --prototype   # pivot a handful of info files, print, exit
    python clean_data.py               # full run
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import os

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
CODE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_DIR = os.path.join(CODE_DIR, "input")
ALL_CSV2_DIR = os.path.join(INPUT_DIR, "all_csv2")
DELIVERIES_CSV = os.path.join(ALL_CSV2_DIR, "all_matches.csv")
PEOPLE_CSV = os.path.join(INPUT_DIR, "people.csv")
ARCH_DIR = os.path.join(CODE_DIR, "architecture")
OUTPUT_DIR = os.path.join(CODE_DIR, "output")

# ---------------------------------------------------------------------------
# Architecture -> arrow schema
# ---------------------------------------------------------------------------
BQ_TO_ARROW = {
    "INT64": pa.int64(),
    "FLOAT64": pa.float64(),
    "STRING": pa.string(),
    "DATE": pa.date32(),
}


def load_arch(table: str) -> list[tuple[str, str]]:
    """Return [(column_name, bigquery_type), ...] in architecture order."""
    path = os.path.join(ARCH_DIR, f"{table}.csv")
    cols: list[tuple[str, str]] = []
    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            cols.append((row["name"], row["bigquery_type"]))
    return cols


def arrow_schema(table: str) -> pa.Schema:
    return pa.schema(
        [(name, BQ_TO_ARROW[bqt]) for name, bqt in load_arch(table)]
    )


# ---------------------------------------------------------------------------
# Small casting helpers
# ---------------------------------------------------------------------------
def s_or_none(v: str | None) -> str | None:
    """Empty string / None -> None; else stripped string."""
    if v is None:
        return None
    v = v.strip()
    return v if v != "" else None


def i_or_none(v: str | None) -> int | None:
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


def parse_date_slash(v: str | None):
    """Parse Cricsheet info-file date 'YYYY/MM/DD' -> datetime.date."""
    v = s_or_none(v)
    if v is None:
        return None
    try:
        return dt.datetime.strptime(v, "%Y/%m/%d").date()
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Table 2 + 3: parse one info file -> (match_dict, [player_dicts])
# ---------------------------------------------------------------------------
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
    match_id = os.path.basename(path)[: -len("_info.csv")]

    teams: list[str] = []
    dates: list[dt.date] = []
    umpires: list[str] = []
    poms: list[str] = []  # player_of_match (may repeat on ties)
    players: list[tuple[str, str]] = []  # (team, name)
    registry: dict[str, str] = {}  # name -> identifier
    scalars: dict[str, str] = {}
    ints: dict[str, int] = {}
    targets: dict[str, dict[str, int]] = {
        "target_runs": {},
        "target_overs": {},
    }

    with open(path, newline="", encoding="utf-8") as fh:
        for row in csv.reader(fh):
            if not row or row[0] != "info":
                continue  # skip 'version' and blanks
            if len(row) < 2:
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
                # info,player,<team>,<name>
                team = s_or_none(val)
                name = s_or_none(row[3] if len(row) > 3 else None)
                if name is not None:
                    players.append((team, name))
            elif field == "registry" and val == "people":
                # info,registry,people,<name>,<identifier>
                nm = s_or_none(row[3] if len(row) > 3 else None)
                ident = s_or_none(row[4] if len(row) > 4 else None)
                if nm is not None:
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
                # info,target_runs,<innings>,<value>
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


# ---------------------------------------------------------------------------
# Prototype
# ---------------------------------------------------------------------------
def run_prototype() -> None:
    samples = {
        "1000851 (multi-day Test)": "1000851_info.csv",
        "1003869 (T20 no-result / D-L target)": "1003869_info.csv",
        "1023647 (tie)": "1023647_info.csv",
        "1002151 (D/L with winner + target)": "1002151_info.csv",
        "1000853 (won by an innings)": "1000853_info.csv",
        "1239545 (uses 'players' plural)": "1239545_info.csv",
    }
    for label, fname in samples.items():
        path = os.path.join(ALL_CSV2_DIR, fname)
        if not os.path.exists(path):
            print(f"[skip] {label}: {fname} not found")
            continue
        match, players = parse_info_file(path)
        print("=" * 78)
        print(label)
        print("-" * 78)
        for k, v in match.items():
            print(f"  {k:16} = {v!r}")
        print(f"  n_players        = {len(players)}")
        resolved = sum(1 for p in players if p["player_identifier"])
        print(f"  players_resolved = {resolved}/{len(players)}")
        print("  first 3 player rows:")
        for p in players[:3]:
            print(f"    {p}")


# ---------------------------------------------------------------------------
# Table 1: deliveries (streaming, per-year ParquetWriter)
# ---------------------------------------------------------------------------
DELIV_INT_COLS = [
    "runs_off_bat",
    "extras",
    "wides",
    "noballs",
    "byes",
    "legbyes",
    "penalty",
]


def build_deliveries_table(df: pd.DataFrame, schema: pa.Schema) -> pa.Table:
    arrays = []
    for field in schema:
        name = field.name
        col = df[name]
        if name == "year":
            arrays.append(pa.array(col.astype("Int64"), type=pa.int64()))
        elif name == "start_date":
            arrays.append(pa.array(col.tolist(), type=pa.date32()))
        elif name in DELIV_INT_COLS:
            arrays.append(
                pa.array(
                    pd.to_numeric(col, errors="coerce").astype("Int64"),
                    type=pa.int64(),
                )
            )
        else:  # STRING
            s = col.astype("string").str.strip()
            s = s.mask(s == "", pd.NA)
            arrays.append(pa.array(s, type=pa.string()))
    return pa.Table.from_arrays(arrays, schema=schema)


def run_deliveries() -> dict[int, int]:
    schema = arrow_schema("deliveries")
    out_root = os.path.join(OUTPUT_DIR, "deliveries")
    os.makedirs(out_root, exist_ok=True)

    writers: dict[int, pq.ParquetWriter] = {}
    counts: dict[int, int] = {}
    total = 0

    reader = pd.read_csv(
        DELIVERIES_CSV,
        dtype=str,
        keep_default_na=False,
        na_values=[],
        chunksize=500_000,
    )
    for chunk in reader:
        # year + start_date parsed from start_date (YYYY-MM-DD)
        sd = pd.to_datetime(
            chunk["start_date"], format="%Y-%m-%d", errors="coerce"
        )
        chunk = chunk.copy()
        chunk["year"] = sd.dt.year.astype("Int64")
        chunk["start_date"] = sd.dt.date

        for yr, grp in chunk.groupby("year", dropna=False):
            if pd.isna(yr):
                raise ValueError(
                    f"delivery rows with unparseable year: {len(grp)} "
                    f"(sample start_date={grp['start_date'].head().tolist()})"
                )
            yr = int(yr)
            table = build_deliveries_table(grp, schema)
            if yr not in writers:
                ydir = os.path.join(out_root, f"year={yr}")
                os.makedirs(ydir, exist_ok=True)
                writers[yr] = pq.ParquetWriter(
                    os.path.join(ydir, "data.parquet"),
                    schema,
                    compression="snappy",
                )
            writers[yr].write_table(table)
            counts[yr] = counts.get(yr, 0) + len(grp)
            total += len(grp)
        print(f"  deliveries: {total:,} rows processed", end="\r", flush=True)

    for w in writers.values():
        w.close()
    print(f"\n  deliveries: done, {total:,} rows across {len(counts)} years")
    return counts


# ---------------------------------------------------------------------------
# Tables 2 + 3: matches + match_players (loop over info files)
# ---------------------------------------------------------------------------
def build_typed_table(rows: list[dict], schema: pa.Schema) -> pa.Table:
    df = pd.DataFrame(rows, columns=[f.name for f in schema])
    arrays = []
    for field in schema:
        col = df[field.name]
        if field.type == pa.int64():
            arrays.append(
                pa.array(
                    pd.to_numeric(col, errors="coerce").astype("Int64"),
                    type=pa.int64(),
                )
            )
        elif field.type == pa.date32():
            arrays.append(pa.array(col.tolist(), type=pa.date32()))
        else:  # string
            s = col.astype("string")
            s = s.mask(s == "", pd.NA)
            arrays.append(pa.array(s, type=pa.string()))
    return pa.Table.from_arrays(arrays, schema=schema)


def write_partitioned(
    rows: list[dict], table_name: str, schema: pa.Schema
) -> dict[int, int]:
    out_root = os.path.join(OUTPUT_DIR, table_name)
    os.makedirs(out_root, exist_ok=True)
    by_year: dict[object, list[dict]] = {}
    for r in rows:
        by_year.setdefault(r["year"], []).append(r)
    counts: dict[int, int] = {}
    for yr, grp in by_year.items():
        if yr is None:
            raise ValueError(f"{table_name}: {len(grp)} rows with null year")
        yr = int(yr)
        table = build_typed_table(grp, schema)
        ydir = os.path.join(out_root, f"year={yr}")
        os.makedirs(ydir, exist_ok=True)
        pq.write_table(
            table, os.path.join(ydir, "data.parquet"), compression="snappy"
        )
        counts[yr] = len(grp)
    return counts


def run_matches_and_players() -> tuple[
    dict[int, int], dict[int, int], int, int
]:
    matches_schema = arrow_schema("matches")
    players_schema = arrow_schema("match_players")

    info_files = sorted(glob.glob(os.path.join(ALL_CSV2_DIR, "*_info.csv")))
    match_rows: list[dict] = []
    player_rows: list[dict] = []
    multi_pom = 0

    for i, path in enumerate(info_files, 1):
        match, players = parse_info_file(path)
        if match.pop("_n_pom") > 1:
            multi_pom += 1
        match_rows.append(match)
        player_rows.extend(players)
        if i % 2000 == 0:
            print(
                f"  info files: {i:,}/{len(info_files):,}",
                end="\r",
                flush=True,
            )
    print(f"  info files: {len(info_files):,}/{len(info_files):,} done")

    resolved = sum(1 for p in player_rows if p["player_identifier"])
    m_counts = write_partitioned(match_rows, "matches", matches_schema)
    p_counts = write_partitioned(player_rows, "match_players", players_schema)
    return m_counts, p_counts, multi_pom, resolved


# ---------------------------------------------------------------------------
# Table 4: people (single dimension file, all STRING)
# ---------------------------------------------------------------------------
def run_people() -> int:
    schema = arrow_schema("people")
    df = pd.read_csv(
        PEOPLE_CSV, dtype=str, keep_default_na=False, na_values=[]
    )
    # architecture order == source header order; assert alignment
    arch_names = [f.name for f in schema]
    assert list(df.columns) == arch_names, (
        f"people header mismatch:\n  arch={arch_names}\n  csv ={list(df.columns)}"
    )
    arrays = []
    for name in arch_names:
        s = df[name].astype("string").str.strip()
        s = s.mask(s == "", pd.NA)
        arrays.append(pa.array(s, type=pa.string()))
    table = pa.Table.from_arrays(arrays, schema=schema)
    out_dir = os.path.join(OUTPUT_DIR, "people")
    os.makedirs(out_dir, exist_ok=True)
    pq.write_table(
        table, os.path.join(out_dir, "data.parquet"), compression="snappy"
    )
    return table.num_rows


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prototype", action="store_true")
    args = ap.parse_args()

    if args.prototype:
        run_prototype()
        return

    print("[1/4] deliveries")
    d_counts = run_deliveries()

    print("[2/4] matches + [3/4] match_players")
    m_counts, p_counts, multi_pom, resolved = run_matches_and_players()

    print("[4/4] people")
    n_people = run_people()

    # ---------------- validation report ----------------
    print("\n" + "=" * 78)
    print("VALIDATION REPORT")
    print("=" * 78)
    n_deliv = sum(d_counts.values())
    n_match = sum(m_counts.values())
    n_players = sum(p_counts.values())
    print(f"deliveries      rows: {n_deliv:,}")
    print(f"matches         rows: {n_match:,}")
    print(f"match_players   rows: {n_players:,}")
    print(f"people          rows: {n_people:,}")
    print(f"\nmatches with >1 player_of_match: {multi_pom}")
    if n_players:
        print(
            f"match_players with resolved identifier: "
            f"{resolved:,}/{n_players:,} "
            f"({100 * resolved / n_players:.1f}%)"
        )
    print("\ndeliveries rows per year:")
    for yr in sorted(d_counts):
        print(f"  {yr}: {d_counts[yr]:,}")
    print(f"\nyear range deliveries: {min(d_counts)}-{max(d_counts)}")
    print(f"year range matches:    {min(m_counts)}-{max(m_counts)}")


if __name__ == "__main__":
    main()
