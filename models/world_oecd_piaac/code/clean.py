"""Clean the PIAAC Public Use Files into partitioned Parquet.

Usage:
    uv run python models/world_oecd_piaac/code/clean.py [--only ISO3 ...] [--limit N]

Three properties of the source drive this transform:

1. **Column order differs between country files within a cycle.** Verified on
   prgecup1 vs prgusap1_2012 and prgnzlp2 vs prgusap2, which diverge from
   position 8 while carrying an identical column set. Everything here addresses
   columns by name; a positional read would silently corrupt every country.

2. **Delimiters differ.** Cycle 1 is comma, Cycle 2 is semicolon with quoted
   fields, and the US Round 3 national file is pipe-delimited with uppercase
   column names. The manifest records which.

3. **Reserved codes are padded to the field width.** Hourly earnings carry
   999999999996 for "valid skip". They are nulled on numeric columns using the
   set enumerated from the codebooks, and each column's architecture entry
   records which values were dropped.

Everything is read and written as STRING: staging is all-STRING by house
convention and the dbt models safe_cast each column to its architecture type.
Casting via Arrow rather than astype(str) keeps NULL as NULL instead of the
literal "nan".
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

import codebook as cb
import constants as piaac

ARCHITECTURE_DIR = Path(__file__).parent / "architecture"
GRAIN = [
    "year",
    "cycle",
    "round",
    "country_id_iso_3",
    "country_id_m49",
    "country_entity_id",
    "respondent_id",
]


def architecture_columns(slug: str) -> list[str]:
    with (ARCHITECTURE_DIR / f"{slug}.csv").open(encoding="utf-8") as handle:
        return [row["name"] for row in csv.DictReader(handle)]


def item_metadata(
    cycle: str,
) -> tuple[dict[str, dict[str, str]], dict[str, str], set[str]]:
    """Per-item scoring schemes and per-item assessment domain.

    PIAAC scoring codes are item-specific, so the decoded label has to be looked
    up with the item's own scheme rather than a shared dictionary.
    """
    from build_architecture import split_packed

    variables = cb.load_codebook(
        piaac.DOCS_ROOT / f"cycle_{cycle}" / "international_codebook.xlsx",
        cycle,
    )
    schemes: dict[str, dict[str, str]] = {}
    domains: dict[str, str] = {}
    known: set[str] = set()
    for variable in variables:
        if not variable.is_item:
            continue
        parsed = cb.split_item(variable.name)
        if not parsed:
            continue
        known.add(variable.name.upper())
        domains.setdefault(parsed[0].upper(), variable.domain)
        if parsed[1] == "scored_response":
            scheme = (
                dict(split_packed(variable.value_scheme))
                if variable.value_scheme
                else {}
            )
            for key, label in split_packed(variable.missing_scheme_sas or ""):
                written = cb.sas_code_as_written(key, variable.cycle)
                if written and written != ".":
                    scheme[written] = label
            if scheme:
                schemes[parsed[0].upper()] = scheme

    if cycle == "1":
        # Cycle 1's value_scheme field holds a descriptive name, not pairs; the
        # actual labels live in the Values sheet, like every other Cycle 1
        # variable. They are item-specific there too -- code 8 is "Any other
        # response" on some items and undefined on others.
        schemes.update(cycle_1_item_schemes())
    return schemes, domains, known


def cycle_1_item_schemes() -> dict[str, dict[str, str]]:
    """Scored-response labels for Cycle 1 items, read from the Values sheet."""
    import openpyxl

    workbook = openpyxl.load_workbook(
        piaac.DOCS_ROOT / "cycle_1" / "international_codebook.xlsx",
        read_only=True,
        data_only=True,
    )
    schemes: dict[str, dict[str, str]] = {}
    for row in workbook["Values"].iter_rows(min_row=2, values_only=True):
        if row[0] is None or row[2] is None:
            continue
        name = str(row[0]).strip().upper()
        parsed = cb.split_item(name)
        if not parsed or parsed[1] != "scored_response":
            continue
        key = cb.sas_code_as_written(str(row[2]).strip(), "1")
        if key:
            schemes.setdefault(parsed[0].upper(), {})[key] = str(
                row[1]
            ).strip()
    workbook.close()
    return schemes


def read_puf(path: Path, delimiter: str) -> pd.DataFrame:
    """Read every column as text so that coded values survive untouched."""
    # A bare "." is the SAS system-missing marker -- "not administered" -- and is
    # genuinely absent data, not a value. It is by far the most common token in
    # the Cycle 2 files, because the adaptive design shows each respondent only a
    # fraction of the item pool.
    frame = pd.read_csv(
        path,
        sep=delimiter,
        dtype=str,
        keep_default_na=False,
        na_values=["", "."],
        low_memory=False,
        encoding="utf-8",
        encoding_errors="replace",
    )
    # Cycle 1 lower-cases parts of its names and the US national file upper-cases
    # everything; normalise so lookups are case-insensitive.
    frame.columns = [c.strip().strip('"').upper() for c in frame.columns]
    return frame


def to_string_table(frame: pd.DataFrame, columns: list[str]) -> pa.Table:
    """Reindex by name to the architecture's order, then cast through Arrow.

    Reindexing onto a column the file does not carry produces float NaN, which
    Arrow rejects for a string field, so missing values are normalised to None
    first. Casting through Arrow rather than astype(str) is what keeps a NULL a
    NULL instead of the literal "nan", which safe_cast would not turn back.
    """
    ordered = frame.reindex(columns=columns)
    arrays = {}
    for name in columns:
        values = ordered[name].to_numpy(dtype=object)
        values[pd.isna(values)] = None
        arrays[name] = pa.array(values, type=pa.string())
    return pa.table(arrays)


def write_partition(table: pa.Table, slug: str, year: int, iso3: str) -> Path:
    destination = piaac.OUTPUT_ROOT / slug / f"year={year}"
    destination.mkdir(parents=True, exist_ok=True)
    path = destination / f"{iso3}.parquet"
    pq.write_table(table, path, compression="snappy")
    return path


def clean_file(
    entry: tuple,
    reserved: dict,
    schemes: dict,
    domains: dict,
    item_names: set[str],
) -> dict[str, int]:
    iso3, cycle, round_, remote, delimiter, _zipped, national = entry
    path = piaac.local_puf_path(iso3, cycle, round_, remote)
    if remote.endswith(".zip"):
        path = path.with_suffix(".csv")
    year = piaac.ROUND_YEAR[(cycle, round_)]

    respondent_slug = (
        "respondent_cycle_1_usa_national"
        if national
        else f"respondent_cycle_{cycle}"
    )
    item_slug = f"item_response_cycle_{cycle}"

    raw = read_puf(path, delimiter)
    n_rows = len(raw)

    grain = pd.DataFrame(
        {
            "year": str(year),
            "cycle": cycle,
            "round": round_,
            "country_id_iso_3": iso3,
            "country_id_m49": piaac.COUNTRY_M49[iso3],
            "country_entity_id": raw.get(
                "CNTRYID_E", pd.Series([None] * n_rows)
            ),
            "respondent_id": raw.get("SEQID", pd.Series([None] * n_rows)),
        },
        index=raw.index,
    )

    # --- respondent table ------------------------------------------------
    wanted = architecture_columns(respondent_slug)
    payload = {}
    for name in wanted:
        if name in GRAIN:
            payload[name] = grain[name]
        elif name.upper() in raw.columns:
            payload[name] = raw[name.upper()]
        else:
            payload[name] = pd.Series([None] * n_rows, index=raw.index)
    respondent = pd.DataFrame(payload, index=raw.index)

    # Reserved codes mean "no answer", not a quantity. Nulling them here is what
    # keeps 999999999996 out of every earnings mean downstream.
    for column, codes in reserved.get(respondent_slug, {}).items():
        if column in respondent.columns:
            respondent[column] = respondent[column].where(
                ~respondent[column].isin(set(codes)), other=None
            )

    write_partition(
        to_string_table(respondent, wanted), respondent_slug, year, iso3
    )

    # --- item table ------------------------------------------------------
    measures: dict[str, dict[str, pd.Series]] = {}
    for column in raw.columns:
        # Only split names the codebook lists as item variables. Splitting on the
        # suffix alone invents items out of ordinary respondent columns:
        # CORESTAGE1_PASS becomes item corestage1_pas, IMYRS becomes imyr.
        if column not in item_names:
            continue
        parsed = cb.split_item(column)
        if not parsed:
            continue
        item_code, measure, _bq, _unit = parsed
        if measure not in {
            "scored_response",
            "raw_response",
            "timing_seconds",
            "timing_first_action_seconds",
            "n_actions",
            "n_visits",
            "n_short_visits",
        }:
            continue
        measures.setdefault(item_code, {})[measure] = raw[column]

    frames = []
    item_columns = architecture_columns(item_slug)
    for item_code, by_measure in measures.items():
        block = pd.DataFrame(by_measure, index=raw.index)
        # A respondent sees only a subset of items; keep a row only where the
        # assessment actually recorded something.
        keep = block.notna().any(axis=1)
        if not keep.any():
            continue
        block = block[keep]
        rows = grain[keep].copy()
        rows["item_code"] = item_code.lower()
        rows["domain"] = domains.get(item_code.upper(), "")
        for measure in item_columns:
            if measure in rows.columns or measure == "scored_response_label":
                continue
            rows[measure] = block.get(measure, None)
        scheme = schemes.get(item_code.upper(), {})
        rows["scored_response_label"] = (
            rows["scored_response"].map(scheme) if scheme else None
        )
        frames.append(rows)

    n_items = 0
    if frames:
        items = pd.concat(frames, ignore_index=True)
        n_items = len(items)
        write_partition(
            to_string_table(items, item_columns), item_slug, year, iso3
        )

    return {"respondents": n_rows, "item_rows": n_items}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--only", nargs="*", default=None, help="ISO3 codes to clean"
    )
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()

    reserved = json.loads(
        (ARCHITECTURE_DIR / "reserved_codes.json").read_text()
    )
    metadata = {cycle: item_metadata(cycle) for cycle in ("1", "2")}

    entries = [
        e for e in piaac.PUF_FILES if not args.only or e[0] in args.only
    ]
    if args.limit:
        entries = entries[: args.limit]

    totals = {"respondents": 0, "item_rows": 0}
    for index, entry in enumerate(entries, start=1):
        iso3, cycle, round_, remote, *_ = entry
        path = piaac.local_puf_path(iso3, cycle, round_, remote)
        if remote.endswith(".zip"):
            path = path.with_suffix(".csv")
        if not path.exists():
            print(
                f"[{index:>2}/{len(entries)}] cy{cycle} {iso3}: not downloaded, skipped"
            )
            continue
        counts = clean_file(entry, reserved, *metadata[cycle])
        totals = {k: totals[k] + v for k, v in counts.items()}
        print(
            f"[{index:>2}/{len(entries)}] cy{cycle} r{round_} {iso3}: "
            f"{counts['respondents']:,} respondents, {counts['item_rows']:,} item rows",
            flush=True,
        )
    print(
        f"=== totals: {totals['respondents']:,} respondents, {totals['item_rows']:,} item rows"
    )


if __name__ == "__main__":
    main()
