"""Clean CORA (Congressional Oratory Research Archive) into all-STRING staging parquet.

Usage:
    uv run --no-sync python models/us_nyuad_cora/code/clean_data.py

Reads ``$US_NYUAD_CORA_DATA_DIR/input/Speeches/speeches_YYYY.jsonl`` (extracted
from the figshare zip, https://doi.org/10.6084/m9.figshare.33321423) and writes
``$US_NYUAD_CORA_DATA_DIR/output/speech/year=YYYY/data.parquet``.
``US_NYUAD_CORA_DATA_DIR`` defaults to ``~/Library/Caches/us_nyuad_cora_data``
(kept out of the repo and out of Dropbox).

One JSON record is one speech, and one output row. The transform renames
columns to the architecture, derives ``year`` and ``state_id``, and normalises
empty strings to NULL. Three values are altered, all documented in the
architecture ``observations``:

* ``speaker_party`` D/R/I and ``speaker_gender`` M/F are decoded to labels.
* Citation lists (bills and the three resolution types) with more than
  ``MAX_CITATIONS`` items are set to NULL. CORA expands every "H.R. a - b"
  range into each number in between, so an OCR'd range end produces absurd
  lists: one 1973 speech carries 55,134,284 bills (an 816 MB JSON line, beyond
  BigQuery's 100 MB row limit) and one 2017 speech 379,301.
* ``bioguide_url`` is dropped; it is ``bioguide_id`` behind a fixed URL prefix.
* ``bioguide_id`` "None" (a string in the source) is NULL, and one malformed id
  is truncated to its well-formed prefix (see :func:`bioguide`).

The output directory is deleted before it is rewritten, so a re-run never keeps
parquet produced by an older version of this code. Row counts and the list of
nulled citation fields go to ``output/_manifest_speech.json``.
"""

import csv
import json
import os
import re
import shutil
from multiprocessing import Pool
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = Path(
    os.environ.get(
        "US_NYUAD_CORA_DATA_DIR",
        Path.home() / "Library" / "Caches" / "us_nyuad_cora_data",
    )
)
INPUT = DATA_DIR / "input" / "Speeches"
OUTPUT = DATA_DIR / "output"
TABLE = "speech"
ARCH = Path(__file__).parent / "architecture" / f"{TABLE}.csv"
WORKERS = int(os.environ.get("US_NYUAD_CORA_WORKERS", "6"))

MAX_CITATIONS = 1000
CITATIONS = {
    "bills": "bills",
    "joint_resolutions": "joint_resolution",
    "concurrent_resolutions": "concurrent_resolution",
    "simple_resolutions": "simple_resolution",
}
PARTY = {"D": "Democratic", "R": "Republican", "I": "Independent"}
GENDER = {"M": "Male", "F": "Female"}
# Postal abbreviation -> FIPS, as in br_bd_diretorios_us.state. CORA also uses
# US and DK (Dakota Territory), which have no FIPS code and map to NULL.
STATE_FIPS = {
    "AK": "02", "AL": "01", "AR": "05", "AS": "60", "AZ": "04", "CA": "06",
    "CO": "08", "CT": "09", "DC": "11", "DE": "10", "FL": "12", "FM": "64",
    "GA": "13", "GU": "66", "HI": "15", "IA": "19", "ID": "16", "IL": "17",
    "IN": "18", "KS": "20", "KY": "21", "LA": "22", "MA": "25", "MD": "24",
    "ME": "23", "MH": "68", "MI": "26", "MN": "27", "MO": "29", "MP": "69",
    "MS": "28", "MT": "30", "NC": "37", "ND": "38", "NE": "31", "NH": "33",
    "NJ": "34", "NM": "35", "NV": "32", "NY": "36", "OH": "39", "OK": "40",
    "OR": "41", "PA": "42", "PR": "72", "PW": "70", "RI": "44", "SC": "45",
    "SD": "46", "TN": "47", "TX": "48", "UM": "74", "UT": "49", "VA": "51",
    "VI": "78", "VT": "50", "WA": "53", "WI": "55", "WV": "54", "WY": "56",
}  # fmt: skip


def read_columns() -> list[str]:
    with open(ARCH, newline="", encoding="utf-8") as f:
        return [row["name"] for row in csv.DictReader(f)]


def blank(v):
    """Return a stripped string, or None for null/empty."""
    if v is None:
        return None
    s = str(v).strip()
    return s or None


BIOGUIDE = re.compile(r"[A-Z][0-9]{6}")


def bioguide(v) -> str | None:
    """Return a Bioguide id, or None.

    The source writes the string "None" for 7,665 unlinked speeches, and
    "R000606R" (Jamie Raskin, R000606, with a stray suffix) for 12. Any value
    that does not start with a well-formed id becomes NULL.
    """
    v = blank(v)
    if v is None:
        return None
    m = BIOGUIDE.match(v)
    return m.group(0) if m else None


def citations(v: str | None) -> tuple[str | None, int]:
    """Return the citation list, or None when it exceeds MAX_CITATIONS items."""
    v = blank(v)
    if v is None:
        return None, 0
    n = v.count(",") + 1
    return (None, n) if n > MAX_CITATIONS else (v, n)


def clean_file(path: Path) -> dict:
    m = re.search(r"(\d{4})", path.name)
    if m is None:
        raise ValueError(f"No year in file name {path.name}")
    year = int(m.group(1))
    cols = read_columns()
    data: dict[str, list] = {c: [] for c in cols}
    nulled = []
    with open(path, "rb") as fh:
        for line in fh:
            r = json.loads(line)
            date = blank(r["date"])
            if date is None or int(date[:4]) != year:
                raise ValueError(
                    f"{path.name}: id {r['id']} has date {date!r}"
                )
            state = blank(r["speaker_state"])
            party = blank(r["speaker_party"])
            gender = blank(r["speaker_gender"])
            row = {
                "year": str(year),
                "date": date,
                "congress": blank(r["congress"]),
                "session": blank(r["session"]),
                "chamber": blank(r["chamber"]),
                "speech_id": blank(r["id"]),
                "bioguide_id": bioguide(r["bioguide_id"]),
                "state_id": STATE_FIPS.get(state) if state else None,
                "state_abbreviation": state,
                "speaker_name_raw": blank(r["speaker_raw"]),
                "speaker_first_name": blank(r["speaker_first"]),
                "speaker_last_name": blank(r["speaker_last"]),
                "party": PARTY.get(party, party) if party else None,
                "gender": GENDER.get(gender, gender) if gender else None,
                "cap_major_topic": blank(r["topic_extracted"]),
                "speech_text": r["speaking"] or None,
                "volume": blank(r["volume"]),
                "pages": blank(r["pages"]),
                "source_document_id": blank(r["origin_id"]),
                "source_url": blank(r["origin_url"]),
                "pdf_url": blank(r["pdf_url"]),
            }
            for col, src in CITATIONS.items():
                row[col], n = citations(r[src])
                if n > MAX_CITATIONS:
                    nulled.append(
                        {
                            "speech_id": row["speech_id"],
                            "column": col,
                            "items": n,
                        }
                    )
            del r
            for c in cols:
                data[c].append(row[c])
    schema = pa.schema([pa.field(c, pa.string()) for c in cols])
    table = pa.Table.from_pydict(data, schema=schema)
    pdir = OUTPUT / TABLE / f"year={year}"
    pdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, pdir / "data.parquet", compression="snappy")
    print(f"  {year}: {table.num_rows:,} rows", flush=True)
    return {"year": year, "rows": table.num_rows, "nulled": nulled}


def main():
    files = sorted(
        INPUT.glob("speeches_*.jsonl"), key=lambda p: -p.stat().st_size
    )
    if not files:
        raise FileNotFoundError(f"No speeches_*.jsonl under {INPUT}")
    shutil.rmtree(OUTPUT / TABLE, ignore_errors=True)
    print(
        f"=== cleaning {len(files)} files -> {OUTPUT / TABLE} ===", flush=True
    )
    with Pool(WORKERS, maxtasksperchild=4) as pool:
        results = pool.map(clean_file, files, chunksize=1)
    results.sort(key=lambda r: r["year"])
    manifest = {
        "table": TABLE,
        "rows": sum(r["rows"] for r in results),
        "rows_by_year": {r["year"]: r["rows"] for r in results},
        "max_citations": MAX_CITATIONS,
        "nulled_citations": [n for r in results for n in r["nulled"]],
    }
    (OUTPUT / f"_manifest_{TABLE}.json").write_text(
        json.dumps(manifest, indent=2)
    )
    print(
        f"DONE: {manifest['rows']:,} rows, "
        f"{len(manifest['nulled_citations'])} citation fields nulled"
    )


if __name__ == "__main__":
    main()
