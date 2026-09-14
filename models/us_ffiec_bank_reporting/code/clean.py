"""Clean every raw source into all-STRING, hive-partitioned Parquet.

Staging is all-STRING by Data Basis convention: the dbt models safe_cast each
column to its architecture type, and pipelines.utils.gcs.dump_header stringifies
the header BigQuery infers the staging schema from, so typed Parquet is rejected.
Values are pushed through their real python types first and cast to string via
arrow, never astype(str) -- which would render a NULL as the literal "nan".

The two fact tables are produced by melting the wide source files. Call Report
schedules and FR Y-9C files are one row per filer and one column per MDRM item,
with the column set changing every few quarters as items are added and retired;
carrying that as a wide table is not possible across 17 and 40 years
respectively. Melting keys every value on its MDRM item code instead, so a
retired item simply stops appearing.

Usage:
    python clean.py [call|bhc|cra|mdrm|all] [--quarters N]
"""

from __future__ import annotations

import csv
import io
import re
import sys
import time
import zipfile
from collections import Counter

import pyarrow as pa
import pyarrow.parquet as pq
from common import (
    BHC_FIRST,
    BHC_LAST,
    CALL_FIRST,
    CALL_LAST,
    CRA_FIRST_YEAR,
    CRA_LAST_YEAR,
    INPUT_DIR,
    OUTPUT_DIR,
    is_numeric_item,
    quarters,
    report_date,
)
from dictionary import rows as dictionary_rows
from mdrm import is_flag_item, load_mdrm, parse_date
from schema_def import columns

# Values the sources use for "nothing here". Kept out of the fact tables so the
# 36% of Call Report cells that carry a value are not buried under blanks.
# CONF marks a figure the filer reported but the FFIEC suppresses as
# confidential; it is a real absence of a public value, not a zero.
NULL_TOKENS = {"", "NA", "N/A", "NULL", ".", "CONF"}


# Some BHCF quarters carry a narrative field longer than csv's default 128 KB
# field cap, which aborts the read mid-file with "field larger than field
# limit". The records are legitimate, so the cap is lifted rather than the rows
# skipped.
csv.field_size_limit(sys.maxsize)


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def string_schema(table: str) -> pa.Schema:
    return pa.schema([pa.field(name, pa.string()) for name in columns(table)])


class PartitionWriter:
    """Append row batches to <output>/<table>/year=YYYY/<name>.parquet.

    One writer per output file. Batches are written as they are produced rather
    than accumulated, which is what keeps a 6-million-row quarter inside a few
    hundred MB of RSS instead of several GB.
    """

    def __init__(self, table: str, year: int, filename: str = "data.parquet"):
        self.schema = string_schema(table)
        self.path = OUTPUT_DIR / table / f"year={year}" / filename
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._writer: pq.ParquetWriter | None = None
        self.rows = 0

    def write(self, cols: dict[str, list]) -> None:
        n = len(next(iter(cols.values()))) if cols else 0
        if not n:
            return
        table = pa.table(
            {
                f.name: pa.array(cols[f.name], type=pa.string())
                for f in self.schema
            },
            schema=self.schema,
        )
        if self._writer is None:
            self._writer = pq.ParquetWriter(
                self.path, self.schema, compression="snappy"
            )
        self._writer.write_table(table)
        self.rows += n

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        elif self.path.exists():
            self.path.unlink()


def _s(value: str | None) -> str | None:
    """Trim, and map the source's null tokens to a real NULL."""
    if value is None:
        return None
    value = value.strip()
    return None if value in NULL_TOKENS else value


def _zero_to_null(value: str | None) -> str | None:
    """Identifier fields the Call Report pads with zeros when absent."""
    value = _s(value)
    if value is None or not value.strip("0"):
        return None
    return value.lstrip("0") or None


# Items the filer answers yes/no. The MDRM types them "financial" and their
# names give nothing away -- "OTHER EXPLANATIONS", "NO COMMENT ON THE BANK
# MANAGEMENT STATEMENT" -- so they are recognised from the value itself rather
# than from the name, and kept out of the FLOAT64 fact tables.
BOOLEAN_TOKENS = {"TRUE", "FALSE", "Y", "N", "YES", "NO"}


def _num(value: str) -> float | None:
    """Parse a reported value, or None if it is not a number.

    Percentage items arrive with a trailing per-cent sign ("20.1508%"), and some
    files thousand-separate large figures. Both are stripped before parsing;
    without the per-cent strip every capital ratio in the dataset would be
    dropped as unparseable.
    """
    value = value.strip().rstrip("%").replace(",", "")
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


# --------------------------------------------------------------------------
# MDRM dictionary
# --------------------------------------------------------------------------


def clean_mdrm() -> dict[str, dict]:
    items = load_mdrm()
    writer_dir = OUTPUT_DIR / "mdrm_item"
    writer_dir.mkdir(parents=True, exist_ok=True)
    schema = string_schema("mdrm_item")
    cols: dict[str, list] = {name: [] for name in columns("mdrm_item")}
    for code in sorted(items):
        rec = items[code]
        unit = rec["measurement_unit"]
        cols["item_code"].append(code)
        cols["mnemonic"].append(rec["mnemonic"])
        cols["item_number"].append(rec["item_number"])
        cols["name"].append(rec["name"] or None)
        cols["description"].append(rec["description"] or None)
        cols["item_type"].append(rec["item_type"] or None)
        cols["measurement_unit"].append(unit or None)
        cols["is_flag"].append("1" if is_flag_item(rec["name"], unit) else "0")
        cols["is_confidential"].append(rec["is_confidential"])
        cols["reporting_form"].append(rec["reporting_form"] or None)
        cols["start_date"].append(parse_date(rec["start_date"]) or None)
        cols["end_date"].append(parse_date(rec["end_date"]) or None)
        cols["series_glossary"].append(rec["series_glossary"] or None)
    table = pa.table(
        {f.name: pa.array(cols[f.name], type=pa.string()) for f in schema},
        schema=schema,
    )
    pq.write_table(table, writer_dir / "data.parquet", compression="snappy")
    _log(f"mdrm_item: {len(items):,} items -> {writer_dir}")
    return items


# --------------------------------------------------------------------------
# Call Report
# --------------------------------------------------------------------------

POR_FIELDS = [
    "rssd_id",
    "fdic_cert_id",
    "occ_charter_id",
    "ots_docket_id",
    "aba_routing_id",
    "name",
    "address",
    "city",
    "state_abbreviation",
    "zip_code",
    "call_report_form_id",
    "last_submission_updated_at",
]

SCHEDULE_RE = re.compile(r"Call Schedule ([A-Z0-9]+) ", re.I)


def _tsv_rows(raw: bytes):
    text = raw.decode("utf-8-sig", errors="replace")
    return list(csv.reader(io.StringIO(text), delimiter="\t"))


def clean_call(items: dict[str, dict], limit: int | None = None) -> None:
    unmapped: Counter = Counter()
    unparsed: Counter = Counter()
    boolean: Counter = Counter()
    conflict_log: dict[str, int] = {}
    wanted = quarters(CALL_FIRST, CALL_LAST)
    if limit:
        wanted = wanted[-limit:]
    for year, quarter in wanted:
        src = INPUT_DIR / "call" / f"call_{year}Q{quarter}.zip"
        if not src.exists():
            _log(f"call {year}Q{quarter}: input missing, skipped")
            continue
        rdate = report_date(year, quarter)
        inst = PartitionWriter("institution", year, f"data_q{quarter}.parquet")
        fact = PartitionWriter(
            "call_report_item", year, f"data_q{quarter}.parquet"
        )
        with zipfile.ZipFile(src) as z:
            names = z.namelist()
            por = next((n for n in names if "Call Bulk POR" in n), None)
            if por:
                _write_por(z.read(por), inst, year, quarter, rdate)
            schedule_files = sorted(n for n in names if "Call Schedule" in n)
            owner, shared = _schedule_owner(z, schedule_files)
            if shared:
                conflicts = _check_shared_agree(z, schedule_files, shared)
                if conflicts:
                    _log(
                        f"call {year}Q{quarter}: WARNING {conflicts} shared item "
                        "codes disagree across schedules"
                    )
                    conflict_log[f"{year}Q{quarter}"] = conflicts
            for name in schedule_files:
                match = SCHEDULE_RE.search(name)
                schedule = match.group(1).upper() if match else "UNKNOWN"
                _melt_schedule(
                    z.read(name),
                    fact,
                    year,
                    quarter,
                    rdate,
                    schedule,
                    items,
                    unmapped,
                    unparsed,
                    boolean,
                    owner,
                )
        inst.close()
        fact.close()
        _log(
            f"call {year}Q{quarter}: {inst.rows:,} institutions, {fact.rows:,} item rows"
        )
    _report_skips("call", unmapped, unparsed, boolean)


def _write_por(
    raw: bytes, writer: PartitionWriter, year, quarter, rdate
) -> None:
    rows = _tsv_rows(raw)
    if not rows:
        return
    cols: dict[str, list] = {name: [] for name in columns("institution")}
    for row in rows[1:]:
        if not row or not _s(row[0]):
            continue
        row = row + [""] * (len(POR_FIELDS) - len(row))
        cols["year"].append(str(year))
        cols["quarter"].append(str(quarter))
        cols["report_date"].append(rdate)
        cols["rssd_id"].append(_s(row[0]))
        cols["fdic_cert_id"].append(_zero_to_null(row[1]))
        cols["occ_charter_id"].append(_zero_to_null(row[2]))
        cols["ots_docket_id"].append(_zero_to_null(row[3]))
        cols["aba_routing_id"].append(_zero_to_null(row[4]))
        cols["name"].append(_s(row[5]))
        cols["address"].append(_s(row[6]))
        cols["city"].append(_s(row[7]))
        cols["state_abbreviation"].append(_s(row[8]))
        cols["zip_code"].append(_s(row[9]))
        cols["call_report_form_id"].append(_s(row[10]))
        cols["last_submission_updated_at"].append(_s(row[11]))
    writer.write(cols)


def _schedule_owner(z, names):
    """Assign each item code to exactly one schedule.

    About 7% of Call Report item codes are filed on more than one schedule --
    RCON2170, total assets, appears on both RC and RC-R Part II carrying the
    same value. Left alone that makes (rssd_id, item_code) a non-key and
    silently double-counts those items in any sum over the long table. The
    first schedule in alphabetical order owns the code and the others skip it.
    That the values agree is checked by _check_shared_agree, not assumed.

    Returns (code -> owning schedule, codes appearing on more than one).
    """
    owner: dict[str, str] = {}
    shared: set[str] = set()
    for name in names:
        match = SCHEDULE_RE.search(name)
        schedule = match.group(1).upper() if match else "UNKNOWN"
        with z.open(name) as fh:
            header = fh.readline().decode("utf-8-sig", errors="replace")
        for code in header.rstrip("\r\n").split("\t")[1:]:
            code = code.strip().strip('"').upper()
            if not code:
                continue
            if code in owner:
                if owner[code] != schedule:
                    shared.add(code)
            else:
                owner[code] = schedule
    return owner, shared


def _check_shared_agree(z, names, shared, sample: int = 400) -> int:
    """Count shared item codes whose values differ between schedules.

    Compares the first `sample` filers of each schedule. A non-zero result means
    dropping the duplicate would lose information, and is reported rather than
    swallowed.
    """
    seen: dict[tuple[str, str], str] = {}
    conflicts: set[str] = set()
    for name in names:
        rows = _tsv_rows(z.read(name))
        if len(rows) < 3:
            continue
        header = [c.strip().strip('"').upper() for c in rows[0]]
        idx = [(i, c) for i, c in enumerate(header) if c in shared and i > 0]
        if not idx:
            continue
        for row in rows[2 : 2 + sample]:
            rssd = _s(row[0]) if row else None
            if rssd is None:
                continue
            for i, code in idx:
                if i >= len(row):
                    continue
                value = row[i].strip()
                if value in NULL_TOKENS:
                    continue
                key = (rssd, code)
                if key in seen and seen[key] != value:
                    conflicts.add(code)
                seen[key] = value
    return len(conflicts)


def _melt_schedule(
    raw: bytes,
    writer: PartitionWriter,
    year,
    quarter,
    rdate,
    schedule,
    items,
    unmapped,
    unparsed,
    boolean,
    owner=None,
) -> None:
    """Melt one wide schedule file into (rssd_id, item_code, value) rows.

    Row 1 is the MDRM item codes, row 2 is a short human label the CDR repeats
    for readability, and the data starts on row 3. Reading row 2 as data would
    add one all-text pseudo-institution per schedule per quarter.
    """
    rows = _tsv_rows(raw)
    if len(rows) < 3:
        return
    header = [c.strip().strip('"') for c in rows[0]]
    keep = _numeric_columns(header, items, unmapped)
    if owner is not None:
        keep = [k for k in keep if owner.get(k[1]) == schedule]
    if not keep:
        return
    cols: dict[str, list] = {name: [] for name in columns("call_report_item")}
    for row in rows[2:]:
        if not row:
            continue
        rssd = _s(row[0])
        if rssd is None:
            continue
        for idx, code, is_usd in keep:
            if idx >= len(row):
                continue
            raw_value = row[idx].strip()
            if raw_value in NULL_TOKENS:
                continue
            if raw_value.upper() in BOOLEAN_TOKENS:
                boolean[code] += 1
                continue
            value = _num(raw_value)
            if value is None:
                unparsed[code] += 1
                continue
            if is_usd:
                value *= 1000.0
            cols["year"].append(str(year))
            cols["quarter"].append(str(quarter))
            cols["report_date"].append(rdate)
            cols["rssd_id"].append(rssd)
            cols["schedule"].append(schedule)
            cols["item_code"].append(code)
            cols["value"].append(repr(value) if value % 1 else str(int(value)))
    writer.write(cols)


def _numeric_columns(
    header: list[str], items: dict[str, dict], unmapped: Counter
) -> list[tuple[int, str, bool]]:
    """Pick the columns of a wide file that belong in a FLOAT64 fact table.

    Returns (column index, item code, whether the value needs the thousands
    rescale). Codes the MDRM does not know are counted into `unmapped` and
    dropped rather than guessed at.
    """
    keep: list[tuple[int, str, bool]] = []
    for idx, code in enumerate(header[1:], start=1):
        code = code.strip().strip('"').upper()
        if not code:
            continue
        rec = items.get(code)
        if rec is None:
            unmapped[code] += 1
            continue
        if not is_numeric_item(code, rec["item_type"]):
            continue
        keep.append((idx, code, rec["measurement_unit"] == "USD"))
    return keep


def _report_skips(
    tag: str,
    unmapped: Counter,
    unparsed: Counter,
    boolean: Counter | None = None,
) -> None:
    """Log every item code the MDRM did not resolve, and every unparseable value.

    The brief asks for unmapped codes to be logged rather than silently dropped.
    """
    path = OUTPUT_DIR.parent / f"unmapped_{tag}.csv"
    with open(path, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh, lineterminator="\n")
        w.writerow(["item_code", "reason", "occurrences"])
        for code, n in unmapped.most_common():
            w.writerow([code, "not_in_mdrm", n])
        for code, n in unparsed.most_common():
            w.writerow([code, "value_not_numeric", n])
        for code, n in (boolean or Counter()).most_common():
            w.writerow([code, "boolean_value", n])
    _log(
        f"{tag}: {len(unmapped)} item codes absent from MDRM, "
        f"{len(unparsed)} with unparseable values, "
        f"{len(boolean or ())} carrying yes/no answers -> {path}"
    )


# --------------------------------------------------------------------------
# FR Y-9C holding company financials
# --------------------------------------------------------------------------

# The BHCF structure fields that make up the holding_company roster. Everything
# else in the file is a financial item and goes to holding_company_item.
HC_STRUCTURE = {
    "name": "RSSD9017",
    "short_name": "RSSD9010",
    "address": "RSSD9028",
    "city": "RSSD9130",
    "state_abbreviation": "RSSD9200",
    "zip_code": "RSSD9220",
    "tax_id": "RSSD6191",
    "charter_type_id": "RSSD9048",
    "organization_type_id": "RSSD9047",
    "primary_activity_id": "RSSD9132",
    "federal_reserve_district_id": "RSSD9032",
    "bank_count": "RSSD9146",
    "is_financial_holding_company": "RSSD9016",
    "is_savings_loan_holding_company": "RSSD9198",
}
HC_STATE_FIPS = "RSSD9210"
HC_COUNTY = "RSSD9150"


def _sniff_delimiter(header: str) -> str:
    """Chicago Fed ships comma-separated CSV, FFIEC NPW ships caret-separated."""
    return max(("^", ",", "\t"), key=header.count)


def clean_bhc(items: dict[str, dict], limit: int | None = None) -> None:
    unmapped: Counter = Counter()
    unparsed: Counter = Counter()
    boolean: Counter = Counter()
    wanted = quarters(BHC_FIRST, BHC_LAST)
    if limit:
        wanted = wanted[-limit:]
    for year, quarter in wanted:
        src = INPUT_DIR / "bhc" / f"bhcf_{year}Q{quarter}.txt"
        if not src.exists():
            continue
        rdate = report_date(year, quarter)
        roster = PartitionWriter(
            "holding_company", year, f"data_q{quarter}.parquet"
        )
        fact = PartitionWriter(
            "holding_company_item", year, f"data_q{quarter}.parquet"
        )
        with open(src, encoding="latin-1", newline="") as fh:
            first = fh.readline()
            delim = _sniff_delimiter(first)
            # Column names are lowercase in the Chicago Fed era and uppercase at
            # NPW, and rssd9001/rssd9999 swap positions between the two. Match by
            # upper-cased NAME, never by position.
            header = [
                c.strip().strip('"').upper()
                for c in first.rstrip("\r\n").split(delim)
            ]
            index = {name: i for i, name in enumerate(header)}
            if "RSSD9001" not in index:
                _log(f"bhc {year}Q{quarter}: no RSSD9001 column, skipped")
                roster.close()
                fact.close()
                continue
            rssd_at = index["RSSD9001"]
            # Every column is a candidate here: unlike the Call Report
            # schedules, the BHCF key column is not always first, so index 0
            # must be scanned too.
            keep = _numeric_columns_all(header, items, unmapped)
            struct_at = {
                field: index[code]
                for field, code in HC_STRUCTURE.items()
                if code in index
            }
            state_at = index.get(HC_STATE_FIPS)
            county_at = index.get(HC_COUNTY)
            rcols: dict[str, list] = {
                name: [] for name in columns("holding_company")
            }
            fcols: dict[str, list] = {
                name: [] for name in columns("holding_company_item")
            }
            reader = csv.reader(fh, delimiter=delim)
            for row in reader:
                if len(row) <= rssd_at:
                    continue
                rssd = _s(row[rssd_at])
                if rssd is None:
                    continue
                rcols["year"].append(str(year))
                rcols["quarter"].append(str(quarter))
                rcols["report_date"].append(rdate)
                rcols["rssd_id"].append(rssd)
                for field in HC_STRUCTURE:
                    at = struct_at.get(field)
                    value = (
                        _s(row[at])
                        if at is not None and at < len(row)
                        else None
                    )
                    if field == "tax_id":
                        value = _zero_to_null(value)
                    rcols[field].append(value)
                rcols["county_id"].append(_county_id(row, state_at, county_at))
                for at, code, is_usd in keep:
                    if at >= len(row):
                        continue
                    raw_value = row[at].strip()
                    if raw_value in NULL_TOKENS:
                        continue
                    if raw_value.upper() in BOOLEAN_TOKENS:
                        boolean[code] += 1
                        continue
                    value = _num(raw_value)
                    if value is None:
                        unparsed[code] += 1
                        continue
                    if is_usd:
                        value *= 1000.0
                    fcols["year"].append(str(year))
                    fcols["quarter"].append(str(quarter))
                    fcols["report_date"].append(rdate)
                    fcols["rssd_id"].append(rssd)
                    fcols["item_code"].append(code)
                    fcols["value"].append(
                        repr(value) if value % 1 else str(int(value))
                    )
                if len(fcols["value"]) > 400_000:
                    fact.write(fcols)
                    fcols = {
                        name: [] for name in columns("holding_company_item")
                    }
            roster.write(rcols)
            fact.write(fcols)
        roster.close()
        fact.close()
        _log(
            f"bhc {year}Q{quarter}: {roster.rows:,} companies, {fact.rows:,} item rows"
        )
    _report_skips("bhc", unmapped, unparsed, boolean)


def _numeric_columns_all(
    header: list[str], items: dict[str, dict], unmapped: Counter
) -> list[tuple[int, str, bool]]:
    """_numeric_columns over every column, including the first."""
    keep: list[tuple[int, str, bool]] = []
    for idx, code in enumerate(header):
        code = code.strip().strip('"').upper()
        if not code:
            continue
        rec = items.get(code)
        if rec is None:
            unmapped[code] += 1
            continue
        if not is_numeric_item(code, rec["item_type"]):
            continue
        if rec["is_flag"]:
            continue
        keep.append((idx, code, rec["measurement_unit"] == "USD"))
    return keep


def _county_id(
    row: list[str], state_at: int | None, county_at: int | None
) -> str | None:
    if state_at is None or county_at is None:
        return None
    if state_at >= len(row) or county_at >= len(row):
        return None
    state = _s(row[state_at])
    county = _s(row[county_at])
    if not state or not county:
        return None
    try:
        code = f"{int(state):02d}{int(county):03d}"
    except ValueError:
        return None
    # 00000 is the filer's "not reported", not a county
    return code if code.strip("0") else None


# --------------------------------------------------------------------------
# CRA aggregate & disclosure flat files
# --------------------------------------------------------------------------

# 4 = Office of Thrift Supervision, abolished by Dodd-Frank in July 2011. It
# appears on 3,133 CRA respondents from 1996 to 2010 and is absent afterwards.
# The CURRENT year's file specification lists only 1/2/3 because OTS no longer
# exists -- the 1997-2010 specs list "1=OCC, 2=FRS, 3=FDIC, or 4=OTS". Reading
# only the latest spec leaves 11% of the CRA rows carrying a bare "4".
AGENCY = {"1": "occ", "2": "frs", "3": "fdic", "4": "ots"}
LOAN_TYPE = {"4": "small_business", "5": "small_farm"}
# Purchases are code 6, not 2 -- confirmed against both the file specification
# ("Value is 6 (Purchases)") and the data, where D1-2 and D2-2 carry 6 in every
# record. Assuming 2 nulled 19% of the table.
ACTION_TAKEN = {"1": "origination", "6": "purchase"}
POPULATION = {"S": "under_500k", "L": "over_500k"}

# Five measure bands, each a (count, amount) pair. The third band's upper bound
# differs by loan type -- $1,000,000 for small business, $500,000 for small
# farm -- so the band label is resolved per record rather than from the offset.
BANDS_BUSINESS = [
    "amount_lt_100k",
    "amount_100k_250k",
    "amount_250k_1m",
    "revenue_lt_1m",
    "affiliate",
]
BANDS_FARM = [
    "amount_lt_100k",
    "amount_100k_250k",
    "amount_250k_500k",
    "revenue_lt_1m",
    "affiliate",
]

DISCLOSURE_LENDING = {"D1-1", "D1-2", "D2-1", "D2-2"}


# The CRA flat files changed layout twice, and the offsets from the current
# specification silently match nothing in the earlier eras -- the records are
# simply shorter, so a fixed `len(line) < 145` guard drops all of 1996-2003.
#
#   1996        table id is 4 characters, not 5, so every field shifts left one
#   1996-2003   the MSA/MD field is 4 characters, not 5
#   1996-2003   counts are 6 characters and amounts 8, not 10 and 10
#
# Each era's widths are summed below and checked against the record length seen
# in the files: lending 113 / 114 / 145, tract 45 / 46 / 47. Field widths come
# from the FFIEC "File Specifications" PDF published for each year, whose own
# start/end columns contain a few off-by-one typos -- the widths are consistent,
# the printed offsets are not, so offsets are accumulated from the widths.
def _cra_era(year: int) -> dict:
    table_id = 4 if year <= 1996 else 5
    msa = 4 if year <= 2003 else 5
    count_w, amount_w = (6, 8) if year <= 2003 else (10, 10)

    def spans(widths):
        out, at = {}, 0
        for name, width in widths:
            out[name] = (at, at + width)
            at += width
        out["_end"] = (at, at)
        return out

    lending = spans(
        [
            ("table_id", table_id),
            ("respondent_id", 10),
            ("agency_id", 1),
            ("year", 4),
            ("loan_type", 1),
            ("action_taken", 1),
            ("state_id", 2),
            ("county", 3),
            ("msa_md_id", msa),
            ("assessment_area_id", 4),
            ("is_partial_county", 1),
            ("is_split_county", 1),
            ("population_classification", 1),
            ("tract_income_group", 3),
            ("report_level", 3),
        ]
    )
    bands = []
    at = lending["_end"][0]
    for _ in range(5):
        bands.append((at, at + count_w, at + count_w + amount_w))
        at += count_w + amount_w
    tract = spans(
        [
            ("table_id", table_id),
            ("respondent_id", 10),
            ("agency_id", 1),
            ("year", 4),
            ("state_id", 2),
            ("county", 3),
            ("msa_md_id", msa),
            ("census_tract_id", 7),
            ("assessment_area_id", 4),
            ("is_partial_county", 1),
            ("is_split_county", 1),
            ("population_classification", 1),
            ("tract_income_group", 3),
        ]
    )
    return {
        "lending": lending,
        "bands": bands,
        "lending_len": at,
        "tract": tract,
        "tract_len": tract["_end"][0],
        "table_id_width": table_id,
    }


def _fw(line: str, start: int, end: int) -> str | None:
    return _s(line[start:end])


def _cra_field(value: str | None) -> str | None:
    """Trim a CRA field, keeping "NA" as a real value.

    In the CRA files blank and "NA" mean different things and both appear on
    the same county. For the assessment area number, "NA" is lending OUTSIDE
    any assessment area while blank is the TOTAL across all of them; for
    MSA/MD, "NA" is outside any metropolitan area. Running both through the
    usual null-token map collapses them, which destroys the distinction, makes
    the row key non-unique, and double-counts the total against its own
    components -- verified on respondent 324 in county 42017, where 0001 (1
    loan, $250k) + NA (1 loan, $200k) = blank (2 loans, $450k).
    """
    if value is None:
        return None
    value = value.strip()
    return value or None


def _yn(value: str | None) -> str | None:
    value = _s(value)
    if value is None:
        return None
    return {"Y": "1", "N": "0"}.get(value.upper(), value)


def _fips(value: str | None, width: int) -> str | None:
    """Zero-pad a FIPS code, treating an all-zero field as "not reported".

    The sources write 0 for both state and county when the location is not
    given, which pads to a county_id of "00000" -- a code that exists in no
    directory and produced 1,432 spurious foreign-key failures.
    """
    value = _s(value)
    if value is None or not value.isdigit():
        return None
    if not value.strip("0"):
        return None
    return value.zfill(width)


def clean_cra() -> None:
    lending_rows = tract_rows = respondent_rows = 0
    bad_length: Counter = Counter()
    for year in range(CRA_FIRST_YEAR, CRA_LAST_YEAR + 1):
        rssd_by_key = _cra_transmittal(year, bad_length)
        if rssd_by_key is None:
            continue
        respondent_rows += _write_cra_respondent(year, rssd_by_key)
        lending_rows += _write_cra_lending(year, rssd_by_key, bad_length)
        tract_rows += _write_cra_tract(year, rssd_by_key, bad_length)
    if bad_length:
        _log(
            f"cra: records of unexpected length, by file -> {dict(bad_length)}"
        )
    _log(
        f"cra: {respondent_rows:,} respondents, {lending_rows:,} lending rows, "
        f"{tract_rows:,} assessment-area tract rows"
    )


def _cra_members(year: int, kind: str) -> dict[str, bytes]:
    src = INPUT_DIR / "cra" / f"cra_{year}_{kind}.zip"
    if not src.exists():
        return {}
    with zipfile.ZipFile(src) as z:
        return {
            n: z.read(n) for n in z.namelist() if n.lower().endswith(".dat")
        }


def _lines(raw: bytes):
    for line in raw.decode("latin-1").splitlines():
        if line.strip():
            yield line


_TRANSMITTAL: dict[int, list[dict]] = {}


def _cra_transmittal(
    year: int, bad_length: Counter
) -> dict[tuple[str, str], str] | None:
    """Build the (respondent, agency) -> RSSD crosswalk for one year.

    The CRA respondent identifier is assigned by the supervising agency -- an OCC
    charter number, an FRS RSSD or an FDIC certificate depending on the agency
    code -- so it is NOT an RSSD and cannot be joined to the Call Report on its
    own. The transmittal sheet is the only file that carries ID_RSSD, which makes
    it the required bridge between CRA and every other table here.

    **1996 is the exception**: its transmittal record stops at Tax ID (132
    characters against 152 from 1997 on) and carries neither ID_RSSD nor assets.
    So 1996 CRA rows have a NULL rssd_id by necessity, not by omission, and
    cannot be joined to the Call Report. Every later year is complete.
    """
    members = _cra_members(year, "trans")
    if not members:
        _log(f"cra {year}: no transmittal file, year skipped")
        return None
    has_rssd = year >= 1997
    minimum = 152 if has_rssd else 132
    records: list[dict] = []
    crosswalk: dict[tuple[str, str], str] = {}
    for _, raw in members.items():
        for line in _lines(raw):
            if len(line) < minimum:
                bad_length[f"{year}:trans"] += 1
                continue
            respondent = _fw(line, 0, 10)
            raw_agency = _s(line[10:11])
            agency = AGENCY.get(raw_agency or "", raw_agency)
            rssd = _zero_to_null(line[132:142]) if has_rssd else None
            if respondent is None or agency is None:
                continue
            records.append(
                {
                    "year": str(year),
                    "respondent_id": respondent,
                    "agency_id": agency,
                    "rssd_id": rssd,
                    "name": _fw(line, 15, 45),
                    "address": _fw(line, 45, 85),
                    "city": _fw(line, 85, 110),
                    "state_abbreviation": _fw(line, 110, 112),
                    "zip_code": _fw(line, 112, 122),
                    "tax_id": _fw(line, 122, 132),
                    "total_assets": _thousands(line[142:152])
                    if has_rssd
                    else None,
                }
            )
            if rssd:
                crosswalk[(respondent, agency)] = rssd
    _TRANSMITTAL[year] = records
    if not has_rssd:
        _log(
            f"cra {year}: transmittal carries no ID_RSSD; rssd_id will be NULL"
        )
    return crosswalk


def _thousands(value: str) -> str | None:
    value = _s(value)
    if value is None:
        return None
    number = _num(value)
    if number is None:
        return None
    return str(int(number * 1000))


def _write_cra_respondent(year: int, crosswalk) -> int:
    writer = PartitionWriter("cra_respondent", year)
    cols: dict[str, list] = {name: [] for name in columns("cra_respondent")}
    for rec in _TRANSMITTAL.get(year, []):
        for name in cols:
            cols[name].append(rec.get(name))
    writer.write(cols)
    writer.close()
    return writer.rows


def _write_cra_lending(year: int, crosswalk, bad_length: Counter) -> int:
    era = _cra_era(year)
    lay = era["lending"]
    writer = PartitionWriter("cra_lending", year)
    cols: dict[str, list] = {name: [] for name in columns("cra_lending")}

    def at(line: str, field: str) -> str | None:
        lo, hi = lay[field]
        return _s(line[lo:hi])

    for _, raw in sorted(_cra_members(year, "discl").items()):
        for line in _lines(raw):
            table_id = line[: era["table_id_width"]].strip()
            if table_id not in DISCLOSURE_LENDING:
                continue
            if len(line) < era["lending_len"]:
                bad_length[f"{year}:{table_id}"] += 1
                continue
            respondent = at(line, "respondent_id")
            raw_agency = at(line, "agency_id")
            agency = AGENCY.get(raw_agency or "", raw_agency)
            if respondent is None or agency is None:
                continue
            raw_loan = at(line, "loan_type")
            loan_type = LOAN_TYPE.get(raw_loan or "", raw_loan)
            bands = BANDS_FARM if loan_type == "small_farm" else BANDS_BUSINESS
            state = _fips(at(line, "state_id"), 2)
            county = _fips(at(line, "county"), 3)
            raw_action = at(line, "action_taken")
            raw_pop = at(line, "population_classification")
            common = {
                "year": str(year),
                "respondent_id": respondent,
                "agency_id": agency,
                "rssd_id": crosswalk.get((respondent, agency)),
                "loan_type": loan_type,
                # fall through to the raw code rather than NULL, so a code the
                # source adds later is visible instead of silently lost
                "action_taken": ACTION_TAKEN.get(raw_action or "", raw_action),
                "state_id": state,
                "county_id": f"{state}{county}" if state and county else None,
                "msa_md_id": _cra_field(line[slice(*lay["msa_md_id"])]),
                "assessment_area_id": _cra_field(
                    line[slice(*lay["assessment_area_id"])]
                ),
                "is_partial_county": _yn(at(line, "is_partial_county")),
                "is_split_county": _yn(at(line, "is_split_county")),
                "population_classification": POPULATION.get(
                    (raw_pop or "").upper(), raw_pop
                ),
                "tract_income_group": at(line, "tract_income_group"),
                "report_level": at(line, "report_level"),
            }
            for band, (lo, mid, hi) in zip(bands, era["bands"], strict=False):
                count = _num(line[lo:mid].strip() or "0")
                amount = _num(line[mid:hi].strip() or "0")
                if not count and not amount:
                    # The source pads every band on every record; a band with no
                    # loans and no dollars carries no information and would
                    # multiply the table for nothing.
                    continue
                for key, value in common.items():
                    cols[key].append(value)
                cols["measure_band"].append(band)
                cols["loan_count"].append(str(int(count or 0)))
                cols["loan_amount"].append(str(int((amount or 0) * 1000)))
            if len(cols["measure_band"]) > 500_000:
                writer.write(cols)
                cols = {name: [] for name in columns("cra_lending")}
    writer.write(cols)
    writer.close()
    return writer.rows


def _write_cra_tract(year: int, crosswalk, bad_length: Counter) -> int:
    era = _cra_era(year)
    lay = era["tract"]
    writer = PartitionWriter("cra_assessment_area_tract", year)
    cols: dict[str, list] = {
        name: [] for name in columns("cra_assessment_area_tract")
    }

    def at(line: str, field: str) -> str | None:
        lo, hi = lay[field]
        return _s(line[lo:hi])

    for _, raw in sorted(_cra_members(year, "discl").items()):
        for line in _lines(raw):
            if line[: era["table_id_width"]].strip() != "D6-0":
                continue
            if len(line) < era["tract_len"]:
                bad_length[f"{year}:D6-0"] += 1
                continue
            respondent = at(line, "respondent_id")
            raw_agency = at(line, "agency_id")
            agency = AGENCY.get(raw_agency or "", raw_agency)
            if respondent is None or agency is None:
                continue
            state = _fips(at(line, "state_id"), 2)
            county = _fips(at(line, "county"), 3)
            county_id = f"{state}{county}" if state and county else None
            raw_pop = at(line, "population_classification")
            lo, hi = lay["census_tract_id"]
            cols["year"].append(str(year))
            cols["respondent_id"].append(respondent)
            cols["agency_id"].append(agency)
            cols["rssd_id"].append(crosswalk.get((respondent, agency)))
            cols["state_id"].append(state)
            cols["county_id"].append(county_id)
            cols["msa_md_id"].append(_cra_field(line[slice(*lay["msa_md_id"])]))
            cols["census_tract_id"].append(
                _census_tract(county_id, line[lo:hi])
            )
            cols["assessment_area_id"].append(
                _cra_field(line[slice(*lay["assessment_area_id"])])
            )
            cols["is_partial_county"].append(
                _yn(at(line, "is_partial_county"))
            )
            cols["is_split_county"].append(_yn(at(line, "is_split_county")))
            cols["population_classification"].append(
                POPULATION.get((raw_pop or "").upper(), raw_pop)
            )
            cols["tract_income_group"].append(at(line, "tract_income_group"))
            if len(cols["year"]) > 500_000:
                writer.write(cols)
                cols = {
                    name: [] for name in columns("cra_assessment_area_tract")
                }
    writer.write(cols)
    writer.close()
    return writer.rows


def _census_tract(county_id: str | None, raw: str) -> str | None:
    """Normalise the CRA tract field to the 11-digit census GEOID.

    CRA writes the tract as NNNN.NN -- four digits, a decimal point and two
    suffix digits. The census GEOID drops the point and zero-pads both halves,
    so 0101.01 in county 01001 becomes 01001010101.
    """
    raw = (raw or "").strip()
    if not county_id or not raw or raw.upper() == "NA":
        return None
    if "." in raw:
        head, _, tail = raw.partition(".")
    else:
        head, tail = raw, ""
    if not head.isdigit() or (tail and not tail.isdigit()):
        return None
    return f"{county_id}{int(head):04d}{(tail or '0').ljust(2, '0')[:2]}"


def clean_dictionary() -> None:
    """Write the value -> label table for every dictionary-covered column."""
    writer_dir = OUTPUT_DIR / "dictionary"
    writer_dir.mkdir(parents=True, exist_ok=True)
    schema = string_schema("dictionary")
    entries = dictionary_rows()
    cols = {
        name: [rec[name] or None for rec in entries]
        for name in columns("dictionary")
    }
    table = pa.table(
        {f.name: pa.array(cols[f.name], type=pa.string()) for f in schema},
        schema=schema,
    )
    pq.write_table(table, writer_dir / "data.parquet", compression="snappy")
    _log(f"dictionary: {len(entries)} entries -> {writer_dir}")


def main() -> None:
    what = sys.argv[1] if len(sys.argv) > 1 else "all"
    limit = None
    if "--quarters" in sys.argv:
        limit = int(sys.argv[sys.argv.index("--quarters") + 1])
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    items = clean_mdrm()
    if what in ("all", "mdrm"):
        clean_dictionary()
    if what in ("all", "call"):
        clean_call(items, limit)
    if what in ("all", "bhc"):
        clean_bhc(items, limit)
    if what in ("all", "cra"):
        clean_cra()
    _log("clean done")


if __name__ == "__main__":
    main()
