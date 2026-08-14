"""Clean the Australian Business Register (ABR) ABN Bulk Extract into partitioned Parquet.

Streams <ABR> records straight out of the two source ZIPs (no full extraction to
disk) and writes three typed tables, hive-partitioned by ``extraction_date``:

    output/entity/extraction_date=YYYY-MM-DD/data_*.parquet
    output/other_name/extraction_date=YYYY-MM-DD/data_*.parquet
    output/dgr/extraction_date=YYYY-MM-DD/data_*.parquet

plus a dicionario CSV (code -> label) built from the observed enums.

The ``extraction_date`` value comes from each file's <ExtractTime>. It is encoded
in the output path only (hive style), never in the parquet body, matching the BD
staging convention. A 0-row ``00_header.parquet`` is written first in every
partition dir so the table-approve CI step never OOMs on a large first file.

Env vars:
    ABR_INPUT_DIR   default ~/Downloads/au_ato_abr_data/input
    ABR_OUTPUT_DIR  default ~/Downloads/au_ato_abr_data/output
    ABR_QUICK       if set, process only the first XML member and stop early
                    (writes to a *_quick output dir for inspection)
"""

from __future__ import annotations

import csv
import os
import sys
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from lxml import etree

HOME = Path.home()
INPUT_DIR = Path(
    os.environ.get("ABR_INPUT_DIR", HOME / "Downloads/au_ato_abr_data/input")
)
QUICK = bool(os.environ.get("ABR_QUICK"))
_default_out = (
    HOME
    / "Downloads/au_ato_abr_data"
    / ("output_quick" if QUICK else "output")
)
OUTPUT_DIR = Path(os.environ.get("ABR_OUTPUT_DIR", _default_out))

CHUNK = 400_000  # rows per parquet file (bounds peak RAM)
DATE_SENTINELS = {"19000101", "00000000", "10000101"}

# ---- typed schemas (extraction_date lives in the path, not the file) --------
ENTITY_COLS = [
    "abn",
    "abn_status",
    "abn_status_from_date",
    "entity_type",
    "entity_name",
    "asic_number",
    "asic_number_type",
    "gst_status",
    "gst_status_from_date",
    "state_code",
    "postcode",
    "record_last_updated_date",
    "replaced",
]
ENTITY_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("abn_status", pa.string()),
        ("abn_status_from_date", pa.date32()),
        ("entity_type", pa.string()),
        ("entity_name", pa.string()),
        ("asic_number", pa.string()),
        ("asic_number_type", pa.string()),
        ("gst_status", pa.string()),
        ("gst_status_from_date", pa.date32()),
        ("state_code", pa.string()),
        ("postcode", pa.string()),
        ("record_last_updated_date", pa.date32()),
        ("replaced", pa.string()),
    ]
)
OTHER_NAME_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("name_type", pa.string()),
        ("name", pa.string()),
    ]
)
DGR_SCHEMA = pa.schema(
    [
        ("abn", pa.string()),
        ("dgr_status_from_date", pa.date32()),
        ("dgr_name", pa.string()),
    ]
)


def pdate(s):
    """Parse an ABR YYYYMMDD string to a date, mapping sentinels/garbage to None."""
    if s is None:
        return None
    s = str(s).strip()
    if len(s) != 8 or not s.isdigit() or s in DATE_SENTINELS:
        return None
    try:
        return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
    except ValueError:
        return None


def clean_text(t):
    if t is None:
        return None
    t = t.strip()
    return t or None


class PartitionWriter:
    """Accumulates rows for one table and flushes fixed-size parquet chunks."""

    def __init__(self, name, schema):
        self.name = name
        self.schema = schema
        self.cols = [f.name for f in schema]
        self.buf = {c: [] for c in self.cols}
        self.n = 0
        self.file_idx = 0
        self.part_dirs = set()

    def part_dir(self, extraction_date):
        d = (
            OUTPUT_DIR
            / self.name
            / f"extraction_date={extraction_date.isoformat()}"
        )
        if d not in self.part_dirs:
            d.mkdir(parents=True, exist_ok=True)
            # 0-row header first (guards table-approve CI against OOM on big first file)
            header = pa.table(
                {
                    c: pa.array([], type=self.schema.field(c).type)
                    for c in self.cols
                },
                schema=self.schema,
            )
            pq.write_table(
                header, d / "00_header.parquet", compression="snappy"
            )
            self.part_dirs.add(d)
        return d

    def add(self, extraction_date, row):
        self._ed = extraction_date
        for c in self.cols:
            self.buf[c].append(row.get(c))
        self.n += 1
        if self.n >= CHUNK:
            self.flush()

    def flush(self):
        if self.n == 0:
            return
        d = self.part_dir(self._ed)
        table = pa.table(
            {
                c: pa.array(self.buf[c], type=self.schema.field(c).type)
                for c in self.cols
            },
            schema=self.schema,
        )
        self.file_idx += 1
        pq.write_table(
            table,
            d / f"data_{self.file_idx:05d}.parquet",
            compression="snappy",
        )
        self.buf = {c: [] for c in self.cols}
        self.n = 0


def parse_extract_date(stream):
    """Read just the <ExtractTime> from the head of an XML stream -> date."""
    ctx = etree.iterparse(stream, events=("end",), tag="ExtractTime")
    for _, el in ctx:
        txt = (el.text or "").strip()  # e.g. 2026-08-12T12:26:57
        return date.fromisoformat(txt[:10])
    return None


def process_member(zf, member, writers, entity_types, asic_types, counts):
    entity_w, other_w, dgr_w = writers
    with zf.open(member) as f:
        extraction_date = parse_extract_date(f)
    if extraction_date is None:
        raise RuntimeError(f"No ExtractTime in {member}")
    with zf.open(member) as f:
        ctx = etree.iterparse(f, events=("end",), tag="ABR")
        for _, abr in ctx:
            abn_el = abr.find("ABN")
            abn = clean_text(abn_el.text) if abn_el is not None else None

            et = abr.find("EntityType")
            entity_type = (
                clean_text(et.findtext("EntityTypeInd"))
                if et is not None
                else None
            )
            et_text = (
                clean_text(et.findtext("EntityTypeText"))
                if et is not None
                else None
            )
            if entity_type and et_text and entity_type not in entity_types:
                entity_types[entity_type] = et_text

            # name + address: MainEntity (non-individual) XOR LegalEntity (individual)
            main = abr.find("MainEntity")
            legal = abr.find("LegalEntity")
            entity_name = None
            addr = None
            if main is not None:
                entity_name = clean_text(
                    main.findtext("NonIndividualName/NonIndividualNameText")
                )
                addr = main.find("BusinessAddress/AddressDetails")
            elif legal is not None:
                ind = legal.find("IndividualName")
                if ind is not None:
                    parts = [
                        clean_text(g.text) for g in ind.findall("GivenName")
                    ]
                    parts.append(clean_text(ind.findtext("FamilyName")))
                    entity_name = " ".join(p for p in parts if p) or None
                addr = legal.find("BusinessAddress/AddressDetails")

            state_code = (
                clean_text(addr.findtext("State"))
                if addr is not None
                else None
            )
            postcode = (
                clean_text(addr.findtext("Postcode"))
                if addr is not None
                else None
            )

            asic_el = abr.find("ASICNumber")
            asic_number = (
                clean_text(asic_el.text) if asic_el is not None else None
            )
            asic_number_type = (
                asic_el.get("ASICNumberType") if asic_el is not None else None
            )
            asic_number_type = clean_text(asic_number_type)
            if asic_number_type:
                asic_types.add(asic_number_type)

            gst_el = abr.find("GST")
            gst_status = (
                clean_text(gst_el.get("status"))
                if gst_el is not None
                else None
            )
            gst_from = (
                pdate(gst_el.get("GSTStatusFromDate"))
                if gst_el is not None
                else None
            )

            entity_w.add(
                extraction_date,
                {
                    "abn": abn,
                    "abn_status": clean_text(abn_el.get("status"))
                    if abn_el is not None
                    else None,
                    "abn_status_from_date": pdate(
                        abn_el.get("ABNStatusFromDate")
                    )
                    if abn_el is not None
                    else None,
                    "entity_type": entity_type,
                    "entity_name": entity_name,
                    "asic_number": asic_number,
                    "asic_number_type": asic_number_type,
                    "gst_status": gst_status,
                    "gst_status_from_date": gst_from,
                    "state_code": state_code,
                    "postcode": postcode,
                    "record_last_updated_date": pdate(
                        abr.get("recordLastUpdatedDate")
                    ),
                    "replaced": clean_text(abr.get("replaced")),
                },
            )
            counts["entity"] += 1

            for oe in abr.findall("OtherEntity"):
                nn = oe.find("NonIndividualName")
                if nn is None:
                    continue
                other_w.add(
                    extraction_date,
                    {
                        "abn": abn,
                        "name_type": clean_text(nn.get("type")),
                        "name": clean_text(
                            nn.findtext("NonIndividualNameText")
                        ),
                    },
                )
                counts["other_name"] += 1

            for dg in abr.findall("DGR"):
                dgr_w.add(
                    extraction_date,
                    {
                        "abn": abn,
                        "dgr_status_from_date": pdate(
                            dg.get("DGRStatusFromDate")
                        ),
                        "dgr_name": clean_text(
                            dg.findtext(
                                "NonIndividualName/NonIndividualNameText"
                            )
                        ),
                    },
                )
                counts["dgr"] += 1

            # free memory: drop the record and its already-processed siblings
            abr.clear()
            parent = abr.getparent()
            if parent is not None:
                while abr.getprevious() is not None:
                    del parent[0]

            if QUICK and counts["entity"] >= 200_000:
                break


# ---- entity_type / other static dictionaries --------------------------------
ABN_STATUS = {"ACT": "Active", "CAN": "Cancelled"}
GST_STATUS = {"ACT": "Registered", "CAN": "Cancelled", "NON": "Not registered"}
NAME_TYPE = {
    "TRD": "Trading name",
    "BN": "Business name",
    "OTN": "Other name",
    "MN": "Main name",
    "LGL": "Legal name",
    "DGR": "DGR name",
}
REPLACED = {"Y": "Yes", "N": "No"}
STATE_CODE = {
    "NSW": "New South Wales",
    "VIC": "Victoria",
    "QLD": "Queensland",
    "WA": "Western Australia",
    "SA": "South Australia",
    "TAS": "Tasmania",
    "NT": "Northern Territory",
    "ACT": "Australian Capital Territory",
    "AAT": "Australian Antarctic Territory",
}
ASIC_TYPE_LABELS = {
    "ACN": "Australian Company Number",
    "ARBN": "Australian Registered Body Number",
    "undetermined": "Undetermined",
}


def write_dicionario(entity_types, asic_types):
    d = OUTPUT_DIR / "dicionario" / "data.csv"
    d.parent.mkdir(parents=True, exist_ok=True)
    rows = []

    def add(table, col, mapping):
        for k in sorted(mapping):
            rows.append([table, col, k, "", mapping[k]])

    # entity table
    add("entity", "abn_status", ABN_STATUS)
    add("entity", "entity_type", {k: entity_types[k] for k in entity_types})
    add("entity", "gst_status", GST_STATUS)
    add("entity", "state_code", STATE_CODE)
    add(
        "entity",
        "asic_number_type",
        {
            k: ASIC_TYPE_LABELS.get(k, k.capitalize())
            for k in sorted(asic_types)
        },
    )
    add("entity", "replaced", REPLACED)
    # other_name table
    add("other_name", "name_type", NAME_TYPE)

    with open(d, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(
            [
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ]
        )
        w.writerows(rows)
    print(f"  dicionario: {len(rows)} rows -> {d}")


def main():
    zips = sorted(INPUT_DIR.glob("*.zip"))
    if not zips:
        sys.exit(f"No zips in {INPUT_DIR}")
    print(f"Input:  {INPUT_DIR}  ({len(zips)} zips)")
    print(f"Output: {OUTPUT_DIR}  QUICK={QUICK}")

    entity_w = PartitionWriter("entity", ENTITY_SCHEMA)
    other_w = PartitionWriter("other_name", OTHER_NAME_SCHEMA)
    dgr_w = PartitionWriter("dgr", DGR_SCHEMA)
    writers = (entity_w, other_w, dgr_w)
    entity_types, asic_types = {}, set()
    counts = {"entity": 0, "other_name": 0, "dgr": 0}

    stop = False
    for zp in zips:
        zf = zipfile.ZipFile(zp)
        for member in sorted(zf.namelist()):
            if not member.lower().endswith(".xml"):
                continue
            print(f"  parsing {zp.name}:{member} ...", flush=True)
            process_member(
                zf, member, writers, entity_types, asic_types, counts
            )
            print(f"    running totals: {counts}", flush=True)
            if QUICK and counts["entity"] >= 200_000:
                stop = True
                break
        zf.close()
        if stop:
            break

    for w in writers:
        w.flush()
    write_dicionario(entity_types, asic_types)

    print("\n=== DONE ===")
    print("counts:", counts)
    print("entity_type codes seen:", len(entity_types))
    print("asic_number_type values:", sorted(asic_types))


if __name__ == "__main__":
    main()
