"""Download + cleaning transform for au_ato_abr (shared by the pipeline and the
one-shot bootstrap in models/au_ato_abr/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
``clean_all`` / ``download_zips`` directly.

Records are streamed straight out of the two source ZIPs with ``lxml.iterparse``
(no full extraction to disk). Output is **all-STRING** hive-partitioned parquet:
``upload_to_gcs`` infers the staging schema from a stringified header, so typed
parquet is rejected; the dbt models ``safe_cast`` every column back to its real
type. ``extraction_date`` is encoded in the path only, never in the file body.
Dates are built as real ``date32`` first, then arrow-cast to string (so NULLs
stay NULL rather than becoming the literal ``"nan"``).
"""

from __future__ import annotations

import logging
import zipfile
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import etree

from pipelines.datasets.au_ato_abr.constants import constants

log = logging.getLogger("au_ato_abr")

CHUNK = 400_000  # rows per parquet file (bounds peak RAM)
DATE_SENTINELS = {"19000101", "00000000", "10000101"}

# Real (typed) build schemas — extraction_date lives in the path, not the file.
_ENTITY_TYPED = pa.schema(
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
_OTHER_NAME_TYPED = pa.schema(
    [
        ("abn", pa.string()),
        ("name_type", pa.string()),
        ("name", pa.string()),
    ]
)
_DGR_TYPED = pa.schema(
    [
        ("abn", pa.string()),
        ("dgr_status_from_date", pa.date32()),
        ("dgr_name", pa.string()),
    ]
)


def _string_schema(schema: pa.Schema) -> pa.Schema:
    return pa.schema([(f.name, pa.string()) for f in schema])


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
    """Accumulates rows for one table and flushes fixed-size all-STRING chunks."""

    def __init__(self, name: str, typed_schema: pa.Schema, output_dir: Path):
        self.name = name
        self.typed = typed_schema
        self.out = _string_schema(typed_schema)
        self.cols = [f.name for f in typed_schema]
        self.output_dir = output_dir
        self.buf = {c: [] for c in self.cols}
        self.n = 0
        self.file_idx = 0
        self.part_dirs: set[Path] = set()
        self._ed: date | None = None

    def _part_dir(self, extraction_date: date) -> Path:
        d = (
            self.output_dir
            / self.name
            / f"extraction_date={extraction_date.isoformat()}"
        )
        if d not in self.part_dirs:
            d.mkdir(parents=True, exist_ok=True)
            # 0-row header first, so table-approve's save_header_files never
            # reads a large first parquet (OOM guard).
            header = pa.table(
                {c: pa.array([], type=pa.string()) for c in self.cols},
                schema=self.out,
            )
            pq.write_table(
                header, d / "00_header.parquet", compression="snappy"
            )
            self.part_dirs.add(d)
        return d

    def add(self, extraction_date: date, row: dict):
        # Flush before switching partitions so buffered rows are never written
        # under a later member's extraction_date (the two ZIPs can carry
        # different ExtractTimes, and dgr/other_name buffer far fewer than CHUNK
        # rows per member).
        if self._ed is not None and extraction_date != self._ed:
            self.flush()
        self._ed = extraction_date
        for c in self.cols:
            self.buf[c].append(row.get(c))
        self.n += 1
        if self.n >= CHUNK:
            self.flush()

    def flush(self):
        if self.n == 0:
            return
        assert (
            self._ed is not None
        )  # n > 0 implies add() set the partition date
        d = self._part_dir(self._ed)
        typed = pa.table(
            {
                c: pa.array(self.buf[c], type=self.typed.field(c).type)
                for c in self.cols
            },
            schema=self.typed,
        )
        # cast to all-STRING via arrow (NULL stays NULL, dates -> "YYYY-MM-DD").
        table = typed.cast(self.out)
        self.file_idx += 1
        pq.write_table(
            table,
            d / f"data_{self.file_idx:05d}.parquet",
            compression="snappy",
        )
        self.buf = {c: [] for c in self.cols}
        self.n = 0


def source_last_modified() -> str:
    """Return the source's newest publication date without downloading the data.

    HEADs both ZIPs and returns the latest ``Last-Modified`` as ``"YYYY-MM-DD"``.
    Used as the poll signal (a publication timestamp, compared against
    ``Table.Update.latest`` via ``compare_against="table_update"``), so the flow
    fetches the ~1 GB payload only when the source has actually republished.
    """
    from email.utils import parsedate_to_datetime

    headers = {"User-Agent": constants.USER_AGENT.value}
    latest: date | None = None
    for url in constants.ZIP_URLS.value:
        r = requests.head(
            url, headers=headers, allow_redirects=True, timeout=60
        )
        r.raise_for_status()
        lm = r.headers.get("Last-Modified")
        if not lm:
            continue
        d = parsedate_to_datetime(lm).date()
        if latest is None or d > latest:
            latest = d
    if latest is None:
        raise RuntimeError("No Last-Modified header on the source ZIPs")
    return latest.isoformat()


def download_zips(input_dir: Path) -> Path:
    """Download the two ABN Bulk Extract ZIPs into ``input_dir``.

    A browser User-Agent is mandatory (data.gov.au 403s automated clients).

    Returns:
        The same ``input_dir``, for chaining.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    headers = {"User-Agent": constants.USER_AGENT.value}
    for url in constants.ZIP_URLS.value:
        dest = input_dir / url.rsplit("/", 1)[-1]
        log.info("downloading %s", dest.name)
        with requests.get(url, headers=headers, stream=True, timeout=600) as r:
            r.raise_for_status()
            with open(dest, "wb") as fh:
                for block in r.iter_content(chunk_size=8 << 20):
                    fh.write(block)
    return input_dir


def _parse_extract_date(stream) -> date | None:
    ctx = etree.iterparse(
        stream,
        events=("end",),
        tag="ExtractTime",
        resolve_entities=False,
        no_network=True,
    )
    for _, el in ctx:
        txt = (el.text or "").strip()  # e.g. 2026-08-12T12:26:57
        return date.fromisoformat(txt[:10])
    return None


def _process_member(zf, member, writers, entity_types, asic_types, counts):
    entity_w, other_w, dgr_w = writers
    with zf.open(member) as f:
        extraction_date = _parse_extract_date(f)
    if extraction_date is None:
        raise RuntimeError(f"No ExtractTime in {member}")
    with zf.open(member) as f:
        ctx = etree.iterparse(
            f,
            events=("end",),
            tag="ABR",
            resolve_entities=False,
            no_network=True,
        )
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
                clean_text(asic_el.get("ASICNumberType"))
                if asic_el is not None
                else None
            )
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

            abr.clear()
            parent = abr.getparent()
            if parent is not None:
                while abr.getprevious() is not None:
                    del parent[0]


# ── dictionary (code -> label) ───────────────────────────────────────────────
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


def _write_dicionario(
    output_dir: Path, entity_types: dict, asic_types: set
) -> Path:
    d = output_dir / "dicionario"
    d.mkdir(parents=True, exist_ok=True)
    rows = []

    def add(table, col, mapping):
        for k in sorted(mapping):
            rows.append([table, col, k, "", mapping[k]])

    add("entity", "abn_status", ABN_STATUS)
    add("entity", "entity_type", dict(entity_types))
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
    add("other_name", "name_type", NAME_TYPE)

    cols = ["id_tabela", "nome_coluna", "chave", "cobertura_temporal", "valor"]
    table = pa.table(
        {
            c: pa.array([r[i] for r in rows], type=pa.string())
            for i, c in enumerate(cols)
        }
    )
    # Only parquet: upload_to_gcs uploads the whole dicionario/ dir into a
    # PARQUET-format external table, so a stray data.csv breaks the staging read.
    pq.write_table(table, d / "data.parquet", compression="snappy")
    return d


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Parse every XML member of every ZIP in ``input_dir`` into partitioned parquet.

    Args:
        input_dir: directory holding the two ``public_split_*.zip`` files.
        output_dir: directory to write ``<table>/extraction_date=.../*.parquet``.

    Returns:
        Mapping of each table slug to its output directory, plus
        ``"max_extraction_date"`` ("YYYY-MM-DD", drives the source-update poll)
        and ``"counts"``.
    """
    zips = sorted(Path(input_dir).glob("*.zip"))
    if not zips:
        raise FileNotFoundError(f"No zips in {input_dir}")
    output_dir = Path(output_dir)

    entity_w = PartitionWriter("entity", _ENTITY_TYPED, output_dir)
    other_w = PartitionWriter("other_name", _OTHER_NAME_TYPED, output_dir)
    dgr_w = PartitionWriter("dgr", _DGR_TYPED, output_dir)
    writers = (entity_w, other_w, dgr_w)
    entity_types: dict[str, str] = {}
    asic_types: set[str] = set()
    counts = {"entity": 0, "other_name": 0, "dgr": 0}
    max_ed: date | None = None

    for zp in zips:
        zf = zipfile.ZipFile(zp)
        for member in sorted(zf.namelist()):
            if not member.lower().endswith(".xml"):
                continue
            log.info("parsing %s:%s", zp.name, member)
            with zf.open(member) as f:
                ed = _parse_extract_date(f)
            if ed is not None and (max_ed is None or ed > max_ed):
                max_ed = ed
            _process_member(
                zf, member, writers, entity_types, asic_types, counts
            )
        zf.close()

    for w in writers:
        w.flush()
    dic_dir = _write_dicionario(output_dir, entity_types, asic_types)

    if max_ed is None:
        raise RuntimeError("No ExtractTime found in any XML member")

    return {
        "entity": entity_w.output_dir / "entity",
        "other_name": other_w.output_dir / "other_name",
        "dgr": dgr_w.output_dir / "dgr",
        "dicionario": dic_dir,
        "max_extraction_date": max_ed.isoformat(),
        "counts": counts,
    }
