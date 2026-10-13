"""Pure download + cleaning transform for us_irs_form990 (IRS Form 990 series).

No Prefect imports: the one-shot bootstrap in ``models/us_irs_form990/code``
and the recurring flow both import from here, so the transform never drifts.

Three sources, three shapes
---------------------------
* **e-file XML** (``return_financial``, ``compensation``): the IRS posts ZIPs of
  raw return XML in irregular batches through the year. Every value is read
  through the Nonprofit Open Data Collective master concordance (see
  ``variables.py``), which maps the thousands of schema-version-specific XPaths
  onto stable variable names. Only Form 990 and 990-EZ are covered by the
  concordance; 990-PF and 990-T returns in the same ZIPs are skipped.
* **Business Master File** (``organization``): a monthly full snapshot of every
  exempt organization. Snapshots stack on an ``extraction_date`` partition.
* **Automatic revocation list** (``revocation``): a monthly cumulative list,
  loaded as a full replacement.

Staging output is **all-STRING** parquet, as ``upload_to_gcs`` requires (see
``prefect-pipeline-conventions``); the dbt models ``safe_cast`` each column.
"""

from __future__ import annotations

import csv
import io
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections import Counter, defaultdict
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from lxml import etree

from pipelines.datasets.us_irs_form990.constants import constants
from pipelines.datasets.us_irs_form990.variables import (
    COMPENSATION_FIELDS,
    COMPENSATION_FLAGS,
    RETURN_FLAGS,
    RETURN_SCALARS,
)

# --------------------------------------------------------------------------
# source discovery
# --------------------------------------------------------------------------

_ZIP_RE = re.compile(
    r"https://apps\.irs\.gov/pub/epostcard/990/xml/(\d{4})/([A-Za-z0-9_]+\.zip)"
)


def _session(session: requests.Session | None = None) -> requests.Session:
    return session or requests.Session()


def list_efile_zips(session: requests.Session | None = None) -> list[str]:
    """Return every e-file ZIP URL the IRS hosts, oldest release year first.

    The listing page is scraped for ZIP links; release years 2017 and 2018 are
    no longer linked but remain hosted, so they are enumerated by pattern
    (``download990xml_<year>_<n>.zip``).
    """
    s = _session(session)
    resp = s.get(
        constants.EFILE_LISTING_URL.value,
        headers=constants.HEADERS.value,
        timeout=120,
    )
    resp.raise_for_status()
    urls = {m.group(0) for m in _ZIP_RE.finditer(resp.text)}
    base = constants.EFILE_BASE_URL.value
    for year, parts in constants.EFILE_UNLISTED_YEARS.value.items():
        for n in range(1, parts + 1):
            urls.add(f"{base}{year}/download990xml_{year}_{n}.zip")
    if len(urls) < 50:
        raise RuntimeError(
            f"only {len(urls)} ZIP links found on the listing page; "
            "the page layout may have changed"
        )
    return sorted(urls, key=lambda u: (u.split("/")[-2], u.split("/")[-1]))


def batch_id(url_or_path: str | Path) -> str:
    """``.../2026_TEOS_XML_01A.zip`` -> ``2026_TEOS_XML_01A``.

    Matches the IRS index files' ``XML_BATCH_ID`` column for the years that
    carry one.
    """
    return Path(str(url_or_path)).name.removesuffix(".zip")


def head_last_modified(
    url: str, session: requests.Session | None = None
) -> str:
    """``Last-Modified`` of a URL as ``YYYY-MM-DD`` (a HEAD request, no body)."""
    s = _session(session)
    resp = s.head(
        url,
        headers=constants.HEADERS.value,
        allow_redirects=True,
        timeout=120,
    )
    resp.raise_for_status()
    stamp = resp.headers.get("Last-Modified")
    if not stamp:
        raise RuntimeError(f"No Last-Modified header on {url}")
    return parsedate_to_datetime(stamp).date().isoformat()


def efile_source_date(
    urls: list[str] | None = None, session: requests.Session | None = None
) -> str:
    """Publication date of the newest e-file ZIP (the cheap poll signal)."""
    s = _session(session)
    urls = urls or list_efile_zips(s)
    # The newest release year's highest-numbered batch is the most recent.
    return max(head_last_modified(u, s) for u in urls[-3:])


def bmf_source_date(session: requests.Session | None = None) -> str:
    """Posting date of the current BMF extract, from the first region file."""
    return head_last_modified(
        constants.BMF_BASE_URL.value + "eo1.csv", _session(session)
    )


def download(
    url: str,
    dest: Path,
    session: requests.Session | None = None,
    timeout: int = 3600,
) -> Path:
    """Stream ``url`` into ``dest`` (resumable-safe: a complete file is kept)."""
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    s = _session(session)
    tmp = dest.with_suffix(dest.suffix + ".part")
    with s.get(
        url, headers=constants.HEADERS.value, stream=True, timeout=timeout
    ) as r:
        r.raise_for_status()
        # ``r.raw`` yields the body exactly as it came off the wire. irs.gov
        # serves the BMF CSVs with ``Content-Encoding: br``, so without this
        # the file on disk is Brotli, not CSV, and the header check in
        # ``clean_bmf`` reports every column missing.
        r.raw.decode_content = True
        with open(tmp, "wb") as fh:
            shutil.copyfileobj(r.raw, fh, length=1 << 22)
    tmp.rename(dest)
    return dest


# --------------------------------------------------------------------------
# concordance
# --------------------------------------------------------------------------


class Concordance:
    """XPath lookup tables built from the trimmed master concordance.

    ``scalar[xpath] -> variable`` for the one-per-return variables, and
    ``group[parent_xpath][relative_xpath] -> variable`` for the Part VII
    repeating group, keyed by the group's container element so each listed
    person becomes one row.
    """

    def __init__(self, path: Path | None = None):
        path = Path(path or constants.CONCORDANCE_PATH.value)
        comp_vars = {v for vs in COMPENSATION_FIELDS.values() for v in vs}
        self.scalar: dict[str, str] = {}
        self.group: dict[str, dict[str, str]] = defaultdict(dict)
        with open(path, newline="", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                var, xp = r["variable_name"], r["xpath"]
                if var in comp_vars:
                    # Explanation text lives outside the person group.
                    if "/CompensationExplanation" in xp:
                        continue
                    parts = xp.split("/")
                    # /Return/ReturnData/IRS990/<Group>/<rel...>
                    parent = "/".join(parts[:5])
                    rel = "/".join(parts[5:])
                    self.group[parent][rel] = var
                else:
                    self.scalar[xp] = var
        self.group_parents = set(self.group)


_CONCORDANCE: Concordance | None = None


def concordance() -> Concordance:
    global _CONCORDANCE
    if _CONCORDANCE is None:
        _CONCORDANCE = Concordance()
    return _CONCORDANCE


# --------------------------------------------------------------------------
# XML parsing
# --------------------------------------------------------------------------

RETURN_KEY_COLUMNS = [
    "year",
    "ein",
    "form_type",
    "object_id",
    "return_version",
    "xml_batch_id",
]
RETURN_DERIVED = ["organization_type", "exempt_status"]
# organization_name_line_2 is folded into organization_name before writing;
# the two derived columns sit after the header block, as in the architecture.
RETURN_COLUMNS = RETURN_KEY_COLUMNS + [
    c
    for c in RETURN_SCALARS
    if not c.startswith("_") and c != "organization_name_line_2"
]
RETURN_COLUMNS[
    RETURN_COLUMNS.index("legal_domicile_state") + 1 : RETURN_COLUMNS.index(
        "legal_domicile_state"
    )
    + 1
] = RETURN_DERIVED
COMPENSATION_KEY_COLUMNS = [
    "year",
    "ein",
    "form_type",
    "object_id",
    "line_number",
]
COMPENSATION_COLUMNS = COMPENSATION_KEY_COLUMNS + list(COMPENSATION_FIELDS)

_TRUE = {"X", "true", "1", "TRUE", "True"}


def _flag(value: str | None) -> str:
    return "true" if value is not None and value.strip() in _TRUE else "false"


def _clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    v = " ".join(value.split())
    return v or None


class ParsedReturn:
    __slots__ = ("header", "hits", "people")

    def __init__(self, header: dict, people: list[dict], hits: Counter):
        self.header = header
        self.people = people
        self.hits = hits


def _walk(el, path: str, scalars: dict, groups: list, parents: set) -> None:
    for child in el:
        tag = child.tag
        if not isinstance(tag, str):  # comments / processing instructions
            continue
        name = tag.rsplit("}", 1)[-1]
        p = f"{path}/{name}"
        if len(child) == 0:
            scalars.setdefault(p, child.text)
        elif p in parents:
            leaves: dict[str, str | None] = {}
            _collect(child, "", leaves)
            groups.append((p, leaves))
        else:
            _walk(child, p, scalars, groups, parents)


def _collect(el, rel: str, out: dict) -> None:
    for child in el:
        tag = child.tag
        if not isinstance(tag, str):
            continue
        name = tag.rsplit("}", 1)[-1]
        r = f"{rel}/{name}" if rel else name
        if len(child) == 0:
            out.setdefault(r, child.text)
        else:
            _collect(child, r, out)


def parse_return(
    xml_bytes: bytes, object_id: str, xml_batch_id: str
) -> ParsedReturn | None:
    """Parse one return; ``None`` when its form type is outside the concordance.

    Every value is located by concordance XPath. ``hits`` counts which XPaths
    matched, so a corpus-wide tally can show a schema version the concordance
    does not yet cover (a variable silently going null on new returns).
    """
    conc = concordance()
    root = etree.fromstring(xml_bytes)
    scalars: dict[str, str | None] = {}
    groups: list[tuple[str, dict]] = []
    _walk(root, "/Return", scalars, groups, conc.group_parents)

    form_type = _clean_text(
        scalars.get("/Return/ReturnHeader/ReturnTypeCd")
        or scalars.get("/Return/ReturnHeader/ReturnType")
    )
    if form_type not in constants.RETURN_TYPES.value:
        return None

    # variable -> first value found, plus which xpath supplied it
    values: dict[str, str | None] = {}
    hits: Counter = Counter()
    for xp, raw in scalars.items():
        var = conc.scalar.get(xp)
        if var is None or var in values:
            continue
        values[var] = raw
        hits[xp] += 1

    header: dict[str, str | None] = {
        "year": _clean_text(values.get("F9_00_TAX_YEAR")),
        "ein": _clean_text(values.get("F9_00_ORG_EIN")),
        "form_type": form_type,
        "object_id": object_id,
        "return_version": root.get("returnVersion"),
        "xml_batch_id": xml_batch_id,
    }
    for col, vars_ in RETURN_SCALARS.items():
        raw = None
        for v in vars_:
            if values.get(v) is not None:
                raw = values[v]
                break
        header[col] = _flag(raw) if col in RETURN_FLAGS else _clean_text(raw)
    if header["organization_name_line_2"]:
        header["organization_name"] = " ".join(
            x
            for x in (
                header["organization_name"],
                header["organization_name_line_2"],
            )
            if x
        )
    header.pop("organization_name_line_2")
    header["organization_type"] = _first_true(
        header,
        [
            ("_type_corp", "corporation"),
            ("_type_trust", "trust"),
            ("_type_assoc", "association"),
            ("_type_other", "other"),
        ],
    )
    header["exempt_status"] = _first_true(
        header,
        [
            ("_exempt_501c3", "501(c)(3)"),
            ("_exempt_501c", "501(c)"),
            ("_exempt_4947a1", "4947(a)(1)"),
            ("_exempt_527", "527"),
        ],
    )
    for k in [k for k in header if k.startswith("_")]:
        header.pop(k)

    people: list[dict] = []
    for n, (parent, leaves) in enumerate(groups, start=1):
        table = conc.group[parent]
        found: dict[str, str | None] = {}
        for rel, raw in leaves.items():
            var = table.get(rel)
            if var is None or var in found:
                continue
            found[var] = raw
            hits[f"{parent}/{rel}"] += 1
        row: dict[str, str | None] = {
            "year": header["year"],
            "ein": header["ein"],
            "form_type": form_type,
            "object_id": object_id,
            "line_number": str(n),
        }
        for col, vars_ in COMPENSATION_FIELDS.items():
            raw = None
            for v in vars_:
                if found.get(v) is not None:
                    raw = found[v]
                    break
            row[col] = (
                _flag(raw) if col in COMPENSATION_FLAGS else _clean_text(raw)
            )
        # 990-EZ Part IV has no position checkboxes; leave them null there so
        # a missing box is not read as "not an officer".
        if form_type == "990EZ":
            for col in COMPENSATION_FLAGS:
                row[col] = None
        people.append(row)
    return ParsedReturn(header, people, hits)


def _first_true(row: dict, pairs: list[tuple[str, str]]) -> str | None:
    for key, label in pairs:
        if row.get(key) == "true":
            return label
    return None


# --------------------------------------------------------------------------
# ZIP -> parquet
# --------------------------------------------------------------------------

_OBJECT_ID_RE = re.compile(r"(\d{18})_public\.xml$")


def _write_parts(
    rows_by_year: dict[str, list[dict]],
    columns: list[str],
    out_dir: Path,
    table: str,
    stem: str,
) -> dict[str, int]:
    """Write ``<out>/<table>/year=<y>/<stem>.parquet`` per tax year."""
    counts: dict[str, int] = {}
    schema = pa.schema([(c, pa.string()) for c in columns if c != "year"])
    for year, rows in rows_by_year.items():
        d = Path(out_dir) / table / f"year={year}"
        d.mkdir(parents=True, exist_ok=True)
        arrays = {
            c: pa.array([r.get(c) for r in rows], type=pa.string())
            for c in schema.names
        }
        pq.write_table(
            pa.table(arrays, schema=schema),
            d / f"{stem}.parquet",
            compression="snappy",
        )
        counts[year] = counts.get(year, 0) + len(rows)
    return counts


def _remove_batch_files(out_dir: Path, stem: str) -> None:
    """Drop every part an earlier run of the same ZIP wrote."""
    for table in ("return_financial", "compensation"):
        for f in Path(out_dir).glob(f"{table}/year=*/{stem}_p*.parquet"):
            f.unlink()


def _iter_members(zip_path: Path):
    """Yield ``(member name, bytes)`` for every file in the ZIP.

    One IRS batch (``2020_TEOS_XML_CT1.zip``) is Deflate64-compressed, which
    Python's ``zipfile`` cannot read. When that happens the archive is expanded
    once with Info-ZIP ``unzip`` (which supports Deflate64) into a temporary
    directory and the files are read from there.
    """
    zip_path = Path(zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        infos = zf.infolist()
        try:
            for info in infos:
                yield info.filename, zf.read(info)
            return
        except NotImplementedError:
            pass  # Deflate64 (method 9): fall through to unzip
    if shutil.which("unzip") is None:
        raise RuntimeError(
            f"{zip_path.name} uses a compression method zipfile cannot read "
            "and no `unzip` binary is available"
        )
    with tempfile.TemporaryDirectory(prefix="form990_") as tmp:
        # The one Deflate64 batch also has 76 stray bytes before its central
        # directory; unzip extracts every member but exits 3, so the exit
        # code is ignored and the member count is checked instead.
        subprocess.run(
            ["unzip", "-q", "-o", str(zip_path), "-d", tmp],
            check=False,
            capture_output=True,
        )
        extracted = sorted(Path(tmp).rglob("*.xml"))
        expected = sum(1 for i in infos if i.filename.endswith(".xml"))
        if len(extracted) != expected:
            raise RuntimeError(
                f"{zip_path.name}: unzip extracted {len(extracted)} of "
                f"{expected} members"
            )
        for path in extracted:
            yield path.name, path.read_bytes()


def clean_efile_zip(
    zip_path: Path,
    out_dir: Path,
    stem: str | None = None,
    flush_every: int = 100_000,
) -> dict:
    """Parse every return in one ZIP into the two e-file tables.

    Output files are named after the ZIP (``<batch>_p<k>.parquet`` under each
    tax-year directory), so re-processing a batch replaces its own files rather
    than duplicating rows: the load is idempotent per batch. Rows are flushed
    every ``flush_every`` returns to bound memory on the multi-GB ZIPs.
    Returns per-year counts, skipped form types and XPath hit tallies.
    """
    zip_path = Path(zip_path)
    stem = stem or batch_id(zip_path)
    _remove_batch_files(out_dir, stem)
    returns: dict[str, list[dict]] = defaultdict(list)
    people: dict[str, list[dict]] = defaultdict(list)
    counts = {"return_financial": Counter(), "compensation": Counter()}
    skipped: Counter = Counter()
    hits: Counter = Counter()
    bad: list[str] = []
    part = 0
    buffered = 0

    def flush() -> None:
        nonlocal part, buffered, returns, people
        if not buffered:
            return
        tag = f"{stem}_p{part:03d}"
        counts["return_financial"].update(
            _write_parts(
                returns, RETURN_COLUMNS, out_dir, "return_financial", tag
            )
        )
        counts["compensation"].update(
            _write_parts(
                people, COMPENSATION_COLUMNS, out_dir, "compensation", tag
            )
        )
        returns, people = defaultdict(list), defaultdict(list)
        part += 1
        buffered = 0

    for name, data in _iter_members(zip_path):
        m = _OBJECT_ID_RE.search(name)
        if not m:
            continue
        object_id = m.group(1)
        try:
            parsed = parse_return(data, object_id, stem)
        except etree.XMLSyntaxError:
            bad.append(object_id)
            continue
        if parsed is None:
            skipped["other_form_type"] += 1
            continue
        year = parsed.header["year"]
        if not year or not year.isdigit():
            skipped["no_tax_year"] += 1
            continue
        returns[year].append(parsed.header)
        people[year].extend(parsed.people)
        hits.update(parsed.hits)
        buffered += 1
        if buffered >= flush_every:
            flush()
    flush()
    return {
        "batch": stem,
        "return_financial": dict(counts["return_financial"]),
        "compensation": dict(counts["compensation"]),
        "skipped": dict(skipped),
        "unparseable": bad,
        "xpath_hits": dict(hits),
    }


# --------------------------------------------------------------------------
# Business Master File -> organization
# --------------------------------------------------------------------------

BMF_SOURCE_COLUMNS = [
    "EIN",
    "NAME",
    "ICO",
    "STREET",
    "CITY",
    "STATE",
    "ZIP",
    "GROUP",
    "SUBSECTION",
    "AFFILIATION",
    "CLASSIFICATION",
    "RULING",
    "DEDUCTIBILITY",
    "FOUNDATION",
    "ACTIVITY",
    "ORGANIZATION",
    "STATUS",
    "TAX_PERIOD",
    "ASSET_CD",
    "INCOME_CD",
    "FILING_REQ_CD",
    "PF_FILING_REQ_CD",
    "ACCT_PD",
    "ASSET_AMT",
    "INCOME_AMT",
    "REVENUE_AMT",
    "NTEE_CD",
    "SORT_NAME",
]

# source column -> output column, in output order (after extraction_date)
BMF_COLUMNS = {
    "EIN": "ein",
    "NAME": "name",
    "SORT_NAME": "sort_name",
    "ICO": "in_care_of_name",
    "STREET": "street",
    "CITY": "city",
    "STATE": "state",
    "ZIP": "zip_code",
    "GROUP": "group_exemption_number",
    "SUBSECTION": "subsection_code",
    "CLASSIFICATION": "classification_code",
    "AFFILIATION": "affiliation_code",
    "RULING": "ruling_date",
    "DEDUCTIBILITY": "deductibility_code",
    "FOUNDATION": "foundation_code",
    "ACTIVITY": "activity_code",
    "ORGANIZATION": "organization_code",
    "STATUS": "status_code",
    "NTEE_CD": "ntee_code",
    "TAX_PERIOD": "tax_period",
    "ACCT_PD": "accounting_period_month",
    "FILING_REQ_CD": "filing_requirement_code",
    "PF_FILING_REQ_CD": "pf_filing_requirement_code",
    "ASSET_CD": "asset_code",
    "INCOME_CD": "income_code",
    "ASSET_AMT": "asset_amount",
    "INCOME_AMT": "income_amount",
    "REVENUE_AMT": "revenue_amount",
}
ORGANIZATION_COLUMNS = ["extraction_date", *BMF_COLUMNS.values()]


def _yyyymm_to_date(value: str | None) -> str | None:
    """``202506`` -> ``2025-06-01``; ``000000``/blank -> null."""
    if not value or not re.fullmatch(r"\d{6}", value):
        return None
    y, m = int(value[:4]), int(value[4:6])
    if y == 0 or not 1 <= m <= 12:
        return None
    return f"{y:04d}-{m:02d}-01"


def clean_bmf(
    input_dir: Path, out_dir: Path, extraction_date: str
) -> dict[str, int]:
    """Stack the regional BMF CSVs into ``organization/extraction_date=<d>/``.

    The six files partition the registry by filing state; an EIN appearing in
    two files would be a source defect and is kept once (first seen).

    Rows are written out in chunks as they are read rather than collected
    first. The registry is ~2M rows wide by 28 string fields, which as Python
    dicts is 5-6 GB — more than the worker's 8 Gi limit once pyarrow builds
    its arrays on top. Deduplication only needs the EINs, so the set of seen
    EINs is the only thing that grows with the whole file (~200 MB); the row
    buffer never exceeds one chunk.
    """
    input_dir = Path(input_dir)
    d = Path(out_dir) / "organization" / f"extraction_date={extraction_date}"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)

    cols = [c for c in ORGANIZATION_COLUMNS if c != "extraction_date"]
    schema = pa.schema([(c, pa.string()) for c in cols])
    chunk = constants.CHUNK_ROWS.value

    seen: set[str] = set()
    buf: list[dict] = []
    per_file: dict[str, int] = {}
    written = 0
    part_no = 0

    def flush() -> None:
        nonlocal buf, part_no, written
        if not buf:
            return
        pq.write_table(
            pa.table(
                {
                    c: pa.array([r[c] for r in buf], type=pa.string())
                    for c in cols
                },
                schema=schema,
            ),
            d / f"data_{part_no:05d}.parquet",
            compression="snappy",
        )
        written += len(buf)
        part_no += 1
        buf = []

    for name in constants.BMF_FILES.value:
        path = input_dir / f"{name}.csv"
        n = 0
        with open(path, newline="", encoding="latin-1") as fh:
            reader = csv.DictReader(fh)
            missing = set(BMF_SOURCE_COLUMNS) - set(reader.fieldnames or [])
            if missing:
                raise RuntimeError(f"{path.name}: missing columns {missing}")
            for r in reader:
                n += 1
                ein = r["EIN"].strip()
                if ein in seen:
                    continue
                seen.add(ein)
                out = {
                    dst: _clean_text(r.get(src))
                    for src, dst in BMF_COLUMNS.items()
                }
                out["ruling_date"] = _yyyymm_to_date(out["ruling_date"])
                # The two 'tax period' fields stay YYYYMM labels; only ruling is
                # a date proper.
                buf.append(out)
                if len(buf) >= chunk:
                    flush()
        per_file[name] = n
    flush()

    per_file["organization"] = written
    return per_file


# --------------------------------------------------------------------------
# Automatic revocation list -> revocation
# --------------------------------------------------------------------------

_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "JAN",
            "FEB",
            "MAR",
            "APR",
            "MAY",
            "JUN",
            "JUL",
            "AUG",
            "SEP",
            "OCT",
            "NOV",
            "DEC",
        ],
        start=1,
    )
}


def _dmy_to_iso(value: str | None) -> str | None:
    """``15-NOV-2017`` -> ``2017-11-15``."""
    if not value:
        return None
    m = re.fullmatch(r"(\d{1,2})-([A-Z]{3})-(\d{4})", value.strip().upper())
    if not m or m.group(2) not in _MONTHS:
        return None
    return f"{m.group(3)}-{_MONTHS[m.group(2)]:02d}-{int(m.group(1)):02d}"


REVOCATION_COLUMNS = list(constants.REVOCATION_COLUMNS.value)


def clean_revocation(zip_path: Path, out_dir: Path) -> dict[str, int]:
    """Convert the pipe-delimited revocation list to ``revocation/data.parquet``.

    The file has no header and a trailing empty field on every line. Exact
    duplicate lines are collapsed; the remaining rows are keyed on
    (ein, revocation_date, revocation_posting_date) downstream.
    """
    cols = REVOCATION_COLUMNS
    seen: set[tuple] = set()
    rows: list[dict] = []
    n_lines = 0
    with zipfile.ZipFile(zip_path) as zf:
        member = next(
            i for i in zf.infolist() if i.filename.lower().endswith(".txt")
        )
        text = io.TextIOWrapper(zf.open(member), encoding="latin-1")
        for line in text:
            line = line.rstrip("\r\n")
            if not line.strip():
                continue
            n_lines += 1
            fields = line.split("|")
            if len(fields) < len(cols):
                raise RuntimeError(
                    f"revocation line with {len(fields)} fields: {line[:80]}"
                )
            vals = [_clean_text(f) for f in fields[: len(cols)]]
            key = tuple(vals)
            if key in seen:
                continue
            seen.add(key)
            row = dict(zip(cols, vals, strict=True))
            # One record carries an unpadded subsection ("7"); the BMF and
            # the rest of this file use two digits.
            if row["exemption_type"] and row["exemption_type"].isdigit():
                row["exemption_type"] = row["exemption_type"].zfill(2)
            for c in (
                "revocation_date",
                "revocation_posting_date",
                "exemption_reinstatement_date",
            ):
                row[c] = _dmy_to_iso(row[c])
            rows.append(row)
    d = Path(out_dir) / "revocation"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    schema = pa.schema([(c, pa.string()) for c in cols])
    pq.write_table(
        pa.table(
            {
                c: pa.array([r[c] for r in rows], type=pa.string())
                for c in cols
            },
            schema=schema,
        ),
        d / "data.parquet",
        compression="snappy",
    )
    return {"lines": n_lines, "revocation": len(rows)}


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def write_dicionario(csv_path: Path, out_dir: Path) -> int:
    """Copy the committed ``dicionario.csv`` to ``dicionario/data.parquet``."""
    with open(csv_path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    d = Path(out_dir) / "dicionario"
    if d.exists():
        shutil.rmtree(d)
    d.mkdir(parents=True)
    schema = pa.schema([(c, pa.string()) for c in DICIONARIO_COLUMNS])
    pq.write_table(
        pa.table(
            {
                c: pa.array([r.get(c) or None for r in rows], type=pa.string())
                for c in DICIONARIO_COLUMNS
            },
            schema=schema,
        ),
        d / "data.parquet",
        compression="snappy",
    )
    return len(rows)


def write_header_parquet(
    out_dir: Path, table: str, columns: list[str]
) -> Path:
    """0-row ``00_header.parquet`` for the one-shot onboarding upload only.

    table-approve reads the first blob in a staging prefix to learn column
    names; an empty file keeps it from loading a large part whole. The
    recurring pipeline must NOT write this (``gcs.dump_header`` would infer
    every column from an empty frame).
    """
    d = Path(out_dir) / table
    d.mkdir(parents=True, exist_ok=True)
    schema = pa.schema([(c, pa.string()) for c in columns])
    path = d / "00_header.parquet"
    pq.write_table(schema.empty_table(), path, compression="snappy")
    return path


def today_iso() -> str:
    return datetime.now().date().isoformat()
