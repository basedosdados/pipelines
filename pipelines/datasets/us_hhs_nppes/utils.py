"""Pure download + cleaning transform for us_hhs_nppes (NPPES / NPI registry).

No Prefect imports: the one-shot bootstrap in ``models/us_hhs_nppes/code`` and
the recurring flow both import from here, so the transform never drifts.

Shape of the work
-----------------
The NPPES monthly file is a **full replacement snapshot** of every NPI. We stack
snapshots (CNPJ model): every table carries an ``extraction_date`` partition and
the dbt models are ``incremental``, so successive months accumulate into a panel.

The source's main file is ~11.6 GB and 330 columns wide, of which 275 are three
repeating groups (taxonomy x15, other identifier x50, taxonomy group x15). Those
are melted into long tables; the 55 flat columns become ``provider``. Everything
is streamed in record batches — the file is never loaded whole.

Staging output is **all-STRING** parquet, as ``upload_to_gcs`` requires (see
``prefect-pipeline-conventions``); the dbt models ``safe_cast`` each column.
"""

import csv
import re
import shutil
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

import pyarrow as pa

# pyarrow.compute builds its kernels at import time, so static checkers
# cannot see them as module attributes; call sites carry a suppression.
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_hhs_nppes.constants import constants

# --------------------------------------------------------------------------
# source discovery + download
# --------------------------------------------------------------------------

_LINK_RE = re.compile(
    r"<a[^>]*id='(?P<id>[^']+)'[^>]*href='\./(?P<href>[^']+)'", re.IGNORECASE
)


def discover_monthly_url(session: requests.Session | None = None) -> str:
    """Return the absolute URL of the current monthly full-replacement ZIP.

    The listing page renders one anchor per file, each tagged with a stable
    ``id``; the monthly full file is ``DDSMTH.ZIP.D``. Discovering the link
    keeps the pipeline from hardcoding a month.
    """
    session = session or requests.Session()
    resp = session.get(
        constants.LISTING_URL.value,
        headers=constants.HEADERS.value,
        timeout=120,
    )
    resp.raise_for_status()
    for m in _LINK_RE.finditer(resp.text):
        if m.group("id") == constants.MONTHLY_LINK_ID.value:
            return constants.DOWNLOAD_BASE.value + m.group("href")
    raise RuntimeError(
        f"No monthly link (id={constants.MONTHLY_LINK_ID.value}) found at "
        f"{constants.LISTING_URL.value}"
    )


def source_last_modified(session: requests.Session | None = None) -> str:
    """Publication date of the current monthly bundle ("YYYY-MM-DD"), via HEAD.

    The cheap poll signal: no payload is downloaded. Compared against
    ``Table.Update.latest`` so a scheduled run is a no-op between monthly
    releases. The snapshot's own reference date lives inside the ZIP (in the
    member file names) and is only read once the bundle is actually fetched.
    """
    session = session or requests.Session()
    url = discover_monthly_url(session)
    resp = session.head(
        url, headers=constants.HEADERS.value, allow_redirects=True, timeout=120
    )
    resp.raise_for_status()
    stamp = resp.headers.get("Last-Modified")
    if not stamp:
        raise RuntimeError(f"No Last-Modified header on {url}")
    return parsedate_to_datetime(stamp).date().isoformat()


def download_monthly(input_dir: Path, url: str | None = None) -> Path:
    """Stream the monthly ZIP into ``input_dir``; return its path."""
    input_dir = Path(input_dir)
    input_dir.mkdir(parents=True, exist_ok=True)
    url = url or discover_monthly_url()
    dest = input_dir / url.rsplit("/", 1)[-1]
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    with requests.get(
        url, headers=constants.HEADERS.value, stream=True, timeout=1800
    ) as r:
        r.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with open(tmp, "wb") as fh:
            shutil.copyfileobj(r.raw, fh, length=1 << 22)
        tmp.rename(dest)
    return dest


def extract_zip(zip_path: Path, input_dir: Path) -> dict[str, Path]:
    """Extract the four CSVs we need; return a role -> path map.

    The bundle also ships two PDFs (readme, code values) which are extracted for
    the auxiliary-file bundle but are not part of the transform.
    """
    input_dir = Path(input_dir)
    roles: dict[str, Path] = {}
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            name = Path(info.filename).name
            if name.endswith("_fileheader.csv"):
                continue
            if name.startswith("npidata_pfile") and name.endswith(".csv"):
                role = "main"
            elif name.startswith("othername_pfile") and name.endswith(".csv"):
                role = "other_name"
            elif name.startswith("pl_pfile") and name.endswith(".csv"):
                role = "practice_location"
            elif name.startswith("endpoint_pfile") and name.endswith(".csv"):
                role = "endpoint"
            elif name.endswith(".pdf"):
                role = f"doc:{name}"
            else:
                continue
            dest = input_dir / name
            if not dest.exists() or dest.stat().st_size != info.file_size:
                with zf.open(info) as src, open(dest, "wb") as out:
                    shutil.copyfileobj(src, out, length=1 << 22)
            roles[role] = dest
    missing = {"main", "other_name", "practice_location", "endpoint"} - set(
        roles
    )
    if missing:
        raise RuntimeError(
            f"{zip_path.name}: missing expected members {missing}"
        )
    return roles


def extraction_date_from_name(path: Path) -> str:
    """``npidata_pfile_20050523-20260809.csv`` -> ``2026-08-09``.

    The end of the file-name date range is the snapshot's data cutoff, which is
    a truer reference date than the day the bundle was posted.
    """
    m = re.search(r"_(\d{8})-(\d{8})", Path(path).name)
    if not m:
        raise RuntimeError(f"Cannot read a date range from {Path(path).name}")
    d = m.group(2)
    return f"{d[:4]}-{d[4:6]}-{d[6:8]}"


# --------------------------------------------------------------------------
# architecture (column order is read from the CSVs, the source of truth)
# --------------------------------------------------------------------------


def architecture_columns(
    table: str, arch_dir: Path | None = None
) -> list[str]:
    arch_dir = Path(arch_dir or constants.ARCHITECTURE_DIR.value)
    with open(arch_dir / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return [row["name"] for row in csv.DictReader(fh)]


# --------------------------------------------------------------------------
# value normalisation
# --------------------------------------------------------------------------

# CMS masks numbers providers wrongly entered into FOIA-disclosable fields:
# SSN -> "$$$$$$$$$", ITIN -> "*********", EIN -> "=========". Per the Data
# Dissemination readme this applies to exactly two field families — the provider
# license number and the other provider identifier — so the mask is applied only
# there. Blanket application also nulled legitimate (if junk) values elsewhere,
# e.g. seven Endpoint rows whose value is literally "*" or "$$$$".
_MASK_RE = r"^(\$+|\*+|=+)$"


def _null_masked(arr: pa.Array) -> pa.Array:
    return pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.match_substring_regex(pc.fill_null(arr, ""), _MASK_RE),  # pyrefly: ignore [missing-attribute]
        pa.scalar(None, pa.string()),
        arr,
    )


def _blank_to_null(arr: pa.Array) -> pa.Array:
    return pc.if_else(  # pyrefly: ignore [missing-attribute]
        pc.equal(pc.fill_null(pc.utf8_trim_whitespace(arr), ""), ""),  # pyrefly: ignore [missing-attribute]
        pa.scalar(None, pa.string()),
        pc.utf8_trim_whitespace(arr),  # pyrefly: ignore [missing-attribute]
    )


def _mdy_to_iso(arr: pa.Array) -> pa.Array:
    """``MM/DD/YYYY`` -> ``YYYY-MM-DD``; anything else becomes null.

    Kept as a string: staging is all-STRING and the dbt model safe_casts.
    """
    a = _blank_to_null(arr)
    ok = pc.match_substring_regex(  # pyrefly: ignore [missing-attribute]
        pc.fill_null(a, ""), r"^\d{2}/\d{2}/\d{4}$"
    )
    iso = pc.binary_join_element_wise(  # pyrefly: ignore [missing-attribute]
        pc.utf8_slice_codeunits(pc.fill_null(a, "01/01/1900"), 6, 10),  # pyrefly: ignore [missing-attribute]
        pc.utf8_slice_codeunits(pc.fill_null(a, "01/01/1900"), 0, 2),  # pyrefly: ignore [missing-attribute]
        pc.utf8_slice_codeunits(pc.fill_null(a, "01/01/1900"), 3, 5),  # pyrefly: ignore [missing-attribute]
        "-",
    )
    return pc.if_else(ok, iso, pa.scalar(None, pa.string()))  # pyrefly: ignore [missing-attribute]


# --------------------------------------------------------------------------
# parquet writing
# --------------------------------------------------------------------------


class PartitionWriter:
    """Append record batches to ``<out>/<table>/extraction_date=<d>/`` .

    Files are chunked so peak RAM stays bounded and the *first* blob in the
    staging prefix is small. A 0-row ``00_header.parquet`` sorts ahead of every
    data file, which keeps the table-approve CI step from loading a large
    parquet whole (see project_table_approve_parquet_header_oom).

    That header is **only for the one-shot onboarding upload** (``write_header``),
    never for the recurring pipeline. ``upload_to_gcs`` builds the staging
    table's schema in ``gcs.dump_header``, which walks the output directory,
    takes the first parquet file it finds and reads
    ``read_row_group(0).slice(0, 1)``. It picks the 0-row header, infers the
    schema from an empty frame, and every column comes back INT64 — the dbt
    model then fails with ``Invalid cast from INT64 to DATE``. Leaving the header
    out of the pipeline's output makes ``dump_header`` read a real data file.
    """

    #: Written into the directory name, never into the file body — BigQuery's
    #: hive partitioning re-materialises it as a column on the external table.
    PARTITION_COL = "extraction_date"

    def __init__(
        self,
        out_dir: Path,
        table: str,
        columns: list[str],
        extraction_date: str | None,
        chunk_rows: int | None = None,
        write_header: bool = False,
    ):
        self.columns = (
            [c for c in columns if c != self.PARTITION_COL]
            if extraction_date is not None
            else list(columns)
        )
        self.schema = pa.schema([(c, pa.string()) for c in self.columns])
        self.chunk_rows = chunk_rows or constants.CHUNK_ROWS.value
        base = Path(out_dir) / table
        if extraction_date is not None:
            base = base / f"{self.PARTITION_COL}={extraction_date}"
        base.mkdir(parents=True, exist_ok=True)
        self.dir = base
        self.buf: list[pa.Table] = []
        self.buffered = 0
        self.seq = 0
        self.rows = 0
        self.write_header = write_header

    def write(self, table: pa.Table) -> None:
        if table.num_rows == 0:
            return
        self.buf.append(table.select(self.columns).cast(self.schema))
        self.buffered += table.num_rows
        self.rows += table.num_rows
        if self.buffered >= self.chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if not self.buf:
            return
        pq.write_table(
            pa.concat_tables(self.buf),
            self.dir / f"data_{self.seq:05d}.parquet",
            compression="snappy",
        )
        self.seq += 1
        self.buf = []
        self.buffered = 0

    def close(self) -> int:
        self._flush()
        if self.write_header:
            # Onboarding upload only — see the class docstring.
            pq.write_table(
                self.schema.empty_table(),
                self.dir / "00_header.parquet",
                compression="snappy",
            )
        return self.rows


# --------------------------------------------------------------------------
# main file: provider (flat) + taxonomy / other_identifier (melted)
# --------------------------------------------------------------------------

DATE_COLUMNS = {
    "enumeration_date",
    "last_update_date",
    "certification_date",
    "deactivation_date",
    "reactivation_date",
    "created_date",
}

# Columns dropped from the source's 55 flat fields, and why.
SUPPRESSED_SOURCE_COLUMNS = {
    "Employer Identification Number (EIN)": "suppressed by CMS; every row reads <UNAVAIL>",
    "Parent Organization TIN": "suppressed by CMS; every row reads <UNAVAIL>",
    "NPI Deactivation Reason Code": "not publicly disseminated; every row is empty",
    "Provider Other Organization Name": (
        "suppressed by CMS; every populated row reads <UNAVAIL>, and the real "
        "other names live in the other_name table (type code 6 points there)"
    ),
}

#: CMS writes this sentinel into columns it suppresses. A kept column made
#: entirely of it is a column we would be shipping empty, so the transform
#: checks the data rather than trusting the list above to stay current.
UNAVAILABLE_SENTINEL = "<UNAVAIL>"


def _assert_not_all_suppressed(table: pa.Table, name: str) -> None:
    """Fail if a column's every non-null value is the suppression sentinel.

    CMS suppresses fields by filling them with ``<UNAVAIL>`` rather than by
    dropping them, and it has done so to a field that reads like real data
    (``Provider Other Organization Name``). Such a column would otherwise ship
    as a wall of ``<UNAVAIL>`` and no test would notice.
    """
    for col in table.column_names:
        arr = table[col]
        if arr.null_count == len(arr):
            continue
        distinct = pc.unique(pc.drop_null(arr))  # pyrefly: ignore [missing-attribute]
        if len(distinct) == 1 and distinct[0].as_py() == UNAVAILABLE_SENTINEL:
            raise RuntimeError(
                f"{name}.{col} is entirely {UNAVAILABLE_SENTINEL}: CMS suppresses "
                "this field, so drop it from the architecture"
            )


TAXONOMY_SLOTS = 15
IDENTIFIER_SLOTS = 50


def _provenance(table: str, arch_dir: Path | None = None) -> dict[str, str]:
    arch_dir = Path(arch_dir or constants.ARCHITECTURE_DIR.value)
    with open(arch_dir / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return {r["name"]: r["original_name"] for r in csv.DictReader(fh)}


def _open_csv(path: Path, block_size: int | None = None):
    """Open a NPPES CSV as an all-string streaming reader."""
    with open(path, newline="", encoding="latin-1") as fh:
        header = next(csv.reader(fh))
    reader = pacsv.open_csv(
        str(path),
        read_options=pacsv.ReadOptions(
            block_size=block_size or constants.CSV_BLOCK_SIZE.value
        ),
        parse_options=pacsv.ParseOptions(quote_char='"'),
        convert_options=pacsv.ConvertOptions(
            strings_can_be_null=True,
            null_values=[""],
            column_types={c: pa.string() for c in header},
        ),
    )
    return reader, header


def _const(value: str | None, n: int) -> pa.Array:
    return pa.array([value] * n, type=pa.string())


def clean_main(
    main_csv: Path,
    out_dir: Path,
    extraction_date: str,
    arch_dir: Path | None = None,
    block_size: int | None = None,
    write_header: bool = False,
) -> dict[str, int]:
    """Stream the 330-column main file into provider + taxonomy + other_identifier."""
    prov_cols = architecture_columns("provider", arch_dir)
    tax_cols = architecture_columns("taxonomy", arch_dir)
    oid_cols = architecture_columns("other_identifier", arch_dir)
    prov_src = _provenance("provider", arch_dir)

    reader, header = _open_csv(main_csv, block_size)
    hset = set(header)
    # Fail loudly if the source layout moved under us.
    for name in prov_cols:
        if name in ("extraction_date", "primary_taxonomy_code"):
            continue
        src = prov_src[name]
        if src not in hset:
            raise RuntimeError(
                f"provider.{name}: source column {src!r} not in the main file"
            )
    for miss in SUPPRESSED_SOURCE_COLUMNS:
        if miss not in hset:
            raise RuntimeError(f"expected suppressed column {miss!r} is gone")

    w_prov = PartitionWriter(
        out_dir,
        "provider",
        prov_cols,
        extraction_date,
        write_header=write_header,
    )
    w_tax = PartitionWriter(
        out_dir,
        "taxonomy",
        tax_cols,
        extraction_date,
        write_header=write_header,
    )
    w_oid = PartitionWriter(
        out_dir,
        "other_identifier",
        oid_cols,
        extraction_date,
        write_header=write_header,
    )

    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        n = batch.num_rows
        col = {name: batch.column(i) for i, name in enumerate(header)}

        # ---- provider -------------------------------------------------
        data = {}
        for name in prov_cols:
            if name == "extraction_date":
                data[name] = _const(extraction_date, n)
            elif name == "primary_taxonomy_code":
                continue  # derived below
            else:
                a = _blank_to_null(col[prov_src[name]])
                data[name] = _mdy_to_iso(a) if name in DATE_COLUMNS else a

        # Primary taxonomy: the code in the slot whose switch is Y. At most one
        # per NPI by construction, so a fold over the slots is exact.
        primary = pa.nulls(n, pa.string())
        for i in range(1, TAXONOMY_SLOTS + 1):
            code = _blank_to_null(
                col[f"Healthcare Provider Taxonomy Code_{i}"]
            )
            switch = pc.fill_null(
                col[f"Healthcare Provider Primary Taxonomy Switch_{i}"], ""
            )
            primary = pc.if_else(  # pyrefly: ignore [missing-attribute]
                pc.and_(pc.equal(switch, "Y"), pc.is_valid(code)),  # pyrefly: ignore [missing-attribute]
                code,
                primary,
            )
        data["primary_taxonomy_code"] = primary
        provider_batch = pa.table(data).select(prov_cols)
        if w_prov.rows == 0:
            _assert_not_all_suppressed(provider_batch, "provider")
        w_prov.write(provider_batch)

        # ---- taxonomy (melt 15 slots) ---------------------------------
        parts = []
        for i in range(1, TAXONOMY_SLOTS + 1):
            code = _blank_to_null(
                col[f"Healthcare Provider Taxonomy Code_{i}"]
            )
            lic = _null_masked(
                _blank_to_null(col[f"Provider License Number_{i}"])
            )
            lic_st = _blank_to_null(
                col[f"Provider License Number State Code_{i}"]
            )
            sw = _blank_to_null(
                col[f"Healthcare Provider Primary Taxonomy Switch_{i}"]
            )
            grp = _blank_to_null(
                col[f"Healthcare Provider Taxonomy Group_{i}"]
            )
            # The group field concatenates a 10-char code and a free-text label.
            grp_code = pc.if_else(  # pyrefly: ignore [missing-attribute]
                pc.is_valid(grp),  # pyrefly: ignore [missing-attribute]
                pc.utf8_slice_codeunits(grp, 0, 10),  # pyrefly: ignore [missing-attribute]
                grp,  # pyrefly: ignore [missing-attribute]
            )  # pyrefly: ignore [missing-attribute]
            grp_name = pc.if_else(  # pyrefly: ignore [missing-attribute]
                pc.is_valid(grp),  # pyrefly: ignore [missing-attribute]
                pc.utf8_trim_whitespace(  # pyrefly: ignore [missing-attribute]
                    pc.utf8_slice_codeunits(grp, 10, 1 << 20)  # pyrefly: ignore [missing-attribute]
                ),  # pyrefly: ignore [missing-attribute]
                grp,
            )
            keep = pc.or_(  # pyrefly: ignore [missing-attribute]
                pc.or_(pc.is_valid(code), pc.is_valid(lic)),  # pyrefly: ignore [missing-attribute]
                pc.is_valid(grp),  # pyrefly: ignore [missing-attribute]
            )  # pyrefly: ignore [missing-attribute]
            if not pc.any(keep).as_py():  # pyrefly: ignore [missing-attribute]
                continue
            t = pa.table(
                {
                    "extraction_date": _const(extraction_date, n),
                    "npi": col["NPI"],
                    "taxonomy_sequence": _const(str(i), n),
                    "taxonomy_code": code,
                    "is_primary_taxonomy": sw,
                    "license_number": lic,
                    "license_state_code": lic_st,
                    "taxonomy_group_code": _blank_to_null(grp_code),
                    "taxonomy_group_name": _blank_to_null(grp_name),
                }
            ).filter(keep)
            if t.num_rows:
                parts.append(t.select(tax_cols))
        if parts:
            w_tax.write(pa.concat_tables(parts))

        # ---- other_identifier (melt 50 slots) -------------------------
        parts = []
        for i in range(1, IDENTIFIER_SLOTS + 1):
            ident = _null_masked(
                _blank_to_null(col[f"Other Provider Identifier_{i}"])
            )
            typ = _blank_to_null(
                col[f"Other Provider Identifier Type Code_{i}"]
            )
            st = _blank_to_null(col[f"Other Provider Identifier State_{i}"])
            issuer = _blank_to_null(
                col[f"Other Provider Identifier Issuer_{i}"]
            )
            keep = pc.or_(pc.is_valid(ident), pc.is_valid(typ))  # pyrefly: ignore [missing-attribute]
            if not pc.any(keep).as_py():  # pyrefly: ignore [missing-attribute]
                continue
            t = pa.table(
                {
                    "extraction_date": _const(extraction_date, n),
                    "npi": col["NPI"],
                    "identifier_sequence": _const(str(i), n),
                    "other_identifier": ident,
                    "other_identifier_type_code": typ,
                    "other_identifier_state_code": st,
                    "other_identifier_issuer": issuer,
                }
            ).filter(keep)
            if t.num_rows:
                parts.append(t.select(oid_cols))
        if parts:
            w_oid.write(pa.concat_tables(parts))

    return {
        "provider": w_prov.close(),
        "taxonomy": w_tax.close(),
        "other_identifier": w_oid.close(),
    }


def clean_reference(
    csv_path: Path,
    table: str,
    out_dir: Path,
    extraction_date: str,
    arch_dir: Path | None = None,
    block_size: int | None = None,
    write_header: bool = False,
) -> int:
    """Clean one of the three companion reference files (other_name / pl / endpoint)."""
    cols = architecture_columns(table, arch_dir)
    src = _provenance(table, arch_dir)
    reader, header = _open_csv(csv_path, block_size)
    hset = set(header)
    for name in cols:
        if name == "extraction_date":
            continue
        if src[name] not in hset:
            raise RuntimeError(
                f"{table}.{name}: source column {src[name]!r} not in "
                f"{Path(csv_path).name}"
            )
    w = PartitionWriter(
        out_dir, table, cols, extraction_date, write_header=write_header
    )
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        n = batch.num_rows
        col = {name: batch.column(i) for i, name in enumerate(header)}
        data = {}
        for name in cols:
            if name == "extraction_date":
                data[name] = _const(extraction_date, n)
            else:
                a = _blank_to_null(col[src[name]])
                data[name] = _mdy_to_iso(a) if name in DATE_COLUMNS else a
        w.write(pa.table(data).select(cols))
    return w.close()


# --------------------------------------------------------------------------
# dicionario + orchestration
# --------------------------------------------------------------------------

DICIONARIO_CSV = constants.ARCHITECTURE_DIR.value.parent / "dicionario.csv"


def clean_dicionario(
    out_dir: Path,
    arch_dir: Path | None = None,
    source_csv: Path | None = None,
    write_header: bool = False,
) -> int:
    """Convert the committed dictionary CSV to parquet. Not partitioned."""
    cols = architecture_columns("dicionario", arch_dir)
    src = Path(source_csv or DICIONARIO_CSV)
    table = pacsv.read_csv(
        str(src),
        convert_options=pacsv.ConvertOptions(
            strings_can_be_null=True,
            null_values=[""],
            column_types={c: pa.string() for c in cols},
        ),
    )
    w = PartitionWriter(
        out_dir, "dicionario", cols, None, write_header=write_header
    )
    w.write(table)
    return w.close()


def clean_all(
    input_dir: Path,
    output_dir: Path,
    arch_dir: Path | None = None,
    block_size: int | None = None,
    write_header: bool = False,
) -> dict:
    """Clean one monthly bundle into partitioned, all-STRING parquet.

    ``input_dir`` must hold the monthly ZIP (or its already-extracted CSVs).
    Returns row counts per table and the snapshot's ``extraction_date``.

    ``write_header`` adds the 0-row ``00_header.parquet`` guard that keeps the
    table-approve CI step from loading a large parquet whole. Only the one-shot
    onboarding upload wants it: ``upload_to_gcs`` would otherwise infer the whole
    staging schema from that empty file and type every column INT64. See
    :class:`PartitionWriter`.
    """
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    zips = sorted(input_dir.glob("NPPES_Data_Dissemination_*_V2.zip"))
    zips = [z for z in zips if "Weekly" not in z.name]
    if zips:
        paths = extract_zip(zips[-1], input_dir)
    else:  # already extracted

        def one(pattern: str) -> Path:
            hits = sorted(input_dir.glob(pattern))
            if not hits:
                raise RuntimeError(f"No {pattern} in {input_dir}")
            return hits[-1]

        paths = {
            "main": one("npidata_pfile_*[0-9].csv"),
            "other_name": one("othername_pfile_*[0-9].csv"),
            "practice_location": one("pl_pfile_*[0-9].csv"),
            "endpoint": one("endpoint_pfile_*[0-9].csv"),
        }

    extraction_date = extraction_date_from_name(paths["main"])
    # The flow sets log_prints=True, so these reach the Prefect run log. The
    # main file alone takes ~10 minutes and is otherwise completely silent.
    print(f"cleaning NPPES snapshot {extraction_date}")
    print(
        f"  main file: {paths['main'].name} "
        f"({paths['main'].stat().st_size / 1e9:.1f} GB)"
    )
    counts = clean_main(
        paths["main"],
        output_dir,
        extraction_date,
        arch_dir,
        block_size,
        write_header=write_header,
    )
    for table, n in counts.items():
        print(f"  {table:<20} {n:>12,} rows")
    for table in ("other_name", "practice_location", "endpoint"):
        counts[table] = clean_reference(
            paths[table],
            table,
            output_dir,
            extraction_date,
            arch_dir,
            block_size,
            write_header=write_header,
        )
        print(f"  {table:<20} {counts[table]:>12,} rows")
    counts["dicionario"] = clean_dicionario(
        output_dir, arch_dir, write_header=write_header
    )
    print(f"  {'dicionario':<20} {counts['dicionario']:>12,} rows")
    result: dict = {
        "counts": counts,
        "extraction_date": extraction_date,
        "max_extraction_date": extraction_date,
    }
    # Per-table output roots, as upload_to_gcs wants them (the hive partition
    # directory sits under each table root).
    for table in constants.TABLES.value:
        result[table] = str(output_dir / table)
    return result
