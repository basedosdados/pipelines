"""Pure download and cleaning functions for ``us_osha_enforcement``.

No Prefect imports live here. The one-shot onboarding bootstrap under
``models/us_osha_enforcement/code/`` and the recurring flow in ``flows.py``
both import these functions, so the cleaning transform exists once.

Three properties of the source drive the design:

* The bulk zips are *stored*, not deflated, so extracting them would double the
  6 GB on disk for nothing. Everything streams out of the zip.
* Only ``inspection`` and ``accident`` carry a usable date. Every child table
  takes its ``year`` from its parent, so a partition refresh covers the same
  set of inspections in every table.
* Numeric columns arrive from the DOL export as float strings (``"757.0"``),
  dates as full timestamps, and blanks sometimes as a single space.
"""

from __future__ import annotations

import csv
import io
import logging
import re
import sys
import zipfile
from collections import Counter
from collections.abc import Iterator
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.us_osha_enforcement.constants import constants

log = logging.getLogger(__name__)

# A single citation-text field runs to 136 KB; the default 128 KB limit trips.
csv.field_size_limit(min(sys.maxsize, 2**31 - 1))

#: Rows buffered per partition before a row group is flushed.
FLUSH_ROWS = 200_000

#: Columns the source ships but never populates, plus the DOL load timestamp,
#: which changes on every refresh and describes DOL's pipeline, not the data.
DROPPED = {
    "LOAD_DT",
    ("inspection", "STATE_FLAG"),
    ("accident", "EVENT_TIME"),
    ("accident", "STATE_FLAG"),
    ("accident", "ABSTRACT_TEXT"),
    ("optional_code_info", "OPT_INFO_ID"),
}


# --------------------------------------------------------------------------- #
# download
# --------------------------------------------------------------------------- #


def download_file(stem: str, input_dir: Path, session=None) -> Path:
    """Download one OSHA bulk zip, streaming to disk.

    Args:
        stem: Source file stem, e.g. ``"violation"``.
        input_dir: Directory to write into.
        session: Optional ``requests.Session``.

    Returns:
        Path to the downloaded zip.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    dest = input_dir / f"OSHA_{stem}.zip"
    url = constants.BULK_URL.value.format(file=stem)
    s = session or requests.Session()
    with s.get(url, stream=True, timeout=(30, 1800)) as r:
        r.raise_for_status()
        expected = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=8 << 20):
                fh.write(chunk)
    got = dest.stat().st_size
    if expected and got != expected:
        raise RuntimeError(
            f"{stem}: downloaded {got:,} bytes, Content-Length said {expected:,}"
        )
    log.info(f"{stem}: {got / 1e6:,.0f} MB -> {dest}")
    return dest


def download_all(input_dir: Path) -> list[Path]:
    """Download every OSHA bulk zip. Raises on the first failure."""
    s = requests.Session()
    return [
        download_file(stem, input_dir, s)
        for stem in constants.SOURCE_FILES.value
    ]


# --------------------------------------------------------------------------- #
# reading
# --------------------------------------------------------------------------- #


def _chunks(zf: zipfile.ZipFile) -> list[str]:
    """Chunk names in numeric order — ``chunk_2`` sorts before ``chunk_10``."""
    return sorted(
        zf.namelist(), key=lambda n: int(n.rsplit("_", 1)[1].split(".")[0])
    )


def read_source(
    stem: str, input_dir: Path
) -> Iterator[tuple[dict[str, int], list[str]]]:
    """Stream ``(header index, row)`` pairs out of one bulk zip.

    Each chunk repeats the header. The header is asserted identical across
    chunks: a source that changed schema mid-file would otherwise be read with
    the wrong column offsets and produce plausible, wrong data.
    """
    path = input_dir / f"OSHA_{stem}.zip"
    with zipfile.ZipFile(path) as zf:
        idx: dict[str, int] | None = None
        first: list[str] | None = None
        for name in _chunks(zf):
            with zf.open(name) as fh:
                reader = csv.reader(io.TextIOWrapper(fh, encoding="utf-8"))
                header = next(reader)
                if idx is None:
                    first = header
                    idx = {c: i for i, c in enumerate(header)}
                elif header != first:
                    raise RuntimeError(
                        f"{stem}: header changed in {name}: {header} != {first}"
                    )
                for row in reader:
                    yield idx, row


# --------------------------------------------------------------------------- #
# value normalisation
# --------------------------------------------------------------------------- #


def norm(value: str) -> str | None:
    """Strip whitespace; an empty or blank field becomes NULL.

    ``related_activity`` stores a literal single space where the flag does not
    apply, which would otherwise survive as a one-character category.
    """
    v = value.strip()
    return v or None


def norm_code(value: str) -> str | None:
    """Normalise a code the DOL export rendered as a float string.

    ``"757.0"`` becomes ``"757"``. Without this the join to ``dicionario``
    silently matches nothing, because the lookup file stores its own keys the
    same way and both would have to agree on the artefact rather than the code.
    """
    v = value.strip()
    if not v:
        return None
    if v.endswith(".0") and v[:-2].lstrip("-").isdigit():
        return v[:-2]
    return v


def norm_int(value: str) -> str | None:
    """Normalise an integer stored as a float string, keeping it textual."""
    return norm_code(value)


def norm_date(value: str) -> str | None:
    """Take the date part of a source timestamp.

    Returns ``None`` for a year outside 1900-2100. The source carries genuine
    data-entry errors — ``close_case_date`` reaches back to ``0120-11-18`` and
    ``hist_abate_date`` forward to ``5015-08-28`` — and BigQuery's DATE type
    accepts them, so a bad year would otherwise survive into published data
    looking like a real date.
    """
    v = value.strip()
    if not v:
        return None
    day = v[:10]
    if len(day) != 10 or day[4] != "-" or day[7] != "-":
        return None
    year = day[:4]
    if not year.isdigit() or not (1900 <= int(year) <= 2100):
        return None
    return day


def norm_year(value: str) -> int | None:
    """Year of a source timestamp, or ``None`` if the date is unusable."""
    day = norm_date(value)
    return int(day[:4]) if day else None


# --------------------------------------------------------------------------- #
# parent-year lookups
# --------------------------------------------------------------------------- #


class YearMap:
    """Compact ``id -> year`` lookup over sorted numpy arrays.

    ``inspection`` has 5.2M keys and ``accident`` 166k. A Python dict of 5.2M
    string keys costs roughly 1 GB, which does not fit the 4 GB the Prefect
    work pool actually grants (see ``prefect-pipeline-conventions``). Two
    numpy arrays plus a binary search cost about 50 MB.

    Ids are numeric in the source but published as STRING, because
    ``reporting_office_id`` and friends carry leading zeros; only the lookup
    keys are parsed to int.
    """

    def __init__(self, pairs: Iterator[tuple[str, int]]):
        keys: list[int] = []
        years: list[int] = []
        for k, y in pairs:
            try:
                keys.append(int(k))
            except (TypeError, ValueError):
                continue
            years.append(y)
        order = np.argsort(np.asarray(keys, dtype=np.int64), kind="stable")
        self._keys = np.asarray(keys, dtype=np.int64)[order]
        self._years = np.asarray(years, dtype=np.int32)[order]

    def __len__(self) -> int:
        return self._keys.size

    def get(self, key: str | None) -> int | None:
        """Year for ``key``, or ``None`` when the parent row does not exist."""
        if key is None:
            return None
        try:
            k = int(key)
        except (TypeError, ValueError):
            return None
        i = int(np.searchsorted(self._keys, k))
        if i >= self._keys.size or int(self._keys[i]) != k:
            return None
        y = int(self._years[i])
        return y if y > 0 else None


def build_inspection_years(input_dir: Path) -> YearMap:
    """``inspection_id -> year(open_date)`` for every inspection."""

    def pairs():
        for idx, row in read_source("inspection", input_dir):
            y = norm_year(row[idx["OPEN_DATE"]])
            if y is not None:
                yield row[idx["ACTIVITY_NR"]], y

    m = YearMap(pairs())
    log.info(f"inspection year map: {len(m):,} ids")
    return m


def build_accident_years(input_dir: Path) -> YearMap:
    """``accident_id -> year(event_date)`` for every investigated incident."""

    def pairs():
        for idx, row in read_source("accident", input_dir):
            y = norm_year(row[idx["EVENT_DATE"]])
            if y is not None:
                yield row[idx["SUMMARY_NR"]], y

    m = YearMap(pairs())
    log.info(f"accident year map: {len(m):,} ids")
    return m


# --------------------------------------------------------------------------- #
# partitioned all-STRING parquet writer
# --------------------------------------------------------------------------- #


class PartitionWriter:
    """Buffer rows per ``year`` and flush row groups to one file per partition.

    Streaming matters: ``violation`` has 13.3M rows, and holding them in Python
    lists before writing would cost tens of GB. Buffering per year and flushing
    at :data:`FLUSH_ROWS` keeps peak memory near one partition's worth.

    Every column is written as STRING. Staging is all-STRING by house
    convention — ``pipelines.utils.gcs.dump_header`` stringifies the header
    BigQuery infers the external schema from, so typed parquet is rejected — and
    the dbt model ``safe_cast``s each column to its architecture type. Values
    are cast through arrow, never ``astype(str)``, which would render a NULL as
    the literal ``"nan"`` and defeat that ``safe_cast``.

    One file per partition, not one per flush: ``upload_to_gcs`` with
    ``dump_mode="append"`` uploads by object name, so a second run writing
    ``data_0001.parquet`` would overwrite the first run's rather than add to it.
    """

    def __init__(
        self,
        table: str,
        columns: list[str],
        out_dir: Path,
        partitioned: bool = True,
    ):
        self.table = table
        self.columns = columns
        self.partitioned = partitioned
        self.schema = pa.schema([pa.field(c, pa.string()) for c in columns])
        self.dir = out_dir / table
        self._buf: dict[int, list[list[str | None]]] = {}
        self._writers: dict[int, pq.ParquetWriter] = {}
        self._buffered = 0
        self.rows = 0
        self.dropped_no_year = 0

    def add(self, year: int | None, values: list[str | None]) -> None:
        """Queue one row. A row with no resolvable year is counted and dropped."""
        if year is None:
            self.dropped_no_year += 1
            return
        self._buf.setdefault(year, []).append(values)
        self._buffered += 1
        self.rows += 1
        if self._buffered >= FLUSH_ROWS:
            self._flush_largest()

    def _flush_largest(self) -> None:
        year = max(self._buf, key=lambda y: len(self._buf[y]))
        self._flush(year)

    def _flush(self, year: int) -> None:
        rows = self._buf.pop(year, None)
        if not rows:
            return
        self._buffered -= len(rows)
        frame = pd.DataFrame(rows, columns=self.columns, dtype="object")
        batch = pa.Table.from_pandas(
            frame, schema=self.schema, preserve_index=False
        )
        writer = self._writers.get(year)
        if writer is None:
            pdir = self.dir / f"year={year}" if self.partitioned else self.dir
            pdir.mkdir(parents=True, exist_ok=True)
            writer = pq.ParquetWriter(
                pdir / "data.parquet", self.schema, compression="snappy"
            )
            self._writers[year] = writer
        writer.write_table(batch)

    def close(self) -> dict[int, int]:
        """Flush every partition and close the files."""
        for year in list(self._buf):
            self._flush(year)
        for writer in self._writers.values():
            writer.close()
        unit = "partitions" if self.partitioned else "files"
        log.info(
            f"{self.table}: {self.rows:,} rows -> {len(self._writers)} {unit}"
            + (
                f" ({self.dropped_no_year:,} rows dropped, no parent year)"
                if self.dropped_no_year
                else ""
            )
        )
        return {y: 0 for y in sorted(self._writers)}


# --------------------------------------------------------------------------- #
# architecture-driven column mapping
# --------------------------------------------------------------------------- #


def _arch():
    """Import the architecture module from ``models/`` without a package."""
    import importlib.util

    path = Path(constants.ARCHITECTURE_DIR.value) / "architecture_def.py"
    spec = importlib.util.spec_from_file_location("us_osha_architecture", path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise RuntimeError(f"cannot import architecture from {path}")
    mod = importlib.util.module_from_spec(spec)
    # dataclass resolution reads sys.modules[cls.__module__]; register first.
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _source_schema() -> dict:
    path = Path(constants.ARCHITECTURE_DIR.value) / "source_schema.json"
    import json

    return json.loads(path.read_text())


def column_normalisers(table) -> list:
    """One normaliser per published column, chosen from type and source type.

    ``.0`` stripping is driven by the DOL catalog's own ``data_type``: a column
    the catalog calls ``number`` arrives from the export as ``"757.0"`` even
    when Data Basis publishes it as STRING, because it is a code. Applying the
    strip to every STRING column instead would risk mangling free text that
    happens to end in ``.0``.
    """
    schema = _source_schema()
    src_cols = schema.get(table.source_file.removeprefix("OSHA_"), {}).get(
        "columns", {}
    )
    out = []
    for col in table.columns:
        if col.src is None:
            out.append(norm)
        elif col.bigquery_type == "DATE":
            out.append(norm_date)
        elif src_cols.get(col.src, {}).get("data_type") == "number":
            out.append(norm_code)
        else:
            out.append(norm)
    return out


# --------------------------------------------------------------------------- #
# table builders
# --------------------------------------------------------------------------- #


def _year_resolver(table, insp_years: YearMap, acc_years: YearMap):
    """Return ``(row, idx) -> year`` for one table's partitioning rule."""
    mode = table.year_from
    if mode == "open_date":
        return lambda idx, row: norm_year(row[idx["OPEN_DATE"]])
    if mode == "event_date":
        return lambda idx, row: norm_year(row[idx["EVENT_DATE"]])
    if mode == "inspection":
        return lambda idx, row: insp_years.get(row[idx["ACTIVITY_NR"]])
    if mode == "accident":
        return lambda idx, row: acc_years.get(row[idx["SUMMARY_NR"]])
    if mode == "accident_or_inspection":
        # 22,509 of the 188,305 incidents referenced by an injury row have no
        # row in `accident`, so the incident date is unavailable for 12% of
        # them. Those fall back to the inspection that investigated the
        # incident, which `rel_insp_nr` gives for every row.
        def resolve(idx, row):
            return acc_years.get(row[idx["SUMMARY_NR"]]) or insp_years.get(
                row[idx["REL_INSP_NR"]]
            )

        return resolve
    raise ValueError(f"unknown year_from: {mode!r}")


def build_simple(
    table,
    input_dir: Path,
    out_dir: Path,
    insp_years: YearMap,
    acc_years: YearMap,
) -> PartitionWriter:
    """Clean one table that maps a source row straight to a published row."""
    cols = [c.name for c in table.columns]
    norms = column_normalisers(table)
    year_of = _year_resolver(table, insp_years, acc_years)
    stem = table.source_file.removeprefix("OSHA_")
    writer = PartitionWriter(table.slug, cols, out_dir)
    for idx, row in read_source(stem, input_dir):
        year = year_of(idx, row)
        values: list[str | None] = []
        for col, fn in zip(table.columns, norms, strict=True):
            if col.name == "year":
                values.append(str(year) if year is not None else None)
            else:
                values.append(fn(row[idx[col.src]]))
        writer.add(year, values)
    writer.close()
    return writer


WORD_RE = re.compile(r"[A-Za-z]+")

#: A token must appear this many times, strictly inside a line, to count as a
#: word. Interior tokens can never be wrap fragments, so the vocabulary they
#: build is clean by construction.
VOCAB_MIN_COUNT = 20


def build_vocabulary(docs: dict) -> set[str]:
    """Vocabulary of real words, from tokens that never touch a line boundary.

    The first and last alphabetic run of every line may be half a word, so both
    are excluded. What remains is ordinary corpus vocabulary — about 9,800
    words for the incident narratives.
    """
    counts: Counter[str] = Counter()
    for lines in docs.values():
        for text in lines.values():
            tokens = WORD_RE.findall(text)
            for token in tokens[1:-1]:
                counts[token.lower()] += 1
    return {w for w, n in counts.items() if n >= VOCAB_MIN_COUNT}


def _boundary_is_midword(a: str, b: str, vocab: set[str]) -> bool:
    """Did an 80-character boundary fall inside a word?

    Under the fixed-80 regime a line is a pure slice of the original text, so
    whatever character sat at position 81 survives — a space or punctuation at
    the boundary settles it immediately.

    Otherwise the two boundary tokens decide. A boundary is a *word* boundary,
    meaning a space was consumed and the regime is word wrapping, only when
    both tokens are corpus words in their own right and their concatenation is
    not: ``employee`` + ``died`` are both words and ``employeedied`` is not.
    Everything else is a mid-word split. Requiring both halves to be words
    matters for technical vocabulary the corpus sees rarely — ``autoe`` +
    ``levator`` joins to ``autoelevator``, which is too rare to be in the
    vocabulary, but ``autoe`` is not a word either, so the boundary is
    correctly read as mid-word.
    """
    if not a or not b:
        return True
    if not a[-1].isalpha() or not b[0].isalpha():
        return True
    head, tail = WORD_RE.findall(a), WORD_RE.findall(b)
    if not head or not tail:
        return True
    left, right = head[-1].lower(), tail[0].lower()
    if (left + right) in vocab:
        return True
    return left not in vocab or right not in vocab


def join_lines(lines: list[str], vocab: set[str]) -> tuple[str, str]:
    """Reassemble one wrapped narrative, returning ``(text, wrap_style)``.

    The source uses two wrapping regimes and never says which. Older IMIS
    records pad every line but the last to exactly 80 characters and split
    mid-word, so the lines join with no separator. Newer records wrap at word
    boundaries and drop the space that was there, so they join with one space.

    Getting this wrong is not cosmetic. Joining everything without a separator
    corrupts the 55,910 word-wrapped narratives into ``"cleanoff a driveway"``
    and ``"theemployee"`` — text that still reads as prose and passes every
    aggregate check. Deciding on line length alone is not enough either: 615
    word-wrapped narratives happen to have every non-final line at exactly 80
    characters, and would be glued the same way. Together that is 34% of the
    165,794 narratives.

    So length only screens: a record with a short non-final line is word
    wrapped for certain. The rest are settled by majority vote over their
    boundaries, using :func:`_boundary_is_midword`.
    """
    if len(lines) == 1:
        return lines[0], "single_line"
    if not all(len(x) == 80 for x in lines[:-1]):
        return " ".join(lines), "word_wrap"
    votes = [
        _boundary_is_midword(lines[i], lines[i + 1], vocab)
        for i in range(len(lines) - 1)
    ]
    if sum(votes) >= len(votes) / 2:
        return "".join(lines), "fixed_80"
    return " ".join(lines), "word_wrap"


def build_accident_narrative(
    table, input_dir: Path, out_dir: Path, acc_years: YearMap
) -> PartitionWriter:
    """Reassemble ``accident_abstract`` into one narrative per incident.

    Grouping goes through a dict rather than flushing when the key changes:
    the source is *not* ordered by ``summary_nr``, so the streaming version
    produces 206,321 fragments where there are 165,794 narratives — a 24%
    corruption that looks entirely plausible in the output.
    """
    docs: dict[str, dict[int, str]] = {}
    for idx, row in read_source("accident_abstract", input_dir):
        key = norm_code(row[idx["SUMMARY_NR"]])
        text = row[idx["ABSTRACT_TEXT"]]
        if key is None:
            continue
        try:
            line = int(float(row[idx["LINE_NR"]]))
        except (TypeError, ValueError):
            continue
        docs.setdefault(key, {})[line] = text

    vocab = build_vocabulary(docs)
    log.info(f"accident_narrative: vocabulary of {len(vocab):,} words")
    writer = PartitionWriter(
        table.slug, [c.name for c in table.columns], out_dir
    )
    for key, lines in docs.items():
        ordered = [lines[n] for n in sorted(lines)]
        text, style = join_lines(ordered, vocab)
        text = text.strip()
        year = acc_years.get(key)
        writer.add(
            year,
            [
                str(year) if year is not None else None,
                key,
                text or None,
                str(len(ordered)),
                style,
            ],
        )
    writer.close()
    return writer


def build_violation_text(
    table, input_dir: Path, out_dir: Path, insp_years: YearMap
) -> PartitionWriter:
    """Reassemble ``violation_gen_duty_std`` into one text per citation.

    Unlike the incident narratives these lines are not fixed-width — a single
    line runs to 32 KB — so they always join with a space.
    """
    docs: dict[tuple[str, str], dict[int, str]] = {}
    for idx, row in read_source("violation_gen_duty_std", input_dir):
        activity = norm_code(row[idx["ACTIVITY_NR"]])
        citation = norm(row[idx["CITATION_ID"]])
        if activity is None or citation is None:
            continue
        try:
            line = int(float(row[idx["LINE_NR"]]))
        except (TypeError, ValueError):
            continue
        docs.setdefault((activity, citation), {})[line] = row[idx["LINE_TEXT"]]

    writer = PartitionWriter(
        table.slug, [c.name for c in table.columns], out_dir
    )
    for (activity, citation), lines in docs.items():
        ordered = [lines[n] for n in sorted(lines)]
        text = " ".join(x.strip() for x in ordered).strip()
        year = insp_years.get(activity)
        writer.add(
            year,
            [
                str(year) if year is not None else None,
                activity,
                citation,
                text or None,
                str(len(ordered)),
            ],
        )
    writer.close()
    return writer


def build_dicionario(table, input_dir: Path, out_dir: Path) -> PartitionWriter:
    """Build the dictionary from the two authoritative code sources.

    OSHA ships 14 code tables in ``osha_accident_lookup2``; the DOL catalog
    publishes code lists in its column descriptions. Nothing else is invented —
    a column whose codes neither source documents simply has no rows here, and
    says so in its own ``observations``.
    """
    import importlib.util

    path = Path(constants.ARCHITECTURE_DIR.value) / "dictionary_def.py"
    spec = importlib.util.spec_from_file_location("us_osha_dictionary", path)
    if spec is None or spec.loader is None:  # pragma: no cover
        raise RuntimeError(f"cannot import dictionary from {path}")
    dd = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = dd
    spec.loader.exec_module(dd)

    rows: list[tuple[str, str, str, str]] = []

    # 1. OSHA's own lookup file.
    labels: dict[str, dict[str, str]] = {}
    for idx, row in read_source("accident_lookup2", input_dir):
        code_table = norm(row[idx["ACCIDENT_CODE"]])
        label = norm(row[idx["ACCIDENT_VALUE"]])
        key = norm(row[idx["ACCIDENT_LETTER"]]) or norm_code(
            row[idx["ACCIDENT_NUMBER"]]
        )
        if code_table and key and label:
            labels.setdefault(code_table, {})[key] = label
    for code_table, targets in dd.LOOKUP_TABLE_TO_COLUMN.items():
        entries = labels.get(code_table)
        if not entries:
            raise RuntimeError(
                f"code table {code_table} missing from osha_accident_lookup2"
            )
        for table_slug, column in targets:
            for key, label in entries.items():
                rows.append((table_slug, column, key, label))

    # 2. Code lists published in the DOL catalog and the Field Operations Manual.
    for (table_slug, column), entries in dd.PUBLISHED_CODES.items():
        for key, label in entries.items():
            rows.append((table_slug, column, key, label))

    writer = PartitionWriter(
        table.slug, [c.name for c in table.columns], out_dir, partitioned=False
    )
    seen: set[tuple[str, str, str]] = set()
    for table_slug, column, key, label in sorted(rows):
        if (table_slug, column, key) in seen:
            continue
        seen.add((table_slug, column, key))
        writer.add(0, [table_slug, column, key, None, label])
    writer.close()
    return writer


# --------------------------------------------------------------------------- #
# driver
# --------------------------------------------------------------------------- #


def clean_all(
    input_dir: Path, output_dir: Path, tables: list[str] | None = None
) -> dict[str, int]:
    """Clean every table, returning ``{table_slug: rows}``.

    ``inspection`` and ``accident`` are read first regardless of the requested
    subset: every other table takes its ``year`` from one of them.
    """
    arch = _arch()
    wanted = tables or [t.slug for t in arch.TABLES]
    insp_years = build_inspection_years(input_dir)
    acc_years = build_accident_years(input_dir)

    counts: dict[str, int] = {}
    for table in arch.TABLES:
        if table.slug not in wanted:
            continue
        if table.slug == "dicionario":
            writer = build_dicionario(table, input_dir, output_dir)
        elif table.slug == "accident_narrative":
            writer = build_accident_narrative(
                table, input_dir, output_dir, acc_years
            )
        elif table.slug == "violation_text":
            writer = build_violation_text(
                table, input_dir, output_dir, insp_years
            )
        else:
            writer = build_simple(
                table, input_dir, output_dir, insp_years, acc_years
            )
        counts[table.slug] = writer.rows
    return counts
