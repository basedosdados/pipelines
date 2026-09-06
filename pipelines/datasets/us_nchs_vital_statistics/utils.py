"""Pure functions for the NCHS Vital Statistics onboarding and refresh pipeline.

No Prefect imports here: the one-shot bootstrap under
``models/us_nchs_vital_statistics/code/`` imports these same functions, so the
cleaning transform exists in exactly one place.

The source is annual fixed-width public-use microdata whose record layout changes
almost every year (56 distinct birth layouts, 57 death layouts over 1968-2024).
``nchs_layouts.csv`` carries every year's column positions; ``COLUMN_SPEC`` below
maps each harmonized output column to the source variable that carries it in a
given year.

Harmonization principle: where the source CODING changed, the eras are kept as
SEPARATE output columns rather than silently merged. That applies to education
(years of schooling under the 1989 certificate vs categories under the 2003
certificate), race (single/bridged race before 2014 vs OMB-1997 multi-race after)
and cause of death (ICD-8/9/10). See ``models/us_nchs_vital_statistics/README.md``.
"""

from __future__ import annotations

import contextlib
import csv
import logging
import shutil
import subprocess
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.us_nchs_vital_statistics.constants import constants

log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Column specification
# --------------------------------------------------------------------------- #
# (column, bigquery_type, measurement_unit, candidate source variables,
#  sentinel values -> NULL, required source width or None, note)
#
# Candidates are tried in order; the first one present in that year wins. A
# `width` constraint disambiguates variables that were REUSED for a different
# concept: `educ` is 2-char years of schooling in 1989-2002 but a 1-char 2003
# revision category in 2003-2005.

COLUMN_SPEC: dict[str, list[tuple]] = {
    "birth": [
        (
            "year",
            "INT64",
            "year",
            ["dob_yy", "datayear"],
            [],
            None,
            "partition column; pre-2003 the source stores a truncated year, so it is taken from the file",
        ),
        (
            "birth_month",
            "INT64",
            "month",
            ["dob_mm", "birmon"],
            ["99"],
            None,
            "",
        ),
        (
            "birth_day_of_week",
            "STRING",
            "",
            ["dob_wk"],
            [],
            None,
            "2003 onward",
        ),
        (
            "state_residence_id",
            "STRING",
            "",
            ["stresfip", "mrstate", "stateres"],
            [],
            None,
            "state FIPS; native FIPS 1982-2002, NCHS alphabetical code mapped 1968-1981, "
            "postal code mapped 2003-2004; ABSENT from the public-use file 2005 onward",
        ),
        (
            "county_residence_id",
            "STRING",
            "",
            ["cntyrfip"],
            [],
            None,
            "5-digit state+county FIPS, 1982-2002 only. Deliberately NOT back-filled from the "
            "pre-1982 `cntyres`, which is an NCHS county code on a different numbering system",
        ),
        ("residence_status", "STRING", "", ["restatus"], [], None, ""),
        (
            "record_weight",
            "INT64",
            "",
            ["recwt"],
            [],
            None,
            "NCHS sampling weight. The natality file is a 50 percent sample for states "
            "outside the 100 percent reporting programme in the early years, so a record "
            "may stand for two births: counts and rates must be weighted by this column. "
            "It is 1 for all states from 1985 and is not published before 1972 (see the "
            "table description for how to treat 1968-1971)",
        ),
        (
            "county_residence_population_code",
            "STRING",
            "",
            ["rcnty_pop", "cntrspop"],
            [],
            None,
            "population-size category of the residence county, not a count",
        ),
        (
            "mother_age_years",
            "INT64",
            "year",
            ["mager", "umagerpt", "dmage"],
            ["99"],
            None,
            "",
        ),
        (
            "mother_age_recode_9",
            "STRING",
            "",
            ["mager9", "mage8"],
            [],
            None,
            "category boundaries differ before and after 2003",
        ),
        (
            "mother_education_years",
            "INT64",
            "year",
            ["dmeduc"],
            ["99", "88"],
            None,
            "1989 certificate: YEARS of schooling completed. 1970-2008 only",
        ),
        (
            "mother_education_code",
            "STRING",
            "",
            ["meduc"],
            [],
            None,
            "2003 certificate: CATEGORIES. NOT comparable to mother_education_years",
        ),
        (
            "mother_race_code",
            "STRING",
            "",
            ["mrace"],
            [],
            None,
            "single/bridged detail race, 1968-2013",
        ),
        (
            "mother_race_bridged_code",
            "STRING",
            "",
            ["mbrace"],
            [],
            None,
            "2003-2019",
        ),
        (
            "mother_race_recode_6",
            "STRING",
            "",
            ["mrace6"],
            [],
            None,
            "OMB 1997 multi-race, 2014 onward. NOT comparable to mother_race_code",
        ),
        (
            "mother_race_recode_31",
            "STRING",
            "",
            ["mrace31"],
            [],
            None,
            "2014 onward",
        ),
        (
            "mother_hispanic_origin_code",
            "STRING",
            "",
            ["mhisp_r", "umhisp"],
            [],
            None,
            "2003 onward",
        ),
        (
            "mother_race_hispanic_code",
            "STRING",
            "",
            ["mracehisp"],
            [],
            None,
            "combined race and Hispanic-origin recode, 2003-2024",
        ),
        ("mother_nativity_code", "STRING", "", ["mbstate_rec"], [], None, ""),
        ("mother_marital_status", "STRING", "", ["dmar", "mar"], [], None, ""),
        (
            "father_age_years",
            "INT64",
            "year",
            ["fagecomb", "dfage"],
            ["99"],
            None,
            "",
        ),
        ("father_race_code", "STRING", "", ["frace"], [], None, "1968-2013"),
        (
            "father_race_recode_6",
            "STRING",
            "",
            ["frace6"],
            [],
            None,
            "2014 onward",
        ),
        (
            "live_birth_order",
            "INT64",
            "",
            ["lbo_rec", "dlivord"],
            ["99"],
            None,
            "order of this birth among the mother's live births",
        ),
        (
            "total_birth_order",
            "INT64",
            "",
            ["tbo_rec", "dtotord"],
            ["99"],
            None,
            "live births plus fetal deaths",
        ),
        (
            "gestation_weeks",
            "INT64",
            "week",
            ["combgest", "dgestat"],
            ["99", "00"],
            None,
            "",
        ),
        (
            "gestation_recode_3",
            "STRING",
            "",
            ["gestrec3"],
            [],
            None,
            "2003 onward",
        ),
        (
            "birth_weight_grams",
            "INT64",
            "gram",
            ["dbwt", "dbirwt"],
            ["9999", "0000"],
            None,
            "",
        ),
        (
            "birth_weight_recode_4",
            "STRING",
            "",
            ["bwtr4"],
            [],
            None,
            "2003 onward",
        ),
        (
            "prenatal_care_month_began",
            "INT64",
            "month",
            ["precare", "monpre"],
            ["99"],
            None,
            "month of pregnancy in which prenatal care began; 0 means no prenatal care",
        ),
        (
            "prenatal_visits",
            "INT64",
            "",
            ["previs", "uprevis", "nprevis"],
            ["99", "88"],
            None,
            "",
        ),
        ("sex", "STRING", "", ["sex", "csex"], [], None, ""),
        (
            "plurality",
            "STRING",
            "",
            ["dplural"],
            [],
            None,
            "top code caps at quintuplet and above",
        ),
        (
            "delivery_method_recode",
            "STRING",
            "",
            ["rdmeth_rec", "dmeth_rec"],
            [],
            None,
            "2003 onward",
        ),
        (
            "birth_attendant",
            "STRING",
            "",
            ["attend", "birattnd"],
            [],
            None,
            "",
        ),
        (
            "apgar_score_5min",
            "INT64",
            "",
            ["apgar5"],
            ["99"],
            None,
            "five-minute Apgar score, 0-10; 2003 onward",
        ),
    ],
    "death": [
        (
            "year",
            "INT64",
            "year",
            ["year", "datayear"],
            [],
            None,
            "partition column",
        ),
        ("death_month", "INT64", "month", ["monthdth"], ["99"], None, ""),
        (
            "death_day_of_week",
            "STRING",
            "",
            ["weekday"],
            [],
            None,
            "1989 onward",
        ),
        (
            "state_residence_id",
            "STRING",
            "",
            ["fipsstr", "staters"],
            [],
            None,
            "state FIPS; native FIPS 1982-2004, NCHS alphabetical code mapped 1968-1981; "
            "ABSENT from the public-use file 2005 onward",
        ),
        (
            "county_residence_id",
            "STRING",
            "",
            ["fipsctyr"],
            [],
            None,
            "5-digit state+county FIPS, 1982-2004 only. Deliberately NOT back-filled from the "
            "pre-1982 `countyrs`, which is an NCHS county code on a different numbering system",
        ),
        ("residence_status", "STRING", "", ["restatus"], [], None, ""),
        (
            "age_detail_code",
            "STRING",
            "",
            ["age"],
            [],
            None,
            "raw unit-prefixed age code as published; 3 characters before 2003, 4 from 2003",
        ),
        (
            "age_years",
            "INT64",
            "year",
            [],
            [],
            None,
            "derived from age_detail_code; NULL when the record's age unit is not years or age is unknown",
        ),
        ("age_recode_27", "STRING", "", ["ager27"], [], None, ""),
        ("age_recode_12", "STRING", "", ["ager12"], [], None, ""),
        ("sex", "STRING", "", ["sex"], [], None, ""),
        (
            "race_code",
            "STRING",
            "",
            ["race"],
            [],
            None,
            "detail race as published",
        ),
        ("race_recode_3", "STRING", "", ["racer3"], [], None, "1968-2020"),
        ("race_recode_5", "STRING", "", ["racer5"], [], None, "2004-2020"),
        (
            "race_bridged_flag",
            "STRING",
            "",
            ["brace"],
            [],
            None,
            "2003 onward",
        ),
        (
            "hispanic_origin_code",
            "STRING",
            "",
            ["hispanic"],
            [],
            None,
            "1989 onward",
        ),
        (
            "hispanic_origin_race_recode",
            "STRING",
            "",
            ["hspanicr"],
            [],
            1,
            "Hispanic origin by BRIDGED race (1977 OMB standards), 1989-2020. Width 1 "
            "separates it from the 1997-standard recode below; NCHS states the two are "
            "not comparable, and 2021 is not populated in the source",
        ),
        (
            "hispanic_origin_race_recode_1997",
            "STRING",
            "",
            ["hspanicr"],
            [],
            2,
            "Hispanic origin by SINGLE race (1997 OMB standards), 2022 onward. NOT "
            "comparable to hispanic_origin_race_recode",
        ),
        (
            "education_years",
            "INT64",
            "year",
            ["educ1989", "educ89", "educ"],
            ["99"],
            2,
            "1989 certificate: YEARS of schooling completed. Width 2 disambiguates it from the "
            "2003-revision `educ`. NOT comparable to education_code",
        ),
        (
            "education_code",
            "STRING",
            "",
            ["educ2003", "educ"],
            [],
            1,
            "2003 certificate: CATEGORIES. NOT comparable to education_years",
        ),
        ("education_reporting_flag", "STRING", "", ["educflag"], [], None, ""),
        ("marital_status", "STRING", "", ["marstat"], [], None, "1979 onward"),
        ("place_of_death", "STRING", "", ["placdth"], [], None, "1989 onward"),
        ("manner_of_death", "STRING", "", ["mandeath"], [], None, ""),
        (
            "underlying_cause_code",
            "STRING",
            "",
            ["ucod"],
            [],
            None,
            "underlying cause of death; the ICD revision varies by year, see icd_revision",
        ),
        (
            "icd_revision",
            "STRING",
            "",
            [],
            [],
            None,
            "derived: ICD-8 for 1968-1978, ICD-9 for 1979-1998, ICD-10 from 1999",
        ),
        (
            "cause_recode_113",
            "STRING",
            "",
            ["ucr113"],
            [],
            None,
            "ICD-10 based, 1999 onward",
        ),
        (
            "cause_recode_358",
            "STRING",
            "",
            ["ucr358"],
            [],
            None,
            "ICD-10 based, 1999 onward",
        ),
        (
            "cause_recode_39",
            "STRING",
            "",
            ["ucr39"],
            [],
            None,
            "ICD-10 based, 1999 onward",
        ),
        (
            "cause_recode_72",
            "STRING",
            "",
            ["ucr72"],
            [],
            None,
            "ICD-9 based, 1979-1998",
        ),
        ("record_axis_condition_count", "INT64", "", ["ranum"], [], None, ""),
        ("autopsy", "STRING", "", ["autopsy"], [], None, ""),
        ("injury_at_work", "STRING", "", ["injwork"], [], None, "1993 onward"),
    ],
}

OUTPUT_COLUMNS = {p: [c[0] for c in spec] for p, spec in COLUMN_SPEC.items()}


# --------------------------------------------------------------------------- #
# Geography crosswalks
# --------------------------------------------------------------------------- #
# Through 1981 both files identify states with the NCHS code: the 50 states plus
# DC numbered 01-51 in alphabetical order. Verified empirically against the
# state-sorted 1975 natality file (01 Alabama, 02 Alaska, 03 Arizona,
# 04 Arkansas, 05 California). FIPS is also alphabetical but skips numbers, so
# the two never coincide beyond Alabama.
_STATES_ALPHABETICAL = [
    ("AL", "01"),
    ("AK", "02"),
    ("AZ", "04"),
    ("AR", "05"),
    ("CA", "06"),
    ("CO", "08"),
    ("CT", "09"),
    ("DE", "10"),
    ("DC", "11"),
    ("FL", "12"),
    ("GA", "13"),
    ("HI", "15"),
    ("ID", "16"),
    ("IL", "17"),
    ("IN", "18"),
    ("IA", "19"),
    ("KS", "20"),
    ("KY", "21"),
    ("LA", "22"),
    ("ME", "23"),
    ("MD", "24"),
    ("MA", "25"),
    ("MI", "26"),
    ("MN", "27"),
    ("MS", "28"),
    ("MO", "29"),
    ("MT", "30"),
    ("NE", "31"),
    ("NV", "32"),
    ("NH", "33"),
    ("NJ", "34"),
    ("NM", "35"),
    ("NY", "36"),
    ("NC", "37"),
    ("ND", "38"),
    ("OH", "39"),
    ("OK", "40"),
    ("OR", "41"),
    ("PA", "42"),
    ("RI", "44"),
    ("SC", "45"),
    ("SD", "46"),
    ("TN", "47"),
    ("TX", "48"),
    ("UT", "49"),
    ("VT", "50"),
    ("VA", "51"),
    ("WA", "53"),
    ("WV", "54"),
    ("WI", "55"),
    ("WY", "56"),
]
NCHS_STATE_TO_FIPS = {
    f"{i + 1:02d}": fips for i, (_, fips) in enumerate(_STATES_ALPHABETICAL)
}
POSTAL_TO_FIPS = {p: f for p, f in _STATES_ALPHABETICAL}
# The 50 states plus DC. The files also carry "00" for foreign or unknown
# residence and, in some years, territory codes; neither is a state FIPS, so
# anything outside this set is emitted as NULL rather than a bogus code.
VALID_STATE_FIPS = {f for _, f in _STATES_ALPHABETICAL}


def load_layouts(path: Path | str | None = None) -> dict:
    """Return {(product, year): {variable: (start_index, width)}} from the layout CSV.

    ``start_index`` is 0-based; the CSV stores the 1-based column from the record
    layout.
    """
    path = Path(path or constants.LAYOUTS_CSV.value)
    layouts: dict[tuple[str, int], dict[str, tuple[int, int]]] = {}
    with open(path, newline="") as f:
        for r in csv.DictReader(f):
            if not r["width"]:
                continue
            key = (r["product"], int(r["year"]))
            layouts.setdefault(key, {})[r["variable"]] = (
                int(r["start_col"]) - 1,
                int(r["width"]),
            )
    return layouts


def resolve_columns(product: str, year: int, layouts: dict) -> dict:
    """Map each output column to its slice in this year's record, or None.

    Returns {column: {"start", "width", "type", "sentinels", "source"}}. A column
    whose source variable is absent that year maps to ``None`` and is emitted as
    all-NULL, so every year has an identical schema.
    """
    avail = layouts.get((product, year), {})
    resolved: dict[str, dict | None] = {}
    for (
        col,
        bqtype,
        _unit,
        candidates,
        sentinels,
        req_width,
        _note,
    ) in COLUMN_SPEC[product]:
        hit = None
        for cand in candidates:
            if cand not in avail:
                continue
            start, width = avail[cand]
            if req_width is not None and width != req_width:
                continue  # variable name reused for a different concept
            hit = {
                "start": start,
                "width": width,
                "type": bqtype,
                "sentinels": set(sentinels),
                "source": cand,
            }
            break
        resolved[col] = hit
    return resolved


def icd_revision_for(year: int) -> str:
    for lo, hi, rev in constants.ICD_REVISIONS.value:
        if lo <= year <= hi:
            return rev
    raise ValueError(f"no ICD revision defined for {year}")


def decode_age_years(codes: pd.Series, year: int) -> pd.Series:
    """Decode the NCHS unit-prefixed detail-age code to completed years.

    The code carries an age UNIT in its leading digit, so reading it as a number
    turns a 5-day-old infant into a 4-digit age. Only records whose unit is
    'years' yield a value; every other unit (months, days, hours, minutes) is an
    infant death under one year and is returned as 0, and unknown ages as NULL.

    Layout differs across the 2003 redesign:
      1968-2002  3 chars: unit digit + 2-digit value, 999 unknown
      2003-      4 chars: unit digit + 3-digit value, 9999 unknown
    """
    s = codes.astype("string").str.strip()
    if year <= 2002:
        unit, val, unknown = s.str[0], s.str[1:3], {"999", ""}
        all_nines = val.str.fullmatch(r"9+").fillna(False)
        # Pre-2003 unit 0 = years under 100, 1 = years 100+, 2 = months,
        # 3 = weeks, 4 = days, 5 = hours, 6 = minutes.
        years = pd.Series(pd.NA, index=s.index, dtype="Float64")
        v = pd.to_numeric(val, errors="coerce")
        years = years.mask(unit == "0", v)
        years = years.mask(unit == "1", v + 100)
        years = years.mask(unit.isin(["2", "3", "4", "5", "6"]), 0)
    else:
        unit, val, unknown = s.str[0], s.str[1:4], {"9999", ""}
        all_nines = val.str.fullmatch(r"9+").fillna(False)
        # 2003+ unit 1 = years, 2 = months, 4 = days, 5 = hours, 6 = minutes,
        # 9 = age not stated.
        v = pd.to_numeric(val, errors="coerce")
        years = pd.Series(pd.NA, index=s.index, dtype="Float64")
        years = years.mask(unit == "1", v)
        years = years.mask(unit.isin(["2", "4", "5", "6"]), 0)
    # A value portion of all nines is "not stated" within its unit, e.g. 2019
    # code 1999 is "age in years unknown", not a 999-year-old.
    years = years.mask(all_nines, pd.NA)
    years = years.mask(s.isin(unknown) | s.isna(), pd.NA)
    return years.astype("Int64")


# --------------------------------------------------------------------------- #
# Fixed-width reading
# --------------------------------------------------------------------------- #
# NCHS zips are not all standard deflate. Several years ship deflate64
# (method 9) and 2015 natality ships PPMd (method 98); Python's zipfile
# supports neither. `7z` handles both and is in the pipeline image
# (p7zip-full); `unzip` covers deflate64 and `bsdtar` covers PPMd, which is
# what is available on a developer machine.
#
# `unzip -p` on a PPMd member exits 0 and writes NOTHING, so a fallback that
# trusted the exit code would silently produce an empty partition. Every
# candidate is therefore required to yield actual bytes before it is accepted.
_EXTRACTORS = (
    ("7z", lambda z, m: ["7z", "e", "-so", str(z), m]),
    ("unzip", lambda z, m: ["unzip", "-p", str(z), m]),
    ("bsdtar", lambda z, m: ["bsdtar", "-xOf", str(z), m]),
)


def _largest_member(zip_path: Path) -> str:
    zf = zipfile.ZipFile(zip_path)
    try:
        members = [i for i in zf.infolist() if not i.is_dir()]
        if not members:
            raise ValueError(f"{zip_path} contains no files")
        return max(members, key=lambda i: i.file_size).filename
    finally:
        zf.close()


class _MemberStream:
    """A read-only byte stream over one member of an NCHS zip.

    Uses Python's zipfile when it understands the compression method, and
    otherwise shells out to the first external extractor that actually
    produces bytes.
    """

    def __init__(self, zip_path: Path):
        self.zip_path = Path(zip_path)
        self.member = _largest_member(self.zip_path)
        self._zf = None
        self._proc = None
        self._stream = None
        self._open()

    def _open(self):
        zf = zipfile.ZipFile(self.zip_path)
        info = zf.getinfo(self.member)
        if info.compress_type in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
            self._zf = zf
            self._stream = zf.open(info)
            return
        zf.close()
        errors = []
        for name, argv in _EXTRACTORS:
            if shutil.which(name) is None:
                errors.append(f"{name}: not installed")
                continue
            proc = subprocess.Popen(
                argv(self.zip_path, self.member),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            head = proc.stdout.read(1 << 16) if proc.stdout else b""
            if head:
                self._proc = proc
                self._stream = proc.stdout
                self._head = head
                log.info(
                    "%s uses compression method %s; extracted with %s",
                    self.zip_path.name,
                    info.compress_type,
                    name,
                )
                return
            proc.kill()
            proc.wait()
            errors.append(f"{name}: produced no output")
        raise RuntimeError(
            f"cannot decompress {self.zip_path.name} "
            f"(compression method {info.compress_type}): " + "; ".join(errors)
        )

    _head = b""

    def read(self, n: int = -1) -> bytes:
        if self._head:
            head, self._head = self._head, b""
            if n is not None and n >= 0 and len(head) > n:
                self._head = head[n:]
                return head[:n]
            rest = (
                self._stream.read(max(0, n - len(head)))
                if n and n > 0
                else self._stream.read()
            )
            return head + (rest or b"")
        return self._stream.read(n) if n is not None else self._stream.read()

    def close(self):
        if self._stream is not None:
            with contextlib.suppress(Exception):
                self._stream.close()
        if self._proc is not None:
            self._proc.kill()
            self._proc.wait()
        if self._zf is not None:
            self._zf.close()


def _detect_stride(probe: bytes) -> tuple[int, int]:
    """Return (record_length, stride) from the first bytes of the file.

    stride includes the line terminator. Returns stride 0 when the file is not
    uniformly strided, so the caller falls back to line-by-line reading.
    """
    nl = probe.find(b"\n")
    if nl == -1:
        return len(probe), 0
    stride = nl + 1
    reclen = nl - 1 if probe[nl - 1 : nl] == b"\r" else nl
    # every stride-th byte must also be a newline for the file to be uniform
    checks = [
        probe[i * stride + stride - 1 : i * stride + stride]
        for i in range(1, min(200, len(probe) // stride))
    ]
    if not all(c == b"\n" for c in checks):
        return reclen, 0
    return reclen, stride


def _columns_from_block(
    buf: bytes,
    stride: int,
    reclen: int,
    resolved: dict,
    product: str,
    year: int,
) -> pd.DataFrame:
    """Vectorised fixed-width slice of a byte block into the harmonized frame."""
    import numpy as np

    nrows = len(buf) // stride
    arr = np.frombuffer(buf[: nrows * stride], dtype=np.uint8).reshape(
        nrows, stride
    )
    out: dict[str, pd.Series] = {}
    idx = pd.RangeIndex(nrows)
    for col, spec in resolved.items():
        if spec is None:
            out[col] = pd.Series(pd.NA, index=idx, dtype="string")
            continue
        s0, w = spec["start"], spec["width"]
        if s0 + w > reclen:  # layout runs past the record: treat as absent
            out[col] = pd.Series(pd.NA, index=idx, dtype="string")
            continue
        chunk = np.ascontiguousarray(arr[:, s0 : s0 + w]).view(f"S{w}").ravel()
        raw = (
            pd.Series(list(chunk), index=idx, dtype="object")
            .str.decode("latin1")
            .astype("string")
            .str.strip()
        )
        raw = raw.mask(raw.eq("") | raw.str.fullmatch(r"\**"), pd.NA)
        if spec["sentinels"]:
            raw = raw.mask(raw.isin(spec["sentinels"]), pd.NA)
        out[col] = raw
    return _finalise(pd.DataFrame(out), resolved, product, year, idx)


def _finalise(
    df: pd.DataFrame, resolved: dict, product: str, year: int, idx
) -> pd.DataFrame:
    """Derived columns, geography normalisation and integer coercion."""
    df["year"] = pd.Series([year] * len(idx), index=idx, dtype="Int64")

    if product == "death":
        df["icd_revision"] = pd.Series(
            [icd_revision_for(year)] * len(idx), index=idx, dtype="string"
        )
        if resolved.get("age_detail_code") is not None:
            df["age_years"] = decode_age_years(df["age_detail_code"], year)
        else:
            df["age_years"] = pd.Series(pd.NA, index=idx, dtype="Int64")

    # 1968-1971 natality is a 50 percent sample and publishes no weight variable.
    # Verified against published births: the file holds exactly 0.500 of the
    # published total in 1968 and 1970, so the implicit record weight is 2.
    lo, hi = constants.BIRTH_SAMPLE_YEARS.value
    if (
        product == "birth"
        and resolved.get("record_weight") is None
        and lo <= year <= hi
    ):
        df["record_weight"] = pd.Series(
            [2] * len(idx), index=idx, dtype="Int64"
        )

    st = resolved.get("state_residence_id")
    if st is not None:
        raw = df["state_residence_id"]
        if st["source"] in ("stateres", "staters"):
            df["state_residence_id"] = raw.map(NCHS_STATE_TO_FIPS).astype(
                "string"
            )
        elif st["source"] == "mrstate":
            df["state_residence_id"] = (
                raw.str.upper().map(POSTAL_TO_FIPS).astype("string")
            )
        else:
            df["state_residence_id"] = raw.str.zfill(2).astype("string")
        df["state_residence_id"] = df["state_residence_id"].mask(
            ~df["state_residence_id"].isin(VALID_STATE_FIPS)
        )

    if resolved.get("county_residence_id") is not None:
        cty = df["county_residence_id"].str.zfill(5).astype("string")
        df["county_residence_id"] = cty.mask(
            ~cty.str[:2].isin(VALID_STATE_FIPS)
        )

    for col, bqtype, *_ in COLUMN_SPEC[product]:
        if bqtype == "INT64" and col not in ("year", "age_years"):
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df[OUTPUT_COLUMNS[product]]


def parse_year(
    product: str,
    year: int,
    zip_path: Path,
    layouts: dict,
    chunk_rows: int = 400_000,
):
    """Yield harmonized DataFrames for one product-year, a chunk at a time."""
    resolved = resolve_columns(product, year, layouts)
    missing = [c for c, v in resolved.items() if v is None]
    log.info(
        "%s %s: %d/%d columns resolved (absent: %s)",
        product,
        year,
        len(resolved) - len(missing),
        len(resolved),
        ",".join(missing) or "none",
    )
    stream = _MemberStream(zip_path)
    total = 0
    try:
        probe = stream.read(1 << 20)
        if not probe:
            raise RuntimeError(f"{zip_path.name}: extractor produced no data")
        reclen, stride = _detect_stride(probe)
        if stride:
            # Uniform stride: slice the raw buffer directly, no per-line work.
            block = stride * chunk_rows
            carry = b""
            buf = probe
            while True:
                buf = carry + buf
                usable = (len(buf) // stride) * stride
                carry = buf[usable:]
                if usable:
                    total += usable // stride
                    yield _columns_from_block(
                        buf[:usable], stride, reclen, resolved, product, year
                    )
                buf = stream.read(block)
                if not buf:
                    break
            if carry.strip():
                total += 1
                yield _columns_from_block(
                    carry.ljust(stride, b" "),
                    stride,
                    reclen,
                    resolved,
                    product,
                    year,
                )
        else:
            # Some years (e.g. 1980 mortality) ship newline-delimited records with
            # trailing blanks stripped, so records vary in length. Pad each record
            # out to the layout width and reuse the same vectorised slicer.
            width = max(
                (sp["start"] + sp["width"] for sp in resolved.values() if sp),
                default=0,
            )
            width = max(width, reclen)
            lines: list[bytes] = []
            carry = b""
            buf = probe
            while True:
                data = carry + buf
                parts = data.split(b"\n")
                carry = parts.pop()
                for raw in parts:
                    raw = raw.rstrip(b"\r")
                    if not raw:
                        continue
                    lines.append(raw.ljust(width, b" ")[:width])
                    if len(lines) >= chunk_rows:
                        total += len(lines)
                        yield _columns_from_block(
                            b"".join(lines),
                            width,
                            width,
                            resolved,
                            product,
                            year,
                        )
                        lines = []
                buf = stream.read(1 << 22)
                if not buf:
                    break
            tail = carry.rstrip(b"\r")
            if tail:
                lines.append(tail.ljust(width, b" ")[:width])
            if lines:
                total += len(lines)
                yield _columns_from_block(
                    b"".join(lines), width, width, resolved, product, year
                )
    finally:
        stream.close()
    if total == 0:
        raise RuntimeError(f"{zip_path.name}: no records parsed")


def _string_schema(product: str) -> pa.Schema:
    """All-STRING arrow schema with a stable column order.

    Staging is all-STRING by house convention and the dbt model safe_casts each
    column, so the schema carries ORDER, not types. Both this pipeline and the
    one-shot onboarding upload write through here so their staging external
    tables cannot diverge.
    """
    return pa.schema([(c, pa.string()) for c in OUTPUT_COLUMNS[product]])


def to_string_table(df: pd.DataFrame, product: str) -> pa.Table:
    """Cast a typed frame to the all-STRING staging schema, via arrow.

    Casting through arrow rather than ``astype(str)`` matters twice over: it keeps
    NULL as NULL instead of the literal "nan" (which safe_cast will not turn back
    into NULL), and it serialises integers as "1959" rather than "1959.0".
    """
    table = pa.Table.from_pandas(df, preserve_index=False)
    return table.cast(_string_schema(product))


def write_year(df_iter, product: str, year: int, output_dir: Path) -> int:
    """Write one year's chunks to output/<product>/year=<year>/ and return rows."""
    part = Path(output_dir) / product / f"year={year}"
    part.mkdir(parents=True, exist_ok=True)
    for old in part.glob("*.parquet"):
        old.unlink()
    total, writer = 0, None
    try:
        for df in df_iter:
            table = to_string_table(df, product)
            if writer is None:
                writer = pq.ParquetWriter(
                    part / "data.parquet", table.schema, compression="snappy"
                )
            writer.write_table(table)
            total += len(df)
    finally:
        if writer is not None:
            writer.close()
    return total


def clean_all(
    input_dir: Path,
    output_dir: Path,
    products=("birth", "death"),
    years: list[int] | None = None,
    layouts: dict | None = None,
    skip_existing: bool = False,
) -> dict:
    """Clean every downloaded product-year into partitioned parquet.

    Returns {(product, year): row_count}. Each year is written independently so a
    partial run is resumable and a single bad year cannot poison the rest.
    """
    layouts = layouts or load_layouts()
    input_dir, output_dir = Path(input_dir), Path(output_dir)
    counts: dict[tuple[str, int], int] = {}
    failures: dict[tuple[str, int], str] = {}
    for product in products:
        src_dir = input_dir / constants.CDC_DIR.value[product]
        for zip_path in sorted(
            src_dir.glob(f"{constants.CDC_DIR.value[product]}_*.zip")
        ):
            year = int(zip_path.stem.rsplit("_", 1)[1])
            if years and year not in years:
                continue
            if (product, year) not in layouts:
                log.warning(
                    "%s %s: no published record layout, skipped", product, year
                )
                continue
            done = Path(output_dir) / product / f"year={year}" / "data.parquet"
            if skip_existing and done.exists() and done.stat().st_size > 0:
                log.info("%s %s already written, skipping", product, year)
                continue
            try:
                n = write_year(
                    parse_year(product, year, zip_path, layouts),
                    product,
                    year,
                    output_dir,
                )
            except Exception as exc:
                # One unreadable year must not abort the other 113. The failure
                # is recorded and re-reported at the end so a partial run is
                # never mistaken for a complete one.
                failures[(product, year)] = str(exc)
                log.error("%s %s FAILED: %s", product, year, exc)
                partial = Path(output_dir) / product / f"year={year}"
                for stale in partial.glob("*.parquet"):
                    stale.unlink()
                continue
            counts[(product, year)] = n
            log.info("%s %s -> %s rows", product, year, f"{n:,}")
    if failures:
        log.error(
            "%d year(s) failed and were NOT written: %s",
            len(failures),
            ", ".join(f"{p} {y}" for p, y in sorted(failures)),
        )
    return counts


def write_dicionario(code_dir: Path, output_dir: Path) -> int:
    """Copy the curated dictionary CSV out as a parquet table."""
    src = Path(code_dir) / "layouts" / "dicionario.csv"
    df = pd.read_csv(src, dtype=str, keep_default_na=False)
    part = Path(output_dir) / "dicionario"
    part.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(df, preserve_index=False).cast(
        pa.schema(
            [
                (c, pa.string())
                for c in [
                    "id_tabela",
                    "nome_coluna",
                    "chave",
                    "cobertura_temporal",
                    "valor",
                ]
            ]
        )
    )
    pq.write_table(table, part / "data.parquet", compression="snappy")
    return len(df)


# --------------------------------------------------------------------------- #
# Source discovery and download (used by the recurring pipeline)
# --------------------------------------------------------------------------- #
def _http_get(url: str, timeout: int = 180) -> bytes:
    import urllib.request

    req = urllib.request.Request(url, headers=constants.HEADERS.value)
    return urllib.request.urlopen(req, timeout=timeout).read()


def latest_source_year(product: str) -> int | None:
    """Newest final data year NCHS publishes for this product.

    Read from the CDC directory listing, which is authoritative for what has been
    released. Returns None if the listing cannot be read, so the caller can decline
    to act rather than guess.
    """
    import re

    folder = constants.CDC_DIR.value[product]
    try:
        html = _http_get(f"{constants.CDC_BASE.value}/{folder}/").decode(
            "utf8", "replace"
        )
    except Exception as exc:
        log.warning("could not list the CDC %s directory: %s", folder, exc)
        return None
    stem = "nat" if product == "birth" else "mort"
    years = {
        int(y)
        for y in re.findall(rf"{stem}[a-z]*(\d{{4}})[a-z]*\.zip", html, re.I)
    }
    return max(years) if years else None


def download_year(product: str, year: int, dest_dir: Path) -> Path:
    """Fetch one product-year zip, preferring the faster NBER mirror."""
    import urllib.error
    import urllib.request

    folder = constants.CDC_DIR.value[product]
    dest_dir = Path(dest_dir) / folder
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"{folder}_{year}.zip"
    stem = "Nat" if product == "birth" else "mort"
    candidates = []
    for s in (stem, stem.lower(), stem.upper()):
        candidates.append(
            f"{constants.NBER_BASE.value}/{folder}/inputs/raw/{year}/{s}{year}us.zip"
        )
    for s in (stem, stem.lower(), stem.upper()):
        candidates.append(
            f"{constants.CDC_BASE.value}/{folder}/{s}{year}us.zip"
        )
    last: Exception | None = None
    for url in candidates:
        try:
            req = urllib.request.Request(url, headers=constants.HEADERS.value)
            with (
                urllib.request.urlopen(req, timeout=600) as r,
                open(out, "wb") as f,
            ):
                while True:
                    b = r.read(1 << 20)
                    if not b:
                        break
                    f.write(b)
            if out.stat().st_size > 1_000_000:
                log.info("%s %s downloaded from %s", product, year, url)
                return out
        except Exception as exc:
            last = exc
    raise RuntimeError(f"could not download {product} {year}: {last}")
