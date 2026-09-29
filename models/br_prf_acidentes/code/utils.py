"""Pure download and cleaning functions for br_prf_acidentes.

No Prefect imports here: the one-shot onboarding runner (clean_data.py) and any
future recurring pipeline both import from this module, so the transform lives
in exactly one place.
"""

from __future__ import annotations

import collections
import csv
import io
import re
import unicodedata
import zipfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from constants import (
    AGE_MISSING_TOKENS,
    AGE_VALID_RANGE,
    AGE_ZERO_IS_MISSING,
    BRAZIL_BBOX,
    CASE_VARIANT_COLUMNS,
    COORDINATE_VALID_RANGE,
    DATE_FORMAT,
    DECIMAL_COMMA_FROM,
    DRIVE_IDS,
    ENCODING,
    HARMONIZE,
    INPUT_DIR,
    MULTIVALUED_COLUMNS,
    MUNICIPALITY_OVERRIDES,
    NULL_TOKENS,
    SEPARATOR,
    UNRESOLVABLE_MUNICIPALITIES,
    VEHICLE_YEAR_MISSING,
    VEHICLE_YEAR_VALID_RANGE,
)

DRIVE_URL = "https://drive.usercontent.google.com/download?id={fid}&export=download&confirm=t"


# ------------------------------------------------------------------ download


def download(shape: str, year: int, dest_dir: Path = INPUT_DIR) -> Path:
    """Fetch one published archive. Returns the local path; skips if present."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    out = dest_dir / f"{shape}_{year}.zip"
    if out.exists() and out.stat().st_size > 0:
        return out
    fid = DRIVE_IDS[(shape, year)]
    with requests.get(
        DRIVE_URL.format(fid=fid), stream=True, timeout=600
    ) as r:
        r.raise_for_status()
        with open(out, "wb") as fh:
            for chunk in r.iter_content(1 << 20):
                fh.write(chunk)
    if not zipfile.is_zipfile(out):
        out.unlink(missing_ok=True)
        raise RuntimeError(
            f"{shape} {year}: Drive returned a non-zip response"
        )
    return out


def read_source(
    shape: str, year: int, src_dir: Path = INPUT_DIR
) -> list[dict]:
    """Decode one archive's single CSV member into a list of dict rows."""
    path = src_dir / f"{shape}_{year}.zip"
    with zipfile.ZipFile(path) as zf:
        member = zf.namelist()[0]
        text = zf.read(member).decode(ENCODING)
    reader = csv.DictReader(
        io.StringIO(text), delimiter=SEPARATOR[(shape, year)]
    )
    return list(reader)


# ------------------------------------------------------------------ scalars


def clean_str(value: str | None) -> str | None:
    """Trim, then map every null token used by the source to None."""
    if value is None:
        return None
    v = value.strip()
    return None if v in NULL_TOKENS else v


def parse_date(value: str | None, shape: str, year: int):
    """Parse `data_inversa` using the format for THIS (shape, year).

    The two shapes diverge for 2012-2015, so the key must include the shape.
    """
    v = clean_str(value)
    if v is None:
        return None
    try:
        return datetime.strptime(v, DATE_FORMAT[(shape, year)]).date()
    except ValueError:
        return None


def parse_time(value: str | None) -> str | None:
    """Parse `horario` and return it as HH:MM:SS.

    Returned as a formatted string rather than a `time`, because arrow renders a
    time64 column to string with microseconds ("18:30:00.000000"), which is not
    what `safe_cast(horario as time)` should receive.
    """
    v = clean_str(value)
    if v is None:
        return None
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            return datetime.strptime(v, fmt).strftime("%H:%M:%S")
        except ValueError:
            continue
    return None


def parse_float(value: str | None, year: int) -> float | None:
    """Parse km / latitude / longitude. Decimal comma from 2016 onward."""
    v = clean_str(value)
    if v is None:
        return None
    if year >= DECIMAL_COMMA_FROM:
        v = (
            v.replace(".", "").replace(",", ".")
            if v.count(",") == 1
            else v.replace(",", ".")
        )
    try:
        return float(v)
    except ValueError:
        return None


def parse_coordinate(value: str | None, year: int, axis: str) -> float | None:
    """Parse a latitude or longitude, discarding values that cannot be one.

    `axis` is "lat" or "lon". In-range values are returned unchanged, including
    points that fall outside Brazil; only impossible values become NULL.
    """
    v = parse_float(value, year)
    if v is None:
        return None
    lo, hi = COORDINATE_VALID_RANGE[axis]
    return v if lo <= v <= hi else None


def parse_int(value: str | None) -> int | None:
    v = clean_str(value)
    if v is None:
        return None
    try:
        return int(float(v))
    except ValueError:
        return None


def parse_age(value: str | None, shape: str, year: int) -> int | None:
    """Age, with all three missing markers resolved and corrupt values dropped.

    -1 and 'NA' always mean missing. 0 means missing in `pessoa` from 2017 only;
    before that it is a genuine under-one-year-old. Values outside 0-120 are
    data-entry corruption (birth years and mangled numbers) and become NULL.
    """
    v = clean_str(value)
    if v is None or v in AGE_MISSING_TOKENS:
        return None
    try:
        age = int(float(v))
    except ValueError:
        return None
    zero_missing_from = AGE_ZERO_IS_MISSING.get(shape)
    if (
        age == 0
        and zero_missing_from is not None
        and year >= zero_missing_from
    ):
        return None
    if age == -1:
        return None
    lo, hi = AGE_VALID_RANGE
    return age if lo <= age <= hi else None


def parse_vehicle_year(value: str | None) -> int | None:
    v = clean_str(value)
    if v is None or v in VEHICLE_YEAR_MISSING:
        return None
    try:
        y = int(float(v))
    except ValueError:
        return None
    lo, hi = VEHICLE_YEAR_VALID_RANGE
    return y if lo < y <= hi else None


def coordinate_in_brazil(lat: float | None, lon: float | None) -> bool | None:
    """Flag for whether a point falls inside Brazil's bounding box.

    Returns None when either coordinate is missing. A 0,0 pair is False. Raw
    values are always kept; this only records the verdict.
    """
    if lat is None or lon is None:
        return None
    if lat == 0 and lon == 0:
        return False
    b = BRAZIL_BBOX
    return (
        b["lat_min"] <= lat <= b["lat_max"]
        and b["lon_min"] <= lon <= b["lon_max"]
    )


# ------------------------------------------------------------------ municipality


def norm_loose(name: str) -> str:
    """Upper-case, strip accents, reduce punctuation to single spaces."""
    s = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode()
        .upper()
    )
    return re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9 ]", " ", s)).strip()


def norm_tight(name: str) -> str:
    """As norm_loose but deleting every non-alphanumeric, including spaces.

    This is what catches the apostrophe class: the directory's
    "Sao Miguel do Oeste" and PRF's "SAO MIGUEL DOESTE" both reduce to
    SAOMIGUELDOOESTE only once the apostrophe is deleted rather than replaced
    by a space.
    """
    s = (
        unicodedata.normalize("NFKD", name)
        .encode("ascii", "ignore")
        .decode()
        .upper()
    )
    return re.sub(r"[^A-Z0-9]", "", s)


def build_municipality_index(directory: pd.DataFrame) -> dict:
    """Index the BD municipality directory for accent-insensitive lookup.

    `directory` needs columns id_municipio, nome, sigla_uf.
    """
    by_uf_loose, by_uf_tight, by_name_tight = (
        {},
        {},
        collections.defaultdict(list),
    )
    # itertuples attributes are typed as a wide union, so the three fields are
    # coerced to str before use rather than relying on the frame's dtypes.
    for row in directory.itertuples(index=False):
        nome, uf = str(row.nome), str(row.sigla_uf)
        code = str(row.id_municipio)
        by_uf_loose[(uf, norm_loose(nome))] = code
        by_uf_tight[(uf, norm_tight(nome))] = code
        by_name_tight[norm_tight(nome)].append(code)
    return {
        "by_uf_loose": by_uf_loose,
        "by_uf_tight": by_uf_tight,
        "by_name_tight": dict(by_name_tight),
    }


def resolve_municipality(uf, name, index: dict) -> tuple[str | None, str]:
    """Resolve (uf, municipality name) to a 7-digit IBGE code.

    Returns (id_municipio, status). Status is one of matched, matched_tight,
    override, matched_no_uf, uf_mismatch, no_municipality, unresolved.
    """
    uf = clean_str(uf)
    name = clean_str(name)
    if name is None:
        return None, "no_municipality"
    loose, tight = norm_loose(name), norm_tight(name)
    if uf:
        if (uf, loose) in index["by_uf_loose"]:
            return index["by_uf_loose"][(uf, loose)], "matched"
        if (uf, tight) in index["by_uf_tight"]:
            return index["by_uf_tight"][(uf, tight)], "matched_tight"
        if (uf, loose) in MUNICIPALITY_OVERRIDES:
            return MUNICIPALITY_OVERRIDES[(uf, loose)], "override"
        if (uf, loose) in UNRESOLVABLE_MUNICIPALITIES:
            return None, "uf_mismatch"
        return None, "unresolved"
    candidates = index["by_name_tight"].get(tight, [])
    if len(candidates) == 1:
        return candidates[0], "matched_no_uf"
    return None, "unresolved"


# ------------------------------------------------------------------ harmonization


def build_case_variant_map(
    values_by_year: dict[str, set[int]],
) -> dict[str, str]:
    """Collapse case / accent / punctuation variants to the newest spelling.

    `values_by_year` maps each raw value to the set of years it appears in. Two
    values collapse only when they are the same string up to case, accents and
    punctuation -- so 'Ceu Claro' and 'Céu Claro' merge, while 'Colisão lateral'
    and 'Colisão lateral mesmo sentido' do not. Within a group the canonical
    spelling is the one used in the most recent year, which is PRF's current
    vocabulary.
    """
    groups = collections.defaultdict(list)
    for value, years in values_by_year.items():
        if value is None:
            continue
        groups[norm_tight(value)].append((max(years), value))
    mapping = {}
    for variants in groups.values():
        if len(variants) == 1:
            continue
        canonical = max(variants)[1]
        for _, value in variants:
            if value != canonical:
                mapping[value] = canonical
    return mapping


def collect_vocabularies(shape: str, years, src_dir: Path = INPUT_DIR) -> dict:
    """Scan a shape's files and record, per categorical column, value -> years."""
    vocab = collections.defaultdict(lambda: collections.defaultdict(set))
    for year in years:
        for row in read_source(shape, year, src_dir):
            for col in CASE_VARIANT_COLUMNS:
                if col in row:
                    v = clean_str(row[col])
                    if v is not None:
                        vocab[col][v].add(year)
    return {col: dict(values) for col, values in vocab.items()}


def harmonize_value(
    column: str, value: str | None, case_maps: dict
) -> str | None:
    """Apply case-variant collapse then the explicit semantic equivalences."""
    if value is None:
        return None
    if column in MULTIVALUED_COLUMNS:
        return value
    value = case_maps.get(column, {}).get(value, value)
    return HARMONIZE.get(column, {}).get(value, value)


def parse_flag(value: str | None) -> str | None:
    """Person-level 0/1 indicator, kept as STRING per house convention."""
    v = clean_str(value)
    if v is None:
        return None
    try:
        return str(int(float(v)))
    except ValueError:
        return v
