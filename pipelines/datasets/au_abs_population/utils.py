"""Pure functions for au_abs_population: download + cleaning transform.

No Prefect imports here. The one-shot onboarding bootstrap
(``models/au_abs_population/code/clean_data.py``) and the recurring Prefect
pipeline both import these functions, so the cleaning transform lives in
exactly one place.

Two source layouts are handled:

* **ABS time-series workbooks** (3101.0 and 3222.0) — an ``Index`` sheet
  listing every series, plus ``Data*`` sheets carrying a metadata block (Unit,
  Frequency, Series Start/End, Series ID) above dated observation rows. One
  parser reads both products; the dimensions (measure, sex, age, region,
  projection series) are recovered from the semicolon-delimited *Data Item
  Description* by classifying each part, because ABS orders those parts
  differently between the state workbooks and the Australia workbook.
* **3218.0 data cubes** — hand-laid-out wide sheets with a two-row header
  (period above, measure below) over the ASGS hierarchy columns.
"""

from __future__ import annotations

import datetime as dt
import os
import re
from pathlib import Path

# pyrefly: ignore [untyped-import]
import openpyxl
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.au_abs_population.constants import constants

COLUMNS = constants.COLUMNS.value
SENTINELS = constants.NULL_SENTINELS.value
STATE_ID = constants.STATE_ID.value

_SEX = {"Male", "Female", "Persons"}
_AGE_RE = re.compile(r"^\d+( and over)?$")
_SERIES_RE = re.compile(r"^Series \d+\(([ABC])\)$")


# --------------------------------------------------------------------------- #
# Download
# --------------------------------------------------------------------------- #
def _get(url: str, session=None):
    import requests

    getter = session or requests
    resp = getter.get(url, headers=constants.HEADERS.value, timeout=180)
    resp.raise_for_status()
    return resp


def resolve_release_slug(product: str, session=None) -> str:
    """Resolve the current release slug for one ABS product.

    The download URLs are dated by reference period, and the three products run
    on different cadences, so the slug is read from the ``latest-release``
    landing page rather than guessed from the calendar.
    """
    html = _get(constants.LANDING.value[product], session).text
    m = re.search(constants.SLUG_RE.value[product], html)
    if not m:
        raise RuntimeError(f"could not resolve ABS release slug for {product}")
    return m.group(1)


def resolve_regional_files(session=None) -> dict[str, str]:
    """Map ``DS0001``..``DS0006`` to the exact filename stem of this release.

    The regional cube filenames embed a coverage span that moves every release
    (``32180DS0003_2001-25`` became ``2001-25`` only once 2025 landed), so the
    stems are read off the landing page instead of being reconstructed.
    """
    html = _get(constants.LANDING.value["regional"], session).text
    out = {}
    for stem in re.findall(r"(32180DS\d{4}_[\d\-]+)\.xlsx", html):
        out[stem.split("_")[0][5:]] = stem
    missing = {f"DS{i:04d}" for i in range(1, 7)} - set(out)
    if missing:
        raise RuntimeError(
            f"regional cubes not found on landing page: {missing}"
        )
    return out


def download_all(out_dir: str, session=None) -> dict[str, str]:
    """Download every source workbook for the current releases.

    Returns the resolved release slugs, which the pipeline records as source
    metadata and uses to detect a new release.
    """
    slugs = {
        p: resolve_release_slug(p, session)
        for p in ("national_state", "regional", "projection")
    }
    files: list[tuple[str, str, str]] = []
    for f in constants.NST_QUARTERLY.value + list(constants.NST_AGE_SEX.value):
        files.append(("national_state", f, "nst"))
    for letter in constants.PROJ_SERIES.value:
        for num in constants.PROJ_REGION.value:
            files.append(("projection", f"3222_Table_{letter}{num}", "proj"))
    for stem in resolve_regional_files(session).values():
        files.append(("regional", stem, "regional"))

    for product, file, sub in files:
        dest = Path(out_dir) / sub
        dest.mkdir(parents=True, exist_ok=True)
        path = dest / f"{file}.xlsx"
        if path.exists():
            continue
        url = constants.PATH.value[product].format(
            slug=slugs[product], file=file
        )
        path.write_bytes(_get(url, session).content)
    return slugs


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #
def _num(v):
    """ABS numeric cell -> float, mapping the documented sentinels to None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s in SENTINELS:
        return None
    s = s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _clean_desc(v) -> str | None:
    """Strip the trailing ``;`` separator artifact from an ABS description."""
    if v is None:
        return None
    return str(v).strip().rstrip(";").strip() or None


def classify_description(desc: str) -> dict:
    """Split an ABS *Data Item Description* into its dimensions.

    ABS delimits the parts with ``;`` but does not fix their order: the state
    projection workbooks read ``measure ; series ; region ; sex ; age`` while
    the Australia workbook reads ``measure ; sex ; age ; series``. Each part is
    therefore classified by shape rather than by position. The measure is
    always the first unclassified part; a second unclassified part, when
    present, is the region.
    """
    parts = [p.strip() for p in desc.split(";")]
    parts = [p for p in parts if p]
    out: dict[str, str | None] = {
        "measure": None,
        "sex": None,
        "age": None,
        "region": None,
        "series": None,
    }
    rest = []
    for p in parts:
        if p in _SEX:
            out["sex"] = p
        elif _AGE_RE.match(p):
            out["age"] = p
        elif (m := _SERIES_RE.match(p)) is not None:
            out["series"] = m.group(1)
        else:
            rest.append(p)
    if rest:
        out["measure"] = rest[0]
    if len(rest) > 1:
        out["region"] = rest[1]
    return out


def snake(text: str) -> str:
    """``Estimated Resident Population (ERP)`` -> ``estimated_resident_population``."""
    t = re.sub(r"\(erp\)", "", text.strip().lower())
    t = re.sub(r"[^a-z0-9]+", "_", t)
    return t.strip("_")


def state_id_for(region: str | None) -> str | None:
    """ABS region label -> ``br_bd_diretorios_au.state:id_state``.

    Returns ``None`` for ``Australia`` (a national aggregate, not a state) and
    for anything unrecognised, so the caller can decide whether that is a bug.
    """
    if region is None:
        return None
    return STATE_ID.get(region.strip().lower())


_STATE_NAME = {
    "1": "New South Wales",
    "2": "Victoria",
    "3": "Queensland",
    "4": "South Australia",
    "5": "Western Australia",
    "6": "Tasmania",
    "7": "Northern Territory",
    "8": "Australian Capital Territory",
    "9": "Other Territories",
}


def _state_name(state_id: str) -> str:
    """``id_state`` -> the ABS state name, as published in the directory."""
    return _STATE_NAME[state_id]


# --------------------------------------------------------------------------- #
# ABS time-series workbook parser (3101.0 and 3222.0)
# --------------------------------------------------------------------------- #
_META_LABELS = ("Unit", "Frequency", "Series Start", "Series End", "Series ID")


def parse_ts_workbook(path: str) -> tuple[list[dict], list[dict]]:
    """Parse one ABS time-series workbook.

    Returns ``(series_rows, observation_rows)``. Series metadata comes from the
    per-sheet block above the data (not from the ``Index`` sheet), because the
    block is the one place that carries the unit and frequency alongside the
    Series ID for every column.
    """
    base = os.path.basename(path).replace(".xlsx", "")
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)

    catalogue, table = None, None
    if "Index" in wb.sheetnames:
        for r in wb["Index"].iter_rows(values_only=True):
            v = r[1] if len(r) > 1 else None
            if isinstance(v, str):
                if v.startswith(("3101.0", "3222.0")):
                    catalogue = v.split(" ")[0]
                if v.upper().startswith("TABLE"):
                    table = v.strip()

    series_rows: list[dict] = []
    obs_rows: list[dict] = []
    for sheet in (s for s in wb.sheetnames if s.lower().startswith("data")):
        ws = wb[sheet]
        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            continue
        label_at = {}
        for i, r in enumerate(rows[:15]):
            a = r[0]
            if isinstance(a, str) and a.strip().rstrip(".") in _META_LABELS:
                label_at[a.strip().rstrip(".")] = i
        if "Series ID" not in label_at:
            continue
        desc_row = rows[0]
        sid_row = rows[label_at["Series ID"]]
        data_start = max(label_at.values()) + 1

        def cell(label, j, _rows=rows, _at=label_at):
            i = _at.get(label)
            if i is None:
                return None
            v = _rows[i][j]
            if isinstance(v, dt.datetime):
                return v.date()
            return str(v).strip() if v is not None else None

        cols = [
            j for j in range(1, len(sid_row)) if sid_row[j] not in (None, "")
        ]
        for j in cols:
            sid = str(sid_row[j]).strip()
            series_rows.append(
                {
                    "series_id": sid,
                    "description": _clean_desc(desc_row[j]),
                    "unit": cell("Unit", j),
                    "frequency": cell("Frequency", j),
                    "source_catalogue": catalogue,
                    "source_table": table,
                    "series_start": cell("Series Start", j),
                    "series_end": cell("Series End", j),
                    "source_file": base,
                }
            )
        for r in rows[data_start:]:
            d = r[0]
            if not isinstance(d, dt.datetime):
                continue
            for j in cols:
                v = _num(r[j])
                if v is None:
                    continue
                obs_rows.append(
                    {
                        "series_id": str(sid_row[j]).strip(),
                        "date": d.date(),
                        "value": v,
                        "source_file": base,
                    }
                )
    wb.close()
    return series_rows, obs_rows


# --------------------------------------------------------------------------- #
# 3218.0 data cube parser
# --------------------------------------------------------------------------- #
_CODE_RE = {"SA2 code": r"\d{9}", "SA3 code": r"\d{5}", "LGA code": r"\d{5}"}


def _cube_sheet(ws, code_col: str):
    """Locate a data-cube sheet's three header rows and its data rows.

    Every 3218.0 sheet is laid out the same way: a group header (the period, or
    ``ERP at 30 June``), a sub header (the year or the measure name), then the
    row of column labels that begins with ``<level> code``. Data follows, with
    footnote and copyright lines appended below it — those are excluded by
    requiring the code cell to match the level's code shape.
    """
    rows = list(ws.iter_rows(values_only=True))
    hi = next(
        i
        for i, r in enumerate(rows)
        if isinstance(r[0], str) and "code" in r[0].lower()
    )
    labels = rows[hi]
    ci = labels.index(code_col)
    pat = re.compile(_CODE_RE[code_col])
    data = [
        r
        for r in rows[hi + 1 :]
        if len(r) > ci
        and r[ci] is not None
        and pat.fullmatch(str(r[ci]).strip())
    ]
    return labels, rows[hi - 1], rows[hi - 2], data, ci


def _ancestry(labels, row) -> dict:
    """Pull the ASGS hierarchy identifiers present on a data-cube row."""
    want = {
        "S/T code": "state_id",
        "GCCSA code": "gccsa_id",
        "SA4 code": "sa4_id",
        "SA3 code": "sa3_id",
        "SA2 code": "sa2_id",
        "SA2 name": "sa2_name",
        "LGA code": "lga_id",
        "LGA name": "lga_name",
    }
    out = {}
    for j, lab in enumerate(labels):
        if lab in want and j < len(row) and row[j] is not None:
            out[want[lab]] = str(row[j]).strip()
    return out


def _fy_end(label: str) -> int | None:
    """Financial-year group header -> the calendar year it ends in.

    ABS writes the span with an en dash ("2021<en>22"), so both dash forms
    are accepted; the year is returned as the FY end year (2022).
    """
    m = re.search(r"(\d{4})\s*[\u2013\u2014-]\s*(\d{2,4})", label)
    if not m:
        return None
    start, end = m.group(1), m.group(2)
    return int(end) if len(end) == 4 else int(start[:2] + end)


def parse_erp_cube(path: str, sheet: str, code_col: str) -> list[dict]:
    """Long ``(geography, year, erp)`` records from DS0003/DS0004."""
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True)[sheet]
    labels, sub, _grp, data, _ci = _cube_sheet(ws, code_col)
    ycols = {
        j: int(sub[j])
        for j in range(len(sub))
        if sub[j] is not None and str(sub[j]).strip().isdigit()
    }
    out = []
    for r in data:
        anc = _ancestry(labels, r)
        for j, year in ycols.items():
            v = _num(r[j]) if j < len(r) else None
            if v is None:
                continue
            out.append({**anc, "year": year, "erp": v})
    return out


def parse_components_cube(path: str, sheet: str, code_col: str) -> list[dict]:
    """Long ``(geography, year, <component>)`` records from DS0005/DS0006.

    The financial year sits on the group header once per block of nine measure
    columns, so it is carried forward across the block.
    """
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True)[sheet]
    labels, sub, grp, data, _ci = _cube_sheet(ws, code_col)
    colmap: dict[int, tuple[int, str]] = {}
    year = None
    for j in range(len(sub)):
        g = grp[j] if j < len(grp) else None
        if g is not None and _fy_end(g) is not None:
            year = _fy_end(g)
        name = sub[j]
        if year is None or name is None:
            continue
        measure = snake(str(name))
        if measure in constants.COMPONENTS.value:
            colmap[j] = (year, measure)
    rec: dict[tuple, dict] = {}
    for r in data:
        anc = _ancestry(labels, r)
        key_col = "sa2_id" if code_col == "SA2 code" else "lga_id"
        for j, (yr, measure) in colmap.items():
            v = _num(r[j]) if j < len(r) else None
            if v is None:
                continue
            k = (anc.get(key_col), yr)
            rec.setdefault(k, {**anc, "year": yr})[measure] = v
    return list(rec.values())


def parse_area_cube(path: str, code_col: str) -> list[dict]:
    """``(geography, area_sqkm)`` from DS0001/DS0002, across every state sheet.

    Area is a property of the ASGS boundary, and the whole 3218.0 series is
    published on a single boundary vintage, so the one published area applies
    to every year of the series.
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    out = []
    for sheet in wb.sheetnames:
        if sheet == "Contents":
            continue
        try:
            labels, sub, _grp, data, _ci = _cube_sheet(wb[sheet], code_col)
        except (StopIteration, ValueError):
            continue
        acols = [
            j
            for j in range(len(sub))
            if sub[j] is not None and str(sub[j]).strip() == "Area"
        ]
        dcols = [
            j
            for j in range(len(sub))
            if sub[j] is not None
            and str(sub[j]).strip().startswith("Population density")
        ]
        dyear = None
        if dcols:
            m = re.search(r"(\d{4})", str(sub[dcols[0]]))
            dyear = int(m.group(1)) if m else None
        if not acols:
            continue
        for r in data:
            anc = _ancestry(labels, r)
            rec = {**anc, "area_sqkm": _num(r[acols[0]])}
            if dcols and dcols[0] < len(r):
                rec["population_density"] = _num(r[dcols[0]])
                rec["density_year"] = dyear
            out.append(rec)
    wb.close()
    return out


# --------------------------------------------------------------------------- #
# Table builders
# --------------------------------------------------------------------------- #
# Where the same measure/region/period is published by more than one 3101.0
# workbook, keep the unrounded version: Table 1 reports Australia in thousands
# while Tables 2, 4 and 16A/B report the same quantity in persons.
_UNIT_RANK = {"Persons": 0, "Number": 0, "Percent": 0, "000": 1}


def _geography(region: str | None, state_id: str | None):
    """Return ``(geography_level, state_id, region_name)`` for one series.

    ``state_id`` is passed in when the workbook (not the description) carries
    the region. Anything that is neither a mapped state nor Australia raises,
    so a new ABS region label fails loudly instead of landing as a null FK.
    """
    if state_id is not None:
        return "state", state_id, region
    if region is None:
        return "australia", None, "Australia"
    if region.strip().lower() == "australia":
        return "australia", None, "Australia"
    sid = state_id_for(region)
    if sid is None:
        raise ValueError(f"unrecognised ABS region label: {region!r}")
    return "state", sid, region


def _ts_frame(paths: list[str]) -> tuple[pd.DataFrame, list[dict]]:
    """Parse a set of time-series workbooks into observations + series metadata."""
    series, obs = [], []
    for p in paths:
        s, o = parse_ts_workbook(p)
        series.extend(s)
        obs.extend(o)
    return pd.DataFrame(obs), series


def clean_national_state(input_dir: str) -> tuple[pd.DataFrame, list[dict]]:
    """Quarterly ERP and components of change, by state and for Australia."""
    paths = [
        os.path.join(input_dir, "nst", f"{f}.xlsx")
        for f in constants.NST_QUARTERLY.value
    ]
    obs, series = _ts_frame(paths)
    meta = {}
    for s in series:
        c = classify_description(s["description"])
        level, sid, name = _geography(c["region"], None)
        meta[s["series_id"]] = {
            "geography_level": level,
            "state_id": sid,
            "region_name": name,
            "sex": c["sex"] or "Persons",
            "measure": snake(c["measure"]),
            "unit": s["unit"],
        }
    md = pd.DataFrame.from_dict(meta, orient="index").rename_axis("series_id")
    df = obs.join(md, on="series_id")
    df["year"] = df["date"].map(lambda d: d.year)
    # ABS labels a quarter by its final month: June quarter -> month 6 -> Q2.
    df["quarter"] = df["date"].map(lambda d: (d.month - 1) // 3 + 1)
    df["rank"] = df["unit"].map(lambda u: _UNIT_RANK.get(u, 9))
    key = ["year", "quarter", "state_id", "region_name", "sex", "measure"]
    df = (
        df.sort_values([*key, "rank"])
        .drop_duplicates(subset=key, keep="first")
        .reset_index(drop=True)
    )
    return df[COLUMNS["national_state"]], series


def clean_erp_age_sex(input_dir: str) -> tuple[pd.DataFrame, list[dict]]:
    """Annual ERP by single year of age and sex, by state and for Australia."""
    frames, series_all = [], []
    for file, sid in constants.NST_AGE_SEX.value.items():
        obs, series = _ts_frame(
            [os.path.join(input_dir, "nst", f"{file}.xlsx")]
        )
        series_all.extend(series)
        level, state_id, name = _geography(
            None if sid is None else _state_name(sid), sid
        )
        meta = {
            s["series_id"]: classify_description(s["description"])
            for s in series
        }
        obs["sex"] = obs["series_id"].map(lambda k, m=meta: m[k]["sex"])
        obs["age"] = obs["series_id"].map(lambda k, m=meta: m[k]["age"])
        obs["geography_level"] = level
        obs["state_id"] = state_id
        obs["region_name"] = name
        frames.append(obs)
    df = pd.concat(frames, ignore_index=True)
    df["year"] = df["date"].map(lambda d: d.year)
    df = df.rename(columns={"value": "erp"})
    return df[COLUMNS["erp_age_sex"]], series_all


def clean_projection(input_dir: str) -> tuple[pd.DataFrame, list[dict]]:
    """Population projections, series A (high), B (medium) and C (low)."""
    frames, series_all = [], []
    for letter, label in constants.PROJ_SERIES.value.items():
        for num, sid in constants.PROJ_REGION.value.items():
            path = os.path.join(
                input_dir, "proj", f"3222_Table_{letter}{num}.xlsx"
            )
            obs, series = _ts_frame([path])
            series_all.extend(series)
            meta = {
                s["series_id"]: classify_description(s["description"])
                for s in series
            }
            bad = {
                k
                for k, c in meta.items()
                if c["series"] is not None and c["series"] != letter
            }
            if bad:
                raise ValueError(
                    f"{path}: series letter disagrees with workbook: {sorted(bad)[:3]}"
                )
            level, state_id, name = _geography(
                None if sid is None else _state_name(sid), sid
            )
            obs["sex"] = obs["series_id"].map(lambda k, m=meta: m[k]["sex"])
            obs["age"] = obs["series_id"].map(lambda k, m=meta: m[k]["age"])
            obs["series"] = label
            obs["geography_level"] = level
            obs["state_id"] = state_id
            obs["region_name"] = name
            frames.append(obs)
    df = pd.concat(frames, ignore_index=True)
    df["year"] = df["date"].map(lambda d: d.year)
    df["projection_base_year"] = int(df["year"].min())
    df = df.rename(columns={"value": "projected_population"})
    return df[COLUMNS["projection"]], series_all


def _regional_level(input_dir: str, level: str) -> pd.DataFrame:
    """Assemble one regional table (``sa2`` or ``lga``) from its three cubes.

    ERP spans the full series; the components span only the four most recent
    financial years, and area and density only the latest.

    Area is carried across every year: it is a property of the boundary, and ABS
    publishes the whole ERP series on a single current boundary vintage.

    Density is *not* carried across years and is *not* recomputed. ABS derives it
    from the unrounded area while publishing area rounded to 0.1 km2, so
    ``erp / area_sqkm`` disagrees with the published figure for small, dense
    areas -- measured against the 2024-25 release, only 35% of SA2s reproduced
    within 0.1 persons/km2 and the worst error was 2,769. The published value is
    therefore kept only for the year ABS attaches it to.
    """
    key = f"{level}_id"
    cube_dir = os.path.join(input_dir, "regional")
    stems = _regional_stems(cube_dir)
    code_col = "SA2 code" if level == "sa2" else "LGA code"

    erp = pd.DataFrame(
        parse_erp_cube(
            os.path.join(cube_dir, f"{stems['erp_' + level]}.xlsx"),
            "Table 1",
            code_col,
        )
    )
    comp = pd.DataFrame(
        parse_components_cube(
            os.path.join(cube_dir, f"{stems['components_' + level]}.xlsx"),
            "Table 1",
            code_col,
        )
    )
    area = pd.DataFrame(
        parse_area_cube(
            os.path.join(cube_dir, f"{stems['area_' + level]}.xlsx"), code_col
        )
    )

    comp_cols = [c for c in constants.COMPONENTS.value if c in comp.columns]
    df = erp.merge(
        comp[[key, "year", *comp_cols]], on=[key, "year"], how="left"
    )
    df = df.merge(area[[key, "area_sqkm"]], on=key, how="left")
    dens = area.dropna(subset=["population_density"])[
        [key, "density_year", "population_density"]
    ].rename(columns={"density_year": "year"})
    df = df.merge(dens, on=[key, "year"], how="left")

    if level == "lga":
        # DS0004 carries only the LGA code and name; the state comes from the
        # components cube, and falls back to the leading digit of the LGA code,
        # which is the state digit in the ASGS LGA coding scheme.
        smap = (
            comp.dropna(subset=["state_id"])
            .drop_duplicates(subset=[key])
            .set_index(key)["state_id"]
        )
        df["state_id"] = df[key].map(smap).fillna(df[key].str[0])

    for col in COLUMNS[f"regional_{level}"]:
        if col not in df.columns:
            df[col] = pd.NA
    return (
        df[COLUMNS[f"regional_{level}"]]
        .sort_values(["year", key])
        .reset_index(drop=True)
    )


def _regional_stems(cube_dir: str) -> dict[str, str]:
    """Match the downloaded cube filenames to their role, by DS number.

    Keying on the DS number rather than the full filename keeps the transform
    working across releases, whose filenames embed a moving coverage span.
    """
    present = {
        f[5:11]: f[:-5]
        for f in os.listdir(cube_dir)
        if f.startswith("32180DS") and f.endswith(".xlsx")
    }
    ds = {
        "erp_sa2": "DS0003",
        "erp_lga": "DS0004",
        "components_sa2": "DS0005",
        "components_lga": "DS0006",
        "area_sa2": "DS0001",
        "area_lga": "DS0002",
    }
    out = {}
    for role, num in ds.items():
        stem = next((v for k, v in present.items() if k.startswith(num)), None)
        if stem is None:
            raise FileNotFoundError(
                f"regional cube {num} missing from {cube_dir}"
            )
        out[role] = stem
    return out


def clean_series(series_rows: list[dict]) -> pd.DataFrame:
    """One row per ABS Series ID, deduplicated across workbooks."""
    df = pd.DataFrame(series_rows)
    df = df.drop_duplicates(subset="series_id", keep="first")
    return (
        df[COLUMNS["series"]].sort_values("series_id").reset_index(drop=True)
    )


# --------------------------------------------------------------------------- #
# Write partitioned parquet (all-STRING)
# --------------------------------------------------------------------------- #
def write_partitioned(df: pd.DataFrame, table: str, out_dir: str) -> int:
    """Write a table as all-STRING Snappy parquet.

    Staging is all-STRING by Data Basis convention: the dbt model ``safe_cast``s
    every column, and ``upload_to_gcs`` infers the staging schema from a
    stringified one-row header, which rejects typed parquet. Values are cast to
    string through arrow rather than ``astype(str)``, which would render NULL as
    the literal ``"nan"`` and defeat ``safe_cast``. Tables listed in
    ``PARTITIONED`` are hive-partitioned by year, with the partition column kept
    in the file; ``series`` is small and unpartitioned.

    An empty partition is skipped: a zero-row first partition makes
    ``dump_header`` infer non-STRING types for the whole staging table.
    """
    cols = COLUMNS[table]
    string_schema = pa.schema([(c, pa.string()) for c in cols])
    frame = df[cols].copy()
    # Integer-valued measures must not serialise as "4480.0"; ABS publishes
    # them as whole persons and safe_cast(... as int64) would return NULL.
    for col in frame.columns:
        if pd.api.types.is_float_dtype(frame[col]):
            whole = frame[col].dropna() % 1 == 0
            if bool(whole.all()):
                frame[col] = frame[col].astype("Int64")

    def _write(part: pd.DataFrame, dest: Path) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        at = pa.Table.from_pandas(part[cols], preserve_index=False)
        at = at.cast(string_schema)
        pq.write_table(at, dest / "data.parquet", compression="snappy")

    n = 0
    if table not in constants.PARTITIONED.value:
        _write(frame, Path(out_dir) / table)
        return len(frame)
    for year, part in frame.groupby("year", sort=True):
        if part.empty:
            continue
        # The groupby key is typed as pandas' broad scalar union, but `year` is
        # an integer column here, so the int() is safe.
        # pyrefly: ignore [bad-argument-type]
        _write(part, Path(out_dir) / table / f"year={int(year)}")
        n += len(part)
    return n


def clean_all(input_dir: str, output_dir: str) -> dict:
    """Build every table into parquet under ``output_dir``.

    Returns each table's partition root, plus the max coverage dates that drive
    the source-update polls: ``max_year_quarter`` for the quarterly 3101.0
    series and ``max_year`` for the annual 3218.0 regional series.
    """
    ns, s1 = clean_national_state(input_dir)
    ag, s2 = clean_erp_age_sex(input_dir)
    pj, s3 = clean_projection(input_dir)
    tables = {
        "national_state": ns,
        "erp_age_sex": ag,
        "projection": pj,
        "regional_sa2": _regional_level(input_dir, "sa2"),
        "regional_lga": _regional_level(input_dir, "lga"),
        "series": clean_series(s1 + s2 + s3),
    }
    result: dict = {}
    for name, df in tables.items():
        rows = write_partitioned(df, name, output_dir)
        result[name] = str(Path(output_dir) / name)
        print(f"{name}: {rows} rows")
    last = ns.sort_values(["year", "quarter"]).iloc[-1]
    result["max_year_quarter"] = (
        f"{int(last['year']):04d}-{int(last['quarter']) * 3:02d}"
    )
    result["max_year"] = int(tables["regional_sa2"]["year"].max())
    return result
