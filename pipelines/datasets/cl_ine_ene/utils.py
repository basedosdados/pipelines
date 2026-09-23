"""Pure download and cleaning functions for cl_ine_ene (INE Chile, Encuesta Nacional de Empleo).

No Prefect imports here on purpose: the one-shot onboarding script under
``models/cl_ine_ene/code/`` imports the same functions, so the transform lives in
exactly one place.

The source publishes one CSV per *moving quarter*, named for its CENTRAL month:

    https://www.ine.gob.cl/docs/default-source/ocupacion-y-desocupacion/bbdd/{year}/csv/ene-{year}-{mm}-{suf}.csv

where ``suf`` is the three month-initials of the quarter (``01`` -> ``def`` for
December-January-February, and so on). The series starts at the enero-marzo 2010
quarter, i.e. ``2010-02``; ``2010-01`` does not exist.
"""

from __future__ import annotations

import gc
import json
import pathlib
import re
from collections.abc import Iterable

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

BASE_URL = (
    "https://www.ine.gob.cl/docs/default-source/ocupacion-y-desocupacion/bbdd"
    "/{year}/csv/ene-{year}-{month:02d}-{suffix}.csv"
)

#: Central month -> the three month-initials of the moving quarter it centres.
QUARTER_SUFFIX = {
    1: "def",
    2: "efm",
    3: "fma",
    4: "mam",
    5: "amj",
    6: "mjj",
    7: "jja",
    8: "jas",
    9: "aso",
    10: "son",
    11: "ond",
    12: "nde",
}

#: The series begins at the enero-marzo 2010 moving quarter.
FIRST_PERIOD = (2010, 2)

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

#: Source name -> published name. Only the partition and geography columns are
#: renamed; every other column keeps the questionnaire code the codebook uses,
#: because that code is how the data is actually read.
RENAMES = {
    "ano_trimestre": "ano",
    "mes_central": "mes",
    "region": "id_region",
    "provincia": "id_provincia",
    "r_p_c": "id_comuna",
}

#: Published name -> width to left-pad with zeros, so the codes join the
#: ``br_bd_diretorios_cl`` directory, which stores them zero-padded.
ZERO_PAD = {"id_region": 2, "id_provincia": 3, "id_comuna": 5}

#: Columns the source writes with a DECIMAL COMMA (``7,232369``). Left as-is they
#: survive the all-string staging load and then ``safe_cast(... as float64)``
#: returns NULL for every row, silently destroying the survey weight.
DECIMAL_COMMA = ("fact", "fact_cal", "fact_anual")

_COLUMN_UNIVERSE = (
    pathlib.Path(__file__)
    .resolve()
    .parents[3]
    .joinpath("models/cl_ine_ene/code/column_universe.json")
)


def periods(first=FIRST_PERIOD, last=None) -> list[tuple[int, int]]:
    """Every (year, central_month) from `first` up to and including `last`."""
    if last is None:
        raise ValueError("last period is required")
    out, (y, m) = [], first
    while (y, m) <= last:
        out.append((y, m))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def file_url(year: int, month: int) -> str:
    return BASE_URL.format(
        year=year, month=month, suffix=QUARTER_SUFFIX[month]
    )


def file_name(year: int, month: int) -> str:
    return f"ene-{year}-{month:02d}-{QUARTER_SUFFIX[month]}.csv"


def column_universe() -> list[str]:
    """Published column order: every column the series has ever carried.

    Newest schema first, then columns retired earlier in the order they last
    appeared. Built once from all 197 published headers and committed, so the
    published schema does not silently change when a period is re-read.
    """
    rows = json.loads(_COLUMN_UNIVERSE.read_text())
    return [RENAMES.get(r["name"], r["name"]) for r in rows]


def download_period(
    year: int, month: int, dest_dir: pathlib.Path
) -> pathlib.Path:
    """Fetch one moving quarter, verifying the body is complete."""
    dest_dir = pathlib.Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file_name(year, month)
    if dest.exists() and dest.stat().st_size > 1_000_000:
        return dest

    tmp = dest.with_suffix(".part")
    with requests.get(
        file_url(year, month),
        headers={"User-Agent": USER_AGENT},
        timeout=300,
        stream=True,
    ) as response:
        response.raise_for_status()
        received = 0
        with open(tmp, "wb") as handle:
            for chunk in response.iter_content(1 << 20):
                handle.write(chunk)
                received += len(chunk)
        declared = response.headers.get("Content-Length")
    # iter_content returns quietly on a dropped connection, so a short body is
    # indistinguishable from success unless the length is checked.
    if declared is not None and int(declared) != received:
        tmp.unlink(missing_ok=True)
        raise OSError(
            f"{file_name(year, month)}: got {received} bytes, expected {declared}"
        )
    tmp.rename(dest)
    return dest


def source_max_period(
    probe_from: tuple[int, int], limit: int = 24
) -> tuple[int, int]:
    """The newest moving quarter the source actually publishes.

    Walks forward from `probe_from` until a period 404s. INE publishes a quarter
    roughly one month after its last month, so the walk is short.
    """
    latest, (y, m) = probe_from, probe_from
    for _ in range(limit):
        response = requests.head(
            file_url(y, m),
            headers={"User-Agent": USER_AGENT},
            timeout=60,
            allow_redirects=True,
        )
        if response.status_code != 200:
            break
        latest = (y, m)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return latest


def read_period(path: pathlib.Path) -> pd.DataFrame:
    """Read one period's CSV exactly as published: ';'-delimited, UTF-8, all text."""
    return pd.read_csv(
        path,
        sep=";",
        dtype=str,
        encoding="utf-8",
        keep_default_na=False,
        na_values=[""],
    )


def clean_period(
    frame: pd.DataFrame, universe: Iterable[str] | None = None
) -> pd.DataFrame:
    """Conform one period to the published union schema.

    Renames the partition and geography columns, zero-pads the geography codes to
    the directory's widths, rewrites decimal commas as points, and reindexes onto
    the full column universe so every period shares one schema -- columns a
    period never carried come back NULL.
    """
    universe = list(universe) if universe is not None else column_universe()
    out = frame.rename(columns=RENAMES)

    for column, width in ZERO_PAD.items():
        if column in out.columns:
            out[column] = out[column].str.strip().str.zfill(width)

    for column in DECIMAL_COMMA:
        if column in out.columns:
            out[column] = out[column].str.replace(",", ".", regex=False)

    unexpected = [c for c in out.columns if c not in set(universe)]
    if unexpected:
        # A new questionnaire module means the committed universe is stale; failing
        # here is the point, because silently dropping it would lose real data.
        raise ValueError(
            f"columns absent from the published universe: {unexpected}. "
            "Rebuild models/cl_ine_ene/code/column_universe.json and the architecture."
        )
    return out.reindex(columns=universe)


def write_partitioned(
    frame: pd.DataFrame, year: int, month: int, out_dir: pathlib.Path
) -> pathlib.Path:
    """Write one period as all-STRING parquet under ``ano=<year>/mes=<month>``.

    Staging is all-STRING by house convention and ``gcs.py::dump_header``
    stringifies the header regardless, so typed parquet is rejected on read. The
    cast goes through arrow rather than ``astype(str)``, which would render NULL
    as the literal ``"nan"`` -- a value ``safe_cast`` will not turn back into NULL.
    """
    out_dir = pathlib.Path(out_dir)
    payload = frame.drop(columns=["ano", "mes"], errors="ignore")
    schema = pa.schema([(name, pa.string()) for name in payload.columns])
    table = pa.Table.from_pandas(payload, schema=schema, preserve_index=False)

    target = out_dir / f"ano={year}" / f"mes={month:02d}"
    target.mkdir(parents=True, exist_ok=True)
    destination = target / "data.parquet"
    pq.write_table(table, destination, compression="snappy")
    return destination


def clean_all(
    input_dir: pathlib.Path,
    output_dir: pathlib.Path,
    wanted: Iterable[tuple[int, int]] | None = None,
    skip_existing: bool = False,
) -> dict[str, int]:
    """Clean every downloaded period, returning {"YYYY-MM": row_count}.

    Periods are processed one at a time and nothing is retained between them, so
    peak memory is one period (~0.7 GB measured on the widest schema), not the
    series. `skip_existing` resumes a back-series build that was interrupted;
    leave it False in the flow, where every run gets a fresh directory and a
    stale partition would be kept silently.
    """
    input_dir, output_dir = pathlib.Path(input_dir), pathlib.Path(output_dir)
    universe = column_universe()
    counts: dict[str, int] = {}
    if wanted is None:
        wanted = sorted(
            (int(m.group(1)), int(m.group(2)))
            for m in (
                re.match(r"ene-(\d{4})-(\d{2})-", p.name)
                for p in input_dir.glob("ene-*.csv")
            )
            if m
        )
    for year, month in wanted:
        destination = (
            output_dir / f"ano={year}" / f"mes={month:02d}" / "data.parquet"
        )
        if skip_existing and destination.exists():
            counts[f"{year}-{month:02d}"] = pq.read_metadata(
                destination
            ).num_rows
            continue
        path = input_dir / file_name(year, month)
        frame = clean_period(read_period(path), universe)
        write_partitioned(frame, year, month, output_dir)
        counts[f"{year}-{month:02d}"] = len(frame)
        # Release the period before the next one is read, rather than relying on
        # the loop variable being rebound.
        del frame
        gc.collect()
    return counts
