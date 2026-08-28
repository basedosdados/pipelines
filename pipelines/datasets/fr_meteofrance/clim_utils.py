"""Download and clean transform for the Météo-France climatological archive.

The *only* copy: `models/fr_meteofrance/code/clim_download.py` and
`clim_clean.py` are thin CLIs that import from here, and the recurring flow's
tasks call the same functions. Nothing Prefect-specific lives in this module.

The archive is published per département in three period slices — `avant-1949`,
`previous-1950-2024` and `latest-<years>`. Only the last one is rewritten as new
observations land, so the monthly refresh fetches and rebuilds `latest-*` alone
and leaves the two historical slices on GCS untouched. Staging objects are named
`<dept>_<period>.parquet`, so re-uploading that slice overwrites those objects
in place rather than accumulating duplicates.
"""

import gzip
import json
import os
import re
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.fr_meteofrance import clim_schema as cs

DATASETS = {
    "quot": "6569b51ae64326786e4e8e1a",  # Données climatologiques de base - quotidiennes
    "mens": "6569b3d7d193b4daf2b43edc",  # Données climatologiques de base - mensuelles
}

INPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_INPUT", "~/Downloads/fr_meteofrance_clim/input")
    )
)


def resource_urls(dataset_id: str) -> list[tuple[str, str, int]]:
    """Return ``(url, filename, size)`` for every ``csv.gz`` resource."""
    url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    with urllib.request.urlopen(url, timeout=120) as fh:
        payload = json.load(fh)
    out = []
    for r in payload.get("resources", []):
        if (r.get("format") or "") != "csv.gz":
            continue
        u = r["url"]
        out.append((u, u.rsplit("/", 1)[-1], r.get("filesize") or 0))
    return out


def fetch(args, attempts: int = 5) -> tuple[str, bool]:
    """Fetch one file, resuming the run by skipping anything already on disk.

    Retries with a linear backoff: pulling ~940 files in parallel reliably trips
    transient DNS and connection-reset failures against the OVH store, and a
    single failure would otherwise abandon the whole download.
    """
    url, dest = args
    if dest.exists() and dest.stat().st_size > 0:
        return dest.name, False
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(1, attempts + 1):
        try:
            with (
                urllib.request.urlopen(url, timeout=900) as r,
                tmp.open("wb") as fh,
            ):
                while chunk := r.read(1 << 20):
                    fh.write(chunk)
            tmp.rename(dest)
            return dest.name, True
        except Exception as exc:  # retry every transport error
            if attempt == attempts:
                raise RuntimeError(
                    f"{dest.name}: giving up after {attempts} tries"
                ) from exc
            time.sleep(2 * attempt)
    raise AssertionError("unreachable")


LATEST = "latest-"


def is_latest(filename: str) -> bool:
    """True for the one period slice Météo-France rewrites as data lands."""
    return LATEST in filename


def download(kind: str, workers: int = 4, only_latest: bool = False) -> None:
    out = INPUT / kind
    out.mkdir(parents=True, exist_ok=True)
    resources = resource_urls(DATASETS[kind])
    if only_latest:
        resources = [r for r in resources if is_latest(r[1])]
    total = sum(s for _u, _n, s in resources)
    print(f"{kind}: {len(resources)} files, {total / 1e9:.2f} GB compressed")

    jobs = [(u, out / n) for u, n, _s in resources]
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for name, fetched in pool.map(fetch, jobs):
            done += 1
            if done % 50 == 0 or not fetched:
                print(
                    f"  {done}/{len(jobs)} {name}{'' if fetched else ' (cached)'}"
                )
    print(f"{kind}: done, {len(list(out.glob('*.csv.gz')))} files on disk")


OUTPUT = Path(
    os.path.expanduser(
        os.environ.get("MFC_OUTPUT", "~/Downloads/fr_meteofrance_clim/output")
    )
)

RE_QUOT = re.compile(
    r"^Q_(?P<dep>[0-9A-Z]+)_(?P<period>.+)_(?P<kind>RR-T-Vent|autres-parametres)\.csv\.gz$"
)
RE_MENS = re.compile(r"^MENSQ_(?P<dep>[0-9A-Z]+)_(?P<period>.+)\.csv\.gz$")

LEADING = ["annee", "mois", "date", "numero_poste"]
POSTE_COLS = [
    "numero_poste",
    "nom_poste",
    "id_departement",
    "latitude",
    "longitude",
    "altitude",
]


def header(path: Path) -> list[str]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        return fh.readline().strip().split(";")


def to_string_table(df: pd.DataFrame, order: list[str]) -> pa.Table:
    table = pa.Table.from_pandas(df[order], preserve_index=False)
    return table.cast(pa.schema([(c, pa.string()) for c in order]))


def typed(series: pd.Series, bq_type: str) -> pd.Series:
    if bq_type == "INT64":
        return pd.to_numeric(series, errors="coerce").round().astype("Int64")
    if bq_type == "FLOAT64":
        return pd.to_numeric(series, errors="coerce").astype("Float64")
    s = series.astype("string").str.strip()
    return s.mask(s.eq(""))


def shape(df: pd.DataFrame, spec, date_col: str) -> pd.DataFrame:
    """Rename, type and add the temporal scaffolding for one source frame."""
    raw = df[date_col].astype("string").str.strip()
    out = pd.DataFrame(index=df.index)
    out["annee"] = pd.to_numeric(raw.str[:4], errors="coerce").astype("Int64")
    out["mois"] = pd.to_numeric(raw.str[4:6], errors="coerce").astype("Int64")
    if date_col == "AAAAMMJJ":
        out["date"] = raw.str[:4] + "-" + raw.str[4:6] + "-" + raw.str[6:8]
    out["numero_poste"] = df["NUM_POSTE"].astype("string").str.strip()
    for target, bq_type, _unit, _dict, _pt, _en, _es, source in spec:
        out[target] = (
            typed(df[source], bq_type) if source in df.columns else pd.NA
        )
    return out


def write_group(
    df: pd.DataFrame, order: list[str], root: Path, stem: str
) -> int:
    """One parquet per (département, period), matching the source's own layout.

    Deliberately NOT hive-partitioned by year. These series span ~175 years, so
    partitioning the staging files by year would emit tens of thousands of tiny
    parquet files — slow to write, slow for BigQuery to read, and pointless:
    the dbt model does a full scan of staging and produces the year-partitioned
    table itself. Keeping the source's dept x period unit also makes the
    incremental refresh natural, since Météo-France only rewrites the
    ``latest-<years>`` files.
    """
    if df.empty:
        return 0
    path = root / f"{stem}.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")
    return len(df)


def collect_poste(df: pd.DataFrame, dep: str, sink: dict) -> None:
    """Keep the most recent non-null station attributes seen for each poste."""
    cols = ["NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI"]
    have = [c for c in cols if c in df.columns]
    sub = (
        df[have]
        .dropna(subset=["NUM_POSTE"])
        .drop_duplicates(subset=["NUM_POSTE"], keep="last")
    )
    for row in sub.itertuples(index=False):
        d = dict(zip(have, row, strict=True))
        sink[d["NUM_POSTE"]] = {
            "numero_poste": d["NUM_POSTE"],
            "nom_poste": d.get("NOM_USUEL"),
            "id_departement": dep,
            "latitude": d.get("LAT"),
            "longitude": d.get("LON"),
            "altitude": d.get("ALTI"),
        }


def clean_quot(descriptors, sink: dict, only_latest: bool = False) -> int:
    files = sorted((INPUT / "quot").glob("*.csv.gz"))
    if only_latest:
        files = [f for f in files if is_latest(f.name)]
    groups = defaultdict(dict)
    for f in files:
        m = RE_QUOT.match(f.name)
        if not m:
            raise ValueError(f"unrecognised file name: {f.name}")
        groups[(m.group("dep"), m.group("period"))][m.group("kind")] = f

    if not groups:
        raise FileNotFoundError(
            f"no quotidienne source files under {INPUT / 'quot'}"
            + (" matching the latest slice" if only_latest else "")
        )

    spec = cs.expand(
        header(next(iter(groups.values()))["RR-T-Vent"])
        + [
            c
            for c in header(
                next(
                    f
                    for g in groups.values()
                    if "autres-parametres" in g
                    for f in [g["autres-parametres"]]
                )
            )
        ],
        cs.QUOT_PARAMS,
        cs.QUOT_FLAGS,
        descriptors,
    )
    seen, order = set(), []
    for row in spec:
        if row[0] not in seen:
            seen.add(row[0])
            order.append(row)
    columns = LEADING + [r[0] for r in order]

    total = 0
    for i, ((dep, period), parts) in enumerate(sorted(groups.items()), 1):
        frames = []
        for kind in ("RR-T-Vent", "autres-parametres"):
            if kind not in parts:
                continue
            df = pd.read_csv(
                parts[kind],
                sep=";",
                dtype=str,
                low_memory=False,
                na_values=[""],
            )
            collect_poste(df, dep, sink)
            frames.append(df.set_index(["NUM_POSTE", "AAAAMMJJ"]))
        if not frames:
            continue
        joined = frames[0]
        for extra in frames[1:]:
            dup = [c for c in extra.columns if c in joined.columns]
            joined = joined.join(extra.drop(columns=dup), how="outer")
        joined = joined.reset_index()
        shaped = shape(joined, order, "AAAAMMJJ")
        total += write_group(
            shaped, columns, OUTPUT / "quotidienne", f"{dep}_{period}"
        )
        print(
            f"  [{i}/{len(groups)}] quot {dep} {period}: {len(shaped):,} rows",
            flush=True,
        )
    return total


def clean_mens(descriptors, sink: dict, only_latest: bool = False) -> int:
    files = sorted((INPUT / "mens").glob("*.csv.gz"))
    if only_latest:
        files = [f for f in files if is_latest(f.name)]
    spec = cs.expand(header(files[0]), cs.MENS_PARAMS, {}, descriptors)
    columns = ["annee", "mois", "numero_poste"] + [r[0] for r in spec]

    total = 0
    for i, f in enumerate(files, 1):
        m = RE_MENS.match(f.name)
        if not m:
            raise ValueError(f"unrecognised file name: {f.name}")
        df = pd.read_csv(
            f, sep=";", dtype=str, low_memory=False, na_values=[""]
        )
        collect_poste(df, m.group("dep"), sink)
        shaped = shape(df, spec, "AAAAMM")
        total += write_group(
            shaped,
            columns,
            OUTPUT / "mensuelle",
            f"{m.group('dep')}_{m.group('period')}",
        )
        if i % 25 == 0:
            print(
                f"  [{i}/{len(files)}] mens {f.name}: {len(shaped):,} rows",
                flush=True,
            )
    return total


def build_poste() -> int:
    """Rebuild the station register from every daily and monthly source file.

    Reads only the five register columns, so it is far cheaper than a full
    clean, and it is the ONLY writer of `poste` — see the note in main().
    """
    sink: dict = {}
    for kind, pattern, regex in (
        ("quot", "quot/*.csv.gz", RE_QUOT),
        ("mens", "mens/*.csv.gz", RE_MENS),
    ):
        files = sorted(INPUT.glob(pattern))
        for i, f in enumerate(files, 1):
            m = regex.match(f.name)
            if not m:
                raise ValueError(f"unrecognised file name: {f.name}")
            df = pd.read_csv(
                f,
                sep=";",
                dtype=str,
                low_memory=False,
                na_values=[""],
                usecols=["NUM_POSTE", "NOM_USUEL", "LAT", "LON", "ALTI"],
            )
            collect_poste(df, m.group("dep"), sink)
            if i % 100 == 0:
                print(f"  [{i}/{len(files)}] poste from {kind}", flush=True)
    return write_poste(sink)


def write_poste(sink: dict) -> int:
    df = pd.DataFrame(sink.values())
    for col in ("latitude", "longitude", "altitude"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    # Vectorised rather than df.apply: the row-wise lambda returned `None` for
    # missing coordinates, which pandas types as `str | None` where the stubs
    # want `NAType | str`. Masking keeps the NULLs as pd.NA, which is also what
    # the all-STRING parquet cast expects.
    point = (
        "POINT("
        + df["longitude"].astype("string")
        + " "
        + df["latitude"].astype("string")
        + ")"
    )
    df["geolocalisation"] = point.mask(
        df["longitude"].isna() | df["latitude"].isna()
    )
    df = df.sort_values("numero_poste").reset_index(drop=True)
    order = [*POSTE_COLS, "geolocalisation"]
    path = OUTPUT / "poste" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")
    return len(df)


RE_COLUMN_NAME = re.compile(r"[A-Z][A-Z0-9_]*")

DESCRIPTOR_FILES = (
    "Q_descriptif_champs_RR-T-Vent.csv",
    "Q_descriptif_champs_autres-parametres.csv",
    "MENSQ_descriptif_champs.csv",
)


def fetch_descriptors() -> dict:
    """Download Météo-France's own field descriptions, keyed by source file.

    These are the `*_descriptif_champs.csv` resources published alongside the
    archive: one `NAME : description francaise` per line. `clim_schema.expand`
    reads them to decide family membership, because the column name alone is
    not enough (`HXY` is "heure de FXY").

    Returns ``{filename: {column: french description}}``.
    """
    out: dict[str, dict] = {}
    for dataset_id in DATASETS.values():
        url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
        with urllib.request.urlopen(url, timeout=120) as fh:
            payload = json.load(fh)
        for resource in payload.get("resources", []):
            name = resource["url"].rsplit("/", 1)[-1]
            if name not in DESCRIPTOR_FILES:
                continue
            with urllib.request.urlopen(resource["url"], timeout=180) as fh:
                text = fh.read().decode("utf-8")
            fields = {}
            for line in text.splitlines():
                key, sep, desc = line.partition(":")
                key = key.strip()
                # Each file ends with a prose legend for the quality codes
                # ("Les valeurs du code qualite sont les suivantes", then
                # "0 : ...", "1 : ..."). Those parse as key/value pairs too, so
                # keep only real column names: uppercase, no spaces.
                if sep and RE_COLUMN_NAME.fullmatch(key):
                    fields[key] = desc.strip()
            out[name] = fields
    missing = set(DESCRIPTOR_FILES) - set(out)
    if missing:
        raise RuntimeError(
            f"descriptor files not published: {sorted(missing)}"
        )
    return out
