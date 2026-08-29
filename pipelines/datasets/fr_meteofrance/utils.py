"""Download + cleaning transform for fr_meteofrance (shared by the pipeline and
the one-shot bootstrap in models/fr_meteofrance/code/).

Pure functions (no Prefect) so they are importable and unit-testable. The
recurring pipeline wraps them in @task (see tasks.py); the bootstrap CLI imports
them directly. Column order and BigQuery types come from the architecture CSVs
(the single source of truth), never from a second copy of the schema.

Output parquet is **all-STRING** by house convention: the dbt models
``safe_cast`` every column to its architecture type. Values pass through their
real pandas dtype first and are cast with arrow, so ``1996`` does not serialize
as ``"1996.0"`` and NULL does not serialize as ``"nan"``.
"""

import csv
import glob
import json
import logging
import os
import re
import unicodedata
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.fr_meteofrance.constants import constants

log = logging.getLogger("fr_meteofrance")

SYNOP_BASE = constants.SYNOP_BASE.value
FICHE_BASE = constants.FICHE_BASE.value
_ARCH = constants.ARCHITECTURE_DIR.value

PERIODS = [
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
    "annee",
]


# ── schema, read from the architecture CSVs ──────────────────────────────────
def architecture(table: str) -> list[dict]:
    """Rows of ``architecture/<table>.csv``, in column order."""
    with (_ARCH / f"{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def column_order(table: str) -> list[str]:
    return [r["name"] for r in architecture(table)]


def _synop_schema():
    """``(source -> target, target -> type, dictionary-coded targets)`` for synop."""
    rows = architecture("synop")
    rename, types, coded = {}, {}, set()
    for r in rows:
        types[r["name"]] = r["bigquery_type"]
        if r["covered_by_dictionary"] == "yes":
            coded.add(r["name"])
        # The four temporal columns and the two message timestamps derive from
        # source fields that are reshaped, not renamed; skip them here.
        src = r["original_name"]
        if src and src not in (
            "validity_time",
            "reference_time",
            "insert_time",
            "geo_id_wmo",
        ):
            rename[src] = r["name"]
    return rename, types, coded


# ── download ─────────────────────────────────────────────────────────────────
def _get(url: str, dest: Path, timeout: int = 600) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with dest.open("wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
    return dest


def download_synop_years(input_dir: Path, years) -> Path:
    """Fetch ``synop_<year>.csv.gz`` for each year in ``years``."""
    input_dir.mkdir(parents=True, exist_ok=True)
    for year in years:
        name = f"synop_{year}.csv.gz"
        log.info("downloading %s", name)
        _get(f"{SYNOP_BASE}/{name}", input_dir / name)
    return input_dir


def download_postes(input_dir: Path) -> Path:
    """Fetch the SYNOP station register (altitude + opening date)."""
    name = constants.POSTES_GEOJSON.value
    return _get(f"{SYNOP_BASE}/{name}", input_dir / name)


def download_fiches(input_dir: Path) -> Path:
    """Fetch every published climatological sheet, plus its register.

    The register lists one station per feature; each sheet is
    ``FICHECLIM_<numero_poste>.data`` under the same prefix.
    """
    listing = _get(
        f"{FICHE_BASE}/{constants.FICHES_GEOJSON.value}",
        input_dir / constants.FICHES_GEOJSON.value,
    )
    features = json.loads(listing.read_text(encoding="utf-8"))["features"]
    out = input_dir / "ficheclim"
    out.mkdir(parents=True, exist_ok=True)
    for i, feature in enumerate(features, 1):
        num = feature["properties"]["num"]
        _get(
            f"{FICHE_BASE}/FICHECLIM_{num}.data",
            out / f"FICHECLIM_{num}.data",
            timeout=120,
        )
        if i % 250 == 0:
            log.info("fetched %d/%d sheets", i, len(features))
    log.info("fetched %d climatological sheets", len(features))
    return out


# ── parquet helpers ──────────────────────────────────────────────────────────
def to_string_table(df: pd.DataFrame, order: list[str]) -> pa.Table:
    """Cast every column to STRING, preserving NULL, in a fixed column order."""
    table = pa.Table.from_pandas(df[order], preserve_index=False)
    return table.cast(pa.schema([(name, pa.string()) for name in order]))


def write_parquet(df: pd.DataFrame, order: list[str], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(to_string_table(df, order), path, compression="snappy")
    return path


def normalise_code(series: pd.Series) -> pd.Series:
    """``'2.0'`` -> ``'2'``; the source writes some code columns as reals."""
    s = series.astype("string").str.strip()
    s = s.mask(s.eq(""))
    return s.str.replace(r"^(-?\d+)\.0+$", r"\1", regex=True)


# ── SYNOP ────────────────────────────────────────────────────────────────────
def read_synop_year(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Clean one annual SYNOP file; return ``(observations, station attributes)``."""
    rename, types, coded = _synop_schema()
    int_cols = {c for c, t in types.items() if t == "INT64"}
    float_cols = {c for c, t in types.items() if t == "FLOAT64"}

    df = pd.read_csv(
        path, sep=";", dtype=str, low_memory=False, na_values=[""]
    )
    validity = pd.to_datetime(
        df["validity_time"], format="ISO8601", utc=True, errors="coerce"
    )
    bad = validity.isna().sum()
    if bad:
        raise ValueError(f"{path}: {bad} rows with unparseable validity_time")

    # A SYNOP message can be retransmitted or corrected: the same station and
    # validity time then appears more than once, distinguished only by
    # insert_time. Keep the latest insertion, which is the corrected report.
    inserted = pd.to_datetime(
        df["insert_time"], format="ISO8601", utc=True, errors="coerce"
    )
    df = df.assign(_validity=validity, _insert=inserted, _row=range(len(df)))
    df = (
        df.sort_values(["_insert", "_row"], na_position="first")
        .drop_duplicates(subset=["geo_id_wmo", "_validity"], keep="last")
        .sort_values(["_validity", "geo_id_wmo"])
        .reset_index(drop=True)
    )

    out = pd.DataFrame(index=df.index)
    local = df["_validity"].dt.tz_convert("UTC").dt.tz_localize(None)
    out["annee"] = local.dt.year.astype("Int64")
    out["mois"] = local.dt.month.astype("Int64")
    out["date"] = local.dt.strftime("%Y-%m-%d")
    out["heure"] = local.dt.strftime("%H:%M:%S")
    out["indicatif_omm"] = df["geo_id_wmo"].astype("string").str.strip()
    for src, tgt in (
        ("reference_time", "date_heure_traitement"),
        ("insert_time", "date_heure_insertion"),
    ):
        ts = pd.to_datetime(
            df[src], format="ISO8601", utc=True, errors="coerce"
        )
        out[tgt] = (
            ts.dt.tz_convert("UTC")
            .dt.tz_localize(None)
            .dt.strftime("%Y-%m-%dT%H:%M:%S")
        )

    for src, tgt in rename.items():
        col = df[src]
        if tgt in coded:
            out[tgt] = normalise_code(col)
        elif tgt in int_cols:
            out[tgt] = (
                pd.to_numeric(col, errors="coerce").round().astype("Int64")
            )
        elif tgt in float_cols:
            out[tgt] = pd.to_numeric(col, errors="coerce").astype("Float64")
        else:
            out[tgt] = col.astype("string").str.strip().replace("", pd.NA)

    stations = df[
        ["geo_id_wmo", "geo_id_wigos", "name", "lat", "lon", "_validity"]
    ].rename(
        columns={
            "geo_id_wmo": "indicatif_omm",
            "geo_id_wigos": "indicatif_wigos",
            "name": "nom_station",
            "lat": "latitude",
            "lon": "longitude",
        }
    )
    return out, stations


def clean_synop(input_dir: Path, output_dir: Path) -> dict:
    """Clean every ``synop_*.csv.gz`` in ``input_dir`` into ``annee=`` partitions.

    Returns a dict with the partition root, the observed station attributes, the
    total row count and ``max_date`` — the latest observation date, which drives
    the source-update poll.
    """
    paths = sorted(glob.glob(str(input_dir / "synop_*.csv.gz")))
    if not paths:
        raise FileNotFoundError(f"no synop_*.csv.gz under {input_dir}")

    order = column_order("synop")
    root = output_dir / "synop"
    total, seen, max_date = 0, [], None
    for path in paths:
        rows, stations = read_synop_year(Path(path))
        for value, part in rows.groupby("annee"):
            write_parquet(
                part, order, root / f"annee={value}" / "data.parquet"
            )
        hi = rows["date"].max()
        max_date = hi if max_date is None else max(max_date, hi)
        total += len(rows)
        seen.append(stations)
        log.info("synop %s: %d rows", Path(path).stem, len(rows))
    return {
        "synop": root,
        "stations": pd.concat(seen, ignore_index=True),
        "rows": total,
        "max_date": max_date,
    }


def clean_station_synop(
    observed: pd.DataFrame, input_dir: Path, output_dir: Path
) -> Path:
    """One row per station: latest published name and position, plus the register."""
    observed = observed.sort_values("_validity")
    latest = observed.groupby("indicatif_omm", as_index=False).last()
    span = observed.groupby("indicatif_omm")["_validity"].agg(["min", "max"])

    geo = json.loads(
        (input_dir / constants.POSTES_GEOJSON.value).read_text(
            encoding="utf-8"
        )
    )
    meta = pd.DataFrame(
        [
            {
                "indicatif_omm": f["properties"]["Id"],
                "altitude": f["properties"].get("Altitude"),
                "date_ouverture": (
                    f["properties"].get("Date_ouverture") or ""
                )[:10]
                or None,
            }
            for f in geo["features"]
        ]
    )

    df = latest.merge(meta, on="indicatif_omm", how="left")
    df["annee_debut_observation"] = (
        df["indicatif_omm"].map(span["min"].dt.year).astype("Int64")
    )
    df["annee_fin_observation"] = (
        df["indicatif_omm"].map(span["max"].dt.year).astype("Int64")
    )
    for col in ("latitude", "longitude", "altitude"):
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("Float64")
    df["geolocalisation"] = _points(df)
    df = df.sort_values("indicatif_omm").reset_index(drop=True)
    return write_parquet(
        df,
        column_order("station_synop"),
        output_dir / "station_synop" / "data.parquet",
    )


def _points(df: pd.DataFrame) -> pd.Series:
    """WKT points, NULL where either coordinate is missing.

    Vectorised rather than a row-wise apply: the lambda returned `None` for a
    missing coordinate, which types as `str | None` where pandas wants
    `NAType | str`. Masking keeps the gaps as pd.NA, which is also what the
    all-STRING parquet cast expects.
    """
    point = (
        "POINT("
        + df["longitude"].astype("string")
        + " "
        + df["latitude"].astype("string")
        + ")"
    )
    return point.mask(df["longitude"].isna() | df["latitude"].isna())


# ── climatological sheets (fiches) ───────────────────────────────────────────
RE_HEADER = re.compile(
    r"^(?P<nom>.+?)\s*\((?P<dep>[^)]*)\)\s*Indicatif\s*:\s*(?P<num>\d+),"
    r"\s*alt\s*:\s*(?P<alt>-?\d+)m,\s*lat\s*:\s*(?P<lat>[^,]+),\s*lon\s*:\s*(?P<lon>.+?)\s*$"
)
RE_EDIT = re.compile(r"^Edité le\s*:\s*(\d{2})/(\d{2})/(\d{4})")
RE_RECORD_PERIOD = re.compile(
    r"^\(Records établis sur la période du (\d{2}-\d{2}-\d{4}) au (\d{2}-\d{2}-\d{4})\)"
)
RE_STAT_PERIOD = re.compile(
    r"^Statistiques établies sur la période (\d{4})-(\d{4})"
)
RE_DMS = re.compile("(\\d+)°(\\d+)'(\\d+)\"([NSEWO])")
RE_RECORD_DATE = re.compile(r"^(?:(\d{2})-)?(\d{4})$")

SECTIONS = {
    "La température la plus élevée (°C)": (
        "temperature_maximale_absolue",
        "celsius_degree",
    ),
    "Température maximale (Moyenne en °C)": (
        "temperature_maximale_moyenne",
        "celsius_degree",
    ),
    "Température moyenne (Moyenne en °C)": (
        "temperature_moyenne",
        "celsius_degree",
    ),
    "Température minimale (Moyenne en °C)": (
        "temperature_minimale_moyenne",
        "celsius_degree",
    ),
    "La température la plus basse (°C)": (
        "temperature_minimale_absolue",
        "celsius_degree",
    ),
    "Précipitations : Hauteur quotidienne maximale (mm)": (
        "precipitation_quotidienne_maximale",
        "millimeter",
    ),
    "Précipitations : Hauteur moyenne mensuelle (mm)": (
        "precipitation_hauteur_moyenne",
        "millimeter",
    ),
    "Degrés Jours Unifiés (Moyenne en °C)": (
        "degres_jours_unifies",
        "celsius_degree",
    ),
    "Rayonnement global (Moyenne en J/cm²)": (
        "rayonnement_global_moyen",
        "joule_per_square_centimeter",
    ),
    "Durée d'insolation (Moyenne en heures)": (
        "duree_insolation_moyenne",
        "hour",
    ),
    "Evapotranspiration Potentielle (ETP Penman moyenne en mm)": (
        "evapotranspiration_potentielle_moyenne",
        "millimeter",
    ),
    "Rafale maximale de vent (m/s)": (
        "rafale_maximale_absolue",
        "meter_per_second",
    ),
    "Vitesse du vent moyenné sur 10 mn (Moyenne en m/s)": (
        "vitesse_vent_moyenne",
        "meter_per_second",
    ),
}

GROUPS = {
    "Nombre moyen de jours avec": ("nombre_jours", "day"),
    "Nombre moyen de jours avec rafales": ("nombre_jours_rafale", "day"),
    "Nombre moyen de jours avec brouillard / orage / grêle / neige": (
        "nombre_jours",
        "day",
    ),
}

NOTE_PREFIXES = (
    "(Tn=",
    "(Rr",
    "Rr :",
    "(16 m/s",
    "- : donn",
    "Ces statistiques",
    "FICHE CLIMATOLOGIQUE",
    "Statistiques  ",
    "Edité le",
    "Données non disponibles",
    "Janv",
)


def slugify(text: str) -> str:
    """``Tx >=  30°C`` -> ``tx_sup_30c``; ``Tn <= -10°C`` -> ``tn_inf_moins_10c``.

    The minus sign is spelled out: stripping it would collapse ``Tn <= -10°C``
    and ``Tn <= 10°C`` onto the same slug, and two stations publish both rows.
    """
    t = (
        text.lower()
        .replace(">=", " sup ")
        .replace("<=", " inf ")
        .replace("°c", "c")
    )
    t = re.sub(r"-(?=\d)", " moins ", t)
    t = unicodedata.normalize("NFKD", t).encode("ascii", "ignore").decode()
    t = re.sub(r"[^a-z0-9]+", "_", t).strip("_")
    return re.sub(r"_+", "_", t)


def dms_to_dd(raw: str):
    """``43°49'47"N`` -> 43.829722."""
    m = RE_DMS.search(raw)
    if not m:
        return None
    value = int(m.group(1)) + int(m.group(2)) / 60 + int(m.group(3)) / 3600
    return -value if m.group(4) in ("S", "W", "O") else value


def parse_value(raw: str):
    """``.`` means zero and ``-`` means missing in the published sheets."""
    v = raw.strip()
    if v in ("", "-"):
        return None
    if v == ".":
        return 0.0
    try:
        return float(v)
    except ValueError:
        return None


def _iso(ddmmyyyy: str) -> str:
    day, month, year = ddmmyyyy.split("-")
    return f"{year}-{month}-{day}"


def parse_fiche(path) -> tuple[dict, list[dict]]:
    """Return ``(station, rows)`` for one climatological sheet."""
    with open(path, encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh]

    station: dict = {}
    rows: list[dict] = []
    section = None
    group = None
    ref_start, ref_end = 1991, 2020
    rec_start = rec_end = None
    pending = (
        None  # rows of the last emitted block, awaiting an optional "Date" row
    )

    for ln in lines:
        body = ln.rstrip().rstrip(";").strip()
        fields = ln.split(";")

        if not station:
            m = RE_HEADER.match(body)
            if m:
                station = {
                    "numero_poste": m.group("num"),
                    "nom_poste": m.group("nom").strip(),
                    "id_departement": m.group("dep").strip(),
                    "altitude": float(m.group("alt")),
                    "latitude": dms_to_dd(m.group("lat")),
                    "longitude": dms_to_dd(m.group("lon")),
                    "date_edition": None,
                }
                continue

        m = RE_EDIT.match(body)
        if m:
            station["date_edition"] = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
            continue

        if len(fields) >= 14 and body:
            label = fields[0].strip()
            values = fields[1:14]

            if label == "Date":
                for row, raw in zip(
                    pending or [], values, strict=bool(pending)
                ):
                    d = raw.strip()
                    row["date_record"] = d or None
                    md = RE_RECORD_DATE.match(d)
                    row["jour_record"] = (
                        int(md.group(1)) if md and md.group(1) else None
                    )
                    row["annee_record"] = int(md.group(2)) if md else None
                continue

            if label:
                if group is None:
                    continue
                prefix, unit = group
                slug = f"{prefix}_{slugify(label)}"
                libelle = f"{section} {label}".strip() if section else label
            else:
                if section not in SECTIONS:
                    continue
                slug, unit = SECTIONS[section]
                libelle = section

            # Annotated: `date_record`, `jour_record` and `annee_record` start
            # as None and are filled in by a later "Date" row, so an inferred
            # value type of None would reject those assignments.
            block: list[dict] = [
                {
                    "numero_poste": station["numero_poste"],
                    "indicateur": slug,
                    "libelle_indicateur": libelle,
                    "periode": period,
                    "valeur": parse_value(raw),
                    "unite": unit,
                    "annee_debut_reference": ref_start,
                    "annee_fin_reference": ref_end,
                    "date_debut_record": rec_start,
                    "date_fin_record": rec_end,
                    "date_record": None,
                    "jour_record": None,
                    "annee_record": None,
                }
                for period, raw in zip(PERIODS, values, strict=True)
            ]
            rows.extend(block)
            pending = block
            continue

        m = RE_RECORD_PERIOD.match(body)
        if m:
            rec_start, rec_end = _iso(m.group(1)), _iso(m.group(2))
            continue

        m = RE_STAT_PERIOD.match(body)
        if m:
            ref_start, ref_end = int(m.group(1)), int(m.group(2))
            continue

        if not body or body.startswith(NOTE_PREFIXES):
            continue

        if body in GROUPS:
            group = GROUPS[body]
            section = body
        else:
            section = body
            group = None
            ref_start, ref_end = 1991, 2020
            rec_start = rec_end = None
        pending = None

    return station, rows


def parse_all_fiches(fiche_dir) -> tuple[list[dict], list[dict]]:
    """Parse every sheet in ``fiche_dir``; return ``(stations, normals)``."""
    stations, rows = [], []
    for path in sorted(glob.glob(os.path.join(str(fiche_dir), "*.data"))):
        station, block = parse_fiche(path)
        if not station:
            raise ValueError(f"no station header parsed from {path}")
        stations.append(station)
        rows.extend(block)
    return stations, rows


def clean_normales(fiche_dir: Path, output_dir: Path) -> dict:
    """Build ``normale_climatologique`` and ``station_climatologique``.

    ``max_date`` is the latest sheet edition date, which drives the source poll.
    """
    stations, normals = parse_all_fiches(fiche_dir)

    st = pd.DataFrame(stations)
    for col in ("latitude", "longitude", "altitude"):
        st[col] = pd.to_numeric(st[col], errors="coerce").astype("Float64")
    st["geolocalisation"] = _points(st)
    st = st.sort_values("numero_poste").reset_index(drop=True)
    st_path = write_parquet(
        st,
        column_order("station_climatologique"),
        output_dir / "station_climatologique" / "data.parquet",
    )

    nm = pd.DataFrame(normals)
    nm["valeur"] = pd.to_numeric(nm["valeur"], errors="coerce").astype(
        "Float64"
    )
    for col in (
        "annee_debut_reference",
        "annee_fin_reference",
        "jour_record",
        "annee_record",
    ):
        nm[col] = pd.to_numeric(nm[col], errors="coerce").astype("Int64")
    nm = nm.sort_values(["numero_poste", "indicateur", "periode"]).reset_index(
        drop=True
    )
    nm_path = write_parquet(
        nm,
        column_order("normale_climatologique"),
        output_dir / "normale_climatologique" / "data.parquet",
    )

    return {
        "station_climatologique": st_path.parent,
        "normale_climatologique": nm_path.parent,
        "rows": len(nm),
        "stations": len(st),
        "max_date": st["date_edition"].dropna().max(),
    }


# ── dicionario ───────────────────────────────────────────────────────────────
def write_dicionario(output_dir: Path) -> Path:
    """Materialize the committed dictionary CSV as parquet.

    The dictionary is hand-authored reference data (WMO/BUFR code tables plus the
    normals indicator, period and unit labels), committed at
    ``models/fr_meteofrance/code/dicionario.csv`` and regenerated by
    ``build_dicionario.py``. The pipeline only materializes it — deliberately, so
    that a newly published indicator fails ``custom_dictionary_coverage`` and a
    human writes the label, rather than a script inventing one.
    """
    src = _ARCH.parent / "dicionario.csv"
    df = pd.read_csv(src, dtype=str, keep_default_na=False).replace("", None)
    return write_parquet(
        df,
        column_order("dicionario"),
        output_dir / "dicionario" / "data.parquet",
    ).parent
