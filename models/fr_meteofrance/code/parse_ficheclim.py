"""Parse Météo-France FICHECLIM ``*.data`` files (climate normals 1991-2020 + records).

Each fiche is a fixed-layout, semicolon-delimited text file holding one station's
monthly normals and absolute records, twelve months plus an annual column. The
parser turns it into (a) one station record and (b) a long table with one row per
station x indicator x period.
"""

import glob
import os
import re
import unicodedata

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

# Single-row section title -> (indicator slug, measurement unit).
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

# Multi-row group header -> (indicator slug prefix, measurement unit).
GROUPS = {
    "Nombre moyen de jours avec": ("nombre_jours", "day"),
    "Nombre moyen de jours avec rafales": ("nombre_jours_rafale", "day"),
    "Nombre moyen de jours avec brouillard / orage / grêle / neige": (
        "nombre_jours",
        "day",
    ),
}

# Lines that are notes or boilerplate, never section titles.
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


def slugify(text):
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


def dms_to_dd(raw):
    """``43°49'47"N`` -> 43.829722."""
    m = RE_DMS.search(raw)
    if not m:
        return None
    value = int(m.group(1)) + int(m.group(2)) / 60 + int(m.group(3)) / 3600
    return -value if m.group(4) in ("S", "W", "O") else value


def parse_value(raw):
    """``.`` means zero and ``-`` means missing in the published fiches."""
    v = raw.strip()
    if v in ("", "-"):
        return None
    if v == ".":
        return 0.0
    try:
        return float(v)
    except ValueError:
        return None


def parse_file(path):
    """Return ``(station, rows)`` for one fiche."""
    with open(path, encoding="utf-8") as fh:
        lines = [ln.rstrip("\n") for ln in fh]

    station = {}
    rows = []
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

            block = [
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
            rec_start, rec_end = (_iso(m.group(1)), _iso(m.group(2)))
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


def _iso(ddmmyyyy):
    day, month, year = ddmmyyyy.split("-")
    return f"{year}-{month}-{day}"


def parse_all(input_dir):
    """Parse every fiche in ``input_dir``; return ``(stations, rows)``."""
    stations, rows = [], []
    for path in sorted(glob.glob(os.path.join(input_dir, "*.data"))):
        station, block = parse_file(path)
        if not station:
            raise ValueError(f"no station header parsed from {path}")
        stations.append(station)
        rows.extend(block)
    return stations, rows


if __name__ == "__main__":
    import collections

    directory = os.path.expanduser(
        "~/Downloads/fr_meteofrance_data/input/ficheclim"
    )
    stations, rows = parse_all(directory)
    print(f"stations={len(stations)} rows={len(rows)}")

    counts = collections.Counter((r["indicateur"], r["unite"]) for r in rows)
    print(f"--- {len(counts)} indicators ---")
    for (slug, unit), n in counts.most_common():
        print(f"{n:8d}  {slug:52s} {unit}")

    keys = collections.Counter(
        (r["numero_poste"], r["indicateur"], r["periode"]) for r in rows
    )
    print("duplicate keys:", sum(1 for v in keys.values() if v > 1))
    print(
        "null valeur share:",
        round(sum(1 for r in rows if r["valeur"] is None) / len(rows), 3),
    )
    print("sample:", rows[0])
    print("station sample:", stations[0])
