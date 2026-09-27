"""Pure download and cleaning functions for br_mapbiomas_estatisticas.

No Prefect imports: the one-shot onboarding bootstrap under
`models/br_mapbiomas_estatisticas/code/` imports these same functions, so the
cleaning transform exists in one place only.

The source is a single 75 MB workbook whose `COVERAGE_11` sheet is wide: one row
per municipality-biome-class, one column per year. Cleaning pivots it to long
form and writes hive-partitioned parquet. The sheet is streamed with openpyxl in
read-only mode and never materialised as a dataframe.
"""

from __future__ import annotations

import csv
import math
import re
import shutil
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

# pyrefly: ignore [untyped-import]
import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_mapbiomas_estatisticas.constants import constants
from pipelines.datasets.br_mapbiomas_estatisticas.legend import (
    CLASS_LABELS,
    CLASS_NOTES,
    ORIGIN_LABELS,
)

UF_BY_NAME = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO",
}

# IBGE assigns the first two digits of the municipality code to the state. This
# is the authoritative source of a municipality's UF, and it is what the
# `sigla_uf` column is derived from -- NOT the workbook's `state` column. The two
# disagree for at least one municipality: Ibateguara (2703007, Alagoas) also
# appears with state "Pernambuco" for a 0.088 ha sliver of its polygon that falls
# on the Pernambuco side of the state layer MapBiomas intersects. Deriving the UF
# from the workbook would file that sliver under PE, contradicting the code.
UF_BY_CODE = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}


@dataclass
class _MunicipalityClass:
    """One municipality-biome-class group, accumulating area across years.

    A plain dict here would carry a heterogeneous value union that defeats type
    checking on the `+=` below, which is where a wrong key would actually bite.
    """

    sigla_uf: str
    levels: list[str]
    areas: dict[int, float] = field(default_factory=dict)


def cell_float(value: object) -> float:
    """Coerce an openpyxl cell value to float; blank cells count as zero.

    openpyxl types a cell as a wide union (formula objects, dates, Decimal,
    None), so the coercion is written out rather than calling `float()` on the
    union directly.
    """
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    if isinstance(value, str) and value.strip():
        return float(value)
    return 0.0


def cell_class_id(value: object) -> str:
    """Read a class code, which the workbook stores as a float, as a string."""
    return str(int(cell_float(value)))


_PREFIX = re.compile(r"^\s*([\d]+(?:\.[\d]+)*)\.?\s*")


def strip_prefix(label: str) -> str:
    """Drop the dotted numeric prefix from a MapBiomas class label.

    The workbook and the published legend PDF disagree on the numbering for
    several classes (Savanna Formation is 1.2 in one and 1.3 in the other), so
    the prefix is not carried into the published labels. The class id is the
    stable identifier.
    """
    return _PREFIX.sub("", label or "").strip()


def hierarchy_code(label: str) -> str | None:
    """Return the dotted numeric prefix of a label, e.g. "3.2.1.1"."""
    match = _PREFIX.match(label or "")
    return match.group(1) if match else None


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------


def resolve_municipality_drive_id(
    session: requests.Session | None = None,
) -> str:
    """Read the statistics page and return the Drive id of the municipality file.

    MapBiomas republishes the workbook under a new Drive id with each collection.
    Reading the page keeps the pipeline pointed at whatever is currently
    published instead of a hardcoded id that goes stale every August.
    """
    session = session or requests.Session()
    resp = session.get(
        constants.STATISTICS_PAGE.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=120,
    )
    resp.raise_for_status()
    ids = re.findall(r"drive\.google\.com/uc\?id=([\w-]+)", resp.text)
    if not ids:
        raise RuntimeError(
            "No Google Drive download found on the MapBiomas statistics page. "
            "The page layout changed; re-check "
            f"{constants.STATISTICS_PAGE.value}"
        )
    return ids[0]


def download_municipality_workbook(
    input_dir: Path, drive_id: str | None = None
) -> Path:
    """Download and unzip the municipality statistics workbook.

    Google Drive interposes a virus-scan interstitial for files this large; the
    confirm token and uuid have to be read off that HTML form and replayed.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    drive_id = drive_id or constants.MUNICIPALITY_DRIVE_ID.value
    session = requests.Session()
    session.headers["User-Agent"] = constants.USER_AGENT.value

    confirm = session.get(
        "https://drive.usercontent.google.com/download",
        params={"id": drive_id, "export": "download"},
        timeout=300,
    )
    confirm.raise_for_status()
    uuid_match = re.search(r'name="uuid" value="([^"]+)"', confirm.text)
    params = {"id": drive_id, "export": "download", "confirm": "t"}
    if uuid_match:
        params["uuid"] = uuid_match.group(1)

    zip_path = input_dir / constants.MUNICIPALITY_ZIP_NAME.value
    with session.get(
        "https://drive.usercontent.google.com/download",
        params=params,
        stream=True,
        timeout=1800,
    ) as resp:
        resp.raise_for_status()
        declared = resp.headers.get("Content-Length")
        # decode_content=True so a Content-Encoding is undone; copying resp.raw
        # without it writes the still-compressed bytes.
        resp.raw.decode_content = True
        with open(zip_path, "wb") as fh:
            shutil.copyfileobj(resp.raw, fh)

    got = zip_path.stat().st_size
    if declared is not None and int(declared) != got:
        raise RuntimeError(
            f"Truncated download: {zip_path} is {got} bytes, server declared "
            f"{declared}. A dropped connection looks like success otherwise."
        )
    if not zipfile.is_zipfile(zip_path):
        raise RuntimeError(
            f"{zip_path} is not a zip archive. Google Drive most likely returned "
            "the virus-scan page instead of the file."
        )
    with zipfile.ZipFile(zip_path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        if len(names) != 1:
            raise RuntimeError(
                f"Expected exactly one .xlsx in {zip_path}, got {names}"
            )
        zf.extract(names[0], input_dir)
        return input_dir / names[0]


def download_biome_state_workbook(input_dir: Path) -> Path:
    """Download the biome/state workbook, which carries the transition matrices.

    Transitions are published at biome x state grain only; there is no municipal
    equivalent in any MapBiomas collection.
    """
    input_dir.mkdir(parents=True, exist_ok=True)
    out = input_dir / constants.BIOME_STATE_NAME.value
    resp = requests.get(
        constants.BIOME_STATE_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=1800,
    )
    resp.raise_for_status()
    out.write_bytes(resp.content)
    return out


def download_legend_csv(input_dir: Path) -> Path:
    """Download MapBiomas' machine-readable legend (class id, PT and EN name, colour)."""
    input_dir.mkdir(parents=True, exist_ok=True)
    out = input_dir / "legend_col11.csv"
    resp = requests.get(
        constants.LEGEND_CSV_URL.value,
        headers={"User-Agent": constants.USER_AGENT.value},
        timeout=300,
    )
    resp.raise_for_status()
    out.write_bytes(resp.content)
    return out


# ---------------------------------------------------------------------------
# read
# ---------------------------------------------------------------------------


def iter_coverage_rows(workbook: Path) -> Iterator[dict]:
    """Stream the wide COVERAGE sheet, one dict per source row.

    Yields `{"geocode", "state", "biome", "class_id", "levels", "origin",
    "areas": {year: ha}}`. The workbook is 75 MB; it is never loaded whole.
    """
    wb = openpyxl.load_workbook(workbook, read_only=True, data_only=True)
    try:
        ws = wb[constants.COVERAGE_SHEET.value]
        rows = ws.iter_rows(values_only=True)
        header = list(next(rows))
        idx = {name: i for i, name in enumerate(header)}
        required = {
            "geocode",
            "state",
            "biome",
            "class",
            "class_level_0",
            "class_level_1",
            "class_level_2",
            "class_level_3",
            "class_level_4",
        }
        missing = required - set(idx)
        if missing:
            raise RuntimeError(
                f"COVERAGE sheet is missing columns: {sorted(missing)}"
            )
        years = {
            int(name[1:]): i
            for name, i in idx.items()
            if isinstance(name, str) and re.fullmatch(r"y\d{4}", name)
        }
        if not years:
            raise RuntimeError("COVERAGE sheet has no yNNNN columns")

        for row in rows:
            if row[idx["geocode"]] is None:
                continue
            yield {
                "geocode": str(row[idx["geocode"]]).strip(),
                "state": str(row[idx["state"]]).strip(),
                "biome": str(row[idx["biome"]]).strip(),
                "class_id": str(row[idx["class"]]).strip(),
                "origin": row[idx["class_level_0"]],
                "levels": tuple(
                    row[idx[f"class_level_{i}"]] for i in range(1, 5)
                ),
                "areas": {y: row[i] for y, i in years.items()},
            }
    finally:
        wb.close()


def read_legend_csv(path: Path) -> dict[str, dict[str, str]]:
    """Read the published legend CSV into `{class_id: {pt, en, hex}}`."""
    out: dict[str, dict[str, str]] = {}
    with open(path, encoding="utf-8-sig", newline="") as fh:
        for row in csv.DictReader(fh):
            out[str(row["class_id"]).strip()] = {
                "pt": row["class_name_pt_br"].strip(),
                "en": row["class_name_en"].strip(),
                "hex": row["hex_code"].strip(),
            }
    return out


# ---------------------------------------------------------------------------
# transform
# ---------------------------------------------------------------------------


def check_label_coverage(labels: set[str]) -> None:
    """Fail if the workbook emits a class label with no PT/ES translation.

    A future collection adds classes. Without this gate they would reach
    BigQuery as untranslated English, which is exactly the failure mode that is
    invisible in aggregate row counts.
    """
    unknown = sorted(label for label in labels if label not in CLASS_LABELS)
    if unknown:
        raise RuntimeError(
            "MapBiomas class labels with no Portuguese/Spanish translation: "
            f"{unknown}. Add them to pipelines/datasets/br_mapbiomas_estatisticas/"
            "legend.py before re-running."
        )


def build_classe(workbook: Path, legend_csv: Path) -> list[dict]:
    """Build the class dictionary: one row per class observed in the statistics.

    The hierarchy comes from the statistics workbook rather than the legend PDF,
    because the workbook is what the area numbers are keyed to. Colours come from
    the published legend CSV, matched on class id.
    """
    legend = read_legend_csv(legend_csv)
    seen: dict[str, dict] = {}
    labels: set[str] = set()
    for row in iter_coverage_rows(workbook):
        if row["class_id"] in seen:
            continue
        levels = [strip_prefix(level) for level in row["levels"]]
        labels.update(levels)
        seen[row["class_id"]] = {
            "levels": levels,
            "origin": row["origin"],
            "codigo_hierarquia": hierarchy_code(str(row["levels"][3])),
        }
    check_label_coverage(labels)

    out = []
    for class_id, info in sorted(seen.items(), key=lambda kv: int(kv[0])):
        levels = info["levels"]
        own_en = levels[-1]
        own_pt, own_es = CLASS_LABELS[own_en]
        record = {
            "chave": class_id,
            # Depth at which the class is defined. The workbook repeats a class's
            # own label down the remaining levels, so the count of distinct
            # labels in the chain is that depth.
            "nivel": str(len(dict.fromkeys(levels))),
            "origem": ORIGIN_LABELS.get(
                str(info["origin"]), str(info["origin"])
            ),
            "codigo_hierarquia": info["codigo_hierarquia"],
            "valor_pt": own_pt,
            "valor_en": own_en,
            "valor_es": own_es,
            "cor_hex": legend.get(class_id, {}).get("hex"),
            "observacoes": CLASS_NOTES.get(class_id),
        }
        for i, level in enumerate(levels, start=1):
            pt, es = CLASS_LABELS[level]
            record[f"nivel_{i}_pt"] = pt
            record[f"nivel_{i}_en"] = level
            record[f"nivel_{i}_es"] = es
        out.append(record)
    return out


def clean_coverage(
    workbook: Path,
) -> tuple[dict[tuple[int, str], list[dict]], dict]:
    """Pivot the wide coverage sheet to long form, bucketed by (year, sigla_uf).

    Two transforms beyond the pivot:

    * `sigla_uf` is derived from the municipality code, not from the workbook's
      `state` column -- see `UF_BY_CODE`.
    * Rows are summed over the workbook's `state` column, which is a genuine
      extra dimension in the source (a municipality's polygon can straddle a
      state boundary) but not one this table carries. Without the sum the table
      would have duplicate keys.

    Returns the buckets plus a report carrying row counts and the disagreements
    found, so the caller can assert against the source rather than trusting that
    the pass completed.
    """
    grouped: dict[tuple[str, str, str], _MunicipalityClass] = {}
    labels: set[str] = set()
    source_rows = 0
    uf_disagreements: list[tuple[str, str, str]] = []
    unknown_codes: set[str] = set()

    for row in iter_coverage_rows(workbook):
        source_rows += 1
        geocode = row["geocode"]
        sigla_uf = UF_BY_CODE.get(geocode[:2])
        if sigla_uf is None:
            unknown_codes.add(geocode)
            continue
        from_workbook = UF_BY_NAME.get(row["state"])
        if from_workbook != sigla_uf:
            uf_disagreements.append((geocode, row["state"], sigla_uf))

        levels = [strip_prefix(level) for level in row["levels"]]
        labels.update(levels)
        key = (geocode, row["biome"], row["class_id"])
        entry = grouped.get(key)
        if entry is None:
            entry = grouped[key] = _MunicipalityClass(
                sigla_uf=sigla_uf,
                levels=levels,
                areas=dict.fromkeys(row["areas"], 0.0),
            )
        for year, area in row["areas"].items():
            entry.areas[year] += cell_float(area)

    check_label_coverage(labels)
    if unknown_codes:
        raise RuntimeError(
            f"Municipality codes with an unknown IBGE state prefix: "
            f"{sorted(unknown_codes)}"
        )

    buckets: dict[tuple[int, str], list[dict]] = {}
    for (geocode, biome, class_id), entry in grouped.items():
        pt_levels = [CLASS_LABELS[level][0] for level in entry.levels]
        for year, area in entry.areas.items():
            buckets.setdefault((year, entry.sigla_uf), []).append(
                {
                    "ano": year,
                    "sigla_uf": entry.sigla_uf,
                    "id_municipio": geocode,
                    "id_classe": class_id,
                    "bioma": biome,
                    "nivel_1": pt_levels[0],
                    "nivel_2": pt_levels[1],
                    "nivel_3": pt_levels[2],
                    "nivel_4": pt_levels[3],
                    "area": area,
                }
            )

    report = {
        "source_rows": source_rows,
        "grouped_rows": len(grouped),
        "long_rows": sum(len(v) for v in buckets.values()),
        "partitions": len(buckets),
        "classes": len(labels),
        "uf_disagreements": sorted(set(uf_disagreements)),
    }
    return buckets, report


PERIOD_RE = re.compile(r"^p(\d{4})_(\d{4})$")


def classify_period(start: int, end: int) -> str | None:
    """Map a transition period to one of the three registered tables.

    The sheet carries 40 consecutive one-year periods, eight five-year periods
    and three ten-year periods, plus eleven ad-hoc spans (1985-2025, 2008-2017 and
    others) that belong to none of the three tables and are skipped.
    """
    span = end - start
    if span == 1:
        return "transicao_uf_de_para_anual"
    if span == 5 and start % 5 == 0:
        return "transicao_uf_de_para_quinquenal"
    if span == 10 and start % 10 == 0:
        return "transicao_uf_de_para_decenal"
    return None


def clean_transitions(
    workbook: Path, uf_by_name: dict[str, str] | None = None
) -> tuple[dict[str, dict[tuple[int, str], list[dict]]], dict]:
    """Read TRANSITION_11 and bucket it per table, year and state."""
    uf_by_name = uf_by_name or UF_BY_NAME
    wb = openpyxl.load_workbook(workbook, read_only=True, data_only=True)
    out: dict[str, dict[tuple[int, str], list[dict]]] = {}
    skipped_periods: set[str] = set()
    unknown_states: set[str] = set()
    source_rows = 0
    try:
        ws = wb[constants.TRANSITION_SHEET.value]
        rows = ws.iter_rows(values_only=True)
        header = list(next(rows))
        idx = {name: i for i, name in enumerate(header)}
        periods = []
        for name, i in idx.items():
            if not isinstance(name, str):
                continue
            match = PERIOD_RE.match(name)
            if not match:
                continue
            start, end = int(match.group(1)), int(match.group(2))
            table = classify_period(start, end)
            if table is None:
                skipped_periods.add(name)
                continue
            periods.append((table, start, end, i))
        if not periods:
            raise RuntimeError(
                "TRANSITION sheet has no recognised pNNNN_NNNN columns"
            )

        for row in rows:
            # openpyxl yields short tuples for the sheet's trailing blank rows
            if len(row) <= idx["state"] or row[idx["state"]] is None:
                continue
            source_rows += 1
            sigla_uf = uf_by_name.get(str(row[idx["state"]]).strip())
            if sigla_uf is None:
                unknown_states.add(str(row[idx["state"]]).strip())
                continue
            bioma = str(row[idx["biome"]]).strip()
            class_from = cell_class_id(row[idx["class_from"]])
            class_to = cell_class_id(row[idx["class_to"]])
            for table, start, end, i in periods:
                area = row[i]
                if area is None:
                    continue
                out.setdefault(table, {}).setdefault(
                    (end, sigla_uf), []
                ).append(
                    {
                        "ano": end,
                        "ano_inicial": start,
                        "sigla_uf": sigla_uf,
                        "id_classe_de": class_from,
                        "id_classe_para": class_to,
                        "bioma": bioma,
                        "area": cell_float(area),
                    }
                )
    finally:
        wb.close()

    if unknown_states:
        raise RuntimeError(
            f"State names with no UF abbreviation: {sorted(unknown_states)}"
        )

    report = {
        "source_rows": source_rows,
        "skipped_periods": sorted(skipped_periods),
        "rows": {
            table: sum(len(v) for v in buckets.values())
            for table, buckets in out.items()
        },
    }
    return out, report


def validate_uf_against_source(
    uf_buckets: dict[tuple[int, str], list[dict]],
    biome_state_workbook: Path,
    uf_by_name: dict[str, str] | None = None,
    tolerance: float = 1.0,
) -> dict:
    """Compare the municipality-derived state totals with MapBiomas' own file.

    The state table is aggregated up from municipalities so it cannot disagree
    with the municipal table. That makes it worth checking against the figures
    MapBiomas publishes directly, which are computed from the raster over state
    boundaries rather than over municipal ones.
    """
    uf_by_name = uf_by_name or UF_BY_NAME
    derived: dict[tuple[int, str], float] = {}
    for (year, sigla_uf), records in uf_buckets.items():
        derived[(year, sigla_uf)] = math.fsum(r["area"] for r in records)

    published: dict[tuple[int, str], list[float]] = {}
    wb = openpyxl.load_workbook(
        biome_state_workbook, read_only=True, data_only=True
    )
    try:
        ws = wb[constants.COVERAGE_SHEET.value]
        rows = ws.iter_rows(values_only=True)
        header = list(next(rows))
        idx = {name: i for i, name in enumerate(header)}
        years = {
            int(name[1:]): i
            for name, i in idx.items()
            if isinstance(name, str) and re.fullmatch(r"y\d{4}", name)
        }
        for row in rows:
            if len(row) <= idx["state"] or row[idx["state"]] is None:
                continue
            sigla_uf = uf_by_name.get(str(row[idx["state"]]).strip())
            if sigla_uf is None:
                continue
            for year, i in years.items():
                published.setdefault((year, sigla_uf), []).append(
                    cell_float(row[i])
                )
    finally:
        wb.close()

    diffs = []
    for key, total in sorted(derived.items()):
        reference = math.fsum(published.get(key, []))
        if reference:
            diffs.append(
                (key, total, reference, (total - reference) / reference * 100)
            )
    worst = max(diffs, key=lambda d: abs(d[3])) if diffs else None
    over = [d for d in diffs if abs(d[3]) > tolerance]
    return {
        "compared": len(diffs),
        "worst": worst,
        "over_tolerance": [(d[0], round(d[3], 4)) for d in over[:20]],
        "n_over_tolerance": len(over),
    }


# ---------------------------------------------------------------------------
# write
# ---------------------------------------------------------------------------

COVERAGE_COLUMNS = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "id_classe",
    "bioma",
    "nivel_1",
    "nivel_2",
    "nivel_3",
    "nivel_4",
    "area",
]

COVERAGE_UF_COLUMNS = [
    "ano",
    "sigla_uf",
    "id_classe",
    "bioma",
    "nivel_1",
    "nivel_2",
    "nivel_3",
    "nivel_4",
    "area",
]

# `chave`, `valor_pt` and `valor_en` are the names already registered for this
# table in the backend; `valor_es` and the hierarchy columns are added here.
# `valor_*` repeats `nivel_4_*` by construction, because the workbook repeats a
# class's own label down the remaining levels. It is kept because it is the
# registered name and gives the class's own label without the reader having to
# know its depth.
TRANSITION_COLUMNS = [
    "ano",
    "ano_inicial",
    "sigla_uf",
    "id_classe_de",
    "id_classe_para",
    "bioma",
    "area",
]

CLASSE_COLUMNS = [
    "chave",
    "nivel",
    "origem",
    "codigo_hierarquia",
    "valor_pt",
    "valor_en",
    "valor_es",
    "nivel_1_pt",
    "nivel_1_en",
    "nivel_1_es",
    "nivel_2_pt",
    "nivel_2_en",
    "nivel_2_es",
    "nivel_3_pt",
    "nivel_3_en",
    "nivel_3_es",
    "nivel_4_pt",
    "nivel_4_en",
    "nivel_4_es",
    "cor_hex",
    "observacoes",
]


def _string_table(records: list[dict], columns: list[str]) -> pa.Table:
    """Build an all-STRING Arrow table with a stable column order.

    Staging is all-STRING by house convention and the dbt model `safe_cast`s
    every column, so the parquet schema carries order, not types. The cast goes
    through Arrow rather than `astype(str)`, which would render NULL as the
    literal "nan" and defeat `safe_cast`. Floats are formatted with repr so a
    round trip does not lose precision, and integers are written without a
    trailing ".0".
    """

    def render(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, float):
            return repr(value)
        return str(value)

    arrays = [
        pa.array([render(rec.get(col)) for rec in records], type=pa.string())
        for col in columns
    ]
    return pa.Table.from_arrays(arrays, names=columns)


def _write_partitioned(
    buckets: dict[tuple[int, str], list[dict]],
    output_dir: Path,
    columns: list[str],
) -> int:
    """Write `ano=<year>/sigla_uf=<uf>/data.parquet` under `output_dir`."""
    written = 0
    payload_columns = [c for c in columns if c not in ("ano", "sigla_uf")]
    for (year, sigla_uf), records in sorted(buckets.items()):
        part = output_dir / f"ano={year}" / f"sigla_uf={sigla_uf}"
        part.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            _string_table(records, payload_columns),
            part / "data.parquet",
            compression="snappy",
        )
        written += 1
    return written


def aggregate_to_uf(
    buckets: dict[tuple[int, str], list[dict]],
) -> dict[tuple[int, str], list[dict]]:
    """Sum the municipal table up to state level.

    Derived from the municipal table rather than read from MapBiomas' separate
    biome/state workbook, so the two published tables cannot disagree. The
    municipalities tile the state exactly, so the sum is the same quantity;
    `validate_uf_against_source` checks that against the published file.
    """
    out: dict[tuple[int, str], list[dict]] = {}
    for (year, sigla_uf), records in buckets.items():
        grouped: dict[tuple[str, str], dict] = {}
        for rec in records:
            key = (rec["bioma"], rec["id_classe"])
            entry = grouped.get(key)
            if entry is None:
                entry = grouped[key] = {
                    "ano": rec["ano"],
                    "sigla_uf": rec["sigla_uf"],
                    "id_classe": rec["id_classe"],
                    "bioma": rec["bioma"],
                    "nivel_1": rec["nivel_1"],
                    "nivel_2": rec["nivel_2"],
                    "nivel_3": rec["nivel_3"],
                    "nivel_4": rec["nivel_4"],
                    "area": 0.0,
                }
            entry["area"] += rec["area"]
        out[(year, sigla_uf)] = list(grouped.values())
    return out


def write_classe(records: list[dict], output_dir: Path) -> Path:
    """Write the unpartitioned class dictionary."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "data.parquet"
    pq.write_table(
        _string_table(records, CLASSE_COLUMNS), path, compression="snappy"
    )
    return path


def clean_all(input_dir: Path, output_dir: Path) -> dict:
    """Run the whole transform: two workbooks in, partitioned parquet out."""
    municipal = next(input_dir.glob("*BIOME_STATE_MUNICIPALITY.xlsx"))
    biome_state = input_dir / constants.BIOME_STATE_NAME.value
    legend_csv = input_dir / "legend_col11.csv"

    report: dict = {}

    classe = build_classe(municipal, legend_csv)
    write_classe(classe, output_dir / "classe")
    report["classe_rows"] = len(classe)

    buckets, coverage_report = clean_coverage(municipal)
    report.update(coverage_report)
    report["municipio_files"] = _write_partitioned(
        buckets, output_dir / "cobertura_municipio_classe", COVERAGE_COLUMNS
    )

    uf_buckets = aggregate_to_uf(buckets)
    report["uf_rows"] = sum(len(v) for v in uf_buckets.values())
    report["uf_files"] = _write_partitioned(
        uf_buckets, output_dir / "cobertura_uf_classe", COVERAGE_UF_COLUMNS
    )
    report["uf_validation"] = validate_uf_against_source(
        uf_buckets, biome_state
    )

    transitions, transition_report = clean_transitions(biome_state)
    report["transitions"] = transition_report
    report["transition_files"] = {
        table: _write_partitioned(
            tbuckets, output_dir / table, TRANSITION_COLUMNS
        )
        for table, tbuckets in transitions.items()
    }
    return report
