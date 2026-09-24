"""Pure download and cleaning functions for br_mj_sinesp.

No Prefect imports: the one-shot onboarding script and the recurring pipeline
both import from here, so the transform exists in exactly one place.

Every workbook is streamed row by row (openpyxl read_only). The full source is
9.2M rows across 12 files; none of it is ever materialised in memory.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import zipfile
from collections import defaultdict

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from constants import (
    ARCH_DIR,
    BASE_URL,
    FIRST_YEAR,
    HTTP_HEADERS,
    INPUT_DIR,
    NON_OPERATING_MUNICIPIOS,
    OUTPUT_DIR,
    TABLE_DICIONARIO,
    TABLE_MUNICIPIO,
    TABLE_UF,
    UF_SENTINEL,
    normalise_name,
    tipo_ocorrencia_key,
)

# --------------------------------------------------------------------------
# download
# --------------------------------------------------------------------------

# SINESP orthography that differs from the BD municipality directory. Without
# these 13, the same 13 municipalities go unmatched in every single year.
NAME_OVERRIDES = {
    (
        "BA",
        "MUQUEM DO SAO FRANCISCO",
    ): "2922250",  # dir: Muquém de São Francisco
    ("BA", "SANTA TEREZINHA"): "2928505",  # dir: Santa Teresinha
    ("CE", "ITAPAJE"): "2306306",  # dir: Itapagé
    ("MG", "DONA EUZEBIA"): "3122900",  # dir: Dona Eusébia
    ("MG", "SAO TOME DAS LETRAS"): "3165206",  # dir: São Thomé das Letras
    ("MT", "POXOREU"): "5107008",  # dir: Poxoréo
    ("PA", "SANTA IZABEL DO PARA"): "1506500",  # dir: Santa Isabel do Pará
    ("PE", "IGUARACY"): "2606903",  # dir: Iguaraci
    ("RN", "JANUARIO CICCO"): "2405306",  # renamed; dir: Boa Saúde
    (
        "SE",
        "AMPARO DO SAO FRANCISCO",
    ): "2800100",  # dir: Amparo de São Francisco
    ("SP", "FLORINEA"): "3516101",  # dir: Florínia
    ("SP", "SAO LUIZ DO PARAITINGA"): "3550001",  # dir: São Luís do Paraitinga
    ("TO", "TABOCAO"): "1708254",  # dir: Fortaleza do Tabocão
}


def is_complete_xlsx(path: str) -> bool:
    """gov.br returns HTTP 200 for a truncated body; only the zip proves it."""
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None and any(
                n.startswith("xl/worksheets") for n in z.namelist()
            )
    except Exception:
        return False


def download_year(
    year: int, dest_dir: str = INPUT_DIR, attempts: int = 4
) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    path = os.path.join(dest_dir, f"bancovde-{year}.xlsx")
    if os.path.exists(path) and is_complete_xlsx(path):
        return path
    url = BASE_URL.format(year=year)
    part = path + ".part"
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(
                url, headers=HTTP_HEADERS, stream=True, timeout=(30, 300)
            ) as r:
                r.raise_for_status()
                written = 0
                with open(part, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
                        written += len(chunk)
                declared = r.headers.get("Content-Length")
                if declared and int(declared) != written:
                    raise OSError(f"short read: {written} of {declared} bytes")
            if is_complete_xlsx(part):
                os.replace(part, path)
                return path
            raise OSError("incomplete xlsx archive")
        except Exception as exc:
            if os.path.exists(part):
                os.remove(part)
            if attempt == attempts:
                raise RuntimeError(f"bancovde-{year}: {exc}") from exc
    raise RuntimeError(f"bancovde-{year}: exhausted attempts")


def available_years(today: dt.date | None = None) -> list[int]:
    today = today or dt.date.today()
    return list(range(FIRST_YEAR, today.year + 1))


# --------------------------------------------------------------------------
# municipality directory
# --------------------------------------------------------------------------


def load_municipio_directory(path: str | None = None) -> tuple[dict, dict]:
    """-> ({(sigla_uf, normalised name): id_municipio}, {sigla_uf: {id_municipio}})"""
    path = path or os.path.join(ARCH_DIR, "municipio_directory.csv")
    by_name: dict[tuple[str, str], str] = {}
    per_uf: dict[str, set[str]] = defaultdict(set)
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            uf, mid = row["sigla_uf"], row["id_municipio"]
            if mid in NON_OPERATING_MUNICIPIOS:
                continue
            by_name[(uf, normalise_name(row["nome"]))] = mid
            per_uf[uf].add(mid)
    return by_name, dict(per_uf)


def resolve_municipio(uf, municipio, by_name) -> tuple[str | None, str]:
    """-> (id_municipio, kind) with kind in {'municipal', 'uf', 'unmatched'}."""
    n = normalise_name(municipio)
    if n is None or n in UF_SENTINEL:
        return None, "uf"
    key = (uf, n)
    mid = by_name.get(key) or NAME_OVERRIDES.get(key)
    return (mid, "municipal") if mid else (None, "unmatched")


# --------------------------------------------------------------------------
# cleaning
# --------------------------------------------------------------------------


def _as_int(v):
    if v is None or v == "":
        return None
    try:
        return round(float(v))
    except (TypeError, ValueError):
        return None


def _as_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def iter_source_rows(path: str):
    """Yield the workbook's rows as dicts, streaming."""
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb[wb.sheetnames[0]]
        it = ws.iter_rows(values_only=True)
        header = next(it)
        idx = {h: i for i, h in enumerate(header)}
        missing = {
            "uf",
            "municipio",
            "evento",
            "data_referencia",
            "abrangencia",
        } - set(idx)
        if missing:
            raise ValueError(
                f"{os.path.basename(path)}: missing columns {sorted(missing)}"
            )
        for row in it:
            yield {
                k: (row[i] if i < len(row) else None) for k, i in idx.items()
            }
    finally:
        wb.close()


# Output schemas. Staging is all-STRING by house convention: the dbt model
# safe_casts every column, so the schema here carries column ORDER, not types.
# Casting goes through arrow, never astype(str), so NULL stays NULL rather than
# becoming the literal "nan" that safe_cast will not undo.
MUNICIPIO_COLUMNS = [
    "ano",
    "mes",
    "sigla_uf",
    "id_municipio",
    "tipo_ocorrencia",
    "abrangencia",
    "situacao_registro",
    "quantidade_ocorrencias",
    "quantidade_vitimas",
    "quantidade_vitimas_feminino",
    "quantidade_vitimas_masculino",
    "quantidade_vitimas_sexo_nao_informado",
]
UF_COLUMNS = [
    "ano",
    "mes",
    "sigla_uf",
    "tipo_ocorrencia",
    "abrangencia",
    "arma",
    "agente",
    "faixa_etaria",
    "quantidade_ocorrencias",
    "quantidade_vitimas",
    "quantidade_vitimas_feminino",
    "quantidade_vitimas_masculino",
    "quantidade_vitimas_sexo_nao_informado",
    "peso_apreendido",
]
DICIONARIO_COLUMNS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]

# Typed intermediates, so `ano` serialises as "2015" and not "2015.0".
_TYPES = {
    "ano": pa.int64(),
    "mes": pa.int64(),
    "quantidade_ocorrencias": pa.int64(),
    "quantidade_vitimas": pa.int64(),
    "quantidade_vitimas_feminino": pa.int64(),
    "quantidade_vitimas_masculino": pa.int64(),
    "quantidade_vitimas_sexo_nao_informado": pa.int64(),
    "peso_apreendido": pa.float64(),
}

SITUACAO_REPORTADO = "reportado"
SITUACAO_NAO_REPORTADO = "nao_reportado"


def _to_string_table(columns: list[str], cols: dict[str, list]) -> pa.Table:
    """Build an all-STRING arrow table, casting through the real type first."""
    arrays = []
    for name in columns:
        arr = pa.array(cols[name], type=_TYPES.get(name, pa.string()))
        arrays.append(arr.cast(pa.string()))
    return pa.Table.from_arrays(arrays, names=columns)


def _write(table: pa.Table, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pq.write_table(table, path, compression="snappy")


def clean_year(
    path: str,
    out_dir: str = OUTPUT_DIR,
    by_name: dict | None = None,
    per_uf: dict | None = None,
) -> dict:
    """Clean one bancovde workbook into partitioned parquet.

    Returns a stats dict, including the observed (key -> raw label) pairs that
    feed the dicionario table.
    """
    if by_name is None or per_uf is None:
        by_name, per_uf = load_municipio_directory()

    year = int(os.path.basename(path).split("-")[1].split(".")[0])

    # municipal rows, keyed so absent cells can be detected afterwards
    mun_rows: dict[str, list[tuple]] = defaultdict(list)  # sigla_uf -> rows
    mun_seen: dict[tuple, set] = defaultdict(
        set
    )  # (tipo, abrang) -> {(id_mun, mes)}
    series_months: dict[tuple, set] = defaultdict(
        set
    )  # (tipo, abrang) -> {mes}
    uf_rows: dict[str, list[tuple]] = defaultdict(list)
    labels: dict[str, set] = defaultdict(set)  # column -> raw labels seen
    tipo_labels: set[tuple[str, str]] = set()
    unmatched: dict[tuple, int] = defaultdict(int)
    n_src = 0

    for r in iter_source_rows(path):
        n_src += 1
        if all(v is None for v in r.values()):
            continue  # trailing blank row
        uf, raw_evento = r["uf"], r["evento"]
        abrang, ref = r["abrangencia"], r["data_referencia"]
        mes = getattr(ref, "month", None)
        # A row missing any key field cannot be placed. Raising beats writing a
        # sigla_uf=None partition, which BigQuery would accept and nobody would
        # notice until the numbers were already in use.
        if (
            not isinstance(uf, str)
            or not isinstance(raw_evento, str)
            or not isinstance(abrang, str)
            or mes is None
        ):
            raise ValueError(
                f"{year}: row {n_src} has an unusable key "
                f"(uf={uf!r}, evento={raw_evento!r}, abrangencia={abrang!r}, "
                f"data_referencia={ref!r})"
            )
        tipo = tipo_ocorrencia_key(raw_evento)
        tipo_labels.add((tipo, raw_evento))
        labels["abrangencia"].add(abrang)

        vit = _as_int(r.get("total_vitima"))
        occ = _as_int(r.get("total"))
        fem = _as_int(r.get("feminino"))
        mas = _as_int(r.get("masculino"))
        nin = _as_int(r.get("nao_informado"))
        peso = _as_float(r.get("total_peso"))

        mid, kind = resolve_municipio(uf, r["municipio"], by_name)
        if kind == "unmatched":
            unmatched[(uf, r["municipio"])] += 1
            continue

        if kind == "municipal":
            series_months[(tipo, abrang)].add(mes)
            mun_seen[(tipo, abrang)].add((mid, mes))
            mun_rows[uf].append(
                (
                    year,
                    mes,
                    uf,
                    mid,
                    tipo,
                    abrang,
                    SITUACAO_REPORTADO,
                    occ,
                    vit,
                    fem,
                    mas,
                    nin,
                )
            )
        else:
            arma, agente, faixa = (
                r.get("arma"),
                r.get("agente"),
                r.get("faixa_etaria"),
            )
            for col, val in (
                ("arma", arma),
                ("agente", agente),
                ("faixa_etaria", faixa),
            ):
                if val is not None:
                    labels[col].add(val)
            uf_rows[uf].append(
                (
                    year,
                    mes,
                    uf,
                    tipo,
                    abrang,
                    arma,
                    agente,
                    faixa,
                    occ,
                    vit,
                    fem,
                    mas,
                    nin,
                    peso,
                )
            )

    if unmatched:
        raise ValueError(
            f"{year}: {len(unmatched)} municipality names did not resolve: "
            f"{sorted(unmatched)[:10]}"
        )

    # ---- flag, do not fill ------------------------------------------------
    # Every municipality-month the source omits from a series it otherwise
    # reports that year gets an explicit row with NULL measures and
    # situacao_registro = 'nao_reportado'. A zero in this table is a reported
    # zero; a NULL is silence. Nothing is imputed.
    n_flagged = 0
    for (tipo, abrang), months in series_months.items():
        months = {m for m in months if m}
        seen = mun_seen[(tipo, abrang)]
        for uf, ids in per_uf.items():
            for mid in ids:
                for mes in months:
                    if (mid, mes) not in seen:
                        mun_rows[uf].append(
                            (
                                year,
                                mes,
                                uf,
                                mid,
                                tipo,
                                abrang,
                                SITUACAO_NAO_REPORTADO,
                                None,
                                None,
                                None,
                                None,
                                None,
                            )
                        )
                        n_flagged += 1

    n_mun = n_uf = 0
    for uf, rows in mun_rows.items():
        cols = {c: [] for c in MUNICIPIO_COLUMNS}
        for row in rows:
            for c, v in zip(MUNICIPIO_COLUMNS, row, strict=True):
                cols[c].append(v)
        _write(
            _to_string_table(MUNICIPIO_COLUMNS, cols),
            os.path.join(
                out_dir,
                TABLE_MUNICIPIO,
                f"ano={year}",
                f"sigla_uf={uf}",
                "data.parquet",
            ),
        )
        n_mun += len(rows)
    for uf, rows in uf_rows.items():
        cols = {c: [] for c in UF_COLUMNS}
        for row in rows:
            for c, v in zip(UF_COLUMNS, row, strict=True):
                cols[c].append(v)
        _write(
            _to_string_table(UF_COLUMNS, cols),
            os.path.join(
                out_dir,
                TABLE_UF,
                f"ano={year}",
                f"sigla_uf={uf}",
                "data.parquet",
            ),
        )
        n_uf += len(rows)

    return {
        "year": year,
        "source_rows": n_src,
        "municipio_rows": n_mun,
        "uf_rows": n_uf,
        "flagged_rows": n_flagged,
        "tipo_labels": sorted(tipo_labels),
        "labels": {k: sorted(v) for k, v in labels.items()},
    }


def build_dicionario(stats: list[dict], out_dir: str = OUTPUT_DIR) -> int:
    """Map every stable key back to the raw source label, per year.

    Derived from what the cleaning run actually observed, not from a hand-kept
    list, so a label the source changes shows up here instead of going missing.
    """
    # (nome_coluna, chave, valor) -> set of years
    seen: dict[tuple[str, str, str], set[int]] = defaultdict(set)
    for s in stats:
        y = s["year"]
        for chave, valor in s["tipo_labels"]:
            seen[("tipo_ocorrencia", chave, valor)].add(y)
        for col, values in s["labels"].items():
            for v in values:
                seen[(col, v, v)].add(y)

    # A key whose label is unchanged across a contiguous run collapses to one
    # row; a label that changed mid-series yields one row per stretch.
    def coverage(years: set[int]) -> str:
        ys = sorted(years)
        spans, start, prev = [], ys[0], ys[0]
        for y in ys[1:]:
            if y != prev + 1:
                spans.append((start, prev))
                start = y
            prev = y
        spans.append((start, prev))
        return ", ".join(f"{a}(1){b}" for a, b in spans)

    rows = []
    for (col, chave, valor), years in sorted(seen.items()):
        cov = coverage(years)
        for tabela in (TABLE_MUNICIPIO, TABLE_UF):
            if tabela == TABLE_MUNICIPIO and col in (
                "arma",
                "agente",
                "faixa_etaria",
            ):
                continue  # those breakdowns exist only on the state-level table
            rows.append((tabela, col, chave, cov, valor))

    cols = {c: [] for c in DICIONARIO_COLUMNS}
    for row in rows:
        for c, v in zip(DICIONARIO_COLUMNS, row, strict=True):
            cols[c].append(v)
    _write(
        _to_string_table(DICIONARIO_COLUMNS, cols),
        os.path.join(out_dir, TABLE_DICIONARIO, "data.parquet"),
    )
    return len(rows)


def clean_all(
    years: list[int] | None = None,
    input_dir: str = INPUT_DIR,
    out_dir: str = OUTPUT_DIR,
) -> list[dict]:
    by_name, per_uf = load_municipio_directory()
    years = years or sorted(
        int(f.split("-")[1].split(".")[0])
        for f in os.listdir(input_dir)
        if f.startswith("bancovde-")
    )
    stats = []
    for y in years:
        path = os.path.join(input_dir, f"bancovde-{y}.xlsx")
        s = clean_year(path, out_dir=out_dir, by_name=by_name, per_uf=per_uf)
        stats.append(s)
        print(
            f"  {y}: source={s['source_rows']:,} municipio={s['municipio_rows']:,} "
            f"(flagged {s['flagged_rows']:,}) uf={s['uf_rows']:,}",
            flush=True,
        )
    n = build_dicionario(stats, out_dir=out_dir)
    print(f"  dicionario: {n:,} rows")
    return stats
