#!/usr/bin/env python3
"""Build br_bd_diretorios_ar from the INDEC Censo 2022 geographic code files.

Source: Instituto Nacional de Estadística y Censos de la República Argentina,
Censo Nacional de Población, Hogares y Viviendas 2022, "Códigos geográficos del
INDEC 2022" (CC BY 4.0, updated 22/10/2025). Five XLSX workbooks published under
https://www.indec.gob.ar/ftp/cuadros/geoestadistica/ and catalogued at
https://datos.gob.ar/dataset/codigos-geograficos-del-indec-2022 :

    c2022_codigos_jurisdicciones.xlsx      24 jurisdicciones
    c2022_codigos_departamentos.xlsx      529 departamentos
    c2022_codigos_gobiernos_locales.xlsx 2315 gobiernos locales (+ Definiciones)
    c2022_codigos_aglomerados.xlsx        119 aglomerados de más de una localidad
    c2022_codigos_localidades.xlsx       4023 localidades censales

Each workbook is a single flat sheet whose last two or three rows are a blank
line and a source note; those are dropped by requiring the code column.

The code system is strictly hierarchical and the loader asserts it: departamento
is jurisdicción(2) + 3, gobierno local is jurisdicción(2) + 4, localidad is
departamento(5) + 3. The aglomerado code is its own 4-digit space and does NOT
nest — 14 aglomerados straddle two jurisdicciones and 62 straddle departamentos,
so `aglomerado` carries no geographic key. Likewise 24 gobiernos locales span
more than one departamento, which is why INDEC keys them to the jurisdicción
only and this directory does the same.

Three derivations, each recorded in the architecture `observations`:

1.  `jurisdiccion.nombre_completo` and `jurisdiccion.sigla` are added by Data
    Basis; INDEC publishes only the code and the short name. The article in the
    long name ("Provincia de" vs "Provincia del") follows each provincial
    constitution, so it is tabulated rather than derived.
2.  `aglomerado` is built from the localidades file, not from the aglomerados
    file. INDEC labels only the 119 aglomerados that group more than one
    locality, but every locality carries an aglomerado code, so the label file
    alone would leave 3,587 codes unresolvable. Single-locality aglomerados take
    the name of their locality. `--validate` asserts the equivalence that makes
    this well defined: a locality is of type CA if and only if its aglomerado
    groups more than one locality.
3.  `dicionario` covers `gobierno_local.categoria` and `localidad.tipo`. The
    category labels are transcribed from the "Definiciones" sheet of the
    gobiernos locales workbook; `read_categorias` checks every transcribed code
    against that sheet and fails if one is missing, and every observed code
    against the transcription.

Names carry trailing whitespace in eight cells of the source; everything is
stripped, and `tipo` is also upper-cased ('LS ' and 'Ls' both occur).

Output: all-STRING Snappy parquet, unpartitioned (six small static catalogs), at
$AR_DIR_DATA/output/<table>/data.parquet. The dbt models safe_cast each column
to its architecture type.

Usage:
    uv run --with pandas --with pyarrow --with openpyxl --with requests python \
        models/br_bd_diretorios_ar/code/clean.py [--validate]
"""

import csv
import logging
import os
import re
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

DATA = Path(
    os.environ.get(
        "AR_DIR_DATA", Path.home() / "Downloads" / "br_bd_diretorios_ar_data"
    )
)
RAW = DATA / "input"
OUTPUT = DATA / "output"
ARCH = Path(__file__).resolve().parent / "architecture"

BASE_URL = "https://www.indec.gob.ar/ftp/cuadros/geoestadistica"
WORKBOOKS = {
    "jurisdicciones": "c2022_codigos_jurisdicciones.xlsx",
    "departamentos": "c2022_codigos_departamentos.xlsx",
    "gobiernos_locales": "c2022_codigos_gobiernos_locales.xlsx",
    "aglomerados": "c2022_codigos_aglomerados.xlsx",
    "localidades": "c2022_codigos_localidades.xlsx",
}

UA = {"User-Agent": "Mozilla/5.0 (DataBasis onboarding)"}

EXPECTED = {
    "jurisdiccion": 24,
    "departamento": 529,
    "gobierno_local": 2315,
    "aglomerado": 3706,
    "localidad": 4023,
}

# INDEC publishes the code and the short name only. `articulo` builds
# `nombre_completo` as f"Provincia {articulo} {nombre}"; the Ciudad Autónoma de
# Buenos Aires is not a province and takes no article. `sigla` is the letter of
# ISO 3166-2:AR without the "AR-" prefix.
JURISDICCION_VARIANTS = {
    "02": {"articulo": None, "sigla": "C"},
    "06": {"articulo": "de", "sigla": "B"},
    "10": {"articulo": "de", "sigla": "K"},
    "14": {"articulo": "de", "sigla": "X"},
    "18": {"articulo": "de", "sigla": "W"},
    "22": {"articulo": "del", "sigla": "H"},
    "26": {"articulo": "del", "sigla": "U"},
    "30": {"articulo": "de", "sigla": "E"},
    "34": {"articulo": "de", "sigla": "P"},
    "38": {"articulo": "de", "sigla": "Y"},
    "42": {"articulo": "de", "sigla": "L"},
    "46": {"articulo": "de", "sigla": "F"},
    "50": {"articulo": "de", "sigla": "M"},
    "54": {"articulo": "de", "sigla": "N"},
    "58": {"articulo": "del", "sigla": "Q"},
    "62": {"articulo": "de", "sigla": "R"},
    "66": {"articulo": "de", "sigla": "A"},
    "70": {"articulo": "de", "sigla": "J"},
    "74": {"articulo": "de", "sigla": "D"},
    "78": {"articulo": "de", "sigla": "Z"},
    "82": {"articulo": "de", "sigla": "S"},
    "86": {"articulo": "de", "sigla": "G"},
    "90": {"articulo": "de", "sigla": "T"},
    "94": {"articulo": "de", "sigla": "V"},
}

# Transcribed from the "Definiciones" sheet of c2022_codigos_gobiernos_locales,
# capitalised per the Data Basis style manual. `read_categorias` verifies every
# entry against that sheet, so an INDEC edit surfaces as a failure here.
CATEGORIAS = {
    "MU": "Municipio de única categoría",
    "M1": "Municipio de 1° categoría",
    "M2": "Municipio de 2° categoría",
    "M3": "Municipio de 3° categoría",
    "CO": "Comuna de única categoría",
    "CO1": "Comuna de 1° categoría",
    "CO2": "Comuna de 2° categoría",
    "CR": "Comuna rural de única categoría",
    "CR1": "Comuna rural de 1° categoría",
    "CR2": "Comuna rural de 2° categoría",
    "CR3": "Comuna rural de 3° categoría",
    "CF": "Comisión de fomento de única categoría",
    "CM": "Comisión municipal de única categoría",
    "CMA": "Comisión municipal categoría A",
    "CMB": "Comisión municipal categoría B",
    "JG1": "Junta de gobierno de 1° categoría",
    "JG2": "Junta de gobierno de 2° categoría",
    "JG3": "Junta de gobierno de 3° categoría",
    "JG4": "Junta de gobierno de 4° categoría",
    "JV": "Junta vecinal de única categoría",
}

TIPOS_LOCALIDAD = {
    "LS": "Localidad simple",
    "CA": "Componente de aglomerado",
}

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("ar_dir")


def download(filename):
    """Fetch one workbook into the scratch input dir once, return its path."""
    RAW.mkdir(parents=True, exist_ok=True)
    dest = RAW / filename
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    url = f"{BASE_URL}/{filename}"
    log.info("downloading %s", url)
    r = requests.get(url, headers=UA, timeout=300)
    r.raise_for_status()
    dest.write_bytes(r.content)
    return dest


def read_workbook(key, code_column, code_width, sheet=0):
    """Read one workbook, drop its footer rows and strip every cell.

    The footer is a blank line plus one or two source notes, and INDEC writes
    those notes *into the first column* — which in three of the five workbooks is
    the code column. Requiring a non-null code is therefore not enough; the
    filter is that the code reads as exactly `code_width` digits, which doubles
    as an early width check.
    """
    df = pd.read_excel(download(WORKBOOKS[key]), sheet_name=sheet, dtype=str)
    codes = df[code_column].str.strip()
    keep = codes.str.fullmatch(rf"\d{{{code_width}}}") == True  # noqa: E712
    dropped = df.loc[~keep, code_column].dropna().tolist()
    if any(len(str(v).strip()) == code_width for v in dropped):
        raise ValueError(
            f"{key}: dropped a row that looks like data: {dropped}"
        )
    df = df[keep].copy()
    for col in df.columns:
        df[col] = df[col].str.strip().replace("", None)
    log.info("%s: %s rows", WORKBOOKS[key], f"{len(df):,}")
    return df


def arch_order(table):
    with open(ARCH / f"{table}.csv", newline="", encoding="utf-8") as fh:
        return [row["name"] for row in csv.DictReader(fh)]


def write_parquet(df, table):
    """Write one all-STRING parquet in architecture column order."""
    order = arch_order(table)
    missing = [c for c in order if c not in df.columns]
    if missing:
        raise ValueError(f"{table}: missing columns {missing}")
    if table in EXPECTED and len(df) != EXPECTED[table]:
        raise ValueError(
            f"{table}: expected {EXPECTED[table]} rows, got {len(df)}"
        )
    out = df[order].astype("object").where(pd.notna(df[order]), None)
    schema = pa.schema([pa.field(c, pa.string()) for c in order])
    arrow = pa.Table.from_pandas(out, schema=schema, preserve_index=False)
    tdir = OUTPUT / table
    tdir.mkdir(parents=True, exist_ok=True)
    pq.write_table(arrow, tdir / "data.parquet", compression="snappy")
    log.info("%s: wrote %s rows", table, f"{arrow.num_rows:,}")


def read_categorias():
    """Check CATEGORIAS against the workbook's own Definiciones sheet."""
    sheet = pd.read_excel(
        download(WORKBOOKS["gobiernos_locales"]),
        sheet_name="Definiciones",
        header=None,
        dtype=str,
    )
    text = "\n".join(sheet[0].dropna())
    published = dict(
        re.findall(r"^([A-Z]{1,3}[0-9]?)\s*-\s*(.+?)\s*$", text, re.MULTILINE)
    )
    missing = set(CATEGORIAS) - set(published)
    if missing:
        raise ValueError(
            f"categoria: codes not found in the Definiciones sheet: "
            f"{sorted(missing)}"
        )
    added = set(published) - set(CATEGORIAS)
    if added:
        raise ValueError(
            f"categoria: INDEC now defines codes we do not transcribe: "
            f"{sorted(added)}"
        )
    log.info(
        "categoria: %s codes match the Definiciones sheet", len(published)
    )


def build_jurisdiccion():
    df = read_workbook("jurisdicciones", "Código de jurisdicción", 2).rename(
        columns={
            "Código de jurisdicción": "id_jurisdiccion",
            "Jurisdicción": "nombre",
        }
    )
    df = df.sort_values("id_jurisdiccion")
    unknown = set(df["id_jurisdiccion"]) - set(JURISDICCION_VARIANTS)
    if unknown:
        raise ValueError(
            f"jurisdiccion: no name variants for {sorted(unknown)}"
        )
    variants = df["id_jurisdiccion"].map(JURISDICCION_VARIANTS)
    df["nombre_completo"] = [
        f"Provincia {v['articulo']} {n}" if v["articulo"] else n
        for v, n in zip(variants, df["nombre"], strict=True)
    ]
    siglas = [v["sigla"] for v in variants]
    if len(set(siglas)) != len(siglas):
        raise ValueError("jurisdiccion: duplicate ISO 3166-2 sigla")
    df["sigla"] = siglas
    write_parquet(df, "jurisdiccion")
    return df


def build_departamento():
    df = read_workbook("departamentos", "Código de departamento", 5).rename(
        columns={
            "Código de departamento": "id_departamento",
            "Código de jurisdicción": "id_jurisdiccion",
            "Departamento": "nombre",
        }
    )
    df = df.sort_values("id_departamento")
    write_parquet(df, "departamento")
    return df


def build_gobierno_local():
    df = read_workbook(
        "gobiernos_locales", "Código de gobierno local", 6
    ).rename(
        columns={
            "Código de gobierno local": "id_gobierno_local",
            "Código de jurisdicción": "id_jurisdiccion",
            "Gobierno local": "nombre",
            "Categoría de gobierno local": "categoria",
        }
    )
    df = df.sort_values("id_gobierno_local")
    unknown = set(df["categoria"].dropna()) - set(CATEGORIAS)
    if unknown:
        raise ValueError(
            f"gobierno_local: undefined categoria {sorted(unknown)}"
        )
    write_parquet(df, "gobierno_local")
    return df


def build_localidad_and_aglomerado():
    """Build both tables from the localidades workbook.

    The aglomerados workbook labels only the 119 multi-locality aglomerados, so
    the directory is derived from the localidades file, which carries an
    aglomerado code for every locality.
    """
    loc = read_workbook("localidades", "Código de localidad", 8).rename(
        columns={
            "Código de localidad": "id_localidad",
            "Código de departamento": "id_departamento",
            "Código de jurisdicción": "id_jurisdiccion",
            "Código  de gobierno Local": "id_gobierno_local",
            "Código de aglomerado": "id_aglomerado",
            "Localidad": "nombre",
            "Tipo de localidad": "tipo",
        }
    )
    loc["tipo"] = loc["tipo"].str.upper()
    unknown = set(loc["tipo"].dropna()) - set(TIPOS_LOCALIDAD)
    if unknown:
        raise ValueError(f"localidad: undefined tipo {sorted(unknown)}")

    size = loc.groupby("id_aglomerado").size()
    multiple = set(size[size > 1].index)
    # INDEC's own definition of the type: CA iff the aglomerado groups more than
    # one locality. Asserting it is what licenses naming single-locality
    # aglomerados after their locality.
    ca = set(loc.loc[loc["tipo"] == "CA", "id_aglomerado"])
    ls = set(loc.loc[loc["tipo"] == "LS", "id_aglomerado"])
    if ca != multiple or ls & multiple:
        raise ValueError(
            "aglomerado: tipo CA does not coincide with multi-locality "
            f"aglomerados ({len(ca)} CA vs {len(multiple)} multiple)"
        )

    labels = read_workbook("aglomerados", "Código de aglomerado", 4).rename(
        columns={
            "Código de aglomerado": "id_aglomerado",
            "Aglomerado": "nombre",
        }
    )
    labelled = dict(
        zip(labels["id_aglomerado"], labels["nombre"], strict=True)
    )
    if set(labelled) != multiple:
        raise ValueError(
            "aglomerado: the label file does not match the multi-locality set "
            f"({len(labelled)} labelled vs {len(multiple)} multiple)"
        )
    # The label carried inside the localidades file must agree with the label
    # file; both are INDEC's, and a mismatch means one was refreshed alone.
    inline = loc.loc[
        loc["Aglomerado"].notna(), ["id_aglomerado", "Aglomerado"]
    ].drop_duplicates()
    conflicts = [
        (a, n, labelled[a])
        for a, n in zip(
            inline["id_aglomerado"], inline["Aglomerado"], strict=True
        )
        if labelled.get(a) != n
    ]
    if conflicts:
        raise ValueError(f"aglomerado: label conflicts {conflicts[:5]}")

    single = (
        loc.loc[loc["tipo"] == "LS", ["id_aglomerado", "nombre"]]
        .drop_duplicates()
        .set_index("id_aglomerado")["nombre"]
        .to_dict()
    )
    agl = pd.DataFrame(
        {"id_aglomerado": a, "nombre": labelled.get(a, single.get(a))}
        for a in size.index
    ).sort_values("id_aglomerado")
    if agl["nombre"].isna().any():
        raise ValueError("aglomerado: unnamed aglomerado after derivation")

    write_parquet(agl, "aglomerado")
    write_parquet(loc.sort_values("id_localidad"), "localidad")
    return loc, agl


def build_dicionario():
    rows = [
        {
            "id_tabela": "gobierno_local",
            "nome_coluna": "categoria",
            "chave": code,
            "cobertura_temporal": None,
            "valor": label,
        }
        for code, label in CATEGORIAS.items()
    ] + [
        {
            "id_tabela": "localidad",
            "nome_coluna": "tipo",
            "chave": code,
            "cobertura_temporal": None,
            "valor": label,
        }
        for code, label in TIPOS_LOCALIDAD.items()
    ]
    write_parquet(pd.DataFrame(rows), "dicionario")


def check_hierarchy(jurisdiccion, departamento, gobierno_local, localidad):
    """The codes are hierarchical: assert the prefixes and the keys agree."""
    checks = [
        (
            "departamento",
            departamento,
            "id_departamento",
            2,
            "id_jurisdiccion",
        ),
        (
            "gobierno_local",
            gobierno_local,
            "id_gobierno_local",
            2,
            "id_jurisdiccion",
        ),
        ("localidad", localidad, "id_localidad", 5, "id_departamento"),
        ("localidad", localidad, "id_localidad", 2, "id_jurisdiccion"),
    ]
    for name, df, code, width, parent in checks:
        bad = df[df[code].str[:width] != df[parent]]
        if len(bad):
            raise ValueError(
                f"{name}: {code}[:{width}] does not match {parent}\n"
                f"{bad.head().to_string()}"
            )
    widths = {
        "jurisdiccion.id_jurisdiccion": (jurisdiccion, "id_jurisdiccion", 2),
        "departamento.id_departamento": (departamento, "id_departamento", 5),
        "gobierno_local.id_gobierno_local": (
            gobierno_local,
            "id_gobierno_local",
            6,
        ),
        "localidad.id_localidad": (localidad, "id_localidad", 8),
    }
    for label, (df, col, width) in widths.items():
        lengths = set(df[col].str.len())
        if lengths != {width}:
            raise ValueError(f"{label}: expected width {width}, saw {lengths}")
        if df[col].duplicated().any():
            raise ValueError(f"{label}: duplicate keys")
    orphans = {
        "departamento -> jurisdiccion": set(departamento["id_jurisdiccion"])
        - set(jurisdiccion["id_jurisdiccion"]),
        "gobierno_local -> jurisdiccion": set(
            gobierno_local["id_jurisdiccion"]
        )
        - set(jurisdiccion["id_jurisdiccion"]),
        "localidad -> departamento": set(localidad["id_departamento"])
        - set(departamento["id_departamento"]),
        "localidad -> gobierno_local": set(localidad["id_gobierno_local"])
        - set(gobierno_local["id_gobierno_local"]),
    }
    for label, missing in orphans.items():
        if missing:
            raise ValueError(
                f"{label}: unresolved keys {sorted(missing)[:10]}"
            )
    log.info("hierarchy: code prefixes, widths and foreign keys consistent")


def spanning(df, group, over):
    """How many `group` values appear against more than one distinct `over`."""
    pairs = df[[group, over]].drop_duplicates()
    return int((pairs.groupby(group).size() > 1).sum())


def report(localidad, aglomerado, gobierno_local):
    """Log the counts the architecture `observations` quote."""
    spans_jur = spanning(localidad, "id_aglomerado", "id_jurisdiccion")
    spans_dep = spanning(localidad, "id_aglomerado", "id_departamento")
    gl_spans_dep = spanning(localidad, "id_gobierno_local", "id_departamento")
    multiple = (localidad.groupby("id_aglomerado").size() > 1).sum()
    log.info(
        "aglomerado: %s of %s group more than one localidad; %s span more than "
        "one jurisdiccion and %s more than one departamento",
        multiple,
        len(aglomerado),
        spans_jur,
        spans_dep,
    )
    log.info(
        "gobierno_local: %s span more than one departamento; %s have no "
        "categoria (%s)",
        gl_spans_dep,
        int(gobierno_local["categoria"].isna().sum()),
        ", ".join(
            f"{n} {v}"
            for v, n in gobierno_local.loc[
                gobierno_local["categoria"].isna(), "nombre"
            ]
            .value_counts()
            .items()
        ),
    )
    log.info(
        "localidad: %s",
        ", ".join(
            f"{n} {v}" for v, n in localidad["tipo"].value_counts().items()
        ),
    )
    dup = localidad.duplicated(["id_departamento", "nombre"]).sum()
    log.info(
        "localidad: %s share nombre with another in the same departamento", dup
    )


def main():
    read_categorias()
    jurisdiccion = build_jurisdiccion()
    departamento = build_departamento()
    gobierno_local = build_gobierno_local()
    localidad, aglomerado = build_localidad_and_aglomerado()
    build_dicionario()
    check_hierarchy(jurisdiccion, departamento, gobierno_local, localidad)
    if "--validate" in sys.argv[1:]:
        report(localidad, aglomerado, gobierno_local)
    print("done")


if __name__ == "__main__":
    main()
