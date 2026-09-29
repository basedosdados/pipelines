"""Clean IBAMA enforcement records (autos de infração and termos de embargo).

Reads the raw CSVs published at dadosabertos.ibama.gov.br and writes hive-partitioned,
all-STRING parquet ready for the Data Basis staging bucket.

Staging is all-STRING by house convention: the dbt model safe_casts each column to its
architecture type. Values are normalised to the *textual* form safe_cast parses
(dot decimals, ISO dates), never to typed arrow columns.
"""

from __future__ import annotations

import csv
import glob
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import date

import pyarrow as pa
import pyarrow.parquet as pq

csv.field_size_limit(10**9)

DATA_DIR = os.environ.get(
    "IBAMA_DATA_DIR",
    os.path.expanduser("~/Downloads/br_ibama_fiscalizacao_data"),
)
IN = os.path.join(DATA_DIR, "input")
OUT = os.path.join(DATA_DIR, "output")

ANO_MIN, ANO_MAX = 1970, date.today().year + 1
PARTITION_COL = "ano"

AUTO_COLS = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "id_auto",
    "numero_auto_infracao",
    "serie_auto_infracao",
    "data_auto",
    "tipo_auto",
    "tipo_infracao",
    "gravidade",
    "valor_multa",
    "moeda",
    "situacao_debito",
    "situacao_auto",
    "indicador_cancelado",
    "tipo_pessoa_infrator",
    "cpf_cnpj_infrator",
    "nome_infrator",
    "latitude",
    "longitude",
    "data_ultima_alteracao",
    "data_extracao",
]

EMBARGO_COLS = [
    "ano",
    "sigla_uf",
    "id_municipio",
    "id_embargo",
    "numero_tad",
    "serie_tad",
    "data_embargo",
    "id_auto",
    "numero_auto_infracao",
    "area_ha",
    "tipo_area",
    "nome_imovel",
    "tipo_pessoa_embargado",
    "cpf_cnpj_embargado",
    "nome_embargado",
    "latitude",
    "longitude",
    "indicador_desembargado",
    "data_desembargo",
    "data_ultima_alteracao",
    "data_extracao",
]


# --------------------------------------------------------------------------- helpers
def s(v: str | None) -> str | None:
    """Trim; empty becomes NULL. Never returns the string 'nan'."""
    if v is None:
        return None
    v = v.strip()
    return v or None


def num(v: str | None) -> str | None:
    """Normalise a Brazilian decimal ('-42,72785') to a dot decimal string."""
    v = s(v)
    if v is None:
        return None
    v = v.replace(" ", "")
    # thousands separator only appears with a comma decimal; IBAMA uses plain comma
    if "," in v and "." in v:
        v = v.replace(".", "").replace(",", ".")
    else:
        v = v.replace(",", ".")
    try:
        float(v)
    except ValueError:
        return None
    return v


def to_date(v: str | None) -> str | None:
    """Accept 'YYYY-MM-DD[ HH:MM:SS]' and 'DD/MM/YYYY[ HH:MM]'; return ISO date."""
    v = s(v)
    if v is None:
        return None
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", v)
    if m:
        y, mo, d = m.groups()
    else:
        m = re.match(r"^(\d{2})/(\d{2})/(\d{4})", v)
        if not m:
            return None
        d, mo, y = m.groups()
    if not (1 <= int(mo) <= 12 and 1 <= int(d) <= 31):
        return None
    return f"{y}-{mo}-{d}"


def year_of(iso: str | None) -> str | None:
    """Partition year, only when the date is calendrically plausible."""
    if not iso:
        return None
    y = int(iso[:4])
    return str(y) if ANO_MIN <= y <= ANO_MAX else None


# Placeholders the source uses where the municipality is unknown. They are
# 7 digits but identify nothing, and left in place they join silently to no row
# of the municipality directory.
MUNICIPIO_SENTINELA = {"9999999", "1400000"}


def municipio(v: str | None) -> str | None:
    """IBGE municipality codes are exactly 7 digits; anything else is unusable."""
    v = s(v)
    if not v or not re.fullmatch(r"\d{7}", v) or v in MUNICIPIO_SENTINELA:
        return None
    return v


def intlike(v: str | None) -> str | None:
    """Normalise an integer written as a decimal ('2126006.0000000000' -> '2126006').

    The embargo file prints its reference to the auto de infração with a decimal
    tail while auto_infracao prints the same id as a bare integer, so the two
    do not join until the tail is stripped.
    """
    v = s(v)
    if v is None:
        return None
    m = re.fullmatch(r"(-?\d+)(?:\.0*)?", v)
    return m.group(1) if m else v


def only_digits(v: str | None) -> str | None:
    v = s(v)
    if v is None:
        return None
    d = re.sub(r"\D", "", v)
    return d or None


def pessoa_from_doc(doc: str | None) -> str | None:
    """PF/PJ inferred from document length: 11 digits is a CPF, 14 a CNPJ."""
    if not doc:
        return None
    return {11: "PF", 14: "PJ"}.get(len(doc))


def write_partitions(rows_by_year, cols, table_slug, stats):
    """One parquet per ano=<year>; every column STRING, stable order.

    ``ano`` is written as the hive directory key only, never as a column inside
    the file. ``basedosdados`` builds the staging external table with
    ``HivePartitioningOptions(mode="STRINGS")``, so BigQuery derives ``ano`` from
    the path; carrying it in the file as well makes it a duplicate column. The
    dbt model ``safe_cast``s the path-derived string to INT64.
    """
    file_cols = [c for c in cols if c != PARTITION_COL]
    keep = [i for i, c in enumerate(cols) if c != PARTITION_COL]
    schema = pa.schema([(c, pa.string()) for c in file_cols])
    for year, rows in sorted(rows_by_year.items()):
        d = os.path.join(OUT, table_slug, f"{PARTITION_COL}={year}")
        os.makedirs(d, exist_ok=True)
        arrays = [
            pa.array([r[i] for r in rows], type=pa.string()) for i in keep
        ]
        pq.write_table(
            pa.Table.from_arrays(arrays, schema=schema),
            os.path.join(d, "data.parquet"),
            compression="snappy",
        )
        stats[table_slug][year] = len(rows)


# ------------------------------------------------------------------- multas (SICAFI)
def load_multas() -> dict[tuple[str, str], tuple[str | None, str | None]]:
    """situacao_debito and moeda, keyed by (numero, serie) of the auto de infração.

    The SICAFI series prints the key as '<numero> - <serie>'.
    """
    out: dict[tuple[str, str], tuple[str | None, str | None]] = {}
    for p in sorted(glob.glob(os.path.join(IN, "mt", "*.csv"))):
        with open(p, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                raw = (row.get("Nº AI") or "").strip()
                numero, _, serie = raw.partition(" -")
                out[(numero.strip(), serie.strip())] = (
                    s(row.get("Situação Débito")),
                    s(row.get("Moeda")),
                )
    return out


# ------------------------------------------------------------------ auto de infração
def clean_auto(multas, stats, diag):
    rows_by_year = defaultdict(list)
    latest: dict[str, tuple[str, list]] = {}
    superseded = 0

    for path in sorted(
        glob.glob(os.path.join(IN, "ai", "auto_infracao_*.csv"))
    ):
        # Ibama names each yearly file auto_infracao_<YYYY>.csv. That year is
        # the fallback partition key when DAT_HORA_AUTO_INFRACAO is missing.
        match = re.search(r"(\d{4})\.csv$", path)
        if match is None:
            sys.exit(f"cannot read the year from source file name: {path}")
        file_year = match.group(1)
        with open(path, newline="", encoding="utf-8-sig") as f:
            for r in csv.DictReader(f, delimiter=";"):
                diag["auto_read"] += 1
                data_auto = to_date(r.get("DAT_HORA_AUTO_INFRACAO"))
                ano = year_of(data_auto) or (
                    file_year if ANO_MIN <= int(file_year) <= ANO_MAX else None
                )
                if ano is None:
                    diag["auto_no_year"] += 1
                    continue

                numero = s(r.get("NUM_AUTO_INFRACAO"))
                serie = s(r.get("SER_AUTO_INFRACAO"))
                sit_deb, moeda = multas.get(
                    (numero or "", serie or ""), (None, None)
                )

                out = [
                    ano,
                    s(r.get("UF")),
                    municipio(r.get("COD_MUNICIPIO")),
                    intlike(r.get("SEQ_AUTO_INFRACAO")),
                    numero,
                    serie,
                    data_auto,
                    s(r.get("TIPO_AUTO")),
                    s(r.get("TIPO_INFRACAO")),
                    s(r.get("GRAVIDADE_INFRACAO")),
                    num(r.get("VAL_AUTO_INFRACAO")),
                    moeda,
                    sit_deb,
                    s(r.get("DES_STATUS_FORMULARIO")),
                    s(r.get("SIT_CANCELADO")),
                    s(r.get("TP_PESSOA_INFRATOR")),
                    only_digits(r.get("CPF_CNPJ_INFRATOR")),
                    s(r.get("NOME_INFRATOR")),
                    num(r.get("NUM_LATITUDE_AUTO")),
                    num(r.get("NUM_LONGITUDE_AUTO")),
                    to_date(r.get("DT_ULT_ALTERACAO")),
                    to_date(r.get("ULTIMA_ATUALIZACAO_RELATORIO")),
                ]
                if sit_deb:
                    diag["auto_multas_hit"] += 1

                # Deduplicate on id_auto, keeping the most recently amended version.
                # Within one extraction this is a no-op; across vintages the source
                # republishes amended records under the same id.
                key = out[3]
                if key is None:
                    rows_by_year[ano].append(out)  # AIe Mob rows carry no SEQ
                    diag["auto_no_id"] += 1
                    continue
                prev = latest.get(key)
                if prev is None or (out[20] or "") >= (prev[1][20] or ""):
                    if prev is not None:
                        superseded += 1
                    latest[key] = (ano, out)
                else:
                    superseded += 1

    for ano, out in latest.values():
        rows_by_year[ano].append(out)
    diag["auto_superseded"] = superseded
    write_partitions(rows_by_year, AUTO_COLS, "auto_infracao", stats)


# ------------------------------------------------------------------ termo de embargo
def clean_embargo(stats, diag):
    rows_by_year = defaultdict(list)
    latest: dict[str, tuple[str, list]] = {}
    superseded = 0

    with open(
        os.path.join(IN, "termo_de_embargo.csv"),
        newline="",
        encoding="utf-8-sig",
    ) as f:
        for r in csv.DictReader(f, delimiter=";"):
            diag["emb_read"] += 1
            data_embargo = to_date(r.get("DAT_EMBARGO"))
            ano = year_of(data_embargo)
            if ano is None:
                # 5 records carry impossible years (1667, 2063, 2080, 2090, 2925).
                # Keep the raw date; park the row in the extraction year's partition
                # so nothing is dropped and no junk partition is created.
                ano = year_of(to_date(r.get("ULTIMA_ATUALIZACAO_RELATORIO")))
                diag["emb_bad_year"] += 1
                if ano is None:
                    diag["emb_dropped"] += 1
                    continue

            doc = only_digits(r.get("CPF_CNPJ_EMBARGADO"))
            out = [
                ano,
                s(r.get("UF")),
                municipio(r.get("COD_MUNICIPIO")),
                intlike(r.get("SEQ_TAD")),
                s(r.get("NUM_TAD")),
                s(r.get("SER_TAD")),
                data_embargo,
                intlike(r.get("SEQ_AUTO_INFRACAO")),
                s(r.get("NUM_AUTO_INFRACAO")),
                num(r.get("QTD_AREA_EMBARGADA")),
                s(r.get("TIPO_AREA")),
                s(r.get("NOME_IMOVEL")),
                pessoa_from_doc(doc),
                doc,
                s(r.get("NOME_EMBARGADO")),
                num(r.get("NUM_LATITUDE_TAD")),
                num(r.get("NUM_LONGITUDE_TAD")),
                s(r.get("SIT_DESEMBARGO")),
                to_date(r.get("DAT_DESEMBARGO")),
                to_date(r.get("DAT_ULT_ALTERACAO")),
                to_date(r.get("ULTIMA_ATUALIZACAO_RELATORIO")),
            ]

            key = out[3]
            if key is None:
                rows_by_year[ano].append(out)
                diag["emb_no_id"] += 1
                continue
            prev = latest.get(key)
            if prev is None or (out[19] or "") >= (prev[1][19] or ""):
                if prev is not None:
                    superseded += 1
                latest[key] = (ano, out)
            else:
                superseded += 1

    for ano, out in latest.values():
        rows_by_year[ano].append(out)
    diag["emb_superseded"] = superseded
    write_partitions(rows_by_year, EMBARGO_COLS, "area_embargada", stats)


def main():
    stats = defaultdict(dict)
    diag = Counter()
    print("loading SICAFI multas series ...", flush=True)
    multas = load_multas()
    print(f"  {len(multas):,} keys", flush=True)

    print("cleaning auto_infracao ...", flush=True)
    clean_auto(multas, stats, diag)
    print("cleaning area_embargada ...", flush=True)
    clean_embargo(stats, diag)

    for t, years in stats.items():
        print(
            f"\n{t}: {sum(years.values()):,} rows in {len(years)} partitions "
            f"({min(years)}-{max(years)})"
        )
    print("\ndiagnostics:")
    for k, v in sorted(diag.items()):
        print(f"  {k:<22} {v:,}")


if __name__ == "__main__":
    sys.exit(main())
