"""Clean the SC and RS state-contract sources into all-STRING staging parquet.

Two staging tables come out: `sc_contrato` and `rs_contrato`. ES already has
`es_contrato` staged from its own onboarding, so it is not touched here.

Sources (both CKAN, neither needs a Brazilian IP):
  SC  dados.sc.gov.br package "contratos" -> contratos.xlsx (51 cols, ~105k rows).
      The XLSX is used, NOT the CSV/JSON: SC's CKAN CSVs carry the same unquoted
      free-text-with-semicolons defect that made its empenho CSV unparseable, and
      there is no portal `visao=contrato` (it returns []). The XLSX is structured.
  RS  dados.rs.gov.br package "contratos-do-estado" -> four typed zips
      (Fornecimento de Bens, Locações, Obras e Serviços de Engenharia, Serviços de
      Terceiros), each one cp1252 ';'-CSV with its own column set. They are unioned
      to the superset of their columns plus a `tipo_contrato` marker.

Everything is written as STRING (house convention; the dbt model safe_casts). Dates
from the XLSX are emitted as 'YYYY-MM-DD HH:MM:SS' and whole numbers without a '.0'
tail, via arrow, so safe_cast recovers them and NULL never becomes the string 'nan'.
"""

from __future__ import annotations

import csv
import io
import os
import zipfile
from datetime import date, datetime
from pathlib import Path

import openpyxl  # pyrefly: ignore [untyped-import]
import pyarrow as pa
import pyarrow.parquet as pq

IN = (
    Path(
        os.environ.get(
            "EXEC_ESTADUAL_DATA_DIR",
            Path.home() / "Downloads" / "br_state_budget_data",
        )
    )
    / "input"
)
OUT = (
    Path(
        os.environ.get(
            "EXEC_ESTADUAL_DATA_DIR",
            Path.home() / "Downloads" / "br_state_budget_data",
        )
    )
    / "output"
)

RS_FILES = {
    "contratos-de-fornecimento-de-bens": "Fornecimento de Bens",
    "contratos-de-locacoes": "Locação",
    "contratos-de-obras": "Obras e Serviços de Engenharia",
    "contratos-de-servicos": "Serviços de Terceiros",
}


def _cell(v) -> str | None:
    """XLSX cell -> stable string. Whole floats lose the '.0'; dates to ISO."""
    if v is None:
        return None
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    s = str(v).strip()
    if s == "" or s.lower() == "none":
        return None
    return s


def clean_sc() -> int:
    src = IN / "sc_contrato" / "contratos.xlsx"
    wb = openpyxl.load_workbook(src, read_only=True)
    ws = wb.active
    if ws is None:
        raise RuntimeError(f"{src}: no active worksheet")
    it = ws.iter_rows(values_only=True)
    header = [str(h).strip() for h in next(it)]
    cols: dict[str, list] = {h: [] for h in header}
    n = 0
    for row in it:
        if row is None or all(c is None for c in row):
            continue
        for i, h in enumerate(header):
            cols[h].append(_cell(row[i]) if i < len(row) else None)
        n += 1
    wb.close()
    table = pa.table({h: pa.array(cols[h], type=pa.string()) for h in header})
    dest = OUT / "sc_contrato"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dest / "data.parquet", compression="snappy")
    print(f"  sc_contrato: {n:,} rows, {len(header)} cols")
    return n


def _read_rs_csv(raw: bytes) -> tuple[list[str], list[list[str]]]:
    txt = raw.decode("cp1252")
    rdr = csv.reader(
        io.StringIO(txt),
        delimiter=";",
        quotechar='"',
        doublequote=True,
        escapechar="\\",
    )
    rows = list(rdr)
    # Lowercase the header: the four files disagree on case (Cod_Orgao vs cod_orgao,
    # UO vs uo), and BigQuery treats column names case-insensitively, so the variants
    # would collide and the load would silently drop columns. Lowercasing merges them.
    header = [h.strip().lower() for h in rows[0]]
    body = rows[1:]
    return header, body


def clean_rs() -> int:
    # superset of columns across the four files, in first-seen order, + tipo_contrato
    superset: list[str] = []
    per_file: list[tuple[str, list[str], list[list[str]]]] = []
    ragged = 0
    for stem, tipo in RS_FILES.items():
        with zipfile.ZipFile(IN / "rs_contrato" / f"{stem}.zip") as zf:
            name = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
            header, body = _read_rs_csv(zf.read(name))
        for h in header:
            if h and h not in superset:
                superset.append(h)
        per_file.append((tipo, header, body))
    superset.append("tipo_contrato")

    data: dict[str, list] = {h: [] for h in superset}
    total = 0
    for tipo, header, body in per_file:
        idx = {h: i for i, h in enumerate(header)}
        for r in body:
            if not any(x.strip() for x in r):
                continue
            # a row wider than the header is a ragged free-text field; a row shorter
            # is padded. Either way place by header position and count the ragged.
            if len(r) != len(header):
                ragged += 1
            for h in superset:
                if h == "tipo_contrato":
                    data[h].append(tipo)
                elif h in idx and idx[h] < len(r):
                    v = r[idx[h]].strip()
                    data[h].append(v or None)
                else:
                    data[h].append(None)
            total += 1
    table = pa.table(
        {h: pa.array(data[h], type=pa.string()) for h in superset}
    )
    dest = OUT / "rs_contrato"
    dest.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, dest / "data.parquet", compression="snappy")
    print(
        f"  rs_contrato: {total:,} rows, {len(superset)} cols, {ragged} ragged row(s)"
    )
    return total


if __name__ == "__main__":
    print("cleaning SC + RS contracts")
    clean_sc()
    clean_rs()
