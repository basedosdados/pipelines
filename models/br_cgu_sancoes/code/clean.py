"""Cleaning code for br_cgu_sancoes (CGU sanctions registries).

Reads the raw CGU CSV extracts (ISO-8859-1, ';'-delimited, '"'-quoted, with
embedded newlines inside quoted fields) and writes partitioned Snappy Parquet
that conforms exactly to the architecture tables.

Raw input  : $BR_CGU_SANCOES_INPUT  (default ~/Downloads/br_cgu_sancoes_data/input)
Parquet out: $BR_CGU_SANCOES_OUTPUT (default ~/Downloads/br_cgu_sancoes_data/output)

Output layout (hive-partitioned by snapshot date):
    output/<table>/data_extracao=YYYY-MM-DD/data.parquet
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path
from typing import TypedDict

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
INPUT_DIR = Path(
    os.environ.get(
        "BR_CGU_SANCOES_INPUT",
        os.path.expanduser("~/Downloads/br_cgu_sancoes_data/input"),
    )
)
OUTPUT_DIR = Path(
    os.environ.get(
        "BR_CGU_SANCOES_OUTPUT",
        os.path.expanduser("~/Downloads/br_cgu_sancoes_data/output"),
    )
)

# --------------------------------------------------------------------------- #
# Table specifications
# Each spec: raw filename, snapshot date, and the ordered list of
# (output_column, kind) AFTER the leading data_extracao column.
# kind ∈ {"str", "date", "float"}.  Columns are mapped POSITIONALLY against the
# raw header (which has been verified to match), skipping the data_extracao
# column that we synthesise.
# --------------------------------------------------------------------------- #


class TableSpec(TypedDict):
    """Specification for one raw CGU registry table.

    Attributes:
        file: Raw CSV filename under the input directory.
        snapshot: Extraction date used as the ``data_extracao`` partition.
        cols: Ordered ``(output_column, kind)`` pairs after ``data_extracao``,
            where ``kind`` is one of ``"str"``, ``"date"``, or ``"float"``.
    """

    file: str
    snapshot: date
    cols: list[tuple[str, str]]


CEIS_COLS = [
    ("cadastro", "str"),
    ("codigo_sancao", "str"),
    ("tipo_pessoa", "str"),
    ("cpf_cnpj_sancionado", "str"),
    ("nome_sancionado", "str"),
    ("nome_informado_orgao", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("numero_processo", "str"),
    ("categoria_sancao", "str"),
    ("data_inicio_sancao", "date"),
    ("data_final_sancao", "date"),
    ("data_publicacao", "date"),
    ("publicacao", "str"),
    ("detalhamento_meio_publicacao", "str"),
    ("data_transito_julgado", "date"),
    ("abrangencia_sancao", "str"),
    ("orgao_sancionador", "str"),
    ("sigla_uf_orgao", "str"),
    ("esfera_orgao", "str"),
    ("fundamentacao_legal", "str"),
    ("data_origem_informacao", "date"),
    ("origem_informacao", "str"),
    ("observacoes", "str"),
]

CNEP_COLS = [
    ("cadastro", "str"),
    ("codigo_sancao", "str"),
    ("tipo_pessoa", "str"),
    ("cpf_cnpj_sancionado", "str"),
    ("nome_sancionado", "str"),
    ("nome_informado_orgao", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("numero_processo", "str"),
    ("categoria_sancao", "str"),
    ("valor_multa", "float"),
    ("data_inicio_sancao", "date"),
    ("data_final_sancao", "date"),
    ("data_publicacao", "date"),
    ("publicacao", "str"),
    ("detalhamento_meio_publicacao", "str"),
    ("data_transito_julgado", "date"),
    ("abrangencia_sancao", "str"),
    ("orgao_sancionador", "str"),
    ("sigla_uf_orgao", "str"),
    ("esfera_orgao", "str"),
    ("fundamentacao_legal", "str"),
    ("data_origem_informacao", "date"),
    ("origem_informacao", "str"),
    ("observacoes", "str"),
]

CEPIM_COLS = [
    ("cnpj_entidade", "str"),
    ("nome_entidade", "str"),
    ("numero_convenio", "str"),
    ("orgao_concedente", "str"),
    ("motivo_impedimento", "str"),
]

ACORDOS_COLS = [
    ("id_acordo", "str"),
    ("cnpj_sancionado", "str"),
    ("razao_social_receita", "str"),
    ("nome_fantasia_receita", "str"),
    ("data_inicio_acordo", "date"),
    ("data_fim_acordo", "date"),
    ("situacao_acordo", "str"),
    ("data_informacao", "date"),
    ("numero_processo", "str"),
    ("termos_acordo", "str"),
    ("orgao_sancionador", "str"),
]

EFEITOS_COLS = [
    ("id_acordo", "str"),
    ("efeito", "str"),
    ("complemento", "str"),
]

TABLES: dict[str, TableSpec] = {
    "ceis": {
        "file": "20260813_CEIS.csv",
        "snapshot": date(2026, 8, 13),
        "cols": CEIS_COLS,
    },
    "cnep": {
        "file": "20260813_CNEP.csv",
        "snapshot": date(2026, 8, 13),
        "cols": CNEP_COLS,
    },
    "cepim": {
        "file": "20260812_CEPIM.csv",
        "snapshot": date(2026, 8, 12),
        "cols": CEPIM_COLS,
    },
    "acordos_leniencia": {
        "file": "20260813_Acordos.csv",
        "snapshot": date(2026, 8, 13),
        "cols": ACORDOS_COLS,
    },
    "acordos_leniencia_efeitos": {
        "file": "20260813_Efeitos.csv",
        "snapshot": date(2026, 8, 13),
        "cols": EFEITOS_COLS,
    },
}

# Static dictionary decoding the coded `tipo_pessoa` column (F/J) that appears
# in the ceis and cnep tables. Authored here (not sourced from a CSV) so the
# staging `dicionario` table is reproducible from this script.
DICIONARIO_COLS = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]
DICIONARIO_ROWS: list[tuple[str, str, str, str, str]] = [
    ("ceis", "tipo_pessoa", "F", "", "Pessoa física"),
    ("ceis", "tipo_pessoa", "J", "", "Pessoa jurídica"),
    ("cnep", "tipo_pessoa", "F", "", "Pessoa física"),
    ("cnep", "tipo_pessoa", "J", "", "Pessoa jurídica"),
]


# --------------------------------------------------------------------------- #
# Cleaning helpers
# --------------------------------------------------------------------------- #
def clean_string(s: pd.Series) -> pd.Series:
    """Trim whitespace; empty string -> NA. 'Sem Informação' is preserved."""
    out = s.astype(str).str.strip()
    out = out.replace({"": pd.NA})
    return out


def clean_date(s: pd.Series) -> pd.Series:
    """DD/MM/YYYY -> python date; invalid/empty -> NaT."""
    stripped = s.astype(str).str.strip().replace({"": pd.NA})
    parsed = pd.to_datetime(stripped, format="%d/%m/%Y", errors="coerce")
    return parsed


def clean_float_brl(s: pd.Series) -> pd.Series:
    """'1.234,56' -> 1234.56; empty -> NaN."""
    stripped = s.astype(str).str.strip()
    stripped = stripped.replace({"": pd.NA})
    normalized = stripped.str.replace(".", "", regex=False).str.replace(
        ",", ".", regex=False
    )
    return pd.to_numeric(normalized, errors="coerce")


def build_schema(cols: list[tuple[str, str]]) -> pa.Schema:
    """Explicit pyarrow schema. data_extracao is the partition column and is
    NOT part of the file schema written by pyarrow.dataset (it becomes the
    directory), but we build the full logical schema for the in-memory table."""
    fields = [pa.field("data_extracao", pa.date32())]
    for name, kind in cols:
        if kind == "date":
            fields.append(pa.field(name, pa.date32()))
        elif kind == "float":
            fields.append(pa.field(name, pa.float64()))
        else:
            fields.append(pa.field(name, pa.string()))
    return pa.schema(fields)


# --------------------------------------------------------------------------- #
# Per-table processing
# --------------------------------------------------------------------------- #
def process_table(name: str, spec: TableSpec) -> pd.DataFrame:
    """Read one raw CSV, map columns positionally, clean, and prepend snapshot.

    Args:
        name: Table slug (used for logging and error messages).
        spec: The table's :class:`TableSpec`.

    Returns:
        A frame with ``data_extracao`` first, then the architecture columns in
        order, each cleaned per its ``kind``.
    """
    path = INPUT_DIR / spec["file"]
    cols = spec["cols"]
    snapshot = spec["snapshot"]

    # Read every field as raw string; keep_default_na=False so we handle
    # emptiness ourselves. The C parser handles embedded newlines within
    # quoted fields correctly.
    raw = pd.read_csv(
        path,
        sep=";",
        encoding="latin-1",
        quotechar='"',
        dtype=str,
        keep_default_na=False,
    )

    n_raw_fields = raw.shape[1]
    if n_raw_fields != len(cols):
        raise ValueError(
            f"{name}: raw file has {n_raw_fields} columns, "
            f"architecture expects {len(cols)}"
        )

    # Positional mapping: rename raw columns to architecture names in order.
    rename = {raw.columns[i]: cols[i][0] for i in range(len(cols))}
    df = raw.rename(columns=rename)

    # Apply per-column cleaning.
    for out_name, kind in cols:
        if kind == "date":
            df[out_name] = clean_date(df[out_name])
        elif kind == "float":
            df[out_name] = clean_float_brl(df[out_name])
        else:
            df[out_name] = clean_string(df[out_name])

    # Leading snapshot column.
    df.insert(0, "data_extracao", snapshot)

    # Enforce final column order.
    ordered = ["data_extracao"] + [c[0] for c in cols]
    df = df[ordered]

    print(f"[{name}] parsed rows = {len(df):,} (raw file, C-parser)")
    return df


def to_arrow(df: pd.DataFrame, cols: list[tuple[str, str]]) -> pa.Table:
    """Build a typed Arrow table (date32/float64/string) from a cleaned frame.

    Args:
        df: Cleaned frame from :func:`process_table`.
        cols: The table's ordered ``(output_column, kind)`` pairs.

    Returns:
        An Arrow table whose schema matches the architecture types.
    """
    schema = build_schema(cols)
    arrays = {}
    # data_extracao
    arrays["data_extracao"] = pa.array(df["data_extracao"], type=pa.date32())
    for name, kind in cols:
        if kind == "date":
            # pandas datetime64 -> date32
            arrays[name] = pa.array(df[name].dt.date, type=pa.date32())
        elif kind == "float":
            arrays[name] = pa.array(
                df[name].astype("float64"), type=pa.float64()
            )
        else:
            arrays[name] = pa.array(
                df[name].where(df[name].notna(), None).astype(object),
                type=pa.string(),
            )
    return pa.Table.from_pydict(
        {f.name: arrays[f.name] for f in schema}, schema=schema
    )


def write_partitioned(table: pa.Table, name: str, snapshot: date) -> Path:
    """Write hive-partitioned Snappy Parquet, dropping the partition column.

    Args:
        table: Typed Arrow table including ``data_extracao``.
        name: Table slug (directory name under the output root).
        snapshot: Extraction date used as the hive partition value.

    Returns:
        Path to the written ``data.parquet`` file.
    """
    out_root = OUTPUT_DIR / name / f"data_extracao={snapshot.isoformat()}"
    out_root.mkdir(parents=True, exist_ok=True)
    # Drop the partition column from the file itself (hive style).
    file_table = table.drop(["data_extracao"])
    dest = out_root / "data.parquet"
    pq.write_table(file_table, dest, compression="snappy")
    return dest


def build_dicionario() -> Path:
    """Write the static dicionario parquet (decodes ``tipo_pessoa`` F/J).

    The dicionario is not sourced from a CSV; its rows are defined in
    ``DICIONARIO_ROWS`` so the staging table is reproducible from this script.

    Returns:
        Path to the written (unpartitioned) ``data.parquet`` file.
    """
    schema = pa.schema([(c, pa.string()) for c in DICIONARIO_COLS])
    table = pa.Table.from_pydict(
        {
            c: [row[i] for row in DICIONARIO_ROWS]
            for i, c in enumerate(DICIONARIO_COLS)
        },
        schema=schema,
    )
    out_root = OUTPUT_DIR / "dicionario"
    out_root.mkdir(parents=True, exist_ok=True)
    dest = out_root / "data.parquet"
    pq.write_table(table, dest, compression="snappy")
    print(f"[dicionario] rows = {len(DICIONARIO_ROWS)} -> {dest}")
    return dest


# --------------------------------------------------------------------------- #
# Validation
# --------------------------------------------------------------------------- #
def validate(name: str, spec: TableSpec, dest: Path) -> None:
    """Reload the written parquet and print integrity diagnostics.

    Reads the hive-partitioned directory so ``data_extracao`` is reconstructed,
    then prints row count, column order, dtypes, null fractions, and a sample.

    Args:
        name: Table slug.
        spec: The table's :class:`TableSpec`.
        dest: Path to the written ``data.parquet`` file.
    """
    cols = spec["cols"]
    expected_order = ["data_extracao"] + [c[0] for c in cols]

    # Read the bare file schema (partition column lives in the directory, not
    # the file — hive style).
    file_schema = pq.read_schema(dest)

    # Read the partition root so the hive partition column (data_extracao) is
    # reconstructed, then reorder to the architecture order (partition first).
    reloaded = pq.read_table(dest.parent.parent, partitioning="hive")
    df = reloaded.to_pandas()
    df = df[[c for c in expected_order if c in df.columns]]

    print(f"\n===== VALIDATE {name} =====")
    print(f"path       : {dest}")
    print(f"row count  : {len(df):,}")
    full_order = ["data_extracao", *file_schema.names]
    print(f"columns    : {full_order}")
    order_ok = full_order == expected_order
    print(f"order match: {order_ok}")
    if not order_ok:
        print(f"  EXPECTED : {expected_order}")

    print("dtypes (partition data_extracao: date32 [from directory]):")
    for f in file_schema:
        print(f"  {f.name}: {f.type}")

    print("null fraction per column:")
    for c in expected_order:
        frac = df[c].isna().mean()
        print(f"  {c}: {frac:.4f}")

    if "valor_multa" in df.columns:
        vm = pd.to_numeric(df["valor_multa"], errors="coerce")
        print(
            f"valor_multa -> dtype={df['valor_multa'].dtype}, "
            f"max={vm.max():,.2f}, min={vm.min():,.2f}, "
            f"non-null={vm.notna().sum():,}"
        )

    if "sigla_uf_orgao" in df.columns:
        uf = df["sigla_uf_orgao"].dropna()
        non_uf = sorted(uf[~uf.str.fullmatch(r"[A-Z]{2}")].unique().tolist())
        print(f"sigla_uf_orgao non-2-letter values: {non_uf}")

    print("sample (head 3):")
    with pd.option_context("display.max_columns", None, "display.width", 200):
        print(df.head(3).to_string())


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def main() -> None:
    """Clean every registry in ``TABLES`` to parquet, then validate.

    Also writes the static ``dicionario`` table. Output goes under
    ``OUTPUT_DIR``; nothing is uploaded here (see ``upload.py``).
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    summary = {}
    for name, spec in TABLES.items():
        df = process_table(name, spec)
        table = to_arrow(df, spec["cols"])
        dest = write_partitioned(table, name, spec["snapshot"])
        summary[name] = (len(df), dest)

    dic_dest = build_dicionario()
    summary["dicionario"] = (len(DICIONARIO_ROWS), dic_dest)

    for name, spec in TABLES.items():
        dest = summary[name][1]
        validate(name, spec, dest)

    print("\n===== SUMMARY =====")
    for name, (n, dest) in summary.items():
        print(f"{name:28s} rows={n:>8,}  -> {dest}")
    print(f"\noutput root: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
