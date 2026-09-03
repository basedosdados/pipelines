"""Build the dicionario table for cl_chilecompra_mercado_publico.

ChileCompra publishes no machine-readable codebook, but most coded columns ship
alongside their own label column in the same row -- `codigoEstado` next to `Estado`,
`CodigoMoneda` next to `Moneda Adquisicion`. This derives the code-to-label mapping
from the data itself, which is both complete and self-consistent, and supplements it
with the handful of definitions the publisher documents only on its Definiciones page.

It also reports which dictionary-flagged columns end up with no entries, so that
`covered_by_dictionary` can be set to "no" for them: the flag is a promise that the
labels exist in this table, and a column with no derivable labels does not keep it.

    uv run python models/cl_chilecompra_mercado_publico/code/build_dicionario.py
"""

from __future__ import annotations

import argparse
import csv
import glob
import os
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pipelines.datasets.cl_chilecompra_mercado_publico import utils

DEFAULT_ROOT = Path(
    os.environ.get(
        "CHILECOMPRA_DATA_DIR",
        Path.home() / "Downloads" / "cl_chilecompra_mercado_publico_data",
    )
)

# (table, code column, label column) triples where the source carries both.
DERIVED_PAIRS = [
    ("orden_compra_item", "codigo_estado", "estado"),
    ("orden_compra_item", "codigo_estado_proveedor", "estado_proveedor"),
    ("orden_compra_item", "codigo_tipo", "descripcion_tipo"),
    ("orden_compra_item", "sigla_tipo", "descripcion_tipo"),
    ("orden_compra_item", "sigla_tipo_abreviada", "descripcion_tipo"),
    ("orden_compra_item", "codigo_forma_pago", "forma_pago"),
    ("licitacion_item", "codigo_estado", "estado"),
    ("licitacion_item", "codigo_estado_licitacion", "estado"),
    ("licitacion_item", "codigo_tipo", "tipo_adquisicion"),
    ("licitacion_item", "sigla_tipo", "tipo_adquisicion"),
    ("licitacion_item", "codigo_moneda", "moneda"),
]

# Currency codes appear bare in órdenes de compra but with their names in licitaciones.
# The mapping is the same vocabulary, so it is carried across rather than left blank.
CROSS_TABLE_CURRENCY = [
    ("orden_compra_item", "moneda"),
    ("orden_compra_item", "moneda_item"),
]

# Documented by ChileCompra on datos-abiertos.chilecompra.cl/datos-abiertos/definiciones
# and nowhere in the data. Several licitación states collapse to one label there.
DOCUMENTED = {
    ("orden_compra_item", "codigo_estado"): {
        "4": "Enviada a proveedor",
        "5": "En proceso",
        "6": "Aceptada",
        "7": "Solicitud de cancelación",
        "12": "Recepción conforme",
    },
    ("licitacion_item", "codigo_estado"): {
        "5": "Publicada",
        "6": "Cerrada",
        "7": "Desierta",
        "8": "Adjudicada",
        "9": "Adjudicada",
        "10": "Adjudicada",
        "11": "Cerrada",
        "12": "Cerrada",
        "13": "Cerrada",
        "14": "Cerrada",
        "15": "Revocada",
        "16": "Suspendida",
    },
}


def _partition_files(root: Path, table: str) -> list[str]:
    return sorted(
        glob.glob(f"{root}/output/{table}/**/*.parquet", recursive=True)
    )


def _collect(root: Path, table: str, code: str, label: str) -> pd.DataFrame:
    """Distinct (code, label) pairs across every partition of one table."""
    seen: dict[str, str] = {}
    for path in _partition_files(root, table):
        frame = pq.read_table(path, columns=[code, label]).to_pandas()
        frame = frame.dropna()
        for key, value in zip(frame[code], frame[label], strict=False):
            key, value = str(key).strip(), str(value).strip()
            if key and value:
                seen.setdefault(key, value)
    return pd.DataFrame(
        {"chave": list(seen), "valor": [seen[k] for k in seen]}
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()

    rows = []
    covered: set[tuple[str, str]] = set()

    for table, code, label in DERIVED_PAIRS:
        if not _partition_files(args.root, table):
            print(f"skip {table}.{code}: no parquet")
            continue
        pairs = _collect(args.root, table, code, label)
        documented = DOCUMENTED.get((table, code), {})
        for key, value in documented.items():
            if key not in set(pairs["chave"]):
                pairs.loc[len(pairs)] = {"chave": key, "valor": value}
        for _, row in pairs.iterrows():
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": code,
                    "chave": row["chave"],
                    "cobertura_temporal": "",
                    "valor": row["valor"],
                }
            )
        covered.add((table, code))
        print(f"{table}.{code}: {len(pairs)} keys")

    # Currency names live only in licitaciones; reuse them for the OC currency columns.
    currency = {
        r["chave"]: r["valor"]
        for r in rows
        if r["id_tabela"] == "licitacion_item"
        and r["nome_coluna"] == "codigo_moneda"
    }
    for table, column in CROSS_TABLE_CURRENCY:
        if not currency:
            break
        for key, value in currency.items():
            rows.append(
                {
                    "id_tabela": table,
                    "nome_coluna": column,
                    "chave": key,
                    "cobertura_temporal": "",
                    "valor": value,
                }
            )
        covered.add((table, column))
        print(f"{table}.{column}: {len(currency)} keys (from licitaciones)")

    out_dir = args.root / "output" / "dicionario"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "dicionario.csv"
    with open(out, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "id_tabela",
                "nome_coluna",
                "chave",
                "cobertura_temporal",
                "valor",
            ],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f"\nwrote {out} with {len(rows)} entries")

    # Any column still flagged but absent from the dictionary must lose the flag.
    print(
        "\nflagged columns with NO dictionary entries "
        "(set covered_by_dictionary=no for these):"
    )
    for table in ("orden_compra_item", "licitacion_item", "licitacion_oferta"):
        arch = utils.read_architecture(table)
        flagged = [
            r.name
            for r in arch.itertuples()
            if r.covered_by_dictionary == "yes"
        ]
        missing = [c for c in flagged if (table, c) not in covered]
        if missing:
            print(f"  {table}: {missing}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
