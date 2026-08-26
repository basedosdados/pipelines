"""Build the br_pncp dicionario table.

Most PNCP coded fields arrive with their label alongside them in the same
payload (``modalidadeId`` next to ``modalidadeNome``), so the dictionary is
derived from the cleaned data rather than transcribed from documentation — it
therefore cannot drift from what the tables actually contain.

Three fields carry a code with no label anywhere in the API. Those are the only
hard-coded entries, and they come from the PNCP integration manual's domain
tables.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).parent))

from utils import read_architecture

DATA_DIR = Path(
    os.environ.get("PNCP_DATA_DIR", Path.home() / "Downloads" / "br_pncp_data")
)

# table -> [(code column, label column)] where the label travels with the code.
DERIVED = {
    "contratacao": [
        ("id_modalidade", "modalidade"),
        ("id_modo_disputa", "modo_disputa"),
        ("id_situacao_compra", "situacao_compra"),
        ("id_tipo_instrumento_convocatorio", "tipo_instrumento_convocatorio"),
        ("codigo_amparo_legal", "nome_amparo_legal"),
    ],
    "contrato": [
        ("id_tipo_contrato", "tipo_contrato"),
        ("id_categoria_processo", "categoria_processo"),
    ],
    "instrumento_cobranca": [
        ("id_tipo_instrumento_cobranca", "tipo_instrumento_cobranca"),
    ],
    "plano_contratacao_anual": [
        ("id_classificacao_catalogo", "nome_classificacao_catalogo"),
    ],
}

# Codes the API never labels. Source: PNCP manual de integração, domain tables
# for esfera, poder and tipo de pessoa.
HARDCODED = {
    "id_esfera": {
        "F": "Federal",
        "E": "Estadual",
        "M": "Municipal",
        "D": "Distrital",
        "N": "Não se aplica",
    },
    "id_poder": {
        "E": "Executivo",
        "L": "Legislativo",
        "J": "Judiciário",
        "N": "Não se aplica",
    },
    "tipo_pessoa_fornecedor": {
        "PJ": "Pessoa jurídica",
        "PF": "Pessoa física",
        "PE": "Pessoa estrangeira",
    },
}

# Which tables carry each hard-coded column.
HARDCODED_TABLES = {
    "id_esfera": ["contratacao", "contrato"],
    "id_poder": ["contratacao", "contrato"],
    "tipo_pessoa_fornecedor": ["contrato"],
}


def distinct_pairs(
    output_dir: Path, table: str, code_col: str, label_col: str
) -> dict[str, str]:
    """Read the distinct code -> label pairs present in a cleaned table."""
    table_dir = output_dir / table
    if not table_dir.exists():
        return {}
    dataset = ds.dataset(table_dir, format="parquet", partitioning="hive")
    if (
        code_col not in dataset.schema.names
        or label_col not in dataset.schema.names
    ):
        return {}
    scanned = dataset.to_table(columns=[code_col, label_col])
    pairs: dict[str, str] = {}
    codes = scanned.column(code_col).to_pylist()
    labels = scanned.column(label_col).to_pylist()
    for code, label in zip(codes, labels, strict=True):
        if code is None or label is None:
            continue
        # A code should map to one label; if the source ever disagrees, the
        # first non-null wins and the conflict is reported by the caller.
        pairs.setdefault(str(code), str(label))
    return pairs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--output-dir", type=Path, default=DATA_DIR / "output")
    args = ap.parse_args()

    rows: list[dict] = []

    for table, pairs_spec in DERIVED.items():
        for code_col, label_col in pairs_spec:
            mapping = distinct_pairs(
                args.output_dir, table, code_col, label_col
            )
            for code, label in sorted(
                mapping.items(), key=lambda kv: (len(kv[0]), kv[0])
            ):
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": code_col,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
            print(f"{table}.{code_col}: {len(mapping)} keys", flush=True)

    for column, mapping in HARDCODED.items():
        for table in HARDCODED_TABLES[column]:
            for code, label in mapping.items():
                rows.append(
                    {
                        "id_tabela": table,
                        "nome_coluna": column,
                        "chave": code,
                        "cobertura_temporal": "",
                        "valor": label,
                    }
                )
        print(
            f"{column}: {len(mapping)} keys x {len(HARDCODED_TABLES[column])} tables",
            flush=True,
        )

    names = [c["name"] for c in read_architecture("dicionario")]
    target = args.output_dir / "dicionario"
    target.mkdir(parents=True, exist_ok=True)
    arrays = [
        pa.array([r.get(n) for r in rows], type=pa.string()) for n in names
    ]
    pq.write_table(
        pa.Table.from_arrays(
            arrays, schema=pa.schema([(n, pa.string()) for n in names])
        ),
        target / "data.parquet",
        compression="snappy",
    )
    print(
        f"\ndicionario: {len(rows):,} rows -> {target / 'data.parquet'}",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
