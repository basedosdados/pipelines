"""Write every br_mgi_compras_publicas architecture CSV.

Usage:  uv run --no-project python build_architecture.py
"""

from pathlib import Path

import arch_arp
import arch_contratacoes
import arch_contratos
import arch_dicionario
import arch_legado
import arch_registries
from arch_common import write

OUT = Path(__file__).resolve().parent / "architecture"

# Order matters: it is the order tables are presented on the Data Basis site.
MODULES = [
    ("Contratações — Lei 14.133/2021", arch_contratacoes),
    ("Contratações — resultados e atas", arch_arp),
    ("Contratos", arch_contratos),
    ("Legado — Lei 8.666/1993", arch_legado),
    ("Cadastros e catálogos", arch_registries),
    ("Dicionário", arch_dicionario),
]

TABLE_ORDER = [
    "contratacao",
    "contratacao_item",
    "contratacao_item_resultado",
    "ata_registro_preco",
    "ata_registro_preco_item",
    "contrato",
    "contrato_item",
    "licitacao",
    "licitacao_pregao",
    "licitacao_item",
    "licitacao_item_pregao",
    "compra_sem_licitacao",
    "compra_sem_licitacao_item",
    "orgao",
    "unidade_administrativa",
    "fornecedor",
    "catalogo_material",
    "catalogo_servico",
    "dicionario",
]


def main() -> None:
    tables = {}
    for _, module in MODULES:
        overlap = set(tables) & set(module.TABLES)
        if overlap:
            raise SystemExit(
                f"table defined in two modules: {sorted(overlap)}"
            )
        tables.update(module.TABLES)

    missing = set(TABLE_ORDER) - set(tables)
    extra = set(tables) - set(TABLE_ORDER)
    if missing or extra:
        raise SystemExit(
            f"TABLE_ORDER mismatch: missing={sorted(missing)} extra={sorted(extra)}"
        )

    total = 0
    for label, module in MODULES:
        print(f"\n{label}")
        for table in TABLE_ORDER:
            if table in module.TABLES:
                write(table, module.TABLES[table], OUT)
                total += len(module.TABLES[table])
    print(f"\n{len(TABLE_ORDER)} tables, {total} columns")


if __name__ == "__main__":
    main()
