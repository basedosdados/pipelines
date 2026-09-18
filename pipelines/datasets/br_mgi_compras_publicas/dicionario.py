"""Build the dicionario table from the code/label pairs the API already carries.

Lives in the package, not in the one-shot onboarding code, because the weekly
refresh flow rebuilds this table too: new codes appear as the source adds them,
and a dicionario frozen at onboarding would slowly stop explaining the data.

Every coded column ships alongside a companion column holding the human-readable
label -- `codigo_modalidade` with `modalidade`, `id_situacao_item` with
`situacao_item` -- so the dictionary is harvested from the data itself rather
than transcribed from a manual that does not exist.

Two code sets reuse the same integers for different things: the SIASG modalidade
codes under Lei 8.666 and the PNCP ones under Lei 14.133. They are keyed
separately, by table, so a reader joining on `id_tabela` and `nome_coluna` can
never pick up the wrong meaning.
"""

from __future__ import annotations

import csv
import os
from pathlib import Path

import pyarrow.dataset as ds

from pipelines.datasets.br_mgi_compras_publicas.constants import (
    ARCHITECTURE_DIR as ARCH,
)

#: coded column -> the column in the same table holding its label.
LABELS: dict[str, str] = {
    # Lei 14.133 contratacao
    "codigo_modalidade": "modalidade",
    "id_modalidade_pncp": "modalidade",
    "codigo_modo_disputa": "modo_disputa",
    "id_modo_disputa_pncp": "modo_disputa",
    "codigo_amparo_legal": "amparo_legal",
    "codigo_tipo_instrumento_convocatorio": "tipo_instrumento_convocatorio",
    "id_situacao_compra": "situacao_compra",
    "codigo_orcamento_sigiloso": "orcamento_sigiloso",
    "esfera": "",
    "poder": "",
    "esfera_subrogado": "",
    "poder_subrogado": "",
    # itens and results
    "tipo_item": "nome_tipo_item",
    "id_item_categoria": "item_categoria",
    "id_criterio_julgamento": "criterio_julgamento",
    "id_situacao_item": "situacao_item",
    "id_tipo_beneficio": "tipo_beneficio",
    "id_situacao_resultado": "situacao_resultado",
    "id_porte_fornecedor": "porte_fornecedor",
    "id_natureza_juridica": "natureza_juridica",
    "tipo_pessoa": "",
    "id_amparo_legal_criterio_desempate": "amparo_legal_criterio_desempate",
    # atas and contracts
    "codigo_modalidade_compra": "modalidade_compra",
    "codigo_tipo": "tipo",
    "codigo_categoria": "categoria",
    "codigo_subcategoria": "subcategoria",
    "receita_despesa": "",
    "situacao_sicaf": "",
    # registries
    "codigo_tipo_administracao": "nome_tipo_administracao",
    "id_porte_empresa": "porte_empresa",
    # legado
    "tipo_pregao_compra": "",
    "indicador_decreto_7174": "",
    "indicador_sustentavel": "",
    "indicador_margem_preferencial": "",
    "tratamento_diferenciado": "",
    "tipo_fornecedor_vencedor": "",
}

#: Codes whose meaning the API never spells out in a companion column. Taken
#: from the observations already recorded in the architecture.
STATIC: dict[str, dict[str, str]] = {
    "esfera": {
        "F": "Federal",
        "E": "Estadual",
        "M": "Municipal",
        "D": "Distrital",
        "N": "Não classificado",
    },
    "poder": {
        "E": "Executivo",
        "L": "Legislativo",
        "J": "Judiciário",
        "N": "Não classificado",
    },
    "tipo_pessoa": {
        "PF": "Pessoa física",
        "PJ": "Pessoa jurídica",
        "PE": "Pessoa estrangeira",
    },
    "receita_despesa": {"D": "Despesa", "R": "Receita", "S": "Sem ônus"},
    "tipo_item": {"M": "Material", "S": "Serviço"},
    "tipo_pregao_compra": {
        "SISPP": "Sistema de preço praticado",
        "SISRP": "Sistema de registro de preços",
    },
}


def data_dir() -> Path:
    return Path(
        os.environ.get(
            "COMPRAS_DATA_DIR",
            Path.home() / "Downloads" / "br_mgi_compras_publicas_data",
        )
    )


def dictionary_columns(table: str) -> list[str]:
    """Columns the architecture marks as covered_by_dictionary."""
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        return [
            row["name"]
            for row in csv.DictReader(handle)
            if row["covered_by_dictionary"] == "yes"
        ]


def pairs_for(table: str, chunk_dir: Path) -> list[tuple[str, str, str, str]]:
    """Distinct (table, column, key, value) rows for one table's coded columns."""
    coded = dictionary_columns(table)
    if not coded:
        return []
    files = sorted(chunk_dir.glob("*.parquet"))
    if not files:
        return []
    dataset = ds.dataset(files, format="parquet")
    present = set(dataset.schema.names)

    wanted: dict[str, str] = {}
    for column in coded:
        if column not in present:
            continue
        label = LABELS.get(column)
        if label and label in present:
            wanted[column] = label
        else:
            wanted[column] = ""

    if not wanted:
        return []

    needed = sorted({c for c in wanted} | {v for v in wanted.values() if v})
    found: dict[tuple[str, str], str] = {}
    for batch in dataset.to_batches(columns=needed, batch_size=100_000):
        data = batch.to_pydict()
        for column, label in wanted.items():
            keys = data[column]
            values = data[label] if label else None
            for i, key in enumerate(keys):
                if key is None or key == "":
                    continue
                value = values[i] if values else None
                if value:
                    found[(column, key)] = value
                else:
                    found.setdefault((column, key), "")

    rows = []
    for (column, key), value in found.items():
        if not value:
            static = STATIC.get(column)
            value = static.get(key, "") if static else ""
        if value:
            rows.append((table, column, key, value))
    return rows


def build_dicionario(output_dir: Path) -> int:
    """Rebuild the dicionario parquet from every harvested table's chunks.

    Args:
        output_dir: scratch root holding ``_chunks/`` and ``output/``.

    Returns:
        Number of key/value rows written.
    """
    from pipelines.datasets.br_mgi_compras_publicas.utils import (
        load_architecture,
        write_chunk,
    )

    root = output_dir / "_chunks"
    rows: list[tuple[str, str, str, str]] = []
    for table in sorted(p.stem for p in ARCH.glob("*.csv")):
        if table == "dicionario":
            continue
        chunk_dir = root / table
        if chunk_dir.is_dir():
            rows.extend(pairs_for(table, chunk_dir))

    records = [
        {
            "id_tabela": table,
            "nome_coluna": column,
            "chave": key,
            "cobertura_temporal": "",
            "valor": value,
        }
        for table, column, key, value in sorted(rows)
    ]
    write_chunk(
        records,
        load_architecture("dicionario"),
        output_dir / "output" / "dicionario" / "data-0.parquet",
    )
    return len(records)
