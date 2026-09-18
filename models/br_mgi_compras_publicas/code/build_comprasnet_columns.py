"""Build the columns_json payload for the two ComprasNet tables.

The architecture CSVs carry `observations` in Portuguese only. The backend
stores observations per language, and a language left empty is blank on the
site for those readers — which is how 3,022 production columns ended up
PT-only. The translations live here so the payload always carries all three.

    uv run --no-project python build_comprasnet_columns.py <table> > payload.json
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ARCH = Path(__file__).resolve().parent / "architecture"

OBSERVATIONS: dict[str, tuple[str, str]] = {
    # Portuguese key -> (English, Spanish)
    "Coluna de particionamento, derivada dos quatro últimos dígitos de id_compra. O ComprasNet responde 'Não existe resultado para o pregão' para todo pregão anterior a 2004; a partir de 2005 a página de resultado por fornecedor está preenchida na maioria dos pregões": (
        "Partition column, derived from the last four digits of id_compra. ComprasNet answers "
        "'Não existe resultado para o pregão' for every reverse auction before 2004; from 2005 the "
        "result-by-supplier page is filled in for most of them",
        "Columna de particionamiento, derivada de los cuatro últimos dígitos de id_compra. ComprasNet "
        "responde 'Não existe resultado para o pregão' para todo pregón anterior a 2004; a partir de "
        "2005 la página de resultado por proveedor está completa en la mayoría de ellos",
    ),
    "Coluna de particionamento, derivada dos quatro últimos dígitos de id_compra. Os pregões de 2001 não têm termo de homologação publicado e por isso não aparecem nesta tabela": (
        "Partition column, derived from the last four digits of id_compra. The 2001 reverse auctions "
        "have no published award decision and therefore do not appear in this table",
        "Columna de particionamiento, derivada de los cuatro últimos dígitos de id_compra. Los pregones "
        "de 2001 no tienen acta de homologación publicada y por eso no aparecen en esta tabla",
    ),
    "Junta com licitacao_pregao e licitacao_item_pregao. Reconstruído a partir da chave do ComprasNet como UASG (6) + modalidade (2) + número (5) + ano (4)": (
        "Joins to licitacao_pregao and licitacao_item_pregao. Rebuilt from the ComprasNet key as "
        "UASG (6) + modality (2) + number (5) + year (4)",
        "Se une a licitacao_pregao y licitacao_item_pregao. Reconstruido a partir de la clave de "
        "ComprasNet como UASG (6) + modalidad (2) + número (5) + año (4)",
    ),
    "Junta com licitacao_pregao e licitacao_item_pregao": (
        "Joins to licitacao_pregao and licitacao_item_pregao",
        "Se une a licitacao_pregao y licitacao_item_pregao",
    ),
    "Junta com numero_item_licitacao em licitacao_item": (
        "Joins to numero_item_licitacao in licitacao_item",
        "Se une a numero_item_licitacao en licitacao_item",
    ),
    "O campo traz CNPJ na quase totalidade dos casos, mas pessoas físicas aparecem com CPF não mascarado, por isso a marcação de dado sensível": (
        "The field carries a CNPJ in almost every case, but individuals appear with an unmasked CPF, "
        "which is why it is flagged as sensitive",
        "El campo trae CNPJ en casi todos los casos, pero las personas físicas aparecen con CPF sin "
        "enmascarar, por lo que se marca como dato sensible",
    ),
    "Texto livre preenchido pelo fornecedor; presente em cerca de 97% das linhas": (
        "Free text filled in by the supplier; present on about 97% of rows",
        "Texto libre completado por el proveedor; presente en cerca del 97% de las filas",
    ),
    "Texto livre preenchido pelo fornecedor": (
        "Free text filled in by the supplier",
        "Texto libre completado por el proveedor",
    ),
    "Texto livre preenchido pelo fornecedor, distinto da descrição de catálogo em descricao_item": (
        "Free text filled in by the supplier, distinct from the catalogue description in descricao_item",
        "Texto libre completado por el proveedor, distinto de la descripción de catálogo en descricao_item",
    ),
    "Vazio quando o item foi disputado isoladamente; preenchido em cerca de 18% das linhas. Lido do rótulo do item, não do resumo de grupos no topo da página": (
        "Empty when the item was disputed on its own; filled in on about 18% of rows. Read from the "
        "item's own label, not from the group summary at the top of the page",
        "Vacío cuando el ítem se disputó de forma aislada; completado en cerca del 18% de las filas. "
        "Leído de la etiqueta del ítem, no del resumen de grupos al inicio de la página",
    ),
    "Atribuída na leitura da página; 1 é o evento mais antigo listado": (
        "Assigned when the page is read; 1 is the oldest event listed",
        "Asignada al leer la página; 1 es el evento más antiguo listado",
    ),
    "Inclui Adjudicado, Homologado, Cancelado, Cancelado no julgamento, Cancelado na adjudicação, Cancelamento de adjudicação e Volta de fase": (
        "Includes Adjudicado, Homologado, Cancelado, Cancelado no julgamento, Cancelado na adjudicação, "
        "Cancelamento de adjudicação and Volta de fase",
        "Incluye Adjudicado, Homologado, Cancelado, Cancelado no julgamento, Cancelado na adjudicação, "
        "Cancelamento de adjudicação y Volta de fase",
    ),
    "Vazio para eventos automáticos do sistema. Nome de servidor público identificado no exercício da função": (
        "Empty for automatic system events. Name of a public official identified in the exercise of "
        "their duties",
        "Vacío para eventos automáticos del sistema. Nombre de un servidor público identificado en el "
        "ejercicio de su función",
    ),
    "Texto livre; em adjudicações costuma repetir fornecedor, CNPJ e melhor lance": (
        "Free text; on awards it usually repeats the supplier, taxpayer id and best bid",
        "Texto libre; en adjudicaciones suele repetir proveedor, CNPJ y mejor puja",
    ),
}


def build(table: str) -> list[dict]:
    rows = []
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        for raw in csv.DictReader(handle):
            column = {
                "name": raw["name"],
                "bigquery_type": raw["bigquery_type"],
                "description_pt": raw["description"],
                "description_en": raw["description_en"],
                "description_es": raw["description_es"],
                "covered_by_dictionary": raw["covered_by_dictionary"].strip()
                == "yes",
                "has_sensitive_data": raw["has_sensitive_data"].strip()
                == "yes",
            }
            if raw["measurement_unit"].strip():
                column["measurement_unit"] = raw["measurement_unit"].strip()
            if raw["directory_column"].strip():
                column["directory_column"] = raw["directory_column"].strip()
            note = raw["observations"].strip()
            if note:
                if note not in OBSERVATIONS:
                    raise SystemExit(
                        f"untranslated observation on {raw['name']}: {note!r}"
                    )
                english, spanish = OBSERVATIONS[note]
                column["observations_pt"] = note
                column["observations_en"] = english
                column["observations_es"] = spanish
            rows.append(column)
    return rows


if __name__ == "__main__":
    print(json.dumps(build(sys.argv[1]), ensure_ascii=False))
