"""Emit the columns_json payload for bulk_upsert_columns, per table.

The architecture CSVs are the source of truth for name, order, type, unit,
directory link and dictionary flag; the trilingual descriptions and notes live
here, keyed by column name, because the architecture carries only one language.

Written as JSON files so the registration is reproducible and reviewable rather
than retyped into tool calls.

Note: bulk_upsert_columns DOES honour bigquery_type on create -- verified
against the backend on 2026-09-03, contradicting an earlier note that it was
silently ignored. It does not set is_partition; that needs update_column.
"""

from __future__ import annotations

import csv
import json
import pathlib

CODE = pathlib.Path(__file__).resolve().parent
ARCH = CODE / "architecture"
OUT = CODE / "columns_json"

# name -> (pt, en, es) description, and optionally (pt, en, es) observations.
DESCRIPTIONS: dict[str, tuple[str, str, str]] = {
    "year": (
        "Ano de referência da demonstração financeira, encerrada em 31 de dezembro",
        "Reference year of the financial statement, ending 31 December",
        "Año de referencia del estado financiero, cerrado el 31 de diciembre",
    ),
    "hep_code": (
        "Código do provedor de ensino superior atribuído pelo Departamento de Educação",
        "Code of the higher education provider assigned by the Department of Education",
        "Código del proveedor de educación superior asignado por el Departamento de Educación",
    ),
    "institution_type": (
        "Setor do provedor a que o valor se refere: instituição inteira, suas "
        "operações de ensino superior ou suas operações de educação profissional",
        "Sector of the provider the amount covers: whole institution, its higher "
        "education operations, or its vocational education operations",
        "Sector del proveedor al que se refiere el monto: institución completa, sus "
        "operaciones de educación superior o sus operaciones de educación vocacional",
    ),
    "line_number": (
        "Posição da rubrica dentro da demonstração, na ordem em que a fonte a apresenta",
        "Position of the line within the statement, in the order the source presents it",
        "Posición de la partida dentro del estado, en el orden en que la fuente la presenta",
    ),
    "line_item": (
        "Rubrica da demonstração a que o valor se refere",
        "Line of the statement the amount reports",
        "Partida del estado a la que se refiere el monto",
    ),
    "line_item_internal": (
        "Nome interno da rubrica no cubo de relatórios do departamento, quando a "
        "fonte o publica",
        "Internal name of the line in the department's reporting cube, where the "
        "source publishes one",
        "Nombre interno de la partida en el cubo de reportes del departamento, cuando "
        "la fuente lo publica",
    ),
    "value": (
        "Valor reportado para a rubrica",
        "Amount reported for the line",
        "Monto reportado para la partida",
    ),
    "category": (
        "Categoria HERDC da receita de pesquisa, de 1 a 4",
        "HERDC research income category, from 1 to 4",
        "Categoría HERDC del ingreso de investigación, de 1 a 4",
    ),
    "sub_category": (
        "Subcategoria HERDC da receita de pesquisa dentro da categoria",
        "HERDC research income sub-category within the category",
        "Subcategoría HERDC del ingreso de investigación dentro de la categoría",
    ),
    "amount": (
        "Receita de pesquisa reportada para a subcategoria",
        "Research income reported for the sub-category",
        "Ingreso de investigación reportado para la subcategoría",
    ),
    "statement": (
        "Demonstração financeira a que a rubrica pertence",
        "Financial statement table the line item belongs to",
        "Estado financiero al que pertenece la partida",
    ),
    "first_year": (
        "Primeiro ano em que o rótulo aparece na série",
        "First year the label appears in the series",
        "Primer año en que la etiqueta aparece en la serie",
    ),
    "last_year": (
        "Último ano em que o rótulo aparece na série",
        "Last year the label appears in the series",
        "Último año en que la etiqueta aparece en la serie",
    ),
    "n_years": (
        "Número de anos em que o rótulo aparece na série",
        "Number of years the label appears in the series",
        "Número de años en que la etiqueta aparece en la serie",
    ),
}

# Only where the note adds something a reader needs.
OBSERVATIONS: dict[str, tuple[str, str, str]] = {
    "hep_code": (
        "Não publicado na Finance Publication, que identifica os provedores apenas "
        "pelo nome. Resolvido a partir do nome contra a lista de provedores da HERDC, "
        "que carrega o código e retroage o nome atual para toda a série",
        "Not published in the Finance Publication, which identifies providers by name "
        "only. Resolved from the provider name against the HERDC provider list, which "
        "carries the code and back-casts every provider to its current name",
        "No publicado en la Finance Publication, que identifica a los proveedores solo "
        "por nombre. Resuelto a partir del nombre contra la lista de proveedores de la "
        "HERDC, que lleva el código y retrotrae el nombre actual a toda la serie",
    ),
    "institution_type": (
        "Somente provedores de setor duplo reportam a separação entre "
        "higher_education e vocational_education, e apenas na demonstração do "
        "resultado; todas as demais linhas são total",
        "Only dual-sector providers report a higher_education and vocational_education "
        "split, and only for the income statement; every other row is total",
        "Solo los proveedores de sector dual reportan la separación entre "
        "higher_education y vocational_education, y únicamente en el estado de "
        "resultados; todas las demás filas son total",
    ),
    "line_number": (
        "A fonte repete rótulos dentro de uma demonstração, de modo que o rótulo não "
        "identifica uma linha e esta coluna sim. Não é estável entre anos: uma linha "
        "inserida desloca todas as seguintes, então faça junções entre anos por "
        "line_item, nunca por esta coluna",
        "The source repeats labels within a statement, so the label does not identify "
        "a line and this does. Not stable across years: a line inserted in one release "
        "shifts every line below it, so join across years on line_item, never on this",
        "La fuente repite etiquetas dentro de un estado, por lo que la etiqueta no "
        "identifica una fila y esta columna sí. No es estable entre años: una partida "
        "insertada desplaza a todas las siguientes, así que una unión entre años debe "
        "usar line_item, nunca esta columna",
    ),
    "line_item": (
        "Rótulo publicado, transcrito da última coluna de rótulos da planilha de "
        "origem. O departamento renomeia rubricas ao longo da série; a tabela "
        "line_item registra em quais anos cada rótulo aparece",
        "The published label, taken verbatim from the last label column of the source "
        "sheet. The department relabels lines across the series; the line_item table "
        "records which labels appear in which years",
        "Etiqueta publicada, transcrita de la última columna de etiquetas de la "
        "planilla de origen. El departamento renombra partidas a lo largo de la serie; "
        "la tabla line_item registra en qué años aparece cada etiqueta",
    ),
    "line_item_internal": (
        "Difere do rótulo publicado: 'DEEWR Research Grants' para 'Education Research "
        "Grants', 'State Govt Total' para 'State and Local Government Financial "
        "Assistance'. Vazio em 2023, a única edição com uma só coluna de rótulos",
        "Differs from the published label: 'DEEWR Research Grants' for 'Education "
        "Research Grants', 'State Govt Total' for 'State and Local Government "
        "Financial Assistance'. Empty for 2023, the one release that ships a single "
        "label column",
        "Difiere de la etiqueta publicada: 'DEEWR Research Grants' por 'Education "
        "Research Grants', 'State Govt Total' por 'State and Local Government "
        "Financial Assistance'. Vacío en 2023, la única edición con una sola columna "
        "de etiquetas",
    ),
    "value": (
        "A fonte publica estas tabelas em milhares de dólares; os valores aqui são "
        "multiplicados por 1.000 para ficarem em dólares, na mesma escala de "
        "research_income. A conversão é exata",
        "The source publishes these tables in thousands of dollars; values here are "
        "multiplied by 1,000 so they are in dollars, matching research_income. The "
        "conversion is exact",
        "La fuente publica estas tablas en miles de dólares; los valores aquí se "
        "multiplican por 1.000 para quedar en dólares, en la misma escala que "
        "research_income. La conversión es exacta",
    ),
    "category": (
        "1 subvenções competitivas australianas, 2 outra receita de pesquisa do setor "
        "público, 3 receita de pesquisa da indústria e outras, 4 receita de "
        "Cooperative Research Centres",
        "1 Australian competitive grants, 2 other public sector research income, "
        "3 industry and other research income, 4 Cooperative Research Centre income",
        "1 subvenciones competitivas australianas, 2 otros ingresos de investigación "
        "del sector público, 3 ingresos de investigación de la industria y otros, "
        "4 ingresos de Cooperative Research Centres",
    ),
    "sub_category": (
        "Subcategorias são criadas e descontinuadas ao longo da série; um valor nulo "
        "indica que a subcategoria não estava em uso naquele ano, o que difere de um "
        "zero reportado",
        "Sub-categories are added and retired over the series; a null amount means the "
        "sub-category was not in use that year, which is not the same as a reported nil",
        "Las subcategorías se crean y se retiran a lo largo de la serie; un valor nulo "
        "indica que la subcategoría no estaba en uso ese año, lo que difiere de un cero "
        "reportado",
    ),
    "amount": (
        "Nulo quando a subcategoria não foi coletada naquele ano",
        "Null where the sub-category was not collected that year",
        "Nulo cuando la subcategoría no fue recopilada ese año",
    ),
    "n_years": (
        "Menor que o intervalo entre first_year e last_year quando um rótulo foi "
        "descontinuado e depois retomado",
        "Lower than the span between first_year and last_year where a label was "
        "retired and later reinstated",
        "Menor que el intervalo entre first_year y last_year cuando una etiqueta fue "
        "retirada y luego reinstaurada",
    ),
}


def build(table: str) -> list[dict]:
    columns = []
    with (ARCH / f"{table}.csv").open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            name = row["name"]
            if name not in DESCRIPTIONS:
                raise KeyError(f"{table}.{name}: no trilingual description")
            pt, en, es = DESCRIPTIONS[name]
            column = {
                "name": name,
                "bigquery_type": row["bigquery_type"],
                "description_pt": pt,
                "description_en": en,
                "description_es": es,
                "covered_by_dictionary": row["covered_by_dictionary"] == "yes",
                "has_sensitive_data": row["has_sensitive_data"] == "yes",
            }
            if row["measurement_unit"]:
                column["measurement_unit"] = row["measurement_unit"]
            if row["directory_column"]:
                column["directory_column"] = row["directory_column"]
            if name in OBSERVATIONS:
                o_pt, o_en, o_es = OBSERVATIONS[name]
                column["observations_pt"] = o_pt
                column["observations_en"] = o_en
                column["observations_es"] = o_es
            columns.append(column)
    return columns


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for path in sorted(ARCH.glob("*.csv")):
        table = path.stem
        if table == "dicionario":
            continue  # registered directly; its columns are the fixed BD schema
        columns = build(table)
        (OUT / f"{table}.json").write_text(
            json.dumps(columns, ensure_ascii=False)
        )
        print(f"{table}: {len(columns)} columns")


if __name__ == "__main__":
    main()
