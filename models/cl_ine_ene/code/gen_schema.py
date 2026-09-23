#!/usr/bin/env python3
"""Generate models/cl_ine_ene/schema.yml from the architecture and the measurements.

Three things here are measured rather than chosen, because on a 271-column table
each of them is a judgement no one can make by eye:

* the `ignore_values` list for not_null_proportion_multiple_columns, read from
  the parquet footers as a union over recent periods (null_proportions.py);
* the columns given to custom_dictionary_coverage, which are exactly those whose
  dicionario is complete against every value in all 197 periods
  (build_architecture.py writes the list);
* the relationships tests, which follow the architecture's directory_column.

    python models/cl_ine_ene/code/gen_schema.py
"""

from __future__ import annotations

import csv
import json
import os
import pathlib

HERE = pathlib.Path(__file__).resolve().parent
DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)

#: dbt ref for each directory an architecture FK can point at.
DIRECTORY_REF = {
    "br_bd_diretorios_data_tempo.ano:ano": (
        "br_bd_diretorios_data_tempo__ano",
        "ano.ano",
    ),
    "br_bd_diretorios_data_tempo.mes:mes": (
        "br_bd_diretorios_data_tempo__mes",
        "mes.mes",
    ),
    "br_bd_diretorios_cl.region:id_region": (
        "br_bd_diretorios_cl__region",
        "id_region",
    ),
    "br_bd_diretorios_cl.provincia:id_provincia": (
        "br_bd_diretorios_cl__provincia",
        "id_provincia",
    ),
    "br_bd_diretorios_cl.comuna:id_comuna": (
        "br_bd_diretorios_cl__comuna",
        "id_comuna",
    ),
}

#: Columns that identify a row. idrph is unique within a moving quarter; the
#: household/line pair is not (2019-11 carries one duplicate).
KEY = ["ano", "mes", "idrph"]

#: Present and populated in every one of the 197 periods.
NOT_NULL = [
    "ano",
    "mes",
    "id_region",
    "id_provincia",
    "id_comuna",
    "idrph",
    "id_identificacion",
    "fact_cal",
    "cae_general",
    "sexo",
    "edad",
]


def quote(text: str) -> str:
    return text.replace('"', "'")


def main():
    with open(HERE / "architecture/microdato.csv", encoding="utf-8") as handle:
        arch = list(csv.DictReader(handle))
    sparse = json.loads((DATA / "sparse_columns.json").read_text())
    complete = json.loads((HERE / "dictionary_complete.json").read_text())

    out = [
        "---",
        "version: 2",
        "models:",
        "  - name: cl_ine_ene__microdato",
        "    description: >",
        "      Microdatos de la Encuesta Nacional de Empleo (ENE) del Instituto Nacional de",
        "      Estadísticas de Chile, con una fila por persona y trimestre móvil. Cada",
        "      trimestre móvil se identifica por su mes central (ano, mes) y abarca ese mes,",
        "      el anterior y el siguiente. La serie publicada cubre 197 trimestres móviles",
        "      entre 2010-02 y 2026-06 bajo 16 esquemas distintos de cuestionario: la tabla",
        "      es la unión de todos ellos, de modo que una columna que un período no preguntó",
        "      aparece nula en ese período. El diseño muestral entrega estimaciones a nivel",
        "      nacional y regional; NO contempla estimaciones comunales. Use siempre fact_cal",
        "      como factor de expansión.",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        f"          combination_of_columns: {KEY}",
        "      # Scoped to one moving quarter: the macro reads every column of the model and",
        "      # runs at compile, so an unscoped pass over 271 columns and 20M rows is",
        "      # expensive enough to matter. ignore_values is the measured union over the",
        "      # twelve most recent periods — 49 columns retired before the window plus 50",
        "      # follow-up questions asked of small subgroups. See code/null_proportions.py.",
        "      - not_null_proportion_multiple_columns:",
        "          at_least: 0.05",
        "          ignore_values:",
    ]
    out += [f"            - {name}" for name in sparse]
    out += [
        "          config:",
        "            where: __most_recent_year_month__",
        "      # Exactly the columns whose dicionario covers every value observed in all 197",
        "      # periods. The rest are dictionary-covered but carry values INE's own codebook",
        "      # never defines, recorded in each column's observations.",
        "      - custom_dictionary_coverage:",
        "          dictionary_model: ref('cl_ine_ene__dicionario')",
        "          columns_covered_by_dictionary:",
    ]
    out += [f"            - {name}" for name in complete]
    out += [
        "          config:",
        "            where: __most_recent_year_month__",
        "    columns:",
    ]

    for row in arch:
        name = row["name"]
        out.append(f"      - name: {name}")
        out.append("        description: >")
        out.append(f"          {quote(row['description'])}")
        tests = []
        if name in NOT_NULL:
            tests.append("          - not_null")
        fk = DIRECTORY_REF.get(row["directory_column"])
        if fk:
            model, field = fk
            tests += [
                "          - relationships:",
                f"              to: ref('{model}')",
                # `field: ano` would bind to the range variable dbt creates from
                # the table name, not the column, and the test could never pass.
                f"              field: {field}",
            ]
        if tests:
            out.append("        tests:")
            out += tests

    out += [
        "",
        "  - name: cl_ine_ene__dicionario",
        "    description: >",
        "      Etiquetas de los valores codificados de cl_ine_ene.microdato, transcritas de",
        "      los dos libros de códigos del INE y de sus anexos de clasificación (CIUO-08 y",
        "      CIUO-88 para ocupación, CAENES para rama de actividad, M49 para países). Los",
        "      microdatos publican códigos y no etiquetas, de modo que esta tabla no se",
        "      deriva de ellos: es una transcripción de la documentación. No cubre las",
        "      columnas resueltas por directorio (id_region, id_provincia, id_comuna,",
        "      mig2_cod, mig5_cod, b18_codigo, b18_region), cuya fuente de verdad es",
        "      br_bd_diretorios_cl.",
        "    tests:",
        "      - dbt_utils.unique_combination_of_columns:",
        "          combination_of_columns: ['id_tabela', 'nome_coluna', 'chave']",
        "    columns:",
        "      - name: id_tabela",
        "        description: >",
        "          Nombre de la tabla del conjunto a la que corresponde la clave",
        "        tests:",
        "          - not_null",
        "      - name: nome_coluna",
        "        description: >",
        "          Nombre de la columna a la que corresponde la clave",
        "        tests:",
        "          - not_null",
        "      - name: chave",
        "        description: >",
        "          Valor de la clave tal como aparece almacenado en la columna",
        "        tests:",
        "          - not_null",
        "      - name: cobertura_temporal",
        "        description: >",
        "          Cobertura temporal de la clave; vacía cuando coincide con la de la tabla",
        "      - name: valor",
        "        description: >",
        "          Etiqueta de la clave según el libro de códigos del INE",
        "        tests:",
        "          - not_null",
    ]

    path = HERE.parent / "schema.yml"
    path.write_text("\n".join(out) + "\n")
    print(f"wrote {path}")
    print(
        f"  {len(arch)} columns, {len(sparse)} null-proportion exemptions, "
        f"{len(complete)} dictionary-coverage columns"
    )
    print(
        f"  not_null on {len(NOT_NULL)}, relationships on "
        f"{sum(1 for r in arch if r['directory_column'] in DIRECTORY_REF)}"
    )


if __name__ == "__main__":
    main()
