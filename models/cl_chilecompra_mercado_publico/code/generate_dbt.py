"""Generate the dbt models and schema.yml from the architecture CSVs.

The architecture is the source of truth for column order, types, directory foreign keys
and dictionary coverage. Deriving the SQL and the tests from it means the three cannot
drift apart -- with 205 columns across three tables, hand-editing them in step is not
realistic.

    uv run python models/cl_chilecompra_mercado_publico/code/generate_dbt.py
"""

from __future__ import annotations

from pathlib import Path

from pipelines.datasets.cl_chilecompra_mercado_publico import utils

DS = "cl_chilecompra_mercado_publico"
OUT = Path("models") / DS
NL = "\n"
TABLES = ["orden_compra_item", "licitacion_item", "licitacion_oferta"]

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "DATE": "safe_cast({c} as date) {c}",
    "STRING": "safe_cast({c} as string) {c}",
}

DIRECTORY_MODEL = {
    "br_bd_diretorios_cl.region:id_region": (
        "br_bd_diretorios_cl__region",
        "id_region",
    ),
    "br_bd_diretorios_cl.comuna:id_comuna": (
        "br_bd_diretorios_cl__comuna",
        "id_comuna",
    ),
}

DESCRIPTIONS = {
    "orden_compra_item": (
        "Ordens de compra do Mercado Publico do Chile no nivel de linha de item, de 2007 "
        "a hoje. Cada linha e um item de uma ordem de compra emitida por um organismo "
        "publico a um fornecedor. A chave logica e (codigo_orden_compra, id_item). "
        "Conforme a propria ChileCompra, o arquivo de descarga massiva inclui ordens de "
        "compra que sao excluidas das estatisticas oficiais por conterem erros de valor "
        "ou de tipo de moeda, de modo que agregados calculados aqui nao coincidem com as "
        "cifras publicadas pela ChileCompra."
    ),
    "licitacion_item": (
        "Licitacoes do Mercado Publico do Chile no nivel de linha de item, de 2007 a "
        "hoje. Cada linha e um item de uma licitacao, com todos os atributos do processo "
        "licitatorio. A chave logica e (codigo_licitacion, codigo_item). As propostas "
        "recebidas em cada item estao na tabela licitacion_oferta."
    ),
    "licitacion_oferta": (
        "Propostas recebidas nas licitacoes do Mercado Publico do Chile, de 2007 a hoje. "
        "Cada linha e a proposta de um fornecedor para uma linha de item de uma "
        "licitacao, incluindo as propostas nao vencedoras. A chave logica e "
        "(codigo_licitacion, codigo_item, codigo_proveedor, nombre_oferta). Os atributos "
        "do processo licitatorio estao na tabela licitacion_item."
    ),
}

# Columns legitimately empty for whole eras, which the null-proportion test must not
# count against the model.
IGNORE = {
    "orden_compra_item": [
        "id_plan_compra",
        "codigo_convenio_marco",
        "fecha_cancelacion",
        "fecha_solicitud_cancelacion",
        "promedio_calificacion",
        "cantidad_evaluacion",
        "codigo_licitacion",
        "id_comuna_proveedor",
        "id_region_proveedor",
    ],
    "licitacion_item": [
        "valor_tiempo_renovacion",
        "sector",
        "indicador_criterios_ambientales",
        "descripcion_criterios_ambientales",
        "indicador_criterios_sociales",
        "descripcion_criterios_sociales",
        "descripcion_criterios_sociales_2",
        "criterios_evaluacion",
        "fecha_visita_terreno",
        "direccion_visita",
        "fecha_entrega_antecedentes",
        "direccion_entrega",
        "fecha_tiempo_evaluacion",
        "periodo_tiempo_renovacion",
        "fecha_adjudicacion",
        "fecha_aprobacion",
        "id_comuna_unidad_compra",
    ],
    "licitacion_oferta": [
        "estado_final_oferta",
        "unidad_medida_oferta",
        "cantidad_adjudicada",
        "monto_linea_adjudicada",
        "fecha_envio_oferta",
    ],
}


def wrap(text: str, indent: str = "     ", width: int = 95) -> list[str]:
    words, line, out = text.split(), indent, []
    for word in words:
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = indent
        line += " " + word
    out.append(line)
    return out


def write_models() -> None:
    for table in TABLES:
        arch = utils.read_architecture(table)
        casts = [
            "    " + CAST[str(r.bigquery_type)].format(c=r.name)
            for r in arch.itertuples()
        ]
        sql = (
            "{{\n"
            "    config(\n"
            f'        schema="{DS}",\n'
            f'        alias="{table}",\n'
            '        materialized="table",\n'
            "        partition_by={\n"
            '            "field": "ano",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 2007, "end": 2031, "interval": 1},\n'
            "        },\n"
            '        cluster_by=["mes"],\n'
            "    )\n"
            "}}\n\n\nselect\n"
            + ("," + NL).join(casts)
            + f'\nfrom\n    {{{{ set_datalake_project("{DS}_staging.{table}") }}}}\n    as t\n'
        )
        (OUT / f"{DS}__{table}.sql").write_text(sql, encoding="utf-8")
        print(f"  model {table}: {len(arch)} columns")

    (OUT / f"{DS}__dicionario.sql").write_text(
        "{{\n    config(\n"
        f'        schema="{DS}",\n'
        '        alias="dicionario",\n'
        '        materialized="table",\n'
        "    )\n}}\n\n\nselect\n"
        "    safe_cast(id_tabela as string) id_tabela,\n"
        "    safe_cast(nome_coluna as string) nome_coluna,\n"
        "    safe_cast(chave as string) chave,\n"
        "    safe_cast(cobertura_temporal as string) cobertura_temporal,\n"
        "    safe_cast(valor as string) valor\n"
        f'from\n    {{{{ set_datalake_project("{DS}_staging.dicionario") }}}}\n    as t\n',
        encoding="utf-8",
    )
    print("  model dicionario")


def write_schema() -> None:
    out = ["---", "version: 2", "models:"]
    for table in TABLES:
        arch = utils.read_architecture(table)
        keys = utils.PRIMARY_KEYS[table]
        out.append(f"  - name: {DS}__{table}")
        out.append("    description: >")
        out.extend(wrap(DESCRIPTIONS[table]))
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(f"          combination_of_columns: [{', '.join(keys)}]")
        out.append("      - not_null_proportion_multiple_columns:")
        out.append("          at_least: 0.05")
        # Scope it: unscoped, this test compiles a scan over every column of a table
        # with ~80M rows, which burns the BigQuery daily quota on its own.
        out.append("          config:")
        out.append("            where: __most_recent_year__")
        out.append("          ignore_values:")
        for column in IGNORE[table]:
            out.append(f"            - {column}")
        dict_cols = [
            r.name
            for r in arch.itertuples()
            if r.covered_by_dictionary == "yes"
        ]
        if dict_cols:
            out.append("      - custom_dictionary_coverage:")
            out.append("          columns_covered_by_dictionary:")
            for column in dict_cols:
                out.append(f"            - {column}")
            out.append(f"          dictionary_model: ref('{DS}__dicionario')")
        out.append("    columns:")
        for r in arch.itertuples():
            out.append(f"      - name: {r.name}")
            out.append(f"        description: {r.description}")
            tests = (
                ["not_null"]
                if r.name in keys or r.name in ("ano", "mes")
                else []
            )
            directory = DIRECTORY_MODEL.get(str(r.directory_column))
            if tests and not directory:
                out.append("        tests: [not_null]")
            elif tests or directory:
                out.append("        tests:")
                for test in tests:
                    out.append(f"          - {test}")
                if directory:
                    out.append("          - relationships:")
                    out.append(f"              to: ref('{directory[0]}')")
                    out.append(f"              field: {directory[1]}")
                    out.append("              config:")
                    out.append("                where: __most_recent_year__")
        out.append("")

    out.append(f"  - name: {DS}__dicionario")
    out.append("    description: >")
    out.extend(
        wrap(
            "Dicionario de valores codificados das tabelas do dataset, com a traducao de "
            "cada chave para o rotulo publicado pela ChileCompra."
        )
    )
    out.append("    columns:")
    for name, desc in [
        ("id_tabela", "Nome da tabela a que se refere a chave"),
        ("nome_coluna", "Nome da coluna a que se refere a chave"),
        ("chave", "Valor codificado tal como aparece na coluna"),
        ("cobertura_temporal", "Cobertura temporal da chave"),
        ("valor", "Rotulo correspondente a chave"),
    ]:
        out.append(f"      - name: {name}")
        out.append(f"        description: {desc}")
    (OUT / "schema.yml").write_text(NL.join(out) + NL, encoding="utf-8")
    print("  schema.yml")


if __name__ == "__main__":
    write_models()
    write_schema()
