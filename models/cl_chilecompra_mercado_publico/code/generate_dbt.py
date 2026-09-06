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

# orden_compra_item is the expensive model: 105.4M rows / 154.9 GB, and a full rebuild
# reads 141.5 GiB of staging. The recurring pipeline only ever re-ingests a rolling
# trailing window -- ChileCompra rewrites roughly the last 15 months of oc-da -- so
# rebuilding twenty years every week is waste. The other three tables are small enough
# that a full rebuild is cheaper than the machinery.
#
# insert_overwrite replaces exactly the `ano` partitions the incremental query returns.
# That is safe here, and is NOT the "trailing window + overwrite erases history" trap:
# the staging external table always holds the FULL history, so each partition it touches
# is rebuilt complete from source rather than from the window alone.
#
# The window is max(ano) - 2, not the current year: a 15-month lookback reaches two
# calendar years back when the run happens in January.
INCREMENTAL_LOOKBACK_YEARS = {"orden_compra_item": 2}

# BigQuery blocks DML on a table that carries row access policies, and insert_overwrite
# is DML -- hence the pre-hook that the house pattern uses on every part_bdpro
# incremental model. The adapter.get_relation guard matters: an unguarded DROP fails on
# the FIRST build, when the table does not exist yet, and fails identically inside the
# GitHub table-approve prod materialisation.
DROP_POLICIES_PRE_HOOK = (
    "{% if adapter.get_relation(this.database, this.schema, this.identifier) %}"
    "DROP ALL ROW ACCESS POLICIES ON {{ this }}"
    "{% else %}SELECT 1{% endif %}"
)

# Tests on this table are scoped to the most recent year partition. Unscoped, the
# uniqueness and dictionary-coverage tests each scan all 105.4M rows.
SCOPED_TEST_TABLES = {"orden_compra_item"}

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
# Optional tender fields the source rarely fills, measured on the most recent year:
# tipo_duracion_contrato 0.3%, fecha_soporte_fisico 2.3%, fecha_estimada_firma 4.1%.
IGNORE["licitacion_item"] += [
    "tipo_duracion_contrato",
    "fecha_soporte_fisico",
    "fecha_estimada_firma",
]

# Key columns carry sparse nulls that a strict not_null cannot express. They are real
# source gaps, not load errors: 47 rows of 105.4M have no codigo_orden_compra, 9,346
# have no id_item (7,916 of them in 2008), and every one of the 1,450,638 licitaciones
# offer rows from lic-da/2014-3 -- the anomalous 111-column file -- identifies its
# supplier by rut_proveedor rather than codigo_proveedor.
#
# A proportion test still catches a systemic break while tolerating the source's own
# holes. Uniqueness of the full key is enforced separately and passes exactly.
KEY_NULL_TOLERANCE = {
    ("licitacion_oferta", "codigo_proveedor"): 0.97,
}
DEFAULT_KEY_PROPORTION = 0.999


def wrap(text: str, indent: str = "     ", width: int = 95) -> list[str]:
    words, line, out = text.split(), indent, []
    for word in words:
        if len(line) + len(word) + 1 > width:
            out.append(line)
            line = indent
        line += " " + word
    out.append(line)
    return out


def incremental_filter(lookback: int) -> str:
    """The WHERE that limits an incremental run to the last `lookback` + 1 years.

    The threshold is resolved at COMPILE time into a bare string literal, deliberately.
    The staging table is an external table with hive partitioning in STRINGS mode, so
    `ano` there is a STRING pseudo-column read out of the object path: a literal
    comparison prunes the parquet files, while a subquery or a cast around the column
    would read all 236 partitions and give back everything the incremental model is for.
    """
    return (
        "{% if is_incremental() %}\n"
        "    {%- set max_year_result = run_query("
        '"select max(ano) as max_year from " ~ this) -%}\n'
        "    {%- set max_year = 0 -%}\n"
        "    {%- if execute and max_year_result.rows[0][0] -%}\n"
        "        {%- set max_year = max_year_result.rows[0][0] -%}\n"
        "    {%- endif -%}\n"
        "    -- rebuild the trailing window the source rewrites; every partition it\n"
        "    -- touches is rebuilt in full from staging, which holds all of history\n"
        f"    where t.ano >= '{{{{ max_year - {lookback} }}}}'\n"
        "{% endif %}\n"
    )


def write_models() -> None:
    for table in TABLES:
        arch = utils.read_architecture(table)
        casts = [
            "    " + CAST[str(r.bigquery_type)].format(c=r.name)
            for r in arch.itertuples()
        ]
        lookback = INCREMENTAL_LOOKBACK_YEARS.get(table)
        if lookback:
            materialization = (
                '        materialized="incremental",\n'
                '        incremental_strategy="insert_overwrite",\n'
            )
        else:
            materialization = '        materialized="table",\n'
        sql = (
            "{{\n"
            "    config(\n"
            f'        schema="{DS}",\n'
            f'        alias="{table}",\n'
            + materialization
            + "        partition_by={\n"
            '            "field": "ano",\n'
            '            "data_type": "int64",\n'
            '            "range": {"start": 2007, "end": 2031, "interval": 1},\n'
            "        },\n"
            '        cluster_by=["mes"],\n'
            + (
                f'        pre_hook="{DROP_POLICIES_PRE_HOOK}",\n'
                if lookback
                else ""
            )
            + "    )\n"
            "}}\n\n\nselect\n"
            + ("," + NL).join(casts)
            + f'\nfrom\n    {{{{ set_datalake_project("{DS}_staging.{table}") }}}}\n    as t\n'
            + (incremental_filter(lookback) if lookback else "")
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
        scoped = table in SCOPED_TEST_TABLES
        out.append("    tests:")
        out.append("      - dbt_utils.unique_combination_of_columns:")
        out.append(f"          combination_of_columns: [{', '.join(keys)}]")
        if scoped:
            out.append("          config:")
            out.append("            where: __most_recent_year__")
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
            if scoped:
                out.append("          config:")
                out.append("            where: __most_recent_year__")
        out.append("    columns:")
        for r in arch.itertuples():
            out.append(f"      - name: {r.name}")
            out.append(f"        description: {r.description}")
            is_partition = r.name in ("ano", "mes")
            is_key = r.name in keys
            directory = DIRECTORY_MODEL.get(str(r.directory_column))
            if is_partition and not directory:
                if scoped and r.name != "ano":
                    out.append("        tests:")
                    out.append("          - not_null:")
                    out.append("              config:")
                    out.append("                where: __most_recent_year__")
                else:
                    # not_null on `ano` is never scoped: __most_recent_year__ expands
                    # to `ano = <max>`, which drops the null rows the test looks for,
                    # so scoping it would make it pass vacuously.
                    out.append("        tests: [not_null]")
            elif is_partition or is_key or directory:
                out.append("        tests:")
                if is_partition:
                    if scoped and r.name != "ano":
                        out.append("          - not_null:")
                        out.append("              config:")
                        out.append(
                            "                where: __most_recent_year__"
                        )
                    else:
                        out.append("          - not_null")
                elif is_key:
                    at_least = KEY_NULL_TOLERANCE.get(
                        (table, str(r.name)), DEFAULT_KEY_PROPORTION
                    )
                    out.append("          - dbt_utils.not_null_proportion:")
                    out.append(f"              at_least: {at_least}")
                    if scoped:
                        out.append("              config:")
                        out.append(
                            "                where: __most_recent_year__"
                        )
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
