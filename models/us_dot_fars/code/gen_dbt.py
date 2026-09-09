"""Write the dbt models and schema.yml for us_dot_fars.

Generated from the architecture CSVs so the SQL cast list, the column order and
the documented descriptions cannot drift from the schema they implement.

Run: uv run python models/us_dot_fars/code/gen_dbt.py
"""

import json
from pathlib import Path

from common import ALL_TABLES, DATA_TABLES, REPO_ROOT, load_cols

MODELS = REPO_ROOT / "models" / "us_dot_fars"
DATASET = "us_dot_fars"

# 2024 is the last published year; the partition range runs five years past it
# per the house convention.
PARTITION_START, PARTITION_END = 1975, 2029

KEYS = {
    "crash": ["year", "state_id", "case_number"],
    "vehicle": ["year", "state_id", "case_number", "vehicle_number"],
    "person": [
        "year",
        "state_id",
        "case_number",
        "vehicle_number",
        "person_number",
    ],
}

# Measured over the full 1975-2024 corpus by verify_parquet.py. Every entry is a
# column the source only began publishing late in the span, or one that applies
# to a small subset of rows by construction, so its non-null share over the
# pooled table sits below the 0.05 floor of not_null_proportion_multiple_columns.
SPARSE_PATH = Path(__file__).resolve().parent / "architecture" / "sparse.json"
SPARSE = json.loads(SPARSE_PATH.read_text()) if SPARSE_PATH.exists() else {}

TABLE_DESCRIPTIONS = {
    "crash": """
      Um registro por acidente de trânsito com vítima fatal ocorrido em via
      pública dos Estados Unidos desde 1975, identificado pela combinação de ano,
      estado e número do caso. Traz data e hora, localização, características da
      via, condições de tempo e iluminação, contagens de veículos e pessoas
      envolvidas e o número de mortes. Fonte: Fatality Analysis Reporting System
      (FARS) da NHTSA, um censo e não uma amostra: entra no arquivo todo acidente
      em via pública que produza ao menos uma morte em até 30 dias. O arquivo
      nacional cobre os 50 estados e o Distrito de Columbia; Porto Rico e os
      demais territórios são publicados à parte e não estão incluídos. Duas
      ressalvas de comparabilidade ao longo do período: as colunas codificadas
      tiveram seus conjuntos de códigos redefinidos várias vezes, e por isso
      devem ser lidas na tabela dicionario pelo par (código, cobertura
      temporal); e um bloco de variáveis de via (velocidade regulamentada,
      número de faixas, alinhamento, perfil, pavimento e controle de tráfego)
      deixou de ser registrado por acidente em 2010 e passou a ser registrado por
      veículo, na tabela vehicle.
    """,
    "vehicle": """
      Um registro por veículo envolvido em acidente fatal, identificado pela
      combinação da chave do acidente com o número do veículo. Traz marca,
      modelo, ano-modelo, tipo de carroceria, velocidade estimada, ponto de
      impacto, extensão dos danos e o histórico e a habilitação do condutor.
      Fonte: Fatality Analysis Reporting System (FARS) da NHTSA. Liga-se a crash
      por year, state_id e case_number. A partir de 2010 esta tabela passou a
      concentrar também as variáveis de via antes registradas por acidente, nas
      colunas com prefixo vehicle_. As colunas codificadas devem ser lidas na
      tabela dicionario pelo par (código, cobertura temporal): o tipo de
      carroceria, por exemplo, teve onze conjuntos de códigos distintos entre
      1975 e 2024.
    """,
    "person": """
      Um registro por pessoa envolvida em acidente fatal, ocupante ou não do
      veículo, identificado pela chave do acidente somada ao número do veículo e
      ao número da pessoa. Traz idade, sexo, tipo de pessoa, posição no veículo,
      uso de retenção, gravidade da lesão, resultado do teste de alcoolemia e,
      para os falecidos, data e hora do óbito e o intervalo entre o acidente e a
      morte. Fonte: Fatality Analysis Reporting System (FARS) da NHTSA. Pedestres,
      ciclistas e demais não ocupantes recebem vehicle_number igual a 0 e não têm
      correspondência na tabela vehicle. A coluna blood_alcohol_content é
      derivada e harmoniza a mudança de escala da fonte em 2015, quando o código
      passou de concentração vezes 100 para concentração vezes 1000; o código
      original permanece em alcohol_test_result_code para preservar as sentinelas
      de teste recusado, não realizado e resultado desconhecido.
    """,
}

DICIONARIO_DESCRIPTION = """
  Registro de códigos das tabelas crash, vehicle e person, com o significado de
  cada código e o intervalo de anos em que ele vigorou. Construído a partir das
  definições PROC FORMAT publicadas pela própria NHTSA para 1975-2014 e das
  colunas de rótulo que acompanham cada variável codificada a partir de 2015. A
  coluna cobertura_temporal é essencial e não decorativa: quase toda variável do
  FARS teve seu conjunto de códigos redefinido ao menos uma vez ao longo do
  período, de modo que o mesmo código significa coisas diferentes em anos
  diferentes e um mapa único de código para rótulo estaria errado na maior parte
  do intervalo.
"""

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}


def write_model(table: str) -> None:
    cols = load_cols(table)
    casts = ",\n    ".join(CAST[c.bq_type].format(c=c.name) for c in cols)
    sql = f"""{{{{
    config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {PARTITION_START}, "end": {PARTITION_END}, "interval": 1}},
        }},
    )
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""
    (MODELS / f"{DATASET}__{table}.sql").write_text(sql)
    print(f"wrote {DATASET}__{table}.sql ({len(cols)} columns)")


def write_dicionario() -> None:
    sql = f"""{{{{
    config(
        schema="{DATASET}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
    safe_cast(id_tabela as string) id_tabela,
    safe_cast(nome_coluna as string) nome_coluna,
    safe_cast(chave as string) chave,
    safe_cast(cobertura_temporal as string) cobertura_temporal,
    safe_cast(valor as string) valor
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}} as t
"""
    (MODELS / f"{DATASET}__dicionario.sql").write_text(sql)
    print(f"wrote {DATASET}__dicionario.sql")


def _block(text: str, indent: int) -> str:
    """Render a description as a folded YAML block at the given indent."""
    pad = " " * indent
    words = " ".join(text.split()).split(" ")
    out, line = [], ""
    for w in words:
        if line and len(pad) + len(line) + 1 + len(w) > 78:
            out.append(pad + line)
            line = w
        else:
            line = w if not line else line + " " + w
    if line:
        out.append(pad + line)
    return "\n".join(out)


def write_schema() -> None:
    lines = ["---", "version: 2", "models:"]
    for table in DATA_TABLES:
        cols = load_cols(table)
        dict_cols = [c.name for c in cols if c.covered_by_dictionary]
        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: >-")
        lines.append(_block(TABLE_DESCRIPTIONS[table], 6))
        lines.append("    tests:")
        lines.append("      - dbt_utils.unique_combination_of_columns:")
        lines.append("          combination_of_columns:")
        for k in KEYS[table]:
            lines.append(f"            - {k}")
        lines.append("      - not_null_proportion_multiple_columns:")
        lines.append("          at_least: 0.05")
        if SPARSE.get(table):
            lines.append(
                "          # Published only over part of the 1975-2024 span, or"
            )
            lines.append(
                "          # populated for a small subset of rows by construction,"
            )
            lines.append(
                "          # so the pooled non-null share sits below the floor."
            )
            lines.append("          ignore_values:")
            for v in SPARSE[table]:
                lines.append(f"            - {v}")
        if dict_cols:
            lines.append("      - custom_dictionary_coverage:")
            lines.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            lines.append("          columns_covered_by_dictionary:")
            for c in dict_cols:
                lines.append(f"            - {c}")
        lines.append("    columns:")
        for c in cols:
            lines.append(f"      - name: {c.name}")
            lines.append("        description: >-")
            lines.append(_block(c.description_pt, 10))
            tests = ["not_null"] if c.name in KEYS[table] else []
            rels = []
            if c.name == "year":
                rels.append(("br_bd_diretorios_data_tempo__ano", "ano.ano"))
            elif c.name == "month":
                rels.append(("br_bd_diretorios_data_tempo__mes", "mes.mes"))
            elif c.name == "state_id":
                rels.append(("br_bd_diretorios_us__state", "id_state"))
            elif c.name == "case_number" and table != "crash":
                # The composite crash key is tested for referential integrity
                # through case_number, which only exists inside a state-year.
                rels.append((f"{DATASET}__crash", "case_number"))
            if tests or rels:
                lines.append("        tests:")
                for t in tests:
                    lines.append(f"          - {t}")
                for to, field in rels:
                    lines.append("          - relationships:")
                    lines.append(f"              to: ref('{to}')")
                    lines.append(f"              field: {field}")

    lines.append(f"  - name: {DATASET}__dicionario")
    lines.append("    description: >-")
    lines.append(_block(DICIONARIO_DESCRIPTION, 6))
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append("          combination_of_columns:")
    for k in ("id_tabela", "nome_coluna", "chave", "cobertura_temporal"):
        lines.append(f"            - {k}")
    lines.append("    columns:")
    for c in load_cols("dicionario"):
        lines.append(f"      - name: {c.name}")
        lines.append("        description: >-")
        lines.append(_block(c.description_pt, 10))
        lines.append("        tests:")
        lines.append("          - not_null")

    (MODELS / "schema.yml").write_text("\n".join(lines) + "\n")
    print(f"wrote schema.yml ({len(ALL_TABLES)} models)")


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in DATA_TABLES:
        write_model(table)
    write_dicionario()
    write_schema()


if __name__ == "__main__":
    main()
