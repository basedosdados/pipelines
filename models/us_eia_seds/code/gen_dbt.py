"""Write the dbt model and schema.yml for us_eia_seds.

    python gen_dbt.py

Generated from the architecture CSV so the SQL cast list, the column order and
the documented descriptions cannot drift from the schema they implement.
"""

import shutil
import subprocess
from pathlib import Path

from common import DATA_TABLES, REPO_ROOT, load_cols

MODELS = REPO_ROOT / "models" / "us_eia_seds"
DATASET = "us_eia_seds"

# 2024 is the latest report year; the partition range runs past it per the
# house convention (earliest .. latest + a few years of headroom).
PARTITION_START, PARTITION_END = 1960, 2030

# Natural key and its collision allowance. Measured over the full 2.57M-row
# corpus: (year, state_code, msn) is exactly unique — SEDS files exactly one
# value per series, state and year — so the key is exact.
KEYS = {"seds_consumption": (["year", "state_code", "msn"], None)}

# Key columns measured non-null. state_id is deliberately null for the non-state
# aggregates (US, X3, X5), so it is not asserted.
NOT_NULL_KEY_COLUMNS = {"seds_consumption": ["year", "state_code", "msn"]}

# 2.57M rows — large enough that the unscoped null-proportion test is not worth
# its BigQuery scan, so it is scoped to the most recent year. Every column is
# >=98% non-null over the corpus, so no ignore_values are needed.
SCOPED = {"seds_consumption"}

TABLE_DESCRIPTIONS = {
    "seds_consumption": """
      Uma linha por estado, ano e série do State Energy Data System (SEDS) da
      U.S. Energy Information Administration: o sistema completo de consumo,
      preço, despesa, emissões de CO2 e indicadores relacionados de energia por
      estado, cobrindo todas as fontes de energia de 1960 ao ano completo mais
      recente. A série é identificada pelo Mnemonic Series Name (MSN) de cinco
      caracteres, cujo significado completo está na tabela dicionario. A coluna
      value não carrega uma unidade única porque a unidade varia por série —
      consumo em Btu ou em unidades físicas, preço, despesa, emissões,
      capacidade —; a unidade de cada linha está em measurement_unit e a família
      da medida em measure_type. state_code cobre os 50 estados, o Distrito de
      Columbia, o total nacional (US) e as áreas federais offshore (X3, X5);
      state_id resolve o código FIPS apenas para os estados. Este conjunto é o
      lado da demanda por todas as fontes de energia, complementar ao us_eia_
      electricity (geração de eletricidade) e ao us_eia_consumption (vendas de
      eletricidade a consumidores finais).
    """,
}

DICIONARIO_DESCRIPTION = """
  Registro dos valores assumidos pelas colunas codificadas da tabela
  seds_consumption, com o significado de cada código e a cobertura temporal em
  que ele aparece. Inclui as descrições completas dos nomes mnemônicos de série
  (MSN) publicadas pela EIA, os códigos de estado e agregado, os tipos de medida
  e as safras dos dados. A coluna cobertura_temporal expõe quando cada série
  entra e sai do sistema ao longo do período de 1960 em diante.
"""

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}


def dictionary_columns(table: str) -> list[str]:
    return [c.name for c in load_cols(table) if c.covered_by_dictionary]


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
    pad = " " * indent
    words = " ".join(text.split())
    out, line = [], pad
    for word in words.split(" "):
        if len(line) + len(word) + 1 > 78 and line != pad:
            out.append(line)
            line = pad + word
        else:
            line = word if line == pad else line + " " + word
            if line == word:
                line = pad + word
    out.append(line)
    return "\n".join(out)


def write_schema() -> None:
    lines = ["---", "version: 2", "models:"]
    for table in DATA_TABLES:
        cols = load_cols(table)
        key, allowance = KEYS[table]
        scoped = table in SCOPED

        lines.append(f"  - name: {DATASET}__{table}")
        lines.append("    description: >-")
        lines.append(_block(TABLE_DESCRIPTIONS[table], 6))
        lines.append("    tests:")
        if key and allowance is None:
            lines.append("      - dbt_utils.unique_combination_of_columns:")
            lines.append("          combination_of_columns:")
            for column in key:
                lines.append(f"            - {column}")
        elif key:
            lines.append("      - custom_unique_combinations_of_columns:")
            lines.append("          combination_of_columns:")
            for column in key:
                lines.append(f"            - {column}")
            lines.append(f"          proportion_allowed_failures: {allowance}")
        lines.append("      - not_null_proportion_multiple_columns:")
        if scoped:
            lines.append("          config:")
            lines.append(
                "            # The macro reads every column of the model and runs at"
            )
            lines.append(
                "            # dbt compile, not only dbt test, so it is scoped to the"
            )
            lines.append(
                "            # most recent year on this table, which is large."
            )
            lines.append("            where: __most_recent_year_en__")
        lines.append("          at_least: 0.05")
        dict_cols = dictionary_columns(table)
        if dict_cols:
            lines.append("      - custom_dictionary_coverage:")
            lines.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            lines.append("          columns_covered_by_dictionary:")
            for column in dict_cols:
                lines.append(f"            - {column}")
        lines.append("    columns:")
        for col in cols:
            lines.append(f"      - name: {col.name}")
            lines.append("        description: >-")
            lines.append(_block(col.description_pt, 10))
            tests = []
            if col.name in NOT_NULL_KEY_COLUMNS[table]:
                tests.append("not_null")
            rels = []
            if col.name == "year":
                rels.append(("br_bd_diretorios_data_tempo__ano", "ano.ano"))
            elif col.name == "state_id":
                rels.append(("br_bd_diretorios_us__state", "id_state"))
            if tests or rels:
                lines.append("        tests:")
                for test in tests:
                    lines.append(f"          - {test}")
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
    for column in ("id_tabela", "nome_coluna", "chave"):
        lines.append(f"            - {column}")
    lines.append("    columns:")
    for name, desc in [
        ("id_tabela", "Nome da tabela a que a coluna codificada pertence"),
        ("nome_coluna", "Nome da coluna codificada"),
        ("chave", "Valor armazenado na coluna"),
        (
            "cobertura_temporal",
            "Anos em que o valor aparece, na notação início(intervalo)fim",
        ),
        ("valor", "Significado do valor armazenado"),
    ]:
        lines.append(f"      - name: {name}")
        lines.append(f"        description: {desc}")
        if name in ("id_tabela", "nome_coluna", "chave"):
            lines.append("        tests: [not_null]")
    (MODELS / "schema.yml").write_text("\n".join(lines) + "\n")
    print(f"wrote schema.yml ({len(lines)} lines)")


def _hook_binary(name: str) -> str | None:
    def works(path: str) -> bool:
        return (
            subprocess.run([path, "--version"], capture_output=True).returncode
            == 0
        )

    found = shutil.which(name)
    if found and works(found):
        return found
    pattern = f".cache/pre-commit/*/py_env-*/bin/{name}"
    for candidate in sorted(Path.home().glob(pattern)):
        if works(str(candidate)):
            return str(candidate)
    return None


def format_output() -> None:
    for name, target in (
        ("sqlfmt", MODELS),
        ("yamlfix", MODELS / "schema.yml"),
    ):
        binary = _hook_binary(name)
        if not binary:
            print(
                f"{name} not found — the commit hook will reformat the output"
            )
            continue
        subprocess.run([binary, str(target)], check=True, capture_output=True)
        print(f"{name} applied")


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in DATA_TABLES:
        write_model(table)
    write_dicionario()
    write_schema()
    format_output()


if __name__ == "__main__":
    main()
