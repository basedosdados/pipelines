"""Write the dbt models and schema.yml for us_eia_consumption.

    python gen_dbt.py

Generated from the architecture CSVs so the SQL cast list, the column order and
the documented descriptions cannot drift. The null-proportion exemptions are
measured from the cleaned parquet (union over each of the last three report
years) rather than guessed, so a column the form stopped collecting is exempted
where it is actually empty.
"""

import shutil
import subprocess
from pathlib import Path

import pyarrow.dataset as pads
from common import DATA_TABLES, OUTPUT, REPO_ROOT, load_cols

MODELS = REPO_ROOT / "models" / "us_eia_consumption"
DATASET = "us_eia_consumption"
PARTITION_START, PARTITION_END = 1990, 2031

# Natural key and collision allowance, measured over the full corpus:
# * utility is exactly unique on (year, utility_id).
# * retail_sales keys on (year, utility_id, state_id, part, service_type,
#   ba_code, customer_sector) — a utility files a row per balancing authority
#   from 2013, so ba_code belongs in the key; the residual 0.05% are genuine
#   repeated filings.
# * service_territory is exactly unique on (year, utility_id, state_id, county).
# * eia861m is exactly unique on (year, month, state_code, customer_sector).
KEYS = {
    "utility": (["year", "utility_id"], None),
    "retail_sales": (
        [
            "year",
            "utility_id",
            "state_id",
            "part",
            "service_type",
            "ba_code",
            "customer_sector",
        ],
        0.001,
    ),
    "service_territory": (
        ["year", "utility_id", "state_id", "county_name"],
        None,
    ),
    "eia861m": (["year", "month", "state_code", "customer_sector"], None),
}

NOT_NULL_KEY_COLUMNS = {
    "utility": ["year", "utility_id"],
    "retail_sales": [
        "year",
        "utility_id",
        "part",
        "service_type",
        "customer_sector",
    ],
    "service_territory": ["year", "utility_id", "state_id", "county_name"],
    "eia861m": ["year", "month", "state_code", "customer_sector"],
}

# Tables large enough that the unscoped null-proportion test is not worth its
# BigQuery scan (the macro reads every column at dbt compile).
SCOPED = {"retail_sales", "service_territory", "eia861m"}

TABLE_DESCRIPTIONS = {
    "utility": """
      Um registro por empresa de eletricidade por ano, conforme o quadro de
      empresas do Formulário EIA-861: nome, tipo de propriedade, unidade
      federativa e região de confiabilidade da NERC. utility_id é o identificador
      atribuído pela EIA e é a chave de junção estável com o conjunto
      us_eia_electricity e entre os formulários da EIA. O conjunto de empresas
      que preenchem esse quadro varia de ano para ano, de modo que a contagem de
      linhas por ano não é constante; isso reflete quem declarou, não uma falha
      de cobertura.
    """,
    "retail_sales": """
      Vendas de eletricidade a consumidores finais por empresa, unidade
      federativa, tipo de serviço e setor de consumo, conforme o Anexo 4 do
      Formulário EIA-861: receita, vendas em MWh, número de consumidores e um
      preço médio derivado. A fonte publica os setores em formato largo; aqui a
      tabela está em formato longo, uma linha por setor, e o setor Total foi
      descartado por ser a soma dos demais. Somar vendas ou receita sobre a
      coluna service_type inteira conta a mesma energia duas vezes: em mercados
      reestruturados a mesma energia aparece como Delivery (entregue pela
      distribuidora) e como Energy (vendida pela comercializadora). Para um total
      sem dupla contagem, some apenas Bundled e Energy, excluindo Delivery. O
      preço médio é derivado como receita dividida por vendas e é nulo onde não há
      vendas. A partir de 2013 uma empresa declara uma linha por autoridade de
      balanceamento (ba_code), o que faz parte da chave.
    """,
    "service_territory": """
      Um registro por empresa, ano e condado atendido, conforme o arquivo de
      território de serviço do Formulário EIA-861, publicado como arquivo próprio
      a partir de 2012: a empresa, o condado e se ela preencheu o formulário
      simplificado. county_id resolve o código FIPS do condado a partir do nome
      declarado, dentro da unidade federativa.
    """,
    "eia861m": """
      Vendas mensais de eletricidade a consumidores finais por unidade
      federativa e setor de consumo, conforme o Formulário EIA-861M: receita,
      vendas em MWh, número de consumidores e preço médio, de 1990 ao mês mais
      recente publicado. A fonte publica os setores em formato largo; aqui a
      tabela está em formato longo, uma linha por setor. O setor Other aparece
      apenas na série histórica de 1990 a 2009, quando a EIA o coletava como um
      setor residual; a partir de 2010 restam quatro setores. state_code cobre as
      unidades federativas, o total nacional e os territórios; state_id resolve o
      código FIPS apenas para as unidades federativas.
    """,
}

DICIONARIO_DESCRIPTION = """
  Registro dos valores assumidos pelas colunas codificadas das tabelas de vendas
  no varejo e de território de serviço, com o significado de cada código e a
  cobertura temporal em que ele aparece.
"""

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}


def dictionary_columns(table: str) -> list[str]:
    return [c.name for c in load_cols(table) if c.covered_by_dictionary]


def measured_sparse(table: str) -> list[str]:
    """Columns <5% non-null in any of the last three report years, from parquet.

    Matches the scoped null-proportion test's window: the test runs on the most
    recent year, and the exemption is the union over each recent year alone so a
    field the form dropped in the latest year is exempted where it is empty.
    """
    part = OUTPUT / table
    if not part.exists() or not any(part.rglob("*.parquet")):
        return []
    frame = pads.dataset(part, format="parquet").to_table().to_pandas()
    frame["_y"] = frame["year"].astype(int)
    recent = sorted(frame["_y"].unique())[-3:]
    sparse: set[str] = set()
    for y in recent:
        sub = frame[frame["_y"] == y]
        for c in sub.columns:
            if c in ("_y", "year"):
                continue
            if sub[c].notna().mean() < 0.05:
                sparse.add(c)
    return sorted(sparse)


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
        sparse = measured_sparse(table) if scoped else []

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
                "            # The macro reads every column at dbt compile, so it is"
            )
            lines.append(
                "            # scoped to the most recent year on the large tables."
            )
            lines.append("            where: __most_recent_year_en__")
        lines.append("          at_least: 0.05")
        if sparse:
            lines.append(
                "          # Measured over the parquet: each is a field the form"
            )
            lines.append(
                "          # collects for a minority of rows, or stopped collecting."
            )
            lines.append("          ignore_values:")
            for column in sparse:
                lines.append(f"            - {column}")
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
            elif col.name == "month":
                rels.append(("br_bd_diretorios_data_tempo__mes", "mes.mes"))
            elif col.name == "state_id":
                rels.append(("br_bd_diretorios_us__state", "id_state"))
            elif col.name == "county_id":
                rels.append(("br_bd_diretorios_us__county", "id_county"))
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
    for candidate in sorted(
        Path.home().glob(f".cache/pre-commit/*/py_env-*/bin/{name}")
    ):
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
