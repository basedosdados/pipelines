"""Write the dbt models and schema.yml for us_eia_electricity.

    python gen_dbt.py

Generated from the architecture CSVs so the SQL cast list, the column order and
the documented descriptions cannot drift from the schema they implement. The
null-proportion exemption lists come from ``measured.json``, written by
``verify_parquet.py --write-measured``, so they are measured over the parquet
rather than guessed.
"""

import json
import shutil
import subprocess
from pathlib import Path

from common import DATA_TABLES, REPO_ROOT, load_cols

MODELS = REPO_ROOT / "models" / "us_eia_electricity"
CODE_DIR = Path(__file__).resolve().parent
DATASET = "us_eia_electricity"

# 2026 is the last report year present; the partition range runs five years past
# it per the house convention.
PARTITION_START, PARTITION_END = 2001, 2031

# The natural key each table's uniqueness test asserts, and how much collision
# the source actually contains. A `None` allowance means the key is exact.
# Measured over the full corpus, not assumed:
#
# * plant is exactly unique on (year, plant_id) — 233,162 rows, 0 collisions.
# * generator collides exactly once in 627,957 rows: plant 1146 generator HMU in
#   2001 is filed twice. Dropping generator_status_group from the key would raise
#   that to 4, because three generators appear in two groups in the same year, so
#   the group belongs in the key.
# * generation_fuel collides on 40,743 of 3,592,515 rows (1.13%). These are
#   genuine repeated filings, not a transform artefact: adding
#   associated_combined_heat_power, fuel_type_code_agg and sector_id to the key
#   only brings it down to 0.72%. PUDL aggregates them; this dataset publishes
#   the microdata as filed and leaves them as separate rows.
# * fuel_receipts_costs has no published identifier for a delivery and two
#   identical deliveries in a month are a fact of the source, so it asserts
#   uniqueness over nothing.
KEYS = {
    "plant": (["year", "plant_id"], None),
    "generator": (
        ["year", "plant_id", "generator_id", "generator_status_group"],
        0.001,
    ),
    "generation_fuel": (
        [
            "year",
            "month",
            "plant_id",
            "energy_source_code",
            "prime_mover_code",
            "nuclear_unit_id",
        ],
        0.05,
    ),
    "fuel_receipts_costs": (None, None),
}

# Key columns that are actually non-null, so `not_null` asserts something rather
# than restating a fact the source contradicts. Measured, not assumed: EIA-923
# did not collect the prime mover until 2003, leaving it null on all 178,966
# rows of 2001 and 2002, and the energy source is null on a further 204 rows of
# the same era. nuclear_unit_id is populated only at nuclear plants by design,
# and a fuel delivery's plant and month, while always present, are not a key.
NOT_NULL_KEY_COLUMNS = {
    "plant": ["year", "plant_id"],
    "generator": [
        "year",
        "plant_id",
        "generator_id",
        "generator_status_group",
    ],
    "generation_fuel": ["year", "month", "plant_id"],
    "fuel_receipts_costs": ["year", "month", "plant_id"],
}


# Columns whose values are enumerated in the dicionario, taken from the
# architecture rather than listed twice.
def dictionary_columns(table: str) -> list[str]:
    return [c.name for c in load_cols(table) if c.covered_by_dictionary]


# Tables large enough that the unscoped null-proportion test is not worth its
# BigQuery scan. The macro reads every column of the model and runs at `dbt
# compile`, not only at `dbt test`.
SCOPED = {"generation_fuel", "fuel_receipts_costs"}

TABLE_DESCRIPTIONS = {
    "plant": """
      Um registro por usina de geração de eletricidade por ano-calendário,
      conforme o Anexo 2 do Formulário EIA-860 da U.S. Energy Information
      Administration: localização, empresa operadora, autoridade de
      balanceamento, situação regulatória, setor econômico e infraestrutura
      associada. plant_id é o identificador atribuído pela EIA e é a chave de
      junção estável entre todas as tabelas deste conjunto e entre os
      formulários EIA-860 e EIA-923; permanece com a usina através de mudanças
      de proprietário e de nome. O escopo do formulário cresceu ao longo do
      período — endereço passa a ser coletado em 2007, coordenadas e
      infraestrutura de gás em 2013 —, de modo que uma coluna vazia num ano
      antigo indica ausência de coleta e não ausência de informação; a cobertura
      de cada coluna está registrada em temporal_coverage.
    """,
    "generator": """
      Um registro por gerador por ano-calendário, conforme o Anexo 3 do
      Formulário EIA-860 da U.S. Energy Information Administration: capacidade
      nominal, de verão e de inverno, máquina primária, combustíveis, datas de
      entrada em operação e de aposentadoria, situação operacional e tecnologia
      de combustão. A chave é (year, plant_id, generator_id): generator_id é
      único apenas dentro de uma usina. A partir de 2009 o formulário separa
      geradores em operação, propostos e aposentados em planilhas distintas,
      unidas aqui em uma tabela só, com generator_status_group registrando a
      origem; antes de 2009 o grupo é derivado de operational_status_code pelo
      próprio vocabulário da EIA. Uma soma de capacity_mw sobre a tabela inteira
      sem filtrar generator_status_group soma capacidade instalada, proposta e
      aposentada no mesmo número.
    """,
    "generation_fuel": """
      Geração líquida de eletricidade e consumo de combustível por usina,
      combustível, máquina primária e mês, conforme a página 1 do Formulário
      EIA-923 da U.S. Energy Information Administration. A fonte publica esta
      página em formato largo, com doze colunas por medida e um total anual;
      aqui ela está em formato longo, uma linha por mês, e os totais anuais
      foram descartados por serem a soma das doze colunas mensais. Um par
      (linha, mês) em que todas as seis medidas são nulas é descartado, o que é o
      que faz um ano parcial funcionar: o arquivo do ano corrente traz doze
      colunas mas só reporta até o mês publicado. Um zero declarado é mantido,
      porque significa que a usina não queimou combustível naquele mês. As
      quantidades físicas de combustível estão em unidades que variam por
      combustível, indicadas em fuel_unit; para agregar entre combustíveis use as
      colunas em MMBtu. Respondentes anuais, identificados em
      reporting_frequency_code, declaram o total do ano na coluna de dezembro.
    """,
    "fuel_receipts_costs": """
      Uma linha por entrega de combustível a uma usina de geração, conforme o
      Anexo 2 Parte C do Formulário EIA-923 da U.S. Energy Information
      Administration: quantidade recebida, conteúdo energético, custo entregue,
      fornecedor, mina de origem, modo de transporte e teores de enxofre, cinzas,
      umidade, mercúrio e cloro. O custo é publicado pela fonte em centavos de
      dólar por MMBtu e está convertido para dólares em fuel_cost_per_mmbtu; é o
      custo entregue na usina, com transporte incluído, e está ausente onde a EIA
      o suprime por confidencialidade do respondente. Uma entrega não tem
      identificador publicado e duas entregas idênticas no mesmo mês são um fato
      da fonte, não um erro, de modo que a tabela não afirma unicidade sobre
      nenhuma combinação de colunas.
    """,
}

DICIONARIO_DESCRIPTION = """
  Registro dos valores assumidos pelas colunas codificadas das tabelas plant,
  generator, generation_fuel e fuel_receipts_costs, com o significado de cada
  código e a cobertura temporal em que ele aparece. Os rótulos vêm dos
  vocabulários publicados pela EIA na forma em que o projeto Public Utility Data
  Liberation os mantém. A coluna cobertura_temporal é o que expõe a deriva do
  vocabulário: máquinas primárias de armazenamento e eólica offshore, por
  exemplo, só aparecem décadas depois do resto da lista.
"""

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
}


def measured() -> dict:
    path = CODE_DIR / "measured.json"
    if not path.exists():
        raise SystemExit(
            "measured.json is missing — run verify_parquet.py --write-measured first, "
            "so the null-proportion exemptions are measured rather than guessed"
        )
    return json.loads(path.read_text())


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
    stats = measured()
    lines = ["---", "version: 2", "models:"]
    for table in DATA_TABLES:
        cols = load_cols(table)
        key, allowance = KEYS[table]
        scoped = table in SCOPED
        sparse = stats[table]["sparse_scope" if scoped else "sparse_full"]

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
                "            # most recent year on the two tables large enough for the"
            )
            lines.append("            # scan to matter.")
            lines.append("            where: __most_recent_year_en__")
        lines.append("          at_least: 0.05")
        if sparse:
            lines.append(
                "          # Measured over the parquet by verify_parquet.py: each of"
            )
            lines.append(
                "          # these is a field the form began collecting part-way"
            )
            lines.append(
                "          # through the record, or one that applies to a small"
            )
            lines.append("          # minority of rows by construction.")
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
    """Find a formatter: on PATH, else in pre-commit's own hook environment.

    The repo installs sqlfmt and yamlfix through pre-commit rather than as
    project dependencies, so they are usually not importable from the project
    venv and the pyenv shims on PATH are dead links.
    """

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
    """Run the repo's formatters over what was just generated.

    Both hooks run at commit time anyway: sqlfmt wraps any cast line past its
    column limit — the widest here is
    ``safe_cast(natural_gas_local_distribution_company as string)`` — and yamlfix
    collapses short YAML sequences to flow style. Without this step the
    generator and the hooks would fight: the generator would undo their
    formatting on every run and the committed files would never match what it
    produces.
    """
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
