"""Write the dbt models and schema.yml for us_nih_reporter.

Generated from the architecture CSVs so the SQL cast list, the column order and
the documented descriptions cannot drift from the schema they are supposed to
implement.

Run: uv run python models/us_nih_reporter/code/gen_dbt.py
"""

from common import (
    ALL_TABLES,
    PARTITIONED_TABLES,
    REPO_ROOT,
    constants,
    load_cols,
)

MODELS = REPO_ROOT / "models" / "us_nih_reporter"
DATASET = "us_nih_reporter"

# FY2025 and CY2025 are the last years published; the partition range runs five
# years past that per the house convention.
PARTITION_RANGE = {
    "project": (1985, 2031),
    "project_abstract": (1985, 2031),
    "publication": (1980, 2031),
    "publication_link": (1980, 2031),
}

# The logical key of each table, used for the uniqueness test. Each was
# confirmed unique over the whole cleaned corpus by verify_parquet.py.
#
# patent_link carries the owner in its key because (patent_id,
# core_project_num) is NOT unique: 36 pairs appear twice, every one of them with
# the same patent title and two different owners — the same patent, supported by
# the same project, reported by two institutions. The published grain is the
# triple, and (patent_id, core_project_num, patent_org_name) is unique on all
# 92,936 rows.
KEYS = {
    "project": ["year", "application_id"],
    "project_abstract": ["year", "application_id"],
    "publication": ["year", "pmid"],
    "publication_link": ["year", "pmid", "core_project_num"],
    "patent_link": ["patent_id", "core_project_num", "patent_org_name"],
    "clinical_study_link": ["nct_id", "core_project_num"],
}

# Tables whose not_null_proportion_multiple_columns test is scoped to the most
# recent year. The test scans every column of the table at compile time, and an
# unscoped run over 2.95M project rows and 8 GB of abstract text is not worth
# its cost on every dbt invocation. verify_parquet.py confirms that no column of
# any of the four clears the 0.05 non-null floor only because of the scoping —
# in the FY2025/CY2025 partition every column is well above it, which is why
# there is no ignore_values list anywhere in this schema.
SCOPED_NULL_TEST = set(PARTITIONED_TABLES)

# Columns that carry a not_null test. This is the key minus the columns the
# source leaves blank — it is deliberately not derived from KEYS.
#
# patent_org_name participates in patent_link's uniqueness but is blank on 2,343
# of 92,936 rows (2.52%): NIH does not always name the patent's owner. The
# triple stays unique regardless — at most one row of each duplicated
# patent-project pair has a blank owner — so uniqueness is enforced and
# not_null is not.
NOT_NULL = {t: list(k) for t, k in KEYS.items()}
NOT_NULL["patent_link"] = ["patent_id", "core_project_num"]

TABLE_NAMES = {
    "project": ("Projeto", "Project", "Proyecto"),
    "project_abstract": (
        "Resumo do projeto",
        "Project abstract",
        "Resumen del proyecto",
    ),
    "publication": ("Publicação", "Publication", "Publicación"),
    "publication_link": (
        "Ligação projeto-publicação",
        "Project-publication link",
        "Enlace proyecto-publicación",
    ),
    "patent_link": (
        "Ligação projeto-patente",
        "Project-patent link",
        "Enlace proyecto-patente",
    ),
    "clinical_study_link": (
        "Ligação projeto-estudo clínico",
        "Project-clinical study link",
        "Enlace proyecto-estudio clínico",
    ),
}

TABLE_DESCRIPTIONS = {
    "project": """
      Um registro por solicitação financiada e ano FISCAL federal, desde o ano
      fiscal de 1985, com título, instituição beneficiária, pesquisadores
      principais, código de atividade, instituto administrador, painel de
      revisão e custo. O ano fiscal federal vai de 1 de outubro a 30 de setembro
      e é nomeado pelo ano em que termina; não é o ano-calendário. Fonte: NIH
      RePORTER ExPORTER. Além do NIH, os arquivos cobrem projetos financiados
      pela ACF, AHRQ, CDC, FDA, HRSA e Departamento de Assuntos de Veteranos,
      mas o custo só está disponível para NIH, CDC, FDA e ACF. A coluna
      core_project_num é o que liga esta tabela às tabelas publication_link,
      patent_link e clinical_study_link; as colunas de componente do número do
      projeto descrevem a concessão como ela está hoje e divergem do número em
      3,09% das linhas, porque o instituto administrador muda quando a concessão
      é transferida. Complementa us_treasury_usaspending, que traz a transação
      orçamentária federal sem o detalhe científico registrado aqui.
    """,
    "project_abstract": """
      Um registro por solicitação financiada e ano FISCAL federal com o resumo
      da pesquisa, ligado a project pelo par (year, application_id). Fonte: NIH
      RePORTER ExPORTER, que publica os resumos em arquivo separado do arquivo
      de projetos por causa do tamanho. Nas concessões o resumo é fornecido ao
      NIH pelo beneficiário; nem todo projeto tem resumo publicado.
    """,
    "publication": """
      Um registro por publicação e ano-CALENDÁRIO de divulgação do arquivo de
      origem, desde 1980, com título, periódico, autores, vínculo do primeiro
      autor e identificadores PubMed e PubMed Central. O ano desta tabela é o do
      arquivo de divulgação, e não o ano fiscal usado em project. Fonte: NIH
      RePORTER ExPORTER, que integra os dados de publicação a partir do PubMed,
      do PubMed Central e da base intramuros do NIH. A ligação aos projetos que
      financiaram a publicação está em publication_link.
    """,
    "publication_link": """
      Um registro por par de publicação e projeto de pesquisa citado como fonte
      de apoio, no formato longo, desde o ano-calendário de 1980. Liga
      publication a project: pmid identifica a publicação e core_project_num o
      projeto. Fonte: NIH RePORTER ExPORTER. A associação vem dos agradecimentos
      do artigo, anotados pelo PubMed, ou do sistema de submissão de manuscritos
      do NIH, e não identifica um ano do projeto nem um ano fiscal de
      financiamento.
    """,
    "patent_link": """
      Um registro por par de patente e projeto de pesquisa reconhecido como
      apoio ao seu desenvolvimento, no formato longo, com o título da patente e
      o nome de seu titular. Fonte: NIH RePORTER ExPORTER, que publica um único
      arquivo cobrindo todos os anos fiscais, e por isso esta tabela não é
      particionada por ano. A associação vem do sistema iEdison, em que as
      organizações beneficiárias reportam invenções. O registro é reconhecidamente
      incompleto: só constam patentes concedidas, não pedidos em andamento, e
      nem toda organização beneficiária cumpre a obrigação de reportar,
      sobretudo depois de encerrado o apoio do NIH. Patentes só são reportadas
      para projetos do NIH, não para os das demais agências presentes em project.
    """,
    "clinical_study_link": """
      Um registro por par de estudo clínico e projeto de pesquisa reconhecido
      como seu apoio, no formato longo, com o título e a situação do estudo.
      Fonte: NIH RePORTER ExPORTER, que publica um único arquivo cobrindo todos
      os anos fiscais, e por isso esta tabela não é particionada por ano. A
      associação vem do próprio ClinicalTrials.gov, que informa ao RePORTER os
      números de concessão declarados no registro do estudo, e não identifica um
      ano do projeto nem um ano fiscal de financiamento. A situação do estudo
      descreve o estágio em que ele se encontrava na data de extração do arquivo.
    """,
}

CAST = {
    "INT64": "safe_cast({c} as int64) {c}",
    "FLOAT64": "safe_cast({c} as float64) {c}",
    "STRING": "safe_cast({c} as string) {c}",
    "DATE": "safe_cast({c} as date) {c}",
    "DATETIME": "safe_cast({c} as datetime) {c}",
}


def write_model(table: str) -> None:
    cols = load_cols(table)
    casts = ",\n    ".join(CAST[c.bq_type].format(c=c.name) for c in cols)
    if table in PARTITIONED_TABLES:
        start, end = PARTITION_RANGE[table]
        config = f"""config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
        partition_by={{
            "field": "year",
            "data_type": "int64",
            "range": {{"start": {start}, "end": {end}, "interval": 1}},
        }},
    )"""
    else:
        config = f"""config(
        schema="{DATASET}",
        alias="{table}",
        materialized="table",
    )"""
    sql = f"""{{{{
    {config}
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}} as t
"""
    (MODELS / f"{DATASET}__{table}.sql").write_text(sql)
    print(f"wrote {DATASET}__{table}.sql ({len(cols)} columns)")


def _block(text: str, indent: int) -> str:
    """Render a description as a folded YAML block at the given indent."""
    pad = " " * indent
    words = " ".join(text.split())
    out, line = [], pad
    for w in words.split(" "):
        if len(line) + len(w) + 1 > 78 and line != pad:
            out.append(line)
            line = pad + w
        else:
            line = w if line == pad else line + " " + w
            if line == w:
                line = pad + w
    out.append(line)
    return "\n".join(out)


def write_schema() -> None:
    dict_test = constants.DICT_TEST_COLUMNS.value
    lines = ["---", "version: 2", "models:"]
    for table in ALL_TABLES:
        if table == "dicionario":
            continue
        cols = load_cols(table)
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
        if table in SCOPED_NULL_TEST:
            lines.append(
                "          # Scoped to the newest partition: the test scans"
            )
            lines.append(
                "          # every column, and the whole table is 2.95M project"
            )
            lines.append(
                "          # rows and 8 GB of abstract text. Every column clears"
            )
            lines.append(
                "          # the floor in that partition by a wide margin, so"
            )
            lines.append("          # there is no ignore_values list.")
            lines.append("          config:")
            lines.append("            where: __most_recent_year_en__")
        if dict_test.get(table):
            lines.append("      - custom_dictionary_coverage:")
            lines.append(
                f"          dictionary_model: ref('{DATASET}__dicionario')"
            )
            lines.append("          columns_covered_by_dictionary:")
            for c in dict_test[table]:
                lines.append(f"            - {c}")
            if table in SCOPED_NULL_TEST:
                lines.append("          config:")
                lines.append("            where: __most_recent_year_en__")
        lines.append("    columns:")
        for c in cols:
            lines.append(f"      - name: {c.name}")
            lines.append("        description: >-")
            lines.append(_block(c.description_pt, 10))
            tests = []
            if c.name in NOT_NULL[table]:
                tests.append("not_null")
            rels = []
            if c.name == "year":
                rels.append(("br_bd_diretorios_data_tempo__ano", "ano.ano"))
            if tests or rels:
                lines.append("        tests:")
                for t in tests:
                    lines.append(f"          - {t}")
                for to, field in rels:
                    lines.append("          - relationships:")
                    lines.append(f"              to: ref('{to}')")
                    lines.append(f"              field: {field}")

    # dicionario
    lines.append(f"  - name: {DATASET}__dicionario")
    lines.append("    description: >-")
    lines.append(
        _block(
            """
        Registro dos valores assumidos pelas colunas codificadas da tabela
        project, com a cobertura temporal de cada valor em anos fiscais. Cobre
        application_type e arra_funded a partir do dicionário de dados publicado
        pelo NIH, activity a partir do registro oficial de códigos de atividade,
        e administering_ic a partir da própria coluna ic_name dos dados. A coluna
        valor fica vazia nos códigos de atividade que o registro oficial não
        cobre, que são os de contratos, projetos intramuros e agências que não o
        NIH.
    """,
            6,
        )
    )
    lines.append("    tests:")
    lines.append("      - dbt_utils.unique_combination_of_columns:")
    lines.append("          combination_of_columns:")
    for k in ("id_tabela", "nome_coluna", "chave"):
        lines.append(f"            - {k}")
    lines.append("    columns:")
    for c in load_cols("dicionario"):
        lines.append(f"      - name: {c.name}")
        lines.append("        description: >-")
        lines.append(_block(c.description_pt, 10))
        if c.name in ("id_tabela", "nome_coluna", "chave"):
            lines.append("        tests: [not_null]")

    (MODELS / "schema.yml").write_text("\n".join(lines) + "\n")
    print(f"wrote schema.yml ({len(lines)} lines)")


def write_dicionario_model() -> None:
    cols = load_cols("dicionario")
    casts = ",\n    ".join(CAST[c.bq_type].format(c=c.name) for c in cols)
    sql = f"""{{{{
    config(
        schema="{DATASET}",
        alias="dicionario",
        materialized="table",
    )
}}}}


select
    {casts}
from {{{{ set_datalake_project("{DATASET}_staging.dicionario") }}}} as t
"""
    (MODELS / f"{DATASET}__dicionario.sql").write_text(sql)
    print(f"wrote {DATASET}__dicionario.sql")


def main() -> None:
    MODELS.mkdir(parents=True, exist_ok=True)
    for table in ALL_TABLES:
        if table == "dicionario":
            write_dicionario_model()
        else:
            write_model(table)
    write_schema()


if __name__ == "__main__":
    main()
