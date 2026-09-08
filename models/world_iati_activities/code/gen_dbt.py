"""Write the dbt models and schema.yml for world_iati_activities.

Everything is derived from the architecture CSVs, so a column added there
appears in the model, in its cast, and in schema.yml without a second edit.

Run: ``python gen_dbt.py``
"""

import csv
import json

from common import ARCH_DIR, OUTPUT, REPO_ROOT

DATASET = "world_iati_activities"
MODEL_DIR = REPO_ROOT / "models" / DATASET

CAST = {
    "STRING": "string",
    "INT64": "int64",
    "FLOAT64": "float64",
    "DATE": "date",
    "DATETIME": "datetime",
    "BOOLEAN": "bool",
}

# Tables partitioned on `year`, and the logical key used for the uniqueness
# test. `_link` is unique within a run for every table that has one, which is
# what the test asserts; transaction_breakdown has no `_link`, so its key is the
# combination the source guarantees.
PARTITIONED = {
    "transaction",
    "transaction_breakdown",
    "budget",
    "planned_disbursement",
    "result_indicator_period",
}

UNIQUE_KEY = {
    "registry_dataset": ["registry_dataset_id"],
    "activity": ["activity_id"],
    "transaction": ["year", "transaction_id"],
    "transaction_breakdown": [
        "year",
        "transaction_id",
        "sector_code",
        "recipient_country_code",
        "recipient_region_code",
    ],
    "transaction_sector": ["transaction_sector_id"],
    "budget": ["year", "budget_id"],
    "planned_disbursement": ["year", "planned_disbursement_id"],
    "sector": ["activity_sector_id"],
    "recipient_country": ["activity_recipient_country_id"],
    "recipient_region": ["activity_recipient_region_id"],
    "participating_org": ["participating_org_id"],
    "related_activity": ["related_activity_id"],
    "policy_marker": ["policy_marker_id"],
    "document_link": ["document_link_id"],
    "location": ["location_id"],
    "result": ["result_id"],
    "result_indicator": ["result_indicator_id"],
    "result_indicator_period": ["year", "result_indicator_period_id"],
    "organisation": ["organisation_id"],
}

# Foreign keys inside this dataset. Directory columns in the architecture point
# at these; the relationships tests below are what actually enforce them.
FK = {
    "registry_dataset_id": "registry_dataset",
    "activity_id": "activity",
    "transaction_id": "transaction",
    "result_id": "result",
    "result_indicator_id": "result_indicator",
}

DESCRIPTION = {
    "registry_dataset": (
        "Conjuntos de dados registrados no Registro IATI, com a licença "
        "declarada por cada organização publicadora. É a tabela de "
        "proveniência e de licenciamento à qual todas as demais se ligam. "
        "Inclui os conjuntos não comerciais, cujas linhas foram removidas das "
        "demais tabelas, e os conjuntos que o Bulk Data Service não conseguiu "
        "baixar (is_downloaded falso), que não têm linhas em nenhuma outra "
        "tabela"
    ),
    "activity": (
        "Atividades de cooperação e ajuda internacional publicadas no padrão "
        "IATI. Uma linha por atividade. É a tabela de topo: todas as demais, "
        "exceto organisation e registry_dataset, ligam-se a ela por activity_id"
    ),
    "transaction": (
        "Transações financeiras declaradas em cada atividade, incluindo "
        "compromissos, desembolsos, gastos e reembolsos. Uma linha por "
        "transação. Particionada pelo ano de transaction_date"
    ),
    "transaction_breakdown": (
        "Decomposição de cada transação em partes proporcionais por setor e "
        "por destino geográfico, seguindo a metodologia do Country Development "
        "Finance Data. Uma linha por combinação de transação, setor, país e "
        "região. Particionada pelo ano de transaction_date"
    ),
    "transaction_sector": (
        "Setores declarados diretamente em cada transação, antes da "
        "decomposição proporcional. Uma linha por setor de cada transação"
    ),
    "budget": (
        "Orçamentos declarados em cada atividade, por período. Uma linha por "
        "período orçamentário. Particionada pelo ano de period_start_date"
    ),
    "planned_disbursement": (
        "Desembolsos planejados em cada atividade, por período. Uma linha por "
        "período. Particionada pelo ano de period_start_date"
    ),
    "sector": (
        "Setores atribuídos a cada atividade, com a parcela da atividade que "
        "cabe a cada um. Uma linha por setor de cada atividade"
    ),
    "recipient_country": (
        "Países receptores de cada atividade, com a parcela da atividade que "
        "cabe a cada um. Uma linha por país de cada atividade"
    ),
    "recipient_region": (
        "Regiões receptoras de cada atividade, com a parcela da atividade que "
        "cabe a cada uma. Uma linha por região de cada atividade"
    ),
    "participating_org": (
        "Organizações que participam de cada atividade e o papel de cada uma, "
        "entre financiador, responsável, extensor e executor. Uma linha por "
        "participação"
    ),
    "related_activity": (
        "Ligações declaradas entre atividades, como atividade-mãe, filha, irmã "
        "e cofinanciada. Uma linha por ligação. A atividade referenciada pode "
        "não existir nesta base"
    ),
    "policy_marker": (
        "Marcadores de política atribuídos a cada atividade, como igualdade de "
        "gênero e mitigação climática, com o grau em que são objetivo da "
        "atividade. Uma linha por marcador de cada atividade"
    ),
    "document_link": (
        "Documentos associados a cada atividade, com endereço, formato e "
        "título. Uma linha por documento. Os endereços são declarados pelo "
        "publicador e não são verificados"
    ),
    "location": (
        "Locais subnacionais associados a cada atividade, com coordenadas "
        "quando declaradas. Uma linha por local de cada atividade"
    ),
    "result": (
        "Resultados declarados em cada atividade, entre produto, efeito e "
        "impacto. Uma linha por resultado"
    ),
    "result_indicator": (
        "Indicadores de cada resultado. Uma linha por indicador"
    ),
    "result_indicator_period": (
        "Períodos de medição de cada indicador, com meta e valor efetivo. Uma "
        "linha por período. Particionada pelo ano de period_start_date"
    ),
    "organisation": (
        "Organizações que publicam arquivos de organização no padrão IATI, "
        "distintos dos arquivos de atividade. Uma linha por organização"
    ),
}


def load(table):
    with (ARCH_DIR / f"sheet_{table}.csv").open(encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def max_year(table):
    """The largest partition actually written, read from the parquet tree."""
    years = [
        int(p.name.split("=")[1])
        for p in (OUTPUT / table).iterdir()
        if p.is_dir() and p.name.startswith("year=")
    ]
    return max(years)


def write_model(table):
    cols = load(table)
    lines = []
    if table in PARTITIONED:
        end = max_year(table) + 5
        partition = (
            "        partition_by={\n"
            '            "field": "year",\n'
            '            "data_type": "int64",\n'
            f'            "range": {{"start": 0, "end": {end}, "interval": 1}},\n'
            "        },\n"
        )
    else:
        partition = ""
    lines.append("{{\n    config(\n")
    lines.append(f'        schema="{DATASET}",\n')
    lines.append(f'        alias="{table}",\n')
    lines.append('        materialized="table",\n')
    lines.append(partition)
    lines.append("    )\n}}\n\n\nselect\n")
    body = [
        f"    safe_cast({c['name']} as {CAST[c['bigquery_type']]}) {c['name']}"
        for c in cols
    ]
    lines.append(",\n".join(body))
    lines.append(
        f'\nfrom\n    {{{{ set_datalake_project("{DATASET}_staging.{table}") }}}}\n'
        "    as t\n"
    )
    path = MODEL_DIR / f"{DATASET}__{table}.sql"
    path.write_text("".join(lines), encoding="utf-8")
    return path


def yaml_quote(text):
    return text.replace('"', "'")


def write_schema(tables):
    out = ["---\n", "version: 2\n", "models:\n"]
    for table in tables:
        cols = load(table)
        partitioned = table in PARTITIONED
        # The wide, tall tables get their tests scoped to the most recent
        # partition: the null-proportion test compiles a scan of every column,
        # which is a full-table read on 24.6M rows otherwise.
        scope = (
            "        config:\n          where: __most_recent_year_en__\n"
            if partitioned
            else ""
        )
        out.append(f"  - name: {DATASET}__{table}\n")
        out.append("    description: >\n")
        out.append(f"      {DESCRIPTION[table]}\n")
        out.append("    tests:\n")
        out.append("      - dbt_utils.unique_combination_of_columns:\n")
        out.append(
            f"          combination_of_columns: [{', '.join(UNIQUE_KEY[table])}]\n"
        )
        if scope:
            out.append(scope)
        out.append("      - not_null_proportion_multiple_columns:\n")
        out.append("          at_least: 0.05\n")
        if scope:
            out.append(scope)
        out.append("    columns:\n")
        for c in cols:
            out.append(f"      - name: {c['name']}\n")
            out.append(
                f"        description: {yaml_quote(c['description_pt'])}\n"
            )
            tests = []
            if c["name"] in UNIQUE_KEY[table] or c["name"] == "year":
                tests.append("not_null")
            fk = FK.get(c["name"])
            # A table never points a relationships test at itself.
            if fk and fk != table:
                out.append("        tests:\n")
                for t in tests:
                    out.append(f"          - {t}\n")
                out.append("          - relationships:\n")
                out.append(f"              to: ref('{DATASET}__{fk}')\n")
                out.append(f"              field: {c['name']}\n")
                if partitioned:
                    out.append("              config:\n")
                    out.append(
                        "                where: __most_recent_year_en__\n"
                    )
            elif tests:
                out.append(f"        tests: [{', '.join(tests)}]\n")
    (MODEL_DIR / "schema.yml").write_text("".join(out), encoding="utf-8")


def main():
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    tables = [
        p.name[len("sheet_") : -4]
        for p in sorted(ARCH_DIR.glob("sheet_*.csv"))
    ]
    # registry_dataset first so ref() resolution reads naturally.
    tables.sort(key=lambda t: (t != "registry_dataset", t != "activity", t))
    for table in tables:
        print(write_model(table).name)
    write_schema(tables)
    print("schema.yml")
    print(json.dumps({"tables": len(tables)}))


if __name__ == "__main__":
    main()
