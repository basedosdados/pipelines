"""Emit the architecture TSVs for us_cfpb_complaints.

The architecture is the source of truth for column names, order, types, and the
raw -> clean name mapping. This script writes it; `common.py` reads it back.

Run: python models/us_cfpb_complaints/code/gen_architecture.py
"""

import csv
from pathlib import Path

CODE_DIR = Path(__file__).resolve().parent

FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]

TAXONOMY_NOTE = (
    "Taxonomia do formulário de reclamação revisada em abril de 2017 e em agosto "
    "de 2023; os valores sao preservados exatamente como publicados pelo CFPB, sem "
    "reclassificacao retroativa. A tabela dicionario registra a cobertura temporal "
    "observada de cada valor"
)

# name, type, description_pt, covered_by_dictionary, directory_column,
# measurement_unit, has_sensitive_data, observations, original_name
COMPLAINT = [
    (
        "year",
        "INT64",
        "Ano-calendário de recebimento da reclamação pelo CFPB",
        "no",
        "diretorios_data_tempo.ano:ano",
        "year",
        "no",
        "Coluna de particionamento; derivada de date_received",
        "Date received",
    ),
    (
        "complaint_id",
        "STRING",
        "Identificador único da reclamação atribuído pelo CFPB",
        "no",
        "",
        "",
        "no",
        "Chave lógica da tabela; numérico na fonte, mas sem significado aritmético",
        "Complaint ID",
    ),
    (
        "state_id",
        "STRING",
        "Código FIPS de duas posições do estado, território ou distrito de "
        "residência do consumidor",
        "no",
        "diretorios_us.state:id_state",
        "",
        "no",
        "Derivado de state_abbreviation pela tabela de estados do diretório "
        "diretorios_us; nulo para os códigos postais militares AA, AE e AP "
        "(4.327 registros) e para registros sem estado informado (62.611)",
        "State",
    ),
    (
        "date_received",
        "DATE",
        "Data em que o CFPB recebeu a reclamação",
        "no",
        "",
        "",
        "no",
        "",
        "Date received",
    ),
    (
        "date_sent_to_company",
        "DATE",
        "Data em que o CFPB encaminhou a reclamação à empresa",
        "no",
        "",
        "",
        "no",
        "Anterior a date_received em 7.050 registros (0,04%); inconsistência "
        "presente na fonte e preservada",
        "Date sent to company",
    ),
    (
        "product",
        "STRING",
        "Tipo de produto ou serviço financeiro identificado pelo consumidor",
        "yes",
        "",
        "",
        "no",
        TAXONOMY_NOTE,
        "Product",
    ),
    (
        "sub_product",
        "STRING",
        "Subtipo do produto ou serviço financeiro identificado pelo consumidor",
        "yes",
        "",
        "",
        "no",
        "Nem todo produto possui subtipo. " + TAXONOMY_NOTE,
        "Sub-product",
    ),
    (
        "issue",
        "STRING",
        "Problema relatado pelo consumidor",
        "yes",
        "",
        "",
        "no",
        "Os valores disponíveis dependem do produto. " + TAXONOMY_NOTE,
        "Issue",
    ),
    (
        "sub_issue",
        "STRING",
        "Detalhamento do problema relatado pelo consumidor",
        "yes",
        "",
        "",
        "no",
        "Nem todo problema possui detalhamento; os valores dependem do produto e "
        "do problema. " + TAXONOMY_NOTE,
        "Sub-issue",
    ),
    (
        "company_name",
        "STRING",
        "Nome da empresa objeto da reclamação, conforme registrado no CFPB",
        "no",
        "",
        "",
        "no",
        "8.101 nomes distintos; a grafia de uma mesma instituição varia ao longo "
        "do tempo e não há identificador de empresa na fonte",
        "Company",
    ),
    (
        "state_abbreviation",
        "STRING",
        "Sigla de duas letras do estado de residência do consumidor, conforme "
        "publicada pelo CFPB",
        "no",
        "",
        "",
        "no",
        "Preservada exatamente como publicada; inclui os códigos postais "
        "militares AA, AE e AP e o valor literal UNITED STATES MINOR OUTLYING "
        "ISLANDS, que não são siglas do diretório. Sem FK: a chave primária do "
        "diretório de estados é o código FIPS, não a sigla — use state_id",
        "State",
    ),
    (
        "zip_code",
        "STRING",
        "Código postal (ZIP code) de cinco dígitos informado pelo consumidor",
        "no",
        "",
        "",
        "no",
        "Mascarado pelo CFPB quando a área postal tem 20 mil habitantes ou menos: "
        "1.130.458 registros trazem apenas o prefixo de três dígitos seguido de "
        "XX e 138.969 vêm inteiramente mascarados como XXXXX; cerca de 65 "
        "registros trazem valores malformados presentes na fonte",
        "ZIP code",
    ),
    (
        "tags",
        "STRING",
        "Marcação atribuída pelo CFPB a reclamações de idosos ou de militares",
        "yes",
        "",
        "",
        "no",
        "Preenchida em 4,56% dos registros; um mesmo registro pode acumular as "
        "duas marcações, separadas por vírgula",
        "Tags",
    ),
    (
        "submitted_via",
        "STRING",
        "Canal pelo qual a reclamação foi enviada ao CFPB",
        "yes",
        "",
        "",
        "no",
        "",
        "Submitted via",
    ),
    (
        "company_public_response",
        "STRING",
        "Resposta pública opcional da empresa, escolhida entre opções "
        "pré-definidas pelo CFPB",
        "yes",
        "",
        "",
        "no",
        "Preenchida em 54,81% dos registros; publicada em até 180 dias quando a "
        "empresa opta por respondê-la",
        "Company public response",
    ),
    (
        "company_response_to_consumer",
        "STRING",
        "Forma de encerramento dada pela empresa à reclamação",
        "yes",
        "",
        "",
        "no",
        "",
        "Company response to consumer",
    ),
    (
        "timely_response",
        "STRING",
        "Indica se a empresa respondeu à reclamação dentro do prazo exigido pelo "
        "CFPB",
        "yes",
        "",
        "",
        "no",
        "Valores Yes e No preservados como publicados; não convertido para "
        "booleano",
        "Timely response?",
    ),
    (
        "consumer_complaint_narrative",
        "STRING",
        "Relato do consumidor em texto livre, publicado somente com "
        "consentimento do consumidor e após remoção de dados pessoais pelo CFPB",
        "no",
        "",
        "",
        "yes",
        "Presente em 3.847.965 registros (21,90%). O CFPB substitui os dados "
        "pessoais identificados por sequências de X. Texto reproduzido "
        "literalmente, sem edição adicional",
        "Consumer complaint narrative",
    ),
]

DICIONARIO = [
    (
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a chave e o valor se referem",
        "no",
        "",
        "",
        "no",
        "",
        "",
    ),
    (
        "nome_coluna",
        "STRING",
        "Nome da coluna à qual a chave e o valor se referem",
        "no",
        "",
        "",
        "no",
        "",
        "",
    ),
    (
        "chave",
        "STRING",
        "Valor da categoria conforme armazenado na coluna",
        "no",
        "",
        "",
        "no",
        "",
        "",
    ),
    (
        "cobertura_temporal",
        "STRING",
        "Intervalo de anos em que o valor foi observado na base",
        "no",
        "",
        "",
        "no",
        "Registra a vigência observada de cada valor da taxonomia, revisada em "
        "2017 e em 2023",
        "",
    ),
    (
        "valor",
        "STRING",
        "Rótulo da categoria conforme publicado pelo CFPB",
        "no",
        "",
        "",
        "no",
        "As categorias do CFPB já são publicadas como rótulos legíveis, de modo "
        "que valor reproduz chave; a informação adicional está em "
        "cobertura_temporal",
        "",
    ),
]

TABLES = {"complaint": COMPLAINT, "dicionario": DICIONARIO}


def write(table: str, rows: list[tuple]) -> Path:
    path = CODE_DIR / f"sheet_{table}.tsv"
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh, delimiter="\t", lineterminator="\n")
        w.writerow(FIELDS)
        for r in rows:
            (
                name,
                bqtype,
                desc,
                covered,
                directory,
                unit,
                sensitive,
                obs,
                original,
            ) = r
            assert not desc.endswith("."), (
                f"{name}: description ends with a period"
            )
            assert desc[0].isupper(), f"{name}: description not capitalised"
            w.writerow(
                [
                    name,
                    bqtype,
                    desc,
                    "",  # temporal_coverage: same as table
                    covered,
                    directory,
                    unit,
                    sensitive,
                    obs,
                    original,
                ]
            )
    return path


if __name__ == "__main__":
    for t, rows in TABLES.items():
        p = write(t, rows)
        print(f"wrote {p}  ({len(rows)} columns)")
