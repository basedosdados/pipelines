"""Generate the five us_sec_edgar architecture CSVs.

Schema columns (data-basis-style order), trilingual descriptions:
  name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
  directory_column, measurement_unit, has_sensitive_data, observations,
  original_name, description_en, description_es

Rule notes applied:
  - Data and documentation are English -> English column names; `year`/`quarter`,
    not `ano`/`trimestre`.
  - Descriptions are bare and carry no trailing period; value legends, ranges and
    caveats live in `observations`.
  - Type by arithmetic meaning: INT64/FLOAT64 only for real quantities, each with a
    measurement_unit. Numeric-looking codes (sic, cik, ein), sequence numbers
    (report, line) and 0/1 flags are STRING; coded ones take
    covered_by_dictionary=yes and their labels live in `dicionario`.
  - `value` mixes units per row -> measurement_unit blank + observations note; the
    real unit is in `unit_of_measure`.
  - `year`/`quarter` describe the RELEASE quarter (which quarterly ZIP the row came
    from), not the fiscal period being reported.
"""

import csv
import os

HERE = os.path.dirname(os.path.abspath(__file__))
HEADER = [
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
    "description_en",
    "description_es",
]


def col(
    name,
    bqtype,
    pt,
    en,
    es,
    *,
    dic="no",
    directory="",
    unit="",
    sensitive="no",
    obs="",
    original="",
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": bqtype,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dic,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": obs,
        "original_name": original,
        "description_en": en,
        "description_es": es,
    }


# --------------------------------------------------------------------------
# Partition columns, shared by the four data tables.
# --------------------------------------------------------------------------
RELEASE_OBS_PT = (
    "Refere-se ao trimestre de divulgação do conjunto de dados (o arquivo ZIP de "
    "origem), não ao período fiscal reportado. Um trimestre contém todas as "
    "submissões protocoladas nele, que podem reportar períodos de anos anteriores."
)


def partition_cols():
    return [
        col(
            "year",
            "INT64",
            "Ano de divulgação do conjunto de dados trimestral",
            "Year of the quarterly data set release",
            "Año de divulgación del conjunto de datos trimestral",
            unit="year",
            obs=RELEASE_OBS_PT,
            original="(derivado do nome do arquivo ZIP)",
        ),
        col(
            "quarter",
            "INT64",
            "Trimestre de divulgação do conjunto de dados",
            "Quarter of the data set release",
            "Trimestre de divulgación del conjunto de datos",
            unit="quarter",
            obs="Valores de 1 a 4. " + RELEASE_OBS_PT,
            original="(derivado do nome do arquivo ZIP)",
        ),
    ]


ACCESSION_PT = "Número de adesão (accession number) da submissão no EDGAR"
ACCESSION_EN = "EDGAR accession number of the submission"
ACCESSION_ES = (
    "Número de adhesión (accession number) de la presentación en EDGAR"
)
ACCESSION_OBS = (
    "Cadeia de 20 caracteres no formato nnnnnnnnnn-nn-nnnnnn. Chave primária de "
    "`submission` e chave estrangeira em `numeric_fact` e `presentation`."
)

TAG_PT = "Identificador do marcador (tag) da taxonomia XBRL"
TAG_EN = "Identifier of the XBRL taxonomy tag"
TAG_ES = "Identificador de la etiqueta (tag) de la taxonomía XBRL"

VERSION_PT = "Versão da taxonomia em que o marcador foi definido"
VERSION_EN = "Taxonomy version in which the tag was defined"
VERSION_ES = "Versión de la taxonomía en la que se definió la etiqueta"
VERSION_OBS = (
    "Para marcadores padrão, a taxonomia de origem (por exemplo us-gaap/2024); "
    "para marcadores customizados, o número de adesão da submissão que o definiu."
)


# --------------------------------------------------------------------------
# submission (sub.txt)
# --------------------------------------------------------------------------
SUBMISSION = [
    *partition_cols(),
    col(
        "accession_number",
        "STRING",
        ACCESSION_PT,
        ACCESSION_EN,
        ACCESSION_ES,
        obs=ACCESSION_OBS,
        original="adsh",
    ),
    col(
        "cik",
        "STRING",
        "Central Index Key (CIK) do registrante",
        "Central Index Key (CIK) of the registrant",
        "Central Index Key (CIK) del registrante",
        obs="Código de dez dígitos atribuído pela SEC a cada entidade que protocola "
        "documentos. Deveria referenciar um diretório de empresas dos EUA "
        "(br_bd_diretorios_us.company:cik), que ainda não existe.",
        original="cik",
    ),
    col(
        "company_name",
        "STRING",
        "Nome do registrante",
        "Name of the registrant",
        "Nombre del registrante",
        obs="Nome da pessoa jurídica conforme registrado no EDGAR na data do protocolo.",
        original="name",
    ),
    col(
        "sic",
        "STRING",
        "Código de classificação industrial padrão (SIC) do registrante",
        "Standard Industrial Classification (SIC) code of the registrant",
        "Código de clasificación industrial estándar (SIC) del registrante",
        directory="br_bd_diretorios_us.sic:id_sic",
        obs="Quatro dígitos, atribuído pela SEC na data do protocolo. Pode estar "
        "em qualquer nível da hierarquia SIC: NN00 é um grupo principal e NNN0 "
        "um grupo industrial.",
        original="sic",
    ),
    col(
        "ein",
        "STRING",
        "Número de identificação de empregador (EIN) do registrante",
        "Employer Identification Number (EIN) of the registrant",
        "Número de identificación de empleador (EIN) del registrante",
        obs="Identificador fiscal de nove dígitos atribuído pelo IRS. Público no EDGAR.",
        original="ein",
    ),
    col(
        "former_name",
        "STRING",
        "Nome anterior mais recente do registrante",
        "Most recent former name of the registrant",
        "Nombre anterior más reciente del registrante",
        original="former",
    ),
    col(
        "former_name_change_date",
        "DATE",
        "Data da mudança a partir do nome anterior",
        "Date of the change from the former name",
        "Fecha del cambio desde el nombre anterior",
        original="changed",
    ),
    col(
        "country_business",
        "STRING",
        "País do endereço comercial do registrante",
        "Country of the registrant's business address",
        "País de la dirección comercial del registrante",
        obs="Código ISO 3166-1 alfa-2.",
        original="countryba",
    ),
    col(
        "state_business",
        "STRING",
        "Estado ou província do endereço comercial do registrante",
        "State or province of the registrant's business address",
        "Estado o provincia de la dirección comercial del registrante",
        obs="Preenchido apenas quando o país é US ou CA; mistura códigos de estados "
        "norte-americanos e de províncias canadenses, por isso não é ligado ao "
        "diretório br_bd_diretorios_us.state.",
        original="stprba",
    ),
    col(
        "city_business",
        "STRING",
        "Cidade do endereço comercial do registrante",
        "City of the registrant's business address",
        "Ciudad de la dirección comercial del registrante",
        original="cityba",
    ),
    col(
        "zip_business",
        "STRING",
        "Código postal do endereço comercial do registrante",
        "Zip code of the registrant's business address",
        "Código postal de la dirección comercial del registrante",
        original="zipba",
    ),
    col(
        "address1_business",
        "STRING",
        "Primeira linha do logradouro do endereço comercial do registrante",
        "First line of the street of the registrant's business address",
        "Primera línea de la calle de la dirección comercial del registrante",
        original="bas1",
    ),
    col(
        "address2_business",
        "STRING",
        "Segunda linha do logradouro do endereço comercial do registrante",
        "Second line of the street of the registrant's business address",
        "Segunda línea de la calle de la dirección comercial del registrante",
        original="bas2",
    ),
    col(
        "phone_business",
        "STRING",
        "Telefone do endereço comercial do registrante",
        "Phone number of the registrant's business address",
        "Teléfono de la dirección comercial del registrante",
        original="baph",
    ),
    col(
        "country_mailing",
        "STRING",
        "País do endereço de correspondência do registrante",
        "Country of the registrant's mailing address",
        "País de la dirección postal del registrante",
        obs="Código ISO 3166-1 alfa-2.",
        original="countryma",
    ),
    col(
        "state_mailing",
        "STRING",
        "Estado ou província do endereço de correspondência do registrante",
        "State or province of the registrant's mailing address",
        "Estado o provincia de la dirección postal del registrante",
        obs="Preenchido apenas quando o país é US ou CA.",
        original="stprma",
    ),
    col(
        "city_mailing",
        "STRING",
        "Cidade do endereço de correspondência do registrante",
        "City of the registrant's mailing address",
        "Ciudad de la dirección postal del registrante",
        original="cityma",
    ),
    col(
        "zip_mailing",
        "STRING",
        "Código postal do endereço de correspondência do registrante",
        "Zip code of the registrant's mailing address",
        "Código postal de la dirección postal del registrante",
        original="zipma",
    ),
    col(
        "address1_mailing",
        "STRING",
        "Primeira linha do logradouro do endereço de correspondência do registrante",
        "First line of the street of the registrant's mailing address",
        "Primera línea de la calle de la dirección postal del registrante",
        original="mas1",
    ),
    col(
        "address2_mailing",
        "STRING",
        "Segunda linha do logradouro do endereço de correspondência do registrante",
        "Second line of the street of the registrant's mailing address",
        "Segunda línea de la calle de la dirección postal del registrante",
        original="mas2",
    ),
    col(
        "country_incorporation",
        "STRING",
        "País de constituição do registrante",
        "Country of incorporation of the registrant",
        "País de constitución del registrante",
        obs="Código ISO 3166-1 alfa-2 nos dados, embora a documentação da SEC indique "
        "alfa-3.",
        original="countryinc",
    ),
    col(
        "state_incorporation",
        "STRING",
        "Estado ou província de constituição do registrante",
        "State or province of incorporation of the registrant",
        "Estado o provincia de constitución del registrante",
        obs="Preenchido apenas quando o país de constituição é US ou CA; mistura "
        "códigos de estados norte-americanos e de províncias canadenses.",
        original="stprinc",
    ),
    col(
        "filer_status",
        "STRING",
        "Situação do registrante perante a SEC no momento da submissão",
        "Filer status with the SEC at the time of submission",
        "Situación del registrante ante la SEC al momento de la presentación",
        dic="yes",
        obs="1-LAF, 2-ACC, 3-SRA, 4-NON, 5-SML; nulo quando não atribuído.",
        original="afs",
    ),
    col(
        "well_known_seasoned_issuer",
        "STRING",
        "Indicador de emissor sazonal amplamente conhecido (WKSI)",
        "Well Known Seasoned Issuer (WKSI) indicator",
        "Indicador de emisor estacional ampliamente conocido (WKSI)",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso.",
        original="wksi",
    ),
    col(
        "fiscal_year_end",
        "STRING",
        "Fim do exercício fiscal do registrante",
        "Fiscal year end of the registrant",
        "Fin del ejercicio fiscal del registrante",
        obs="Mês e dia no formato mmdd, arredondado para o fim do mês; não é uma data "
        "completa, por isso é STRING.",
        original="fye",
    ),
    col(
        "form",
        "STRING",
        "Tipo de formulário da submissão",
        "Form type of the submission",
        "Tipo de formulario de la presentación",
        obs="Por exemplo 10-K, 10-Q, 20-F, 40-F, 8-K, S-1. O próprio código é o nome "
        "canônico do formulário, por isso não é coberto por dicionário.",
        original="form",
    ),
    col(
        "period",
        "DATE",
        "Data do balanço patrimonial da submissão",
        "Balance sheet date of the submission",
        "Fecha del balance de la presentación",
        obs="Arredondada para o fim do mês.",
        original="period",
    ),
    col(
        "fiscal_year",
        "INT64",
        "Ano fiscal de referência da submissão",
        "Fiscal year focus of the submission",
        "Año fiscal de referencia de la presentación",
        unit="year",
        original="fy",
    ),
    col(
        "fiscal_period",
        "STRING",
        "Período fiscal de referência dentro do ano fiscal",
        "Fiscal period focus within the fiscal year",
        "Período fiscal de referencia dentro del año fiscal",
        dic="yes",
        obs="FY, Q1, Q2, Q3, Q4; os dados também trazem CY, H1, H2, T1 e M9. "
        "Rótulos em `dicionario`.",
        original="fp",
    ),
    col(
        "filed_date",
        "DATE",
        "Data do protocolo do registrante junto à SEC",
        "Date the registrant filed with the Commission",
        "Fecha de la presentación del registrante ante la SEC",
        original="filed",
    ),
    col(
        "accepted_datetime",
        "DATETIME",
        "Data e hora de aceitação do protocolo pela SEC",
        "Date and time the filing was accepted by the Commission",
        "Fecha y hora de aceptación de la presentación por la SEC",
        original="accepted",
    ),
    col(
        "previous_report",
        "STRING",
        "Indicador de que a submissão foi posteriormente aditada",
        "Indicator that the submission was subsequently amended",
        "Indicador de que la presentación fue posteriormente enmendada",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso.",
        original="prevrpt",
    ),
    col(
        "detail",
        "STRING",
        "Indicador de que a submissão XBRL contém divulgações quantitativas detalhadas",
        "Indicator that the XBRL submission contains detailed quantitative disclosures",
        "Indicador de que la presentación XBRL contiene divulgaciones cuantitativas detalladas",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso. Refere-se a notas explicativas e anexos no "
        "nível de detalhe exigido.",
        original="detail",
    ),
    col(
        "instance",
        "STRING",
        "Nome do documento de instância XBRL submetido",
        "Name of the submitted XBRL instance document",
        "Nombre del documento de instancia XBRL presentado",
        obs="Frequentemente começa pelo símbolo de negociação (ticker) da empresa.",
        original="instance",
    ),
    col(
        "quantity_ciks",
        "INT64",
        "Número de registrantes incluídos na submissão consolidada",
        "Number of registrants included in the consolidated submission",
        "Número de registrantes incluidos en la presentación consolidada",
        unit="registrant",
        original="nciks",
    ),
    col(
        "additional_ciks",
        "STRING",
        "CIKs adicionais dos corregistrantes incluídos na submissão",
        "Additional CIKs of the co-registrants included in the submission",
        "CIKs adicionales de los corregistrantes incluidos en la presentación",
        obs="Separados por espaço; nulo quando quantity_ciks é 1. Para poucos "
        "protocolos a lista completa não cabe no campo.",
        original="aciks",
    ),
]


# --------------------------------------------------------------------------
# numeric_fact (num.txt)
# --------------------------------------------------------------------------
NUMERIC_FACT = [
    *partition_cols(),
    col(
        "accession_number",
        "STRING",
        ACCESSION_PT,
        ACCESSION_EN,
        ACCESSION_ES,
        obs=ACCESSION_OBS,
        original="adsh",
    ),
    col(
        "tag",
        "STRING",
        TAG_PT,
        TAG_EN,
        TAG_ES,
        obs="Junto com version, referencia a tabela `tag`.",
        original="tag",
    ),
    col(
        "version",
        "STRING",
        VERSION_PT,
        VERSION_EN,
        VERSION_ES,
        obs=VERSION_OBS,
        original="version",
    ),
    col(
        "period_end_date",
        "DATE",
        "Data final do período a que o valor se refere",
        "End date of the period the value refers to",
        "Fecha final del período al que se refiere el valor",
        obs="Arredondada para o fim do mês.",
        original="ddate",
    ),
    col(
        "quantity_quarters",
        "INT64",
        "Número de trimestres cobertos pelo valor",
        "Number of quarters covered by the value",
        "Número de trimestres cubiertos por el valor",
        unit="quarter",
        obs="0 indica um valor pontual (estoque); 1 indica um trimestre, 4 um ano.",
        original="qtrs",
    ),
    col(
        "unit_of_measure",
        "STRING",
        "Unidade de medida do valor",
        "Unit of measure of the value",
        "Unidad de medida del valor",
        obs="Por exemplo USD, EUR, shares, pure. Rótulos já legíveis, por isso não "
        "coberto por dicionário.",
        original="uom",
    ),
    col(
        "segments",
        "STRING",
        "Eixos e membros XBRL que qualificam o valor",
        "XBRL axes and members qualifying the value",
        "Ejes y miembros XBRL que califican el valor",
        obs="Pares eixo=membro separados por ponto e vírgula; nulo para o valor não "
        "dimensional.",
        original="segments",
    ),
    col(
        "coregistrant",
        "STRING",
        "Corregistrante, empresa controladora ou outra entidade a que o valor se refere",
        "Co-registrant, parent company or other entity the value refers to",
        "Corregistrante, empresa matriz u otra entidad a la que se refiere el valor",
        obs="Nulo indica a entidade consolidada.",
        original="coreg",
    ),
    col(
        "value",
        "FLOAT64",
        "Valor numérico reportado",
        "Reported numeric value",
        "Valor numérico reportado",
        obs="A unidade varia por linha e está em unit_of_measure, por isso "
        "measurement_unit fica em branco. Conforme protocolado, sem escala, "
        "limitado a quatro casas decimais.",
        original="value",
    ),
    col(
        "footnote",
        "STRING",
        "Texto das notas de rodapé associadas ao valor",
        "Text of the footnotes attached to the value",
        "Texto de las notas al pie asociadas al valor",
        obs="Truncado em 512 caracteres.",
        original="footnote",
    ),
]


# --------------------------------------------------------------------------
# tag (tag.txt)
# --------------------------------------------------------------------------
TAG = [
    *partition_cols(),
    col(
        "tag",
        "STRING",
        TAG_PT,
        TAG_EN,
        TAG_ES,
        obs="Único dentro de uma versão de taxonomia.",
        original="tag",
    ),
    col(
        "version",
        "STRING",
        VERSION_PT,
        VERSION_EN,
        VERSION_ES,
        obs=VERSION_OBS,
        original="version",
    ),
    col(
        "custom",
        "STRING",
        "Indicador de marcador customizado pelo declarante",
        "Indicator of a tag customized by the filer",
        "Indicador de etiqueta personalizada por el declarante",
        dic="yes",
        obs="1 se customizado (version igual ao número de adesão), 0 se padrão.",
        original="custom",
    ),
    col(
        "abstract",
        "STRING",
        "Indicador de que o marcador não representa um fato numérico",
        "Indicator that the tag does not represent a numeric fact",
        "Indicador de que la etiqueta no representa un hecho numérico",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso. Quando 1, datatype, balance e period_type "
        "são nulos.",
        original="abstract",
    ),
    col(
        "datatype",
        "STRING",
        "Tipo de dado do marcador",
        "Data type of the tag",
        "Tipo de dato de la etiqueta",
        dic="yes",
        obs="Por exemplo monetary, shares, percent, perShare, pure.",
        original="datatype",
    ),
    col(
        "period_type",
        "STRING",
        "Natureza temporal do valor do marcador",
        "Temporal nature of the tag's value",
        "Naturaleza temporal del valor de la etiqueta",
        dic="yes",
        obs="I para valor pontual (instant), D para duração.",
        original="iord",
    ),
    col(
        "balance",
        "STRING",
        "Saldo contábil natural do marcador monetário",
        "Natural accounting balance of the monetary tag",
        "Saldo contable natural de la etiqueta monetaria",
        dic="yes",
        obs="C para crédito, D para débito; nulo quando não definido ou quando o "
        "marcador não é monetário.",
        original="crdr",
    ),
    col(
        "label",
        "STRING",
        "Rótulo de documentação do marcador",
        "Documentation label of the tag",
        "Etiqueta de documentación de la etiqueta",
        obs="Texto da taxonomia para marcadores padrão; texto do declarante para "
        "marcadores customizados.",
        original="tlabel",
    ),
    col(
        "documentation",
        "STRING",
        "Definição detalhada do marcador",
        "Detailed definition of the tag",
        "Definición detallada de la etiqueta",
        original="doc",
    ),
]


# --------------------------------------------------------------------------
# presentation (pre.txt)
# --------------------------------------------------------------------------
PRESENTATION = [
    *partition_cols(),
    col(
        "accession_number",
        "STRING",
        ACCESSION_PT,
        ACCESSION_EN,
        ACCESSION_ES,
        obs=ACCESSION_OBS,
        original="adsh",
    ),
    col(
        "report",
        "STRING",
        "Identificador do relatório dentro das demonstrações financeiras",
        "Identifier of the report within the financial statements",
        "Identificador del informe dentro de los estados financieros",
        obs="Número sequencial, correspondente ao arquivo R publicado no EDGAR. "
        "Ordenar exige safe_cast(report as int64).",
        original="report",
    ),
    col(
        "line",
        "STRING",
        "Ordem de apresentação da linha dentro do relatório",
        "Presentation order of the line within the report",
        "Orden de presentación de la línea dentro del informe",
        obs="Número sequencial. Ordenar exige safe_cast(line as int64).",
        original="line",
    ),
    col(
        "statement",
        "STRING",
        "Demonstração financeira a que o relatório pertence",
        "Financial statement the report belongs to",
        "Estado financiero al que pertenece el informe",
        dic="yes",
        obs="BS, IS, CF, EQ, CI, SI ou UN; os dados também trazem CP (página de "
        "rosto), valor ausente da documentação da SEC. Rótulos em `dicionario`.",
        original="stmt",
    ),
    col(
        "parenthetical",
        "STRING",
        "Indicador de valor apresentado entre parênteses no corpo da demonstração",
        "Indicator of a value presented parenthetically in the statement",
        "Indicador de valor presentado entre paréntesis en el estado financiero",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso.",
        original="inpth",
    ),
    col(
        "render_file",
        "STRING",
        "Tipo de arquivo de dados interativos renderizado no EDGAR",
        "Type of interactive data file rendered on EDGAR",
        "Tipo de archivo de datos interactivos renderizado en EDGAR",
        dic="yes",
        obs="H para arquivo .htm, X para arquivo .xml.",
        original="rfile",
    ),
    col(
        "tag",
        "STRING",
        TAG_PT,
        TAG_EN,
        TAG_ES,
        obs="Marcador escolhido pelo declarante para esta linha.",
        original="tag",
    ),
    col(
        "version",
        "STRING",
        VERSION_PT,
        VERSION_EN,
        VERSION_ES,
        obs=VERSION_OBS,
        original="version",
    ),
    col(
        "preferred_label",
        "STRING",
        "Texto apresentado na linha da demonstração financeira",
        "Text presented on the financial statement line",
        "Texto presentado en la línea del estado financiero",
        obs="Também conhecido como rótulo preferencial (preferred label).",
        original="plabel",
    ),
    col(
        "negating",
        "STRING",
        "Indicador de que o rótulo preferencial inverte o sinal do valor",
        "Indicator that the preferred label negates the value",
        "Indicador de que la etiqueta preferencial invierte el signo del valor",
        dic="yes",
        obs="1 se verdadeiro, 0 se falso.",
        original="negating",
    ),
]


# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------
DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a coluna pertence",
        "Name of the table the column belongs to",
        "Nombre de la tabla a la que pertenece la columna",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna codificada",
        "Name of the coded column",
        "Nombre de la columna codificada",
    ),
    col(
        "chave",
        "STRING",
        "Valor codificado",
        "Coded value",
        "Valor codificado",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do mapeamento",
        "Temporal coverage of the mapping",
        "Cobertura temporal del mapeo",
    ),
    col(
        "valor",
        "STRING",
        "Rótulo legível correspondente à chave",
        "Human-readable label corresponding to the key",
        "Etiqueta legible correspondiente a la clave",
    ),
]


TABLES = {
    "submission": SUBMISSION,
    "numeric_fact": NUMERIC_FACT,
    "tag": TAG,
    "presentation": PRESENTATION,
    "dicionario": DICIONARIO,
}


def main():
    for name, rows in TABLES.items():
        path = os.path.join(HERE, f"{name}.csv")
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=HEADER)
            w.writeheader()
            w.writerows(rows)
        print(f"{name}.csv: {len(rows)} columns")


if __name__ == "__main__":
    main()
