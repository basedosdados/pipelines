"""Column definitions for us_cms_hcris — the schema source of truth in code.

``gen_architecture.py`` renders these into ``architecture/*.csv``, which every
other step reads: the cleaning transform takes column order from them,
``gen_dbt.py`` writes the dbt models and ``schema.yml``, and ``register.py``
builds the backend column payloads. Edit the definitions **here**, never the
CSVs.

``Column.measurementUnit`` is free text on the backend, not a foreign key into
its 65-slug unit vocabulary, so the units here name what is actually being
counted rather than being rounded to the nearest listed slug: ``usd``, ``day``
and ``year`` exist in that vocabulary, while ``bed``, ``bed_day`` and
``discharge`` do not and are written anyway. Verified on the staging backend --
all six read back unchanged. Ratios carry no unit at all.
"""

from dataclasses import dataclass

from measures import MEASURES, provenance

USD = "usd"


@dataclass
class Col:
    """One architecture row.

    Args:
        name: BigQuery column name.
        type: ``bigquery_type``.
        en: English description.
        pt: Portuguese description.
        es: Spanish description.
        original: Source field name, or ``@`` plus a word for a derived column.
        unit: Measurement unit slug, from the backend vocabulary.
        dictionary: Whether the values are codes resolved by ``dicionario``.
        directory: Directory foreign key, ``<dataset>.<table>:<column>``.
        obs: English observation; the other two languages are translated in
            ``gen_architecture.py`` from the parallel dictionaries there.
        obs_pt: Portuguese observation.
        obs_es: Spanish observation.
        coverage: Temporal coverage notation; empty means same as the table.
    """

    name: str
    type: str
    en: str
    pt: str
    es: str
    original: str
    unit: str = ""
    dictionary: bool = False
    directory: str = ""
    obs: str = ""
    obs_pt: str = ""
    obs_es: str = ""
    coverage: str = ""


YEAR = Col(
    "year",
    "INT64",
    "Calendar year the reporting period's fiscal year ends in",
    "Ano-calendário em que termina o exercício fiscal do período do relatório",
    "Año calendario en que termina el ejercicio fiscal del período del informe",
    "@partition",
    unit="year",
    directory="br_bd_diretorios_data_tempo.ano:ano",
    obs=(
        "Partition column, taken from fiscal_year_end_date. It is NOT a "
        "calendar year of activity and NOT the federal fiscal year of the CMS "
        "extract the report was published in: hospital fiscal years do not "
        "align to the calendar and vary by hospital, so a report partitioned "
        "to 2023 may cover any twelve months ending in 2023. Use "
        "fiscal_year_begin_date and fiscal_year_end_date for the real period"
    ),
    obs_pt=(
        "Coluna de particionamento, derivada de fiscal_year_end_date. NÃO é um "
        "ano-calendário de atividade nem o ano fiscal federal do extrato da CMS "
        "em que o relatório foi publicado: os exercícios fiscais hospitalares "
        "não coincidem com o calendário e variam por hospital, portanto um "
        "relatório particionado em 2023 pode cobrir quaisquer doze meses "
        "encerrados em 2023. Use fiscal_year_begin_date e fiscal_year_end_date "
        "para o período real"
    ),
    obs_es=(
        "Columna de particionamiento, derivada de fiscal_year_end_date. NO es "
        "un año calendario de actividad ni el año fiscal federal del extracto "
        "de CMS en que se publicó el informe: los ejercicios fiscales "
        "hospitalarios no coinciden con el calendario y varían por hospital, "
        "por lo que un informe particionado en 2023 puede cubrir cualesquiera "
        "doce meses terminados en 2023. Use fiscal_year_begin_date y "
        "fiscal_year_end_date para el período real"
    ),
)

REPORT_ID = Col(
    "report_id",
    "STRING",
    "HCRIS-assigned identifier of the cost report",
    "Identificador do relatório de custos atribuído pelo HCRIS",
    "Identificador del informe de costos asignado por el HCRIS",
    "RPT_REC_NUM",
    obs=(
        "CMS restarted this sequence when it introduced Form CMS-2552-10, so "
        "it is unique only WITHIN a form version: 107 ids are reused between "
        "the two forms, for unrelated hospitals and unrelated years. The key "
        "of every table here is therefore (form_version, report_id), and a "
        "join on report_id alone silently conflates those 107 pairs. STRING "
        "because it is an identifier, not a quantity"
    ),
    obs_pt=(
        "A CMS reiniciou esta sequência ao introduzir o formulário CMS-2552-10, "
        "portanto ela é única apenas DENTRO de uma versão do formulário: 107 "
        "identificadores são reutilizados entre os dois formulários, para "
        "hospitais e anos sem relação entre si. A chave de todas as tabelas "
        "deste conjunto é, portanto, (form_version, report_id), e uma junção "
        "apenas por report_id funde silenciosamente esses 107 pares. STRING "
        "porque é um identificador, não uma quantidade"
    ),
    obs_es=(
        "CMS reinició esta secuencia al introducir el formulario CMS-2552-10, "
        "por lo que es única solo DENTRO de una versión del formulario: 107 "
        "identificadores se reutilizan entre ambos formularios, para hospitales "
        "y años sin relación entre sí. La clave de todas las tablas de este "
        "conjunto es, por tanto, (form_version, report_id), y una unión solo "
        "por report_id fusiona silenciosamente esos 107 pares. STRING porque es "
        "un identificador, no una cantidad"
    ),
)

PROVIDER_CCN = Col(
    "provider_ccn",
    "STRING",
    "CMS Certification Number (CCN) of the Medicare provider",
    "CMS Certification Number (CCN) do prestador Medicare",
    "CMS Certification Number (CCN) del proveedor de Medicare",
    "PRVDR_NUM",
    obs=(
        "Six characters, documented by CMS as two digits of SSA state code "
        "followed by a four-digit facility range that identifies the facility "
        "type. The stable identifier of a hospital across years and across "
        "every other CMS dataset. STRING because it is an identifier, and "
        "because leading zeros are significant"
    ),
    obs_pt=(
        "Seis caracteres, documentados pela CMS como dois dígitos de código "
        "estadual da SSA seguidos de uma faixa de quatro dígitos que "
        "identifica o tipo de estabelecimento. É o identificador estável de um "
        "hospital ao longo dos anos e nos demais conjuntos da CMS. STRING "
        "porque é um identificador e porque os zeros à esquerda são "
        "significativos"
    ),
    obs_es=(
        "Seis caracteres, documentados por CMS como dos dígitos de código "
        "estatal de la SSA seguidos de un rango de cuatro dígitos que "
        "identifica el tipo de establecimiento. Es el identificador estable de "
        "un hospital a lo largo de los años y en los demás conjuntos de CMS. "
        "STRING porque es un identificador y porque los ceros a la izquierda "
        "son significativos"
    ),
)

STATE_ID = Col(
    "state_id",
    "STRING",
    "Two-digit FIPS code of the state the hospital is in",
    "Código FIPS de duas posições do estado onde o hospital está localizado",
    "Código FIPS de dos dígitos del estado donde se ubica el hospital",
    "@state_id",
    directory="br_bd_diretorios_us.state:id_state",
    obs=(
        "Resolved from the SSA state code in the first two characters of the "
        "CCN, using the SSA-to-state table CMS publishes as "
        "HCRIS_STATE_CODES.csv, then mapped to FIPS through "
        "br_bd_diretorios_us.state. Complete for every report except the "
        "handful filed under SSA codes 00 (UNKNOWN) and 99 (Other). Preferred "
        "over the address typed on Worksheet S-2, which is free text"
    ),
    obs_pt=(
        "Resolvido a partir do código estadual da SSA nos dois primeiros "
        "caracteres do CCN, usando a tabela SSA-estado que a CMS publica como "
        "HCRIS_STATE_CODES.csv, e então mapeado para FIPS via "
        "br_bd_diretorios_us.state. Completo para todos os relatórios, exceto "
        "os poucos apresentados sob os códigos SSA 00 (UNKNOWN) e 99 (Other). "
        "Preferível ao endereço digitado na planilha S-2, que é texto livre"
    ),
    obs_es=(
        "Resuelto a partir del código estatal de la SSA en los dos primeros "
        "caracteres del CCN, usando la tabla SSA-estado que CMS publica como "
        "HCRIS_STATE_CODES.csv, y luego mapeado a FIPS mediante "
        "br_bd_diretorios_us.state. Completo para todos los informes, salvo "
        "los pocos presentados bajo los códigos SSA 00 (UNKNOWN) y 99 (Other). "
        "Preferible a la dirección escrita en la hoja S-2, que es texto libre"
    ),
)

STATE_ABBREVIATION = Col(
    "state_abbreviation",
    "STRING",
    "Two-letter postal abbreviation of the state the hospital is in",
    "Sigla postal de duas letras do estado onde o hospital está localizado",
    "Sigla postal de dos letras del estado donde se ubica el hospital",
    "@state_abbreviation",
    obs="Resolved alongside state_id; see that column",
    obs_pt="Resolvida junto com state_id; ver aquela coluna",
    obs_es="Resuelta junto con state_id; ver esa columna",
)

FORM_VERSION = Col(
    "form_version",
    "STRING",
    "CMS cost report form the report was filed on",
    "Formulário de relatório de custos da CMS em que o relatório foi apresentado",
    "Formulario de informe de costos de CMS en que se presentó el informe",
    "@form",
    dictionary=True,
    obs=(
        "2552-96 or 2552-10. The forms are renumbered throughout, so the same "
        "measure sits in different worksheet cells in each, and column numbers "
        "are four characters wide under 2552-96 and five under 2552-10. The "
        "two overlap for fiscal years around 2010 and 2011"
    ),
    obs_pt=(
        "2552-96 ou 2552-10. Os formulários foram renumerados por completo, "
        "portanto a mesma medida ocupa células diferentes em cada um, e os "
        "números de coluna têm quatro caracteres no 2552-96 e cinco no "
        "2552-10. Os dois se sobrepõem nos exercícios fiscais de 2010 e 2011"
    ),
    obs_es=(
        "2552-96 o 2552-10. Los formularios fueron renumerados por completo, "
        "por lo que la misma medida ocupa celdas distintas en cada uno, y los "
        "números de columna tienen cuatro caracteres en el 2552-96 y cinco en "
        "el 2552-10. Ambos se superponen en los ejercicios fiscales de 2010 y "
        "2011"
    ),
)


def _date(
    name: str,
    en: str,
    pt: str,
    es: str,
    original: str,
    obs: str = "",
    obs_pt: str = "",
    obs_es: str = "",
) -> Col:
    """Build a DATE column. CMS writes every date as MM/DD/YYYY; the cleaning
    transform rewrites them to ISO so ``safe_cast`` can read them."""
    return Col(
        name,
        "DATE",
        en,
        pt,
        es,
        original,
        obs=obs,
        obs_pt=obs_pt,
        obs_es=obs_es,
    )


FISCAL_YEAR_BEGIN = _date(
    "fiscal_year_begin_date",
    "First day of the cost reporting period",
    "Primeiro dia do período do relatório de custos",
    "Primer día del período del informe de costos",
    "FY_BGN_DT",
    obs=(
        "Hospital fiscal years do not align to the calendar year and differ "
        "between hospitals. Always read the period from this column and "
        "fiscal_year_end_date rather than treating year as a calendar year"
    ),
    obs_pt=(
        "Os exercícios fiscais hospitalares não coincidem com o ano-calendário "
        "e diferem entre hospitais. Leia sempre o período nesta coluna e em "
        "fiscal_year_end_date, em vez de tratar year como ano-calendário"
    ),
    obs_es=(
        "Los ejercicios fiscales hospitalarios no coinciden con el año "
        "calendario y difieren entre hospitales. Lea siempre el período en "
        "esta columna y en fiscal_year_end_date, en lugar de tratar year como "
        "año calendario"
    ),
)

FISCAL_YEAR_END = _date(
    "fiscal_year_end_date",
    "Last day of the cost reporting period",
    "Último dia do período do relatório de custos",
    "Último día del período del informe de costos",
    "FY_END_DT",
    obs="The year partition is taken from this date",
    obs_pt="A partição year é derivada desta data",
    obs_es="La partición year se deriva de esta fecha",
)

FISCAL_YEAR_DAYS = Col(
    "fiscal_year_days",
    "INT64",
    "Length of the cost reporting period in days, both endpoints included",
    "Duração do período do relatório de custos em dias, incluindo ambos os extremos",
    "Duración del período del informe de costos en días, incluidos ambos extremos",
    "@days",
    unit="day",
    obs=(
        "Usually around 365, but short and long periods are common and "
        "legitimate: a hospital changing its fiscal year, or changing owner "
        "mid-year, files a period of a few weeks or of more than a year. Any "
        "aggregation of flow measures across hospitals should weight by this "
        "column rather than assume a full year"
    ),
    obs_pt=(
        "Normalmente em torno de 365, mas períodos curtos e longos são comuns "
        "e legítimos: um hospital que muda seu exercício fiscal, ou que muda "
        "de proprietário no meio do ano, apresenta um período de algumas "
        "semanas ou de mais de um ano. Qualquer agregação de medidas de fluxo "
        "entre hospitais deve ponderar por esta coluna em vez de supor um ano "
        "completo"
    ),
    obs_es=(
        "Normalmente alrededor de 365, pero los períodos cortos y largos son "
        "comunes y legítimos: un hospital que cambia su ejercicio fiscal, o "
        "que cambia de propietario a mitad de año, presenta un período de unas "
        "semanas o de más de un año. Cualquier agregación de medidas de flujo "
        "entre hospitales debe ponderar por esta columna en lugar de suponer "
        "un año completo"
    ),
)

REPORT_STATUS = Col(
    "report_status_code",
    "STRING",
    "Status of the cost report",
    "Situação do relatório de custos",
    "Situación del informe de costos",
    "RPT_STUS_CD",
    dictionary=True,
    obs=(
        "CMS publishes only the highest status it holds for a report: where "
        "both an as-submitted and a settled report exist, only the settled one "
        "appears. Codes are documented in HCRIS_DataDictionary.csv"
    ),
    obs_pt=(
        "A CMS publica apenas a situação mais alta que possui para um "
        "relatório: quando existem tanto um relatório como apresentado quanto "
        "um homologado, apenas o homologado aparece. Os códigos estão "
        "documentados em HCRIS_DataDictionary.csv"
    ),
    obs_es=(
        "CMS publica solo la situación más alta que tiene para un informe: "
        "cuando existen tanto un informe como presentado como uno homologado, "
        "solo aparece el homologado. Los códigos están documentados en "
        "HCRIS_DataDictionary.csv"
    ),
)

REPORT_COLS = [
    YEAR,
    REPORT_ID,
    PROVIDER_CCN,
    STATE_ID,
    STATE_ABBREVIATION,
    Col(
        "npi",
        "STRING",
        "National Provider Identifier of the hospital",
        "National Provider Identifier do hospital",
        "National Provider Identifier del hospital",
        "NPI",
        obs=(
            "The HIPAA provider identifier. Frequently blank: CMS records it "
            "as optional on the report record, and it is absent for most "
            "reports filed before it came into use"
        ),
        obs_pt=(
            "Identificador de prestador da HIPAA. Frequentemente vazio: a CMS "
            "o registra como opcional no registro do relatório e ele está "
            "ausente na maioria dos relatórios anteriores ao seu uso"
        ),
        obs_es=(
            "Identificador de proveedor de HIPAA. Frecuentemente vacío: CMS lo "
            "registra como opcional en el registro del informe y está ausente "
            "en la mayoría de los informes anteriores a su uso"
        ),
    ),
    FORM_VERSION,
    Col(
        "source_extract_year",
        "INT64",
        "US federal fiscal year of the CMS extract the report was published in",
        "Ano fiscal federal dos EUA do extrato da CMS em que o relatório foi publicado",
        "Año fiscal federal de EE. UU. del extracto de CMS en que se publicó el informe",
        "@extract",
        unit="year",
        obs=(
            "Which quarterly extract file the report came from, not the "
            "period it covers. CMS assigns a report to the extract of the "
            "federal fiscal year its own fiscal year ends in, so one extract "
            "feeds two or three year partitions and one partition is fed by "
            "two or three extracts"
        ),
        obs_pt=(
            "De qual arquivo de extrato trimestral o relatório veio, não o "
            "período que ele cobre. A CMS atribui um relatório ao extrato do "
            "ano fiscal federal em que termina seu próprio exercício fiscal, "
            "de modo que um extrato alimenta duas ou três partições year e uma "
            "partição é alimentada por dois ou três extratos"
        ),
        obs_es=(
            "De qué archivo de extracto trimestral proviene el informe, no el "
            "período que cubre. CMS asigna un informe al extracto del año "
            "fiscal federal en que termina su propio ejercicio fiscal, de modo "
            "que un extracto alimenta dos o tres particiones year y una "
            "partición es alimentada por dos o tres extractos"
        ),
    ),
    Col(
        "provider_control_type_code",
        "STRING",
        "Type of ownership and control the hospital operates under",
        "Tipo de propriedade e controle sob o qual o hospital opera",
        "Tipo de propiedad y control bajo el que opera el hospital",
        "PRVDR_CTRL_TYPE_CD",
        dictionary=True,
        obs=(
            "The thirteen codes of Worksheet S-2 Part I line 21, defined in "
            "CMS Publication 15-2 section 4004.1. The same value is also "
            "filed on the worksheet itself and is reachable through "
            "report_value at worksheet S200001 line 02100"
        ),
        obs_pt=(
            "Os treze códigos da linha 21 da planilha S-2 Parte I, definidos "
            "na seção 4004.1 da Publicação 15-2 da CMS. O mesmo valor também é "
            "declarado na própria planilha e pode ser acessado em report_value "
            "na planilha S200001, linha 02100"
        ),
        obs_es=(
            "Los trece códigos de la línea 21 de la hoja S-2 Parte I, "
            "definidos en la sección 4004.1 de la Publicación 15-2 de CMS. El "
            "mismo valor también se declara en la propia hoja y es accesible "
            "en report_value en la hoja S200001, línea 02100"
        ),
    ),
    REPORT_STATUS,
    FISCAL_YEAR_BEGIN,
    FISCAL_YEAR_END,
    FISCAL_YEAR_DAYS,
    _date(
        "process_date",
        "Date the cost report was processed into HCRIS",
        "Data em que o relatório de custos foi processado no HCRIS",
        "Fecha en que el informe de costos se procesó en el HCRIS",
        "PROC_DT",
    ),
    Col(
        "initial_report_indicator",
        "STRING",
        "Whether this is the first cost report filed for the provider",
        "Indica se este é o primeiro relatório de custos apresentado pelo prestador",
        "Indica si este es el primer informe de costos presentado por el proveedor",
        "INITL_RPT_SW",
        dictionary=True,
        obs="CMS notes this switch is not actively used, so it is usually N",
        obs_pt="A CMS observa que este indicador não é usado ativamente, portanto é normalmente N",
        obs_es="CMS señala que este indicador no se usa activamente, por lo que normalmente es N",
    ),
    Col(
        "last_report_indicator",
        "STRING",
        "Whether this is the final cost report filed for the provider",
        "Indica se este é o último relatório de custos apresentado pelo prestador",
        "Indica si este es el último informe de costos presentado por el proveedor",
        "LAST_RPT_SW",
        dictionary=True,
        obs="CMS notes this switch is not actively used, so it is usually N",
        obs_pt="A CMS observa que este indicador não é usado ativamente, portanto é normalmente N",
        obs_es="CMS señala que este indicador no se usa activamente, por lo que normalmente es N",
    ),
    Col(
        "transmittal_number",
        "STRING",
        "Transmittal version of the form used to create the cost report",
        "Versão de transmissão do formulário usada para criar o relatório de custos",
        "Versión de transmisión del formulario usada para crear el informe de costos",
        "TRNSMTL_NUM",
    ),
    Col(
        "fiscal_intermediary_number",
        "STRING",
        "Number of the fiscal intermediary or Medicare Administrative Contractor",
        "Número do intermediário fiscal ou Medicare Administrative Contractor",
        "Número del intermediario fiscal o Medicare Administrative Contractor",
        "FI_NUM",
        obs="In effect at the time the cost report was filed",
        obs_pt="Vigente no momento em que o relatório de custos foi apresentado",
        obs_es="Vigente en el momento en que se presentó el informe de costos",
    ),
    Col(
        "adr_vendor_code",
        "STRING",
        "Automated desk review vendor used by the fiscal intermediary",
        "Fornecedor de revisão automatizada usado pelo intermediário fiscal",
        "Proveedor de revisión automatizada usado por el intermediario fiscal",
        "ADR_VNDR_CD",
        dictionary=True,
    ),
    _date(
        "fiscal_intermediary_create_date",
        "Date the fiscal intermediary created the HCRIS file",
        "Data em que o intermediário fiscal criou o arquivo HCRIS",
        "Fecha en que el intermediario fiscal creó el archivo HCRIS",
        "FI_CREAT_DT",
    ),
    Col(
        "utilization_code",
        "STRING",
        "Level of Medicare utilization of the filed cost report",
        "Nível de utilização do Medicare no relatório de custos apresentado",
        "Nivel de utilización de Medicare en el informe de costos presentado",
        "UTIL_CD",
        dictionary=True,
        obs=(
            "Low and no-utilization filers complete a much reduced form, so "
            "most measures in hospital_financial are null for them. Filter on "
            "this column before comparing hospitals"
        ),
        obs_pt=(
            "Prestadores de baixa ou nenhuma utilização preenchem um "
            "formulário bastante reduzido, de modo que a maioria das medidas "
            "em hospital_financial é nula para eles. Filtre por esta coluna "
            "antes de comparar hospitais"
        ),
        obs_es=(
            "Los prestadores de baja o nula utilización completan un "
            "formulario mucho más reducido, por lo que la mayoría de las "
            "medidas en hospital_financial son nulas para ellos. Filtre por "
            "esta columna antes de comparar hospitales"
        ),
    ),
    _date(
        "notice_of_program_reimbursement_date",
        "Date the provider received the Notice of Program Reimbursement",
        "Data em que o prestador recebeu o Notice of Program Reimbursement",
        "Fecha en que el proveedor recibió el Notice of Program Reimbursement",
        "NPR_DT",
    ),
    Col(
        "special_indicator",
        "STRING",
        "HCRIS code used for special purposes",
        "Código do HCRIS usado para fins especiais",
        "Código del HCRIS usado para fines especiales",
        "SPEC_IND",
        obs="CMS documents no value list for this field",
        obs_pt="A CMS não documenta uma lista de valores para este campo",
        obs_es="CMS no documenta una lista de valores para este campo",
    ),
    _date(
        "fiscal_intermediary_receipt_date",
        "Date the cost report was received by the fiscal intermediary",
        "Data em que o relatório de custos foi recebido pelo intermediário fiscal",
        "Fecha en que el informe de costos fue recibido por el intermediario fiscal",
        "FI_RCPT_DT",
    ),
]


REPORT_VALUE_COLS = [
    YEAR,
    REPORT_ID,
    PROVIDER_CCN,
    FORM_VERSION,
    Col(
        "worksheet_code",
        "STRING",
        "HCRIS code of the worksheet the cell belongs to",
        "Código HCRIS da planilha à qual a célula pertence",
        "Código HCRIS de la hoja a la que pertenece la celda",
        "WKSHT_CD",
        obs=(
            "Seven characters, e.g. S300001 for Worksheet S-3 Part I and "
            "G300000 for Worksheet G-3. The full list is in "
            "HOSP2010_Worksheet Codes.pdf, bundled with this table's "
            "auxiliary files"
        ),
        obs_pt=(
            "Sete caracteres, por exemplo S300001 para a planilha S-3 Parte I "
            "e G300000 para a planilha G-3. A lista completa está em "
            "HOSP2010_Worksheet Codes.pdf, incluído nos arquivos auxiliares "
            "desta tabela"
        ),
        obs_es=(
            "Siete caracteres, por ejemplo S300001 para la hoja S-3 Parte I y "
            "G300000 para la hoja G-3. La lista completa está en "
            "HOSP2010_Worksheet Codes.pdf, incluido en los archivos auxiliares "
            "de esta tabla"
        ),
    ),
    Col(
        "line_number",
        "STRING",
        "Line number of the cell on the worksheet",
        "Número da linha da célula na planilha",
        "Número de línea de la celda en la hoja",
        "LINE_NUM",
        obs=(
            "Five characters, xxxyy, where xxx is the line and yy the "
            "sub-line: line 1 is 00100 and line 1.01 is 00101. CMS subscripts "
            "lines for repeated cost centres, so a measure often spans a run "
            "of lines rather than one. Under CMS Form 2552-96 the line number "
            "of a cost-center-coded line was replaced by the cost centre code "
            "itself; under 2552-10 it is the line as reported, and the cost "
            "centre code is in the alpha value of worksheet A000000 column "
            "00000 on the same line. STRING because leading zeros are "
            "significant and arithmetic on a line number is meaningless"
        ),
        obs_pt=(
            "Cinco caracteres, xxxyy, em que xxx é a linha e yy a sublinha: a "
            "linha 1 é 00100 e a linha 1.01 é 00101. A CMS subscreve linhas "
            "para centros de custo repetidos, portanto uma medida costuma "
            "abranger um intervalo de linhas em vez de uma só. No formulário "
            "CMS 2552-96, o número da linha de uma linha codificada por centro "
            "de custo foi substituído pelo próprio código do centro de custo; "
            "no 2552-10 é a linha tal como declarada, e o código do centro de "
            "custo está no valor alfanumérico da planilha A000000, coluna "
            "00000, na mesma linha. STRING porque os zeros à esquerda são "
            "significativos e aritmética sobre um número de linha não tem "
            "sentido"
        ),
        obs_es=(
            "Cinco caracteres, xxxyy, donde xxx es la línea y yy la sublínea: "
            "la línea 1 es 00100 y la línea 1.01 es 00101. CMS subscribe "
            "líneas para centros de costo repetidos, por lo que una medida "
            "suele abarcar un rango de líneas en vez de una sola. En el "
            "formulario CMS 2552-96 el número de línea de una línea codificada "
            "por centro de costo fue reemplazado por el propio código del "
            "centro de costo; en el 2552-10 es la línea tal como se declara, y "
            "el código del centro de costo está en el valor alfanumérico de la "
            "hoja A000000, columna 00000, en la misma línea. STRING porque los "
            "ceros a la izquierda son significativos y la aritmética sobre un "
            "número de línea no tiene sentido"
        ),
    ),
    Col(
        "column_number",
        "STRING",
        "Column number of the cell on the worksheet",
        "Número da coluna da célula na planilha",
        "Número de columna de la celda en la hoja",
        "CLMN_NUM",
        obs=(
            "Five characters under CMS Form 2552-10 and four under 2552-96, a "
            "difference CMS documents in HCRIS_DataDictionary.csv. Column 1 is "
            "00100 on the newer form and 0100 on the older one, so a query "
            "spanning both form versions must give both literals. STRING "
            "because leading zeros are significant and arithmetic on a column "
            "number is meaningless"
        ),
        obs_pt=(
            "Cinco caracteres no formulário CMS 2552-10 e quatro no 2552-96, "
            "diferença que a CMS documenta em HCRIS_DataDictionary.csv. A "
            "coluna 1 é 00100 no formulário mais novo e 0100 no mais antigo, "
            "portanto uma consulta que abranja as duas versões deve informar "
            "ambos os literais. STRING porque os zeros à esquerda são "
            "significativos e aritmética sobre um número de coluna não tem "
            "sentido"
        ),
        obs_es=(
            "Cinco caracteres en el formulario CMS 2552-10 y cuatro en el "
            "2552-96, diferencia que CMS documenta en "
            "HCRIS_DataDictionary.csv. La columna 1 es 00100 en el formulario "
            "más nuevo y 0100 en el más antiguo, por lo que una consulta que "
            "abarque ambas versiones debe indicar ambos literales. STRING "
            "porque los ceros a la izquierda son significativos y la "
            "aritmética sobre un número de columna no tiene sentido"
        ),
    ),
    Col(
        "numeric_value",
        "FLOAT64",
        "Numeric value reported in the cell",
        "Valor numérico declarado na célula",
        "Valor numérico declarado en la celda",
        "ITM_VAL_NUM",
        obs=(
            "No column-level measurement unit, because the unit is a property "
            "of the cell and not of this column: the same column carries US "
            "dollars, bed counts, bed days, discharges, ratios and "
            "percentages depending on the worksheet, line and column. Null "
            "where the cell holds text instead; exactly one of numeric_value "
            "and alpha_value is populated on every row"
        ),
        obs_pt=(
            "Sem unidade de medida no nível da coluna, porque a unidade é uma "
            "propriedade da célula e não desta coluna: a mesma coluna carrega "
            "dólares americanos, contagens de leitos, leitos-dia, altas, "
            "razões e percentuais conforme a planilha, a linha e a coluna. "
            "Nulo quando a célula contém texto; exatamente um entre "
            "numeric_value e alpha_value é preenchido em cada linha"
        ),
        obs_es=(
            "Sin unidad de medida a nivel de columna, porque la unidad es una "
            "propiedad de la celda y no de esta columna: la misma columna "
            "lleva dólares estadounidenses, recuentos de camas, camas-día, "
            "altas, razones y porcentajes según la hoja, la línea y la "
            "columna. Nulo cuando la celda contiene texto; exactamente uno "
            "entre numeric_value y alpha_value está poblado en cada fila"
        ),
    ),
    Col(
        "alpha_value",
        "STRING",
        "Text value reported in the cell",
        "Valor de texto declarado na célula",
        "Valor de texto declarado en la celda",
        "ALPHNMRC_ITM_TXT",
        obs=(
            "Hospital names, addresses, cost centre labels and yes/no answers. "
            "Null where the cell holds a number instead. On worksheet A000000 "
            "column 00000 the text is the cost centre code followed by its "
            "label, which is how a numeric line on worksheet A is tied to a "
            "cost centre"
        ),
        obs_pt=(
            "Nomes de hospitais, endereços, rótulos de centros de custo e "
            "respostas sim/não. Nulo quando a célula contém um número. Na "
            "planilha A000000, coluna 00000, o texto é o código do centro de "
            "custo seguido de seu rótulo, que é como uma linha numérica da "
            "planilha A se liga a um centro de custo"
        ),
        obs_es=(
            "Nombres de hospitales, direcciones, etiquetas de centros de costo "
            "y respuestas sí/no. Nulo cuando la celda contiene un número. En "
            "la hoja A000000, columna 00000, el texto es el código del centro "
            "de costo seguido de su etiqueta, que es como una línea numérica "
            "de la hoja A se vincula a un centro de costo"
        ),
    ),
]


def _measure_col(m) -> Col:
    """Render one mapped measure as an architecture row.

    The provenance sentence is generated rather than written by hand, so a
    published column can never claim a cell address the mapping does not use.

    Args:
        m: A ``measures.Measure``.

    Returns:
        The architecture row for that measure.
    """
    prov = provenance(m)
    return Col(
        m.name,
        "STRING" if m.kind == "alpha" else "FLOAT64",
        m.en,
        m.pt,
        m.es,
        f"@{m.name}",
        unit=m.unit,
        obs=prov,
        obs_pt=prov,
        obs_es=prov,
    )


HOSPITAL_FINANCIAL_COLS = [
    YEAR,
    REPORT_ID,
    PROVIDER_CCN,
    STATE_ID,
    STATE_ABBREVIATION,
    FORM_VERSION,
    FISCAL_YEAR_BEGIN,
    FISCAL_YEAR_END,
    FISCAL_YEAR_DAYS,
    REPORT_STATUS,
    *[_measure_col(m) for m in MEASURES],
]

DICIONARIO_COLS = [
    Col(
        "id_tabela",
        "STRING",
        "Table the covered column belongs to",
        "Tabela à qual pertence a coluna coberta",
        "Tabla a la que pertenece la columna cubierta",
        "@dicionario",
    ),
    Col(
        "nome_coluna",
        "STRING",
        "Name of the column the key belongs to",
        "Nome da coluna à qual a chave pertence",
        "Nombre de la columna a la que pertenece la clave",
        "@dicionario",
    ),
    Col(
        "chave",
        "STRING",
        "Code as stored in the covered column",
        "Código tal como armazenado na coluna coberta",
        "Código tal como se almacena en la columna cubierta",
        "@dicionario",
    ),
    Col(
        "cobertura_temporal",
        "STRING",
        "Temporal coverage of the key",
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
        "@dicionario",
    ),
    Col(
        "valor",
        "STRING",
        "Meaning of the code",
        "Significado do código",
        "Significado del código",
        "@dicionario",
    ),
]

TABLES: dict[str, list[Col]] = {
    "report": REPORT_COLS,
    "report_value": REPORT_VALUE_COLS,
    "hospital_financial": HOSPITAL_FINANCIAL_COLS,
    "dicionario": DICIONARIO_COLS,
}

for _table, _cols in TABLES.items():
    _n = [c.name for c in _cols]
    assert len(_n) == len(set(_n)), f"{_table}: duplicate column"

if __name__ == "__main__":
    for table, cols in TABLES.items():
        print(f"{table:<20} {len(cols):>3} columns")
