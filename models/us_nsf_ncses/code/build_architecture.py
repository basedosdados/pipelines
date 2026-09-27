"""Write the architecture CSVs for us_nsf_ncses.

The architecture table is the source of truth for column names, types,
descriptions and directory links; the dbt models and the backend metadata are
both generated from it.

Run
---
    python models/us_nsf_ncses/code/build_architecture.py
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

ARCH_DIR = Path(__file__).resolve().parent / "architecture"

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
    "observations_en",
    "observations_es",
]

# Portuguese observation text -> (English, Spanish). Keyed on the Portuguese
# so the 48 call sites below stay single-language and cannot drift out of
# step with their translations.
OBSERVATION_TRANSLATIONS = {
    "Vários códigos mudaram de significado entre as duas eras da pesquisa, por isso cada entrada traz sua própria cobertura": (
        "Several codes changed meaning between the two eras of the survey, so each entry carries its own coverage",
        "Varios códigos cambiaron de significado entre las dos eras de la encuesta, por eso cada entrada trae su propia cobertura",
    ),
    "Coluna de partição. O ano fiscal varia por instituição; a Questão 17 do questionário registra o mês de encerramento, que não consta do arquivo de uso público": (
        "Partition column. The fiscal year varies by institution; questionnaire item 17 records the month it ends, which the public use file does not carry",
        "Columna de partición. El año fiscal varía por institución; el ítem 17 del cuestionario registra el mes de cierre, que no consta en el archivo de uso público",
    ),
    "Chamado 'fice' nos arquivos de 1972 a 2009 e 'inst_id' a partir de 2010; é o mesmo código nas duas eras": (
        "Called 'fice' in the 1972 to 2009 files and 'inst_id' from 2010; it is the same code in both eras",
        "Llamado 'fice' en los archivos de 1972 a 2009 e 'inst_id' desde 2010; es el mismo código en las dos eras",
    ),
    "Presente na fonte a partir do ano fiscal de 2010. Para 1972-2009 é transportado do mesmo institution_id observado em 2010 ou depois; instituições que deixaram a pesquisa antes de 2010 ficam nulas": (
        "Present in the source from fiscal year 2010. For 1972-2009 it is carried back from the same institution_id observed in 2010 or later; institutions that left the survey before 2010 stay null",
        "Presente en la fuente desde el año fiscal de 2010. Para 1972-2009 se traslada desde el mismo institution_id observado en 2010 o después; las instituciones que dejaron la encuesta antes de 2010 quedan nulas",
    ),
    "O formulário curto existe a partir do ano fiscal de 2012, para instituições com menos de US$ 1 milhão em P&D total": (
        "The short form exists from fiscal year 2012, for institutions with less than $1 million in total R&D",
        "El formulario corto existe desde el año fiscal de 2012, para instituciones con menos de 1 millón de dólares en I+D total",
    ),
    "Os códigos foram harmonizados pelo NCSES com o formato do ano fiscal de 2024 dentro de cada era. Itens com o prefixo 'NA_' não têm número no questionário": (
        "NCSES harmonised the codes with the fiscal year 2024 format within each era. Items prefixed 'NA_' have no questionnaire number",
        "El NCSES armonizó los códigos con el formato del año fiscal de 2024 dentro de cada era. Los ítems con el prefijo 'NA_' no tienen número en el cuestionario",
    ),
    "Necessário junto de question_code para identificar o item: de 1972 a 2009 o mesmo código cobre assuntos diferentes": (
        "Needed alongside question_code to identify the item: from 1972 to 2009 the same code covers different subjects",
        "Necesario junto a question_code para identificar el ítem: de 1972 a 2009 el mismo código cubre asuntos diferentes",
    ),
    "Conforme o item, identifica a fonte de recursos, o campo de pesquisa, o tipo de despesa ou a agência federal": (
        "Depending on the item, it names the source of funds, the field of research, the type of cost or the federal agency",
        "Según el ítem, identifica la fuente de recursos, el campo de investigación, el tipo de gasto o la agencia federal",
    ),
    "Conforme o item, identifica a agência federal, a origem federal ou não federal dos recursos ou a fonte não federal. Vazio nos itens de uma única coluna": (
        "Depending on the item, it names the federal agency, whether the funds are federal or nonfederal, or the nonfederal source. Empty on single-column items",
        "Según el ítem, identifica la agencia federal, el origen federal o no federal de los recursos o la fuente no federal. Vacío en los ítems de una sola columna",
    ),
    "O NCSES publica os valores em milhares de dólares; aqui estão multiplicados por mil, de modo que a precisão de origem é o milhar. Valores em dólares correntes, sem deflacionamento": (
        "NCSES publishes the values in thousands of dollars; here they are multiplied by a thousand, so the source precision is the thousand. Current dollars, not deflated",
        "El NCSES publica los valores en miles de dólares; aquí están multiplicados por mil, de modo que la precisión de origen es el millar. Dólares corrientes, sin deflactar",
    ),
    "Vazio indica resposta normal. Os arquivos de 1972 a 2009 gravam o mesmo código em maiúsculas e minúsculas; aqui está padronizado em minúsculas": (
        "Empty means a normal response. The 1972 to 2009 files write the same code in upper and lower case; here it is standardised to lower case",
        "Vacío indica respuesta normal. Los archivos de 1972 a 2009 graban el mismo código en mayúsculas y minúsculas; aquí está estandarizado en minúsculas",
    ),
    "No item 10 traz o nome da agência federal informada pela instituição. Presente apenas a partir do ano fiscal de 2010. O campo de situação que a fonte publica ao lado deste (othinfo_s) está vazio em todos os anos, por isso não foi incluído": (
        "On item 10 it carries the name of the federal agency the institution reported. Present only from fiscal year 2010. The status field the source publishes beside it (othinfo_s) is empty in every year, so it is not included",
        "En el ítem 10 trae el nombre de la agencia federal informada por la institución. Presente solo desde el año fiscal de 2010. El campo de situación que la fuente publica junto a este (othinfo_s) está vacío en todos los años, por eso no se incluyó",
    ),
    "Presente apenas no formulário padrão a partir do ano fiscal de 2010": (
        "Present only on the standard form from fiscal year 2010",
        "Presente solo en el formulario estándar desde el año fiscal de 2010",
    ),
    "Presente apenas a partir do ano fiscal de 2010": (
        "Present only from fiscal year 2010",
        "Presente solo desde el año fiscal de 2010",
    ),
    "Presente apenas de 1972 a 2009. O valor de origem '000000', que significa 'não combinar', é gravado como nulo": (
        "Present only from 1972 to 2009. The source value '000000', which means \"do not combine\", is written as null",
        "Presente solo de 1972 a 2009. El valor de origen '000000', que significa \"no combinar\", se graba como nulo",
    ),
    "Sem vínculo de diretório: br_bd_diretorios_us.state é chaveado no código FIPS (id_state), não na sigla. A checagem referencial é feita por teste dbt contra a coluna abbreviation. O marcador de origem '??', que indica agregação de instituições, é nulo aqui": (
        "No directory link: br_bd_diretorios_us.state is keyed on the FIPS code (id_state), not on the abbreviation. The referential check runs as a dbt test against the abbreviation column instead. The source marker '??', which flags an aggregation of institutions, is null here",
        "Sin vínculo de directorio: br_bd_diretorios_us.state está clavado en el código FIPS (id_state), no en la sigla. La comprobación referencial se hace con una prueba dbt contra la columna abbreviation. El marcador de origen '??', que indica agregación de instituciones, es nulo aquí",
    ),
    "O marcador de origem '?????', que indica agregação de instituições, é nulo aqui": (
        "The source marker '?????', which flags an aggregation of institutions, is null here",
        "El marcador de origen '?????', que indica agregación de instituciones, es nulo aquí",
    ),
    "O conjunto de códigos muda entre 1972-2009 e 2010-2024": (
        "The code set changes between 1972-2009 and 2010-2024",
        "El conjunto de códigos cambia entre 1972-2009 y 2010-2024",
    ),
    "Os códigos de 1972-2009 referem-se ao grau mais alto em ciência e engenharia e não são comparáveis aos de 2010-2024": (
        "The 1972-2009 codes describe the highest science and engineering degree and are not comparable with the 2010-2024 ones",
        "Los códigos de 1972-2009 se refieren al grado más alto en ciencia e ingeniería y no son comparables con los de 2010-2024",
    ),
    "Presente apenas nos arquivos de 1972 a 2009": (
        "Present only in the 1972 to 2009 files",
        "Presente solo en los archivos de 1972 a 2009",
    ),
    "De 2010 a 2019 distingue pesquisadores principais e demais integrantes; de 2010 a 2015 há também a contagem de pós-doutorandos": (
        "From 2010 to 2019 it separates principal investigators from other personnel; from 2010 to 2015 it also carries the postdoc count",
        "De 2010 a 2019 distingue investigadores principales y demás integrantes; de 2010 a 2015 también trae el conteo de posdoctorandos",
    ),
    "Preenchido a partir do ano fiscal de 2022": (
        "Populated from fiscal year 2022",
        "Completado desde el año fiscal de 2022",
    ),
    "Os arquivos de uso público não trazem contagem de pessoal para os anos fiscais de 2020 e 2021": (
        "The public use files carry no personnel count for fiscal years 2020 and 2021",
        "Los archivos de uso público no traen conteo de personal para los años fiscales de 2020 y 2021",
    ),
    "Coletado a partir do ano fiscal de 2022": (
        "Collected from fiscal year 2022",
        "Recolectado desde el año fiscal de 2022",
    ),
    "O item 01.1 registra a composição dos recursos próprios da instituição, o item 05.1 a inclusão de ensaios clínicos no relatório do ano fiscal de 2009 e o item 13 os limites de capitalização": (
        "Item 01.1 records what the institution counted as its own funds, item 05.1 whether clinical trials were included in the fiscal year 2009 report, and item 13 the capitalization thresholds",
        "El ítem 01.1 registra la composición de los recursos propios de la institución, el ítem 05.1 la inclusión de ensayos clínicos en el informe del año fiscal de 2009 y el ítem 13 los límites de capitalización",
    ),
    "No ano fiscal de 2012 o item 01.1 foi respondido também para o ano fiscal de 2011, registrado nesta coluna": (
        "In fiscal year 2012 item 01.1 was answered for fiscal year 2011 as well, recorded in this column",
        "En el año fiscal de 2012 el ítem 01.1 se respondió también para el año fiscal de 2011, registrado en esta columna",
    ),
    "Nulo nos itens cuja resposta é um valor monetário": (
        "Null on the items whose answer is a monetary value",
        "Nulo en los ítems cuya respuesta es un valor monetario",
    ),
    "Preenchido apenas no item 13, que registra os limites de capitalização de equipamentos e de software. O NCSES publica o valor em milhares de dólares; aqui está multiplicado por mil": (
        "Populated only on item 13, which records the capitalization thresholds for equipment and software. NCSES publishes the value in thousands of dollars; here it is multiplied by a thousand",
        "Completado solo en el ítem 13, que registra los límites de capitalización de equipos y de software. El NCSES publica el valor en miles de dólares; aquí está multiplicado por mil",
    ),
    "No item 01.1 traz o motivo de determinados tipos de recurso não terem sido incluídos. O campo de situação que a fonte publica ao lado deste item está vazio em todos os anos, por isso não foi incluído": (
        "On item 01.1 it carries why particular kinds of funds were left out. The status field the source publishes beside this item is empty in every year, so it is not included",
        "En el ítem 01.1 trae el motivo por el que ciertos tipos de recursos no se incluyeron. El campo de situación que la fuente publica junto a este ítem está vacío en todos los años, por eso no se incluyó",
    ),
    "Coluna de partição. Cada ciclo republica a própria série histórica, então um ciclo é uma safra fechada: para os números correntes, filtre pelo maior reference_year em vez de somar entre ciclos": (
        "Partition column. Each cycle republishes its own history, so a cycle is a closed vintage: for the current numbers, filter to the largest reference_year rather than summing across cycles",
        "Columna de partición. Cada ciclo republica su propia serie histórica, así que un ciclo es una cosecha cerrada: para los números corrientes, filtre por el mayor reference_year en vez de sumar entre ciclos",
    ),
    "Por exemplo, '1-5' para a Tabela 1-5": (
        "For example, '1-5' for Table 1-5",
        "Por ejemplo, '1-5' para la Tabla 1-5",
    ),
    "Os grupos reúnem tendências, compromissos após a titulação, características do campo e demográficas, apoio financeiro e dívida, histórico educacional, salários, instituições, perfis estatísticos e planos após a titulação": (
        "The groups gather trends, postgraduation commitments, field and demographic characteristics, financial support and debt, educational background, salaries, institutions, statistical profiles and postgraduation plans",
        "Los grupos reúnen tendencias, compromisos tras la titulación, características del campo y demográficas, apoyo financiero y deuda, historial educativo, salarios, instituciones, perfiles estadísticos y planes tras la titulación",
    ),
    "Texto literal da fonte, como 'Number and percent'. A unidade resolvida por célula está na coluna unit de sed_estimate": (
        "Verbatim text from the source, such as 'Number and percent'. The unit resolved per cell is in sed_estimate's unit column",
        "Texto literal de la fuente, como 'Number and percent'. La unidad resuelta por celda está en la columna unit de sed_estimate",
    ),
    "Por exemplo, 'nsf25349' para o ciclo de 2024": (
        "For example, 'nsf25349' for the 2024 cycle",
        "Por ejemplo, 'nsf25349' para el ciclo de 2024",
    ),
    "Igual ao número de linhas de sed_estimate para a tabela": (
        "Equal to the number of sed_estimate rows for the table",
        "Igual al número de filas de sed_estimate para la tabla",
    ),
    "Lido do eixo temporal da tabela, esteja ele nas linhas ou nas colunas. Nas tabelas sem eixo temporal é igual a reference_year. O ano acadêmico de 2024 vai de 1 de julho de 2023 a 30 de junho de 2024": (
        "Read from the table's time axis, whether that runs down the rows or across the columns. On a table with no time axis it equals reference_year. Academic year 2024 runs from 1 July 2023 to 30 June 2024",
        "Leído del eje temporal de la tabla, esté en las filas o en las columnas. En las tablas sin eje temporal es igual a reference_year. El año académico de 2024 va del 1 de julio de 2023 al 30 de junio de 2024",
    ),
    "Posição, não uma quantidade: ordenar exige safe_cast(row_number as int64). Junto de table_id e column_number identifica a célula de forma única, o que os rótulos nem sempre fazem — a tabela 5-3 recua duas seções diferentes no mesmo nível e produz dois row_path iguais": (
        "A position, not a quantity: ordering needs safe_cast(row_number as int64). With table_id and column_number it identifies the cell uniquely, which the labels do not always do — table 5-3 indents two different sections at the same level and so publishes two identical row_path values",
        "Una posición, no una cantidad: ordenar exige safe_cast(row_number as int64). Junto con table_id y column_number identifica la celda de forma única, lo que las etiquetas no siempre hacen — la tabla 5-3 sangra dos secciones diferentes al mismo nivel y produce dos row_path iguales",
    ),
    "Posição, não uma quantidade: ordenar exige safe_cast(column_number as int64)": (
        "A position, not a quantity: ordering needs safe_cast(column_number as int64)",
        "Una posición, no una cantidad: ordenar exige safe_cast(column_number as int64)",
    ),
    "Marcadores de nota de rodapé, gravados como sobrescrito na planilha, foram removidos": (
        "Footnote markers, written as superscript in the worksheet, have been removed",
        "Los marcadores de nota al pie, grabados como superíndice en la hoja, fueron eliminados",
    ),
    "Níveis separados por ' > '. Reconstruído a partir do recuo da célula na planilha, que é como o NCSES marca a hierarquia de campos e características": (
        "Levels separated by ' > '. Reconstructed from the cell's indentation in the worksheet, which is how NCSES marks the hierarchy of fields and characteristics",
        "Niveles separados por ' > '. Reconstruido a partir de la sangría de la celda en la hoja, que es como el NCSES marca la jerarquía de campos y características",
    ),
    "Ordinal, não uma quantidade. Igual ao número de níveis em row_path menos um": (
        "An ordinal, not a quantity. Equal to the number of levels in row_path minus one",
        "Un ordinal, no una cantidad. Igual al número de niveles en row_path menos uno",
    ),
    "Último nível de column_path. Nulo quando o cabeçalho da coluna é apenas um ano, já registrado em year": (
        "The last level of column_path. Null when the column header is only a year, already recorded in year",
        "Último nivel de column_path. Nulo cuando el encabezado de la columna es solo un año, ya registrado en year",
    ),
    "Níveis separados por ' > '. O cabeçalho tem de uma a três linhas, reconstruídas a partir das células mescladas; algumas tabelas reescrevem os rótulos no meio do corpo, e essa reescrita entra como um nível adicional": (
        "Levels separated by ' > '. The header runs one to three rows, reconstructed from the merged cells; some tables restate the labels part way down the body, and that restatement enters as a further level",
        "Niveles separados por ' > '. El encabezado tiene de una a tres filas, reconstruidas a partir de las celdas combinadas; algunas tablas reescriben las etiquetas en medio del cuerpo, y esa reescritura entra como un nivel adicional",
    ),
    "Resolvida por célula a partir do rótulo mais específico disponível: o da coluna, o do grupo de colunas, o da seção da linha e, por fim, a declaração de unidade da tabela": (
        "Resolved per cell from the most specific label available: the column's, the column group's, the row section's and, last, the table's unit statement",
        "Resuelta por celda a partir de la etiqueta más específica disponible: la de la columna, la del grupo de columnas, la de la sección de la fila y, por último, la declaración de unidad de la tabla",
    ),
    "A unidade varia por célula e está na coluna unit, por isso não há unidade de medida única para esta coluna. Contagens de pessoas, porcentagens, dólares correntes e medianas de anos convivem na mesma coluna. Células suprimidas ou não aplicáveis na fonte são nulas": (
        "The unit varies by cell and is in the unit column, so there is no single measurement unit for this one. Counts of people, percentages, current dollars and medians of years live in the same column. Cells the source suppresses or marks not applicable are null",
        "La unidad varía por celda y está en la columna unit, por eso no hay una unidad de medida única para esta columna. Conteos de personas, porcentajes, dólares corrientes y medianas de años conviven en la misma columna. Las celdas suprimidas o no aplicables en la fuente son nulas",
    ),
}


YEAR_FK = "diretorios_data_tempo.ano:ano"
INSTITUTION_FK = "diretorios_us.higher_education_institution:id_institution"


def col(
    name: str,
    bq_type: str,
    pt: str,
    en: str,
    es: str,
    *,
    coverage: str = "",
    dictionary: str = "no",
    directory: str = "",
    unit: str = "",
    sensitive: str = "no",
    observations: str = "",
    original: str = "",
) -> dict[str, str]:
    """Assemble one architecture row, translating its observations.

    Args:
        name: BigQuery column name, snake_case.
        bq_type: BigQuery type, chosen by arithmetic meaning rather than by the
            source's storage format.
        pt: Portuguese description, no trailing period.
        en: English description.
        es: Spanish description.
        coverage: Temporal coverage in ``START(INTERVAL)END`` notation. Empty
            means the column inherits the table's coverage.
        dictionary: ``"yes"`` when the stored values are codes the ``dicionario``
            table decodes.
        directory: Data Basis directory foreign key, ``<dataset>.<table>:<col>``.
        measurement_unit is passed as ``unit``: required on every numeric column
            except ``sed_estimate.value``, which mixes units by design.
        sensitive: ``"yes"`` when the column carries sensitive data.
        observations: Portuguese free-text note. Must have an entry in
            :data:`OBSERVATION_TRANSLATIONS`.
        original: The column's name in the raw source.

    Returns:
        One row keyed by :data:`HEADER`, ready for the architecture CSV.

    Raises:
        SystemExit: If ``observations`` has no registered translation, which
            would otherwise ship a Portuguese-only note to the backend.
    """
    if observations and observations not in OBSERVATION_TRANSLATIONS:
        raise SystemExit(
            f"{name}: no translation for observations {observations!r}"
        )
    observations_en, observations_es = (
        OBSERVATION_TRANSLATIONS[observations] if observations else ("", "")
    )
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description": pt,
        "temporal_coverage": coverage,
        "covered_by_dictionary": dictionary,
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": sensitive,
        "observations": observations,
        "original_name": original,
        "description_en": en,
        "description_es": es,
        "observations_en": observations_en,
        "observations_es": observations_es,
    }


# --------------------------------------------------------------------------
# Columns shared by the HERD fact tables.
# --------------------------------------------------------------------------

HERD_YEAR = col(
    "year",
    "INT64",
    "Ano fiscal da instituição a que se referem os dados",
    "Institution fiscal year the data describe",
    "Año fiscal de la institución al que se refieren los datos",
    directory=YEAR_FK,
    unit="year",
    observations=(
        "Coluna de partição. O ano fiscal varia por instituição; a Questão 17 "
        "do questionário registra o mês de encerramento, que não consta do "
        "arquivo de uso público"
    ),
    original="year",
)


def herd_institution_id(observations: str = "") -> dict[str, str]:
    """The NCSES institution code, shared by both survey eras.

    Args:
        observations: Overrides the default note, for tables where the column
            needs a different caveat.

    Returns:
        The ``institution_id`` architecture row.
    """
    return col(
        "institution_id",
        "STRING",
        "Código de identificação da instituição atribuído pelo NCSES",
        "NCSES institution identification code",
        "Código de identificación de la institución asignado por el NCSES",
        observations=observations
        or (
            "Chamado 'fice' nos arquivos de 1972 a 2009 e 'inst_id' a partir de "
            "2010; é o mesmo código nas duas eras"
        ),
        original="inst_id / fice",
    )


def herd_unitid() -> dict[str, str]:
    """The IPEDS UNITID, the join key to ``us_ed_ipeds``.

    Returns:
        The ``unitid`` architecture row.
    """
    return col(
        "unitid",
        "STRING",
        (
            "Código de identificação da instituição no IPEDS (UNITID). Mesma "
            "chave das tabelas us_ed_ipeds e us_ed_college_scorecard; como em "
            "us_ed_ipeds o tipo é INT64, o cruzamento exige "
            "safe_cast(unitid as int64)"
        ),
        (
            "IPEDS institution identifier (UNITID). The same key as us_ed_ipeds "
            "and us_ed_college_scorecard; us_ed_ipeds types it INT64, so a join "
            "needs safe_cast(unitid as int64)"
        ),
        (
            "Código de identificación de la institución en IPEDS (UNITID). La "
            "misma clave que us_ed_ipeds y us_ed_college_scorecard; en "
            "us_ed_ipeds el tipo es INT64, por lo que el cruce exige "
            "safe_cast(unitid as int64)"
        ),
        directory=INSTITUTION_FK,
        observations=(
            "Presente na fonte a partir do ano fiscal de 2010. Para 1972-2009 é "
            "transportado do mesmo institution_id observado em 2010 ou depois; "
            "instituições que deixaram a pesquisa antes de 2010 ficam nulas"
        ),
        original="ipeds_unitid",
    )


def herd_survey_form() -> dict[str, str]:
    """Which questionnaire version the institution answered.

    Returns:
        The ``survey_form`` architecture row.
    """
    return col(
        "survey_form",
        "STRING",
        "Versão do questionário respondida pela instituição no ano",
        "Questionnaire version the institution answered in the year",
        "Versión del cuestionario respondida por la institución en el año",
        dictionary="yes",
        observations=(
            "O formulário curto existe a partir do ano fiscal de 2012, para "
            "instituições com menos de US$ 1 milhão em P&D total"
        ),
        original="(nome do arquivo)",
    )


def herd_status(
    name: str, subject_pt: str, subject_en: str, subject_es: str
) -> dict[str, str]:
    """A status code qualifying one measured value.

    Args:
        name: Column name for this particular status column.
        subject_pt: Portuguese name of the value the status qualifies.
        subject_en: English name of that value.
        subject_es: Spanish name of that value.

    Returns:
        The status architecture row.
    """
    return col(
        name,
        "STRING",
        f"Código de situação do valor de {subject_pt}",
        f"Status code for the {subject_en} value",
        f"Código de situación del valor de {subject_es}",
        dictionary="yes",
        observations=(
            "Vazio indica resposta normal. Os arquivos de 1972 a 2009 gravam o "
            "mesmo código em maiúsculas e minúsculas; aqui está padronizado em "
            "minúsculas"
        ),
        original="status",
    )


# --------------------------------------------------------------------------
# herd_institution
# --------------------------------------------------------------------------

HERD_INSTITUTION = [
    HERD_YEAR,
    herd_institution_id(),
    col(
        "ncses_institution_id",
        "STRING",
        "Identificador interno da instituição no NCSES",
        "NCSES internal institution identifier",
        "Identificador interno de la institución en el NCSES",
        observations="Presente apenas a partir do ano fiscal de 2010",
        original="ncses_inst_id",
    ),
    herd_unitid(),
    col(
        "combined_institution_id",
        "STRING",
        "Código da instituição com a qual esta é combinada no relatório",
        "Identifier of the institution this one is combined with for reporting",
        "Código de la institución con la que esta se combina en el informe",
        observations=(
            "Presente apenas de 1972 a 2009. O valor de origem '000000', que "
            "significa 'não combinar', é gravado como nulo"
        ),
        original="fice_combined",
    ),
    herd_survey_form(),
    col(
        "institution_name",
        "STRING",
        "Nome da instituição",
        "Institution name",
        "Nombre de la institución",
        original="inst_name_long",
    ),
    col(
        "institution_city",
        "STRING",
        "Município em que a instituição está localizada",
        "City where the institution is located",
        "Ciudad en la que se ubica la institución",
        original="inst_city",
    ),
    col(
        "state_abbreviation",
        "STRING",
        "Sigla do estado norte-americano em que a instituição está localizada",
        "Abbreviation of the U.S. state where the institution is located",
        "Sigla del estado estadounidense en el que se ubica la institución",
        observations=(
            "Sem vínculo de diretório: br_bd_diretorios_us.state é chaveado no "
            "código FIPS (id_state), não na sigla. A checagem referencial é "
            "feita por teste dbt contra a coluna abbreviation. O marcador de "
            "origem '??', que indica agregação de instituições, é nulo aqui"
        ),
        original="inst_state_code / inst_state",
    ),
    col(
        "zip_code",
        "STRING",
        "Código postal (ZIP) da instituição",
        "Institution ZIP code",
        "Código postal (ZIP) de la institución",
        observations=(
            "O marcador de origem '?????', que indica agregação de "
            "instituições, é nulo aqui"
        ),
        original="inst_zip",
    ),
    col(
        "hbcu_indicator",
        "STRING",
        "Indica se a instituição é uma HBCU (faculdade ou universidade "
        "historicamente negra)",
        "Whether the institution is a historically black college or university",
        "Indica si la institución es una HBCU (universidad históricamente negra)",
        dictionary="yes",
        observations="O conjunto de códigos muda entre 1972-2009 e 2010-2024",
        original="hbcu_flag",
    ),
    col(
        "medical_school_indicator",
        "STRING",
        "Indica se a instituição tem escola de medicina",
        "Whether the institution has a medical school",
        "Indica si la institución tiene escuela de medicina",
        dictionary="yes",
        original="med_sch_flag / has_med_sch_flag",
    ),
    col(
        "high_hispanic_enrollment_indicator",
        "STRING",
        "Indica se a instituição tem alta matrícula de estudantes hispânicos",
        "Whether the institution is a high Hispanic enrollment institution",
        "Indica si la institución tiene alta matrícula de estudiantes hispanos",
        dictionary="yes",
        observations="O conjunto de códigos muda entre 1972-2009 e 2010-2024",
        original="hhe_flag",
    ),
    col(
        "institution_type_code",
        "STRING",
        "Código do tipo de instituição",
        "Institution type code",
        "Código del tipo de institución",
        dictionary="yes",
        original="toi_code",
    ),
    col(
        "highest_degree_code",
        "STRING",
        "Código do grau mais alto concedido pela instituição",
        "Code for the highest degree the institution grants",
        "Código del grado más alto que otorga la institución",
        dictionary="yes",
        observations=(
            "Os códigos de 1972-2009 referem-se ao grau mais alto em ciência e "
            "engenharia e não são comparáveis aos de 2010-2024"
        ),
        original="hdg_code",
    ),
    col(
        "control_type_code",
        "STRING",
        "Código do tipo de controle da instituição",
        "Institution control type code",
        "Código del tipo de control de la institución",
        dictionary="yes",
        original="toc_code",
    ),
    col(
        "fy09_pilot_indicator",
        "STRING",
        "Indica se a instituição participou do piloto da pesquisa HERD no ano "
        "fiscal de 2009",
        "Whether the institution took part in the FY2009 HERD pilot survey",
        "Indica si la institución participó en el piloto de la encuesta HERD en "
        "el año fiscal 2009",
        dictionary="yes",
        observations="Presente apenas nos arquivos de 1972 a 2009",
        original="pilot_fy09_flag",
    ),
]

# --------------------------------------------------------------------------
# herd_expenditure
# --------------------------------------------------------------------------

HERD_EXPENDITURE = [
    HERD_YEAR,
    herd_institution_id(),
    herd_unitid(),
    herd_survey_form(),
    col(
        "question_code",
        "STRING",
        "Código do item do questionário",
        "Questionnaire item code",
        "Código del ítem del cuestionario",
        observations=(
            "Os códigos foram harmonizados pelo NCSES com o formato do ano "
            "fiscal de 2024 dentro de cada era. Itens com o prefixo 'NA_' não "
            "têm número no questionário"
        ),
        original="questionnaire_no",
    ),
    col(
        "question",
        "STRING",
        "Assunto do item do questionário",
        "Subject of the questionnaire item",
        "Asunto del ítem del cuestionario",
        observations=(
            "Necessário junto de question_code para identificar o item: de "
            "1972 a 2009 o mesmo código cobre assuntos diferentes"
        ),
        original="question",
    ),
    col(
        "row_label",
        "STRING",
        "Rótulo da linha do item do questionário",
        "Row label of the questionnaire item",
        "Etiqueta de la fila del ítem del cuestionario",
        observations=(
            "Conforme o item, identifica a fonte de recursos, o campo de "
            "pesquisa, o tipo de despesa ou a agência federal"
        ),
        original="row",
    ),
    col(
        "column_label",
        "STRING",
        "Rótulo da coluna do item do questionário",
        "Column label of the questionnaire item",
        "Etiqueta de la columna del ítem del cuestionario",
        observations=(
            "Conforme o item, identifica a agência federal, a origem federal "
            "ou não federal dos recursos ou a fonte não federal. Vazio nos "
            "itens de uma única coluna"
        ),
        original="column",
    ),
    col(
        "expenditure",
        "FLOAT64",
        "Despesa de pesquisa e desenvolvimento, em dólares correntes",
        "Research and development expenditure, in current dollars",
        "Gasto en investigación y desarrollo, en dólares corrientes",
        unit="USD",
        observations=(
            "O NCSES publica os valores em milhares de dólares; aqui estão "
            "multiplicados por mil, de modo que a precisão de origem é o "
            "milhar. Valores em dólares correntes, sem deflacionamento"
        ),
        original="data",
    ),
    herd_status("status_code", "despesa", "expenditure", "gasto"),
    col(
        "other_information",
        "STRING",
        "Informação complementar registrada pela instituição para o item",
        "Additional information the institution recorded for the item",
        "Información complementaria registrada por la institución para el ítem",
        observations=(
            "No item 10 traz o nome da agência federal informada pela "
            "instituição. Presente apenas a partir do ano fiscal de 2010. O "
            "campo de situação que a fonte publica ao lado deste (othinfo_s) "
            "está vazio em todos os anos, por isso não foi incluído"
        ),
        original="othinfo",
    ),
    col(
        "standardized_agency_name",
        "STRING",
        "Nome padronizado pelo NCSES da agência federal informada no item 10",
        "NCSES standardized name of the federal agency reported in item 10",
        "Nombre normalizado por el NCSES de la agencia federal informada en el "
        "ítem 10",
        observations=(
            "Presente apenas no formulário padrão a partir do ano fiscal de 2010"
        ),
        original="standardized_agency_names",
    ),
]

# --------------------------------------------------------------------------
# herd_personnel
# --------------------------------------------------------------------------

HERD_PERSONNEL = [
    HERD_YEAR,
    herd_institution_id(),
    herd_unitid(),
    herd_survey_form(),
    col(
        "personnel_group",
        "STRING",
        "Grupo de pessoal a que se refere a contagem",
        "Personnel group the count refers to",
        "Grupo de personal al que se refiere el conteo",
        observations=(
            "De 2010 a 2019 distingue pesquisadores principais e demais "
            "integrantes; de 2010 a 2015 há também a contagem de pós-doutorandos"
        ),
        original="row",
    ),
    col(
        "personnel_function",
        "STRING",
        "Função de pesquisa e desenvolvimento exercida pelo pessoal",
        "Research and development function the personnel perform",
        "Función de investigación y desarrollo que ejerce el personal",
        observations="Preenchido a partir do ano fiscal de 2022",
        original="column",
    ),
    col(
        "headcount",
        "INT64",
        "Número de pessoas que apoiam atividades de pesquisa e desenvolvimento",
        "Number of people supporting research and development activities",
        "Número de personas que apoyan actividades de investigación y desarrollo",
        unit="person",
        observations=(
            "Os arquivos de uso público não trazem contagem de pessoal para os "
            "anos fiscais de 2020 e 2021"
        ),
        original="data",
    ),
    herd_status("headcount_status_code", "contagem", "headcount", "conteo"),
    col(
        "full_time_equivalent",
        "FLOAT64",
        "Equivalentes de tempo integral do pessoal de pesquisa e desenvolvimento",
        "Full-time equivalents of research and development personnel",
        "Equivalentes de tiempo completo del personal de investigación y "
        "desarrollo",
        unit="full_time_equivalent",
        observations="Coletado a partir do ano fiscal de 2022",
        original="data",
    ),
    herd_status(
        "full_time_equivalent_status_code",
        "equivalente de tempo integral",
        "full-time equivalent",
        "equivalente de tiempo completo",
    ),
]

# --------------------------------------------------------------------------
# herd_survey_item
# --------------------------------------------------------------------------

HERD_SURVEY_ITEM = [
    HERD_YEAR,
    herd_institution_id(),
    herd_unitid(),
    herd_survey_form(),
    col(
        "question_code",
        "STRING",
        "Código do item do questionário",
        "Questionnaire item code",
        "Código del ítem del cuestionario",
        observations=(
            "O item 01.1 registra a composição dos recursos próprios da "
            "instituição, o item 05.1 a inclusão de ensaios clínicos no "
            "relatório do ano fiscal de 2009 e o item 13 os limites de "
            "capitalização"
        ),
        original="questionnaire_no",
    ),
    col(
        "question",
        "STRING",
        "Assunto do item do questionário",
        "Subject of the questionnaire item",
        "Asunto del ítem del cuestionario",
        original="question",
    ),
    col(
        "row_label",
        "STRING",
        "Rótulo da linha do item do questionário",
        "Row label of the questionnaire item",
        "Etiqueta de la fila del ítem del cuestionario",
        original="row",
    ),
    col(
        "column_label",
        "STRING",
        "Rótulo da coluna do item do questionário",
        "Column label of the questionnaire item",
        "Etiqueta de la columna del ítem del cuestionario",
        observations=(
            "No ano fiscal de 2012 o item 01.1 foi respondido também para o "
            "ano fiscal de 2011, registrado nesta coluna"
        ),
        original="column",
    ),
    col(
        "response_code",
        "STRING",
        "Resposta da instituição ao item, quando o item pede um código",
        "Institution's answer to the item, when the item asks for a code",
        "Respuesta de la institución al ítem, cuando el ítem pide un código",
        dictionary="yes",
        observations="Nulo nos itens cuja resposta é um valor monetário",
        original="data",
    ),
    col(
        "amount",
        "FLOAT64",
        "Valor monetário informado no item, em dólares correntes",
        "Monetary value reported in the item, in current dollars",
        "Valor monetario informado en el ítem, en dólares corrientes",
        unit="USD",
        observations=(
            "Preenchido apenas no item 13, que registra os limites de "
            "capitalização de equipamentos e de software. O NCSES publica o "
            "valor em milhares de dólares; aqui está multiplicado por mil"
        ),
        original="data",
    ),
    col(
        "other_information",
        "STRING",
        "Informação complementar registrada pela instituição para o item",
        "Additional information the institution recorded for the item",
        "Información complementaria registrada por la institución para el ítem",
        observations=(
            "No item 01.1 traz o motivo de determinados tipos de recurso não "
            "terem sido incluídos. O campo de situação que a fonte publica ao "
            "lado deste item está vazio em todos os anos, por isso não foi "
            "incluído"
        ),
        original="othinfo",
    ),
]

# --------------------------------------------------------------------------
# SED
# --------------------------------------------------------------------------

SED_REFERENCE_YEAR = col(
    "reference_year",
    "INT64",
    "Ano do ciclo da pesquisa a que pertence a publicação",
    "Survey cycle year the publication belongs to",
    "Año del ciclo de la encuesta al que pertenece la publicación",
    directory=YEAR_FK,
    unit="year",
    observations=(
        "Coluna de partição. Cada ciclo republica a própria série histórica, "
        "então um ciclo é uma safra fechada: para os números correntes, filtre "
        "pelo maior reference_year em vez de somar entre ciclos"
    ),
    original="(publicação)",
)

SED_TABLE_ID = col(
    "table_id",
    "STRING",
    "Identificador da tabela publicada, no formato grupo-número",
    "Published table identifier, as group-number",
    "Identificador de la tabla publicada, en formato grupo-número",
    observations="Por exemplo, '1-5' para a Tabela 1-5",
    original="Table 1-5",
)

SED_DATA_TABLE = [
    SED_REFERENCE_YEAR,
    SED_TABLE_ID,
    col(
        "table_group",
        "STRING",
        "Grupo temático da tabela publicada",
        "Thematic group of the published table",
        "Grupo temático de la tabla publicada",
        observations=(
            "Os grupos reúnem tendências, compromissos após a titulação, "
            "características do campo e demográficas, apoio financeiro e "
            "dívida, histórico educacional, salários, instituições, perfis "
            "estatísticos e planos após a titulação"
        ),
        original="Table 1-5",
    ),
    col(
        "table_title",
        "STRING",
        "Título da tabela publicada",
        "Title of the published table",
        "Título de la tabla publicada",
        original="(linha 2 da planilha)",
    ),
    col(
        "unit_statement",
        "STRING",
        "Declaração de unidade impressa abaixo do título da tabela",
        "Unit statement printed under the table title",
        "Declaración de unidad impresa debajo del título de la tabla",
        observations=(
            "Texto literal da fonte, como 'Number and percent'. A unidade "
            "resolvida por célula está na coluna unit de sed_estimate"
        ),
        original="(linha 3 da planilha)",
    ),
    col(
        "publication_id",
        "STRING",
        "Código da publicação do NCSES que contém a tabela",
        "NCSES publication identifier that contains the table",
        "Código de la publicación del NCSES que contiene la tabla",
        observations="Por exemplo, 'nsf25349' para o ciclo de 2024",
        original="(nome do arquivo)",
    ),
    col(
        "source_file",
        "STRING",
        "Nome do arquivo Excel de origem da tabela",
        "Name of the source Excel workbook for the table",
        "Nombre del archivo Excel de origen de la tabla",
        original="(nome do arquivo)",
    ),
    col(
        "estimate_count",
        "INT64",
        "Número de células de dados extraídas da tabela",
        "Number of data cells extracted from the table",
        "Número de celdas de datos extraídas de la tabla",
        unit="unit",
        observations="Igual ao número de linhas de sed_estimate para a tabela",
        original="(calculado)",
    ),
]

SED_ESTIMATE = [
    SED_REFERENCE_YEAR,
    SED_TABLE_ID,
    col(
        "year",
        "INT64",
        "Ano acadêmico a que o valor se refere",
        "Academic year the value refers to",
        "Año académico al que se refiere el valor",
        directory=YEAR_FK,
        unit="year",
        observations=(
            "Lido do eixo temporal da tabela, esteja ele nas linhas ou nas "
            "colunas. Nas tabelas sem eixo temporal é igual a reference_year. "
            "O ano acadêmico de 2024 vai de 1 de julho de 2023 a 30 de junho "
            "de 2024"
        ),
        original="(cabeçalho ou rótulo de linha)",
    ),
    col(
        "row_number",
        "STRING",
        "Número da linha da célula na planilha publicada",
        "Worksheet row number of the cell in the published table",
        "Número de fila de la celda en la hoja publicada",
        observations=(
            "Posição, não uma quantidade: ordenar exige "
            "safe_cast(row_number as int64). Junto de table_id e "
            "column_number identifica a célula de forma única, o que os "
            "rótulos nem sempre fazem — a tabela 5-3 recua duas seções "
            "diferentes no mesmo nível e produz dois row_path iguais"
        ),
        original="(posição na planilha)",
    ),
    col(
        "column_number",
        "STRING",
        "Número da coluna da célula na planilha publicada",
        "Worksheet column number of the cell in the published table",
        "Número de columna de la celda en la hoja publicada",
        observations=(
            "Posição, não uma quantidade: ordenar exige "
            "safe_cast(column_number as int64)"
        ),
        original="(posição na planilha)",
    ),
    col(
        "row_label",
        "STRING",
        "Rótulo da linha da tabela publicada",
        "Row label of the published table",
        "Etiqueta de la fila de la tabla publicada",
        observations=(
            "Marcadores de nota de rodapé, gravados como sobrescrito na "
            "planilha, foram removidos"
        ),
        original="(coluna A da planilha)",
    ),
    col(
        "row_path",
        "STRING",
        "Caminho hierárquico completo da linha, do nível mais alto até ela",
        "Full hierarchical path of the row, from the top level down to it",
        "Ruta jerárquica completa de la fila, desde el nivel más alto hasta ella",
        observations=(
            "Níveis separados por ' > '. Reconstruído a partir do recuo da "
            "célula na planilha, que é como o NCSES marca a hierarquia de "
            "campos e características"
        ),
        original="(recuo da coluna A)",
    ),
    col(
        "row_level",
        "STRING",
        "Profundidade da linha na hierarquia, começando em zero",
        "Depth of the row in the hierarchy, starting at zero",
        "Profundidad de la fila en la jerarquía, empezando en cero",
        observations=(
            "Ordinal, não uma quantidade. Igual ao número de níveis em "
            "row_path menos um"
        ),
        original="(recuo da coluna A)",
    ),
    col(
        "column_label",
        "STRING",
        "Rótulo mais específico do cabeçalho da coluna",
        "Most specific header label of the column",
        "Etiqueta más específica del encabezado de la columna",
        observations=(
            "Último nível de column_path. Nulo quando o cabeçalho da coluna é "
            "apenas um ano, já registrado em year"
        ),
        original="(cabeçalho da planilha)",
    ),
    col(
        "column_path",
        "STRING",
        "Caminho hierárquico completo do cabeçalho da coluna",
        "Full hierarchical path of the column header",
        "Ruta jerárquica completa del encabezado de la columna",
        observations=(
            "Níveis separados por ' > '. O cabeçalho tem de uma a três linhas, "
            "reconstruídas a partir das células mescladas; algumas tabelas "
            "reescrevem os rótulos no meio do corpo, e essa reescrita entra "
            "como um nível adicional"
        ),
        original="(cabeçalho da planilha)",
    ),
    col(
        "unit",
        "STRING",
        "Unidade em que o valor está expresso",
        "Unit the value is expressed in",
        "Unidad en la que se expresa el valor",
        dictionary="yes",
        observations=(
            "Resolvida por célula a partir do rótulo mais específico "
            "disponível: o da coluna, o do grupo de colunas, o da seção da "
            "linha e, por fim, a declaração de unidade da tabela"
        ),
        original="(derivado)",
    ),
    col(
        "value",
        "FLOAT64",
        "Valor publicado da célula",
        "Published value of the cell",
        "Valor publicado de la celda",
        observations=(
            "A unidade varia por célula e está na coluna unit, por isso não há "
            "unidade de medida única para esta coluna. Contagens de pessoas, "
            "porcentagens, dólares correntes e medianas de anos convivem na "
            "mesma coluna. Células suprimidas ou não aplicáveis na fonte são "
            "nulas"
        ),
        original="(célula de dados)",
    ),
]

# --------------------------------------------------------------------------
# dicionario
# --------------------------------------------------------------------------

DICIONARIO = [
    col(
        "id_tabela",
        "STRING",
        "Nome da tabela",
        "Table name",
        "Nombre de la tabla",
        original="(derivado)",
    ),
    col(
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
        original="(derivado)",
    ),
    col(
        "chave",
        "STRING",
        "Chave do dicionário, isto é, o valor armazenado na coluna",
        "Dictionary key, that is, the value stored in the column",
        "Clave del diccionario, es decir, el valor almacenado en la columna",
        original="(derivado)",
    ),
    col(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal em que a chave tem o significado indicado",
        "Temporal coverage over which the key carries the stated meaning",
        "Cobertura temporal en la que la clave tiene el significado indicado",
        observations=(
            "Vários códigos mudaram de significado entre as duas eras da "
            "pesquisa, por isso cada entrada traz sua própria cobertura"
        ),
        original="(derivado)",
    ),
    col(
        "valor",
        "STRING",
        "Significado da chave",
        "Meaning of the key",
        "Significado de la clave",
        original="(derivado)",
    ),
]

TABLES = {
    "herd_institution": HERD_INSTITUTION,
    "herd_expenditure": HERD_EXPENDITURE,
    "herd_personnel": HERD_PERSONNEL,
    "herd_survey_item": HERD_SURVEY_ITEM,
    "sed_data_table": SED_DATA_TABLE,
    "sed_estimate": SED_ESTIMATE,
    "dicionario": DICIONARIO,
}


def main() -> int:
    """Write every table's architecture CSV, validating it first.

    Returns:
        0 on success; the process exits non-zero on any validation failure.

    Raises:
        SystemExit: On a duplicate column name, a numeric column with no
            measurement unit, or a description ending in a period. These are
            the defects that are expensive to undo once the columns are
            registered in the backend, so they abort the build.
    """
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, columns in TABLES.items():
        names = [c["name"] for c in columns]
        if len(names) != len(set(names)):
            raise SystemExit(f"{table}: duplicate column names")
        for c in columns:
            numeric = c["bigquery_type"] in {"INT64", "FLOAT64"}
            # sed_estimate.value mixes units by design; unit lives in a column.
            exempt = table == "sed_estimate" and c["name"] == "value"
            if numeric and not c["measurement_unit"] and not exempt:
                raise SystemExit(
                    f"{table}.{c['name']}: numeric without a unit"
                )
            for field in ("description", "description_en", "description_es"):
                if c[field].rstrip().endswith("."):
                    raise SystemExit(
                        f"{table}.{c['name']}: {field} ends with a period"
                    )
        with open(
            ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as f:
            writer = csv.DictWriter(f, fieldnames=HEADER, lineterminator="\n")
            writer.writeheader()
            writer.writerows(columns)
        print(f"{table}.csv: {len(columns)} columns")
    return 0


if __name__ == "__main__":
    sys.exit(main())
