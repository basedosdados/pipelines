"""Portuguese -> English / Spanish for every description in br_ibama_fiscalizacao.

Keyed on the FULL description string, never composed from a phrase glossary. A
glossary composes rather than understands: any phrase it lacks passes through
untouched, and the result reads plausibly enough to survive a skim.

`build_metadata_payloads.py` fails the build on any description missing here, so
a new column cannot ship with Portuguese text in all three language fields.
"""

from __future__ import annotations

# {portuguese: (english, spanish)}
COLUMNS: dict[str, tuple[str, str]] = {
    # --- area_embargada ---------------------------------------------------
    "Ano do termo de embargo": (
        "Year of the embargo order",
        "Año del acta de embargo",
    ),
    "Sigla da unidade da federação onde a área embargada se localiza": (
        "Abbreviation of the federative unit where the embargoed area is located",
        "Sigla de la unidad federativa donde se localiza el área embargada",
    ),
    "Identificador IBGE de 7 dígitos do município onde a área embargada se localiza": (
        "Seven-digit IBGE identifier of the municipality where the embargoed area is located",
        "Identificador IBGE de 7 dígitos del municipio donde se localiza el área embargada",
    ),
    "Identificador único do termo de embargo no sistema do Ibama": (
        "Unique identifier of the embargo order in the Ibama system",
        "Identificador único del acta de embargo en el sistema del Ibama",
    ),
    "Número impresso do termo de embargo": (
        "Printed number of the embargo order",
        "Número impreso del acta de embargo",
    ),
    "Série do formulário do termo de embargo": (
        "Form series of the embargo order",
        "Serie del formulario del acta de embargo",
    ),
    "Data do embargo": ("Date of the embargo", "Fecha del embargo"),
    "Identificador do auto de infração que originou o embargo": (
        "Identifier of the infraction notice that gave rise to the embargo",
        "Identificador del acta de infracción que originó el embargo",
    ),
    "Número impresso do auto de infração que originou o embargo": (
        "Printed number of the infraction notice that gave rise to the embargo",
        "Número impreso del acta de infracción que originó el embargo",
    ),
    "Área embargada": ("Embargoed area", "Área embargada"),
    "Tipo de área embargada": (
        "Type of embargoed area",
        "Tipo de área embargada",
    ),
    "Nome do imóvel onde se localiza a área embargada": (
        "Name of the property where the embargoed area is located",
        "Nombre del inmueble donde se localiza el área embargada",
    ),
    "Natureza jurídica do embargado": (
        "Legal nature of the embargoed party",
        "Naturaleza jurídica del embargado",
    ),
    "CPF ou CNPJ do embargado somente dígitos": (
        "CPF or CNPJ of the embargoed party, digits only",
        "CPF o CNPJ del embargado, solo dígitos",
    ),
    "Nome ou razão social do embargado": (
        "Name or corporate name of the embargoed party",
        "Nombre o razón social del embargado",
    ),
    "Latitude do local embargado em graus decimais": (
        "Latitude of the embargoed site in decimal degrees",
        "Latitud del sitio embargado en grados decimales",
    ),
    "Longitude do local embargado em graus decimais": (
        "Longitude of the embargoed site in decimal degrees",
        "Longitud del sitio embargado en grados decimales",
    ),
    "Indica se o ponto publicado cai dentro do polígono do município declarado": (
        "Indicates whether the published point falls inside the polygon of the declared municipality",
        "Indica si el punto publicado cae dentro del polígono del municipio declarado",
    ),
    "Indica se a área foi desembargada": (
        "Indicates whether the area was released from the embargo",
        "Indica si el área fue liberada del embargo",
    ),
    "Data do desembargo da área": (
        "Date the area was released from the embargo",
        "Fecha de liberación del embargo del área",
    ),
    "Data da última alteração do registro no sistema do Ibama": (
        "Date of the last change to the record in the Ibama system",
        "Fecha de la última modificación del registro en el sistema del Ibama",
    ),
    "Data da extração do arquivo publicado pelo Ibama": (
        "Date the file published by Ibama was extracted",
        "Fecha de extracción del archivo publicado por el Ibama",
    ),
    # --- auto_infracao ----------------------------------------------------
    "Ano da lavratura do auto de infração": (
        "Year the infraction notice was issued",
        "Año de emisión del acta de infracción",
    ),
    "Sigla da unidade da federação onde a infração foi registrada": (
        "Abbreviation of the federative unit where the infraction was recorded",
        "Sigla de la unidad federativa donde se registró la infracción",
    ),
    "Identificador IBGE de 7 dígitos do município onde a infração foi registrada": (
        "Seven-digit IBGE identifier of the municipality where the infraction was recorded",
        "Identificador IBGE de 7 dígitos del municipio donde se registró la infracción",
    ),
    "Identificador único do auto de infração no sistema do Ibama": (
        "Unique identifier of the infraction notice in the Ibama system",
        "Identificador único del acta de infracción en el sistema del Ibama",
    ),
    "Número impresso do auto de infração": (
        "Printed number of the infraction notice",
        "Número impreso del acta de infracción",
    ),
    "Série do formulário do auto de infração": (
        "Form series of the infraction notice",
        "Serie del formulario del acta de infracción",
    ),
    "Data da lavratura do auto de infração": (
        "Date the infraction notice was issued",
        "Fecha de emisión del acta de infracción",
    ),
    "Tipo de auto lavrado": ("Type of notice issued", "Tipo de acta emitida"),
    "Bem tutelado atingido pela infração": (
        "Protected environmental asset affected by the infraction",
        "Bien tutelado afectado por la infracción",
    ),
    "Gravidade atribuída à infração": (
        "Severity assigned to the infraction",
        "Gravedad atribuida a la infracción",
    ),
    "Valor da multa aplicada no auto de infração": (
        "Amount of the fine imposed in the infraction notice",
        "Monto de la multa impuesta en el acta de infracción",
    ),
    "Moeda em que o valor da multa foi fixado": (
        "Currency in which the fine amount was set",
        "Moneda en que se fijó el monto de la multa",
    ),
    "Situação do débito da multa na cobrança administrativa e judicial": (
        "Status of the fine debt in administrative and judicial collection",
        "Situación de la deuda de la multa en el cobro administrativo y judicial",
    ),
    "Situação do formulário do auto de infração": (
        "Status of the infraction notice form",
        "Situación del formulario del acta de infracción",
    ),
    "Indica se o auto de infração foi cancelado": (
        "Indicates whether the infraction notice was cancelled",
        "Indica si el acta de infracción fue cancelada",
    ),
    "Natureza jurídica do autuado": (
        "Legal nature of the cited party",
        "Naturaleza jurídica del infractor",
    ),
    "CPF ou CNPJ do autuado somente dígitos": (
        "CPF or CNPJ of the cited party, digits only",
        "CPF o CNPJ del infractor, solo dígitos",
    ),
    "Nome ou razão social do autuado": (
        "Name or corporate name of the cited party",
        "Nombre o razón social del infractor",
    ),
    "Latitude do local da infração em graus decimais": (
        "Latitude of the infraction site in decimal degrees",
        "Latitud del sitio de la infracción en grados decimales",
    ),
    "Longitude do local da infração em graus decimais": (
        "Longitude of the infraction site in decimal degrees",
        "Longitud del sitio de la infracción en grados decimales",
    ),
    # --- dicionario -------------------------------------------------------
    "Nome da tabela que contém a coluna descrita pela entrada do dicionário": (
        "Name of the table containing the column described by the dictionary entry",
        "Nombre de la tabla que contiene la columna descrita por la entrada del diccionario",
    ),
    "Nome da coluna descrita pela entrada do dicionário": (
        "Name of the column described by the dictionary entry",
        "Nombre de la columna descrita por la entrada del diccionario",
    ),
    "Código armazenado na coluna": (
        "Code stored in the column",
        "Código almacenado en la columna",
    ),
    "Cobertura temporal da entrada do dicionário": (
        "Temporal coverage of the dictionary entry",
        "Cobertura temporal de la entrada del diccionario",
    ),
    "Significado do código armazenado na coluna": (
        "Meaning of the code stored in the column",
        "Significado del código almacenado en la columna",
    ),
}

TABLE_NAMES: dict[str, tuple[str, str, str]] = {
    "auto_infracao": (
        "Autos de infração",
        "Infraction notices",
        "Actas de infracción",
    ),
    "area_embargada": (
        "Áreas embargadas",
        "Embargoed areas",
        "Áreas embargadas",
    ),
    "dicionario": ("Dicionário", "Dictionary", "Diccionario"),
}

TABLE_DESCRIPTIONS: dict[str, tuple[str, str, str]] = {
    "auto_infracao": (
        "Autos de infração ambiental lavrados pelo Ibama, com o bem tutelado atingido, "
        "o valor da multa, a situação do débito e a localização declarada. Uma linha por "
        "auto de infração. A situação do débito e a moeda vêm da série de multas "
        "ambientais distribuídas por bens tutelados (Sicafi).",
        "Environmental infraction notices issued by Ibama, with the protected asset "
        "affected, the fine amount, the debt status and the declared location. One row per "
        "infraction notice. Debt status and currency come from the Sicafi series of "
        "environmental fines by protected asset.",
        "Actas de infracción ambiental emitidas por el Ibama, con el bien tutelado "
        "afectado, el monto de la multa, la situación de la deuda y la ubicación "
        "declarada. Una fila por acta de infracción. La situación de la deuda y la moneda "
        "provienen de la serie de multas ambientales por bien tutelado (Sicafi).",
    ),
    "area_embargada": (
        "Termos de embargo lavrados pelo Ibama, com a área embargada, o tipo de área, a "
        "situação de desembargo e a ligação com o auto de infração que os originou. Uma "
        "linha por termo de embargo.",
        "Embargo orders issued by Ibama, with the embargoed area, the area type, the "
        "release status and the link to the infraction notice that gave rise to them. One "
        "row per embargo order.",
        "Actas de embargo emitidas por el Ibama, con el área embargada, el tipo de área, "
        "la situación de liberación y el vínculo con el acta de infracción que las "
        "originó. Una fila por acta de embargo.",
    ),
    "dicionario": (
        "Dicionário dos códigos usados nas colunas categóricas de br_ibama_fiscalizacao.",
        "Dictionary of the codes used in the categorical columns of br_ibama_fiscalizacao.",
        "Diccionario de los códigos usados en las columnas categóricas de "
        "br_ibama_fiscalizacao.",
    ),
}

DATASET_NAME = (
    "Fiscalização Ambiental",
    "Environmental Enforcement",
    "Fiscalización Ambiental",
)

DATASET_DESCRIPTION = (
    "Registros de fiscalização ambiental do Ibama: autos de infração lavrados desde 1977 "
    "e termos de embargo desde 1987, com o enquadramento, o valor da multa, a situação do "
    "débito, a área embargada e a localização declarada. Os dados são publicados "
    "diariamente no portal de dados abertos do Ibama e reproduzem os sistemas Sifisc e "
    "Sicafi.",
    "Environmental enforcement records from Ibama: infraction notices issued since 1977 "
    "and embargo orders since 1987, with the legal basis, the fine amount, the debt "
    "status, the embargoed area and the declared location. The data are published daily on "
    "Ibama's open data portal and mirror the Sifisc and Sicafi systems.",
    "Registros de fiscalización ambiental del Ibama: actas de infracción emitidas desde "
    "1977 y actas de embargo desde 1987, con el encuadre legal, el monto de la multa, la "
    "situación de la deuda, el área embargada y la ubicación declarada. Los datos se "
    "publican diariamente en el portal de datos abiertos del Ibama y reproducen los "
    "sistemas Sifisc y Sicafi.",
)
