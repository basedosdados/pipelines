"""Portuguese and Spanish for every column description, written out in full.

Keyed by the English string so a description reused across tables is translated
once. Word-by-word substitution was tried on an earlier dataset and produced
broken grammar ("de o instituição"), so each entry is written explicitly.

Rules from .claude/rules/data-basis-style.md: every description starts with a
capital letter and none ends with a full stop, in all three languages.
"""

from __future__ import annotations

# english -> (portuguese, spanish)
DESCRIPTIONS: dict[str, tuple[str, str]] = {
    "Abbreviated name of the holding company": (
        "Nome abreviado da holding",
        "Nombre abreviado de la sociedad controladora",
    ),
    "Assessment area number the institution assigned to the area": (
        "Número da área de avaliação atribuído pela instituição à região",
        "Número del área de evaluación asignado por la institución a la región",
    ),
    "Calendar quarter of the report, 1 to 4": (
        "Trimestre civil do relatório, de 1 a 4",
        "Trimestre calendario del informe, de 1 a 4",
    ),
    "Call Report form the institution filed for the quarter": (
        "Formulário do Call Report entregue pela instituição no trimestre",
        "Formulario del Call Report presentado por la institución en el trimestre",
    ),
    "Call Report schedule the item was filed on, such as RC for the balance sheet or RI for the income statement": (
        "Anexo do Call Report em que o item foi declarado, como RC para o balanço "
        "patrimonial ou RI para a demonstração de resultados",
        "Anexo del Call Report en el que se declaró la partida, como RC para el "
        "balance o RI para el estado de resultados",
    ),
    "Charter type code assigned by the Federal Reserve": (
        "Código do tipo de carta patente atribuído pelo Federal Reserve",
        "Código del tipo de licencia asignado por la Reserva Federal",
    ),
    "Chave do dicionário": ("Chave do dicionário", "Clave del diccionario"),
    "City of the holding company's physical location": (
        "Cidade do endereço físico da holding",
        "Ciudad del domicilio físico de la sociedad controladora",
    ),
    "City of the institution's main office": (
        "Cidade da sede da instituição",
        "Ciudad de la casa matriz de la institución",
    ),
    "City of the reporting institution": (
        "Cidade da instituição declarante",
        "Ciudad de la institución declarante",
    ),
    "Cobertura temporal da chave": (
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    "Eight-character MDRM identifier of the FR Y-9C item; joins to mdrm_item for the item's name, type and unit": (
        "Identificador MDRM de oito caracteres do item do FR Y-9C; relaciona-se a "
        "mdrm_item para obter nome, tipo e unidade do item",
        "Identificador MDRM de ocho caracteres de la partida del FR Y-9C; se une a "
        "mdrm_item para obtener nombre, tipo y unidad de la partida",
    ),
    "Eight-character MDRM identifier, the concatenation of mnemonic and item number": (
        "Identificador MDRM de oito caracteres, formado pela concatenação do "
        "mnemônico com o número do item",
        "Identificador MDRM de ocho caracteres, formado por la concatenación del "
        "mnemónico con el número de la partida",
    ),
    "Eight-character MDRM identifier: a four-character mnemonic giving the report and consolidation basis, then the four-character item number; joins to mdrm_item for the item's name, type and unit": (
        "Identificador MDRM de oito caracteres: um mnemônico de quatro caracteres "
        "que indica o relatório e a base de consolidação, seguido do número do item "
        "com quatro caracteres; relaciona-se a mdrm_item para obter nome, tipo e "
        "unidade do item",
        "Identificador MDRM de ocho caracteres: un mnemónico de cuatro caracteres "
        "que indica el informe y la base de consolidación, seguido del número de la "
        "partida con cuatro caracteres; se une a mdrm_item para obtener nombre, tipo "
        "y unidad de la partida",
    ),
    "Eleven-digit census tract identifier, the county FIPS code followed by the six-digit tract number": (
        "Identificador do setor censitário com onze dígitos, formado pelo código "
        "FIPS do condado seguido do número do setor com seis dígitos",
        "Identificador del sector censal de once dígitos, formado por el código FIPS "
        "del condado seguido del número del sector con seis dígitos",
    ),
    "Employer identification number of the entity": (
        "Número de identificação fiscal do empregador da entidade",
        "Número de identificación fiscal del empleador de la entidad",
    ),
    "Employer identification number of the institution": (
        "Número de identificação fiscal do empregador da instituição",
        "Número de identificación fiscal del empleador de la institución",
    ),
    "FDIC certificate number of the holding company, when it holds one": (
        "Número do certificado FDIC da holding, quando ela possui um",
        "Número de certificado FDIC de la sociedad controladora, cuando posee uno",
    ),
    "FDIC certificate number, the institution key used by us_fdic_bankfind": (
        "Número do certificado FDIC, chave da instituição usada em us_fdic_bankfind",
        "Número de certificado FDIC, clave de la institución usada en us_fdic_bankfind",
    ),
    "Federal Reserve district that supervises the entity, 1 to 12": (
        "Distrito do Federal Reserve que supervisiona a entidade, de 1 a 12",
        "Distrito de la Reserva Federal que supervisa la entidad, de 1 a 12",
    ),
    "First date on which the item was collected": (
        "Primeira data em que o item passou a ser coletado",
        "Primera fecha en que se comenzó a recolectar la partida",
    ),
    "First four characters of the identifier, naming the report series and the consolidation basis, such as RCON for domestic offices or RCFD for consolidated domestic and foreign offices": (
        "Quatro primeiros caracteres do identificador, que nomeiam a série do "
        "relatório e a base de consolidação, como RCON para agências domésticas ou "
        "RCFD para agências domésticas e estrangeiras consolidadas",
        "Primeros cuatro caracteres del identificador, que nombran la serie del "
        "informe y la base de consolidación, como RCON para oficinas nacionales o "
        "RCFD para oficinas nacionales y extranjeras consolidadas",
    ),
    "Five-digit FIPS code of the county of the physical location": (
        "Código FIPS de cinco dígitos do condado do endereço físico",
        "Código FIPS de cinco dígitos del condado del domicilio físico",
    ),
    "Five-digit county FIPS code, the state code followed by the county code": (
        "Código FIPS do condado com cinco dígitos, formado pelo código do estado "
        "seguido do código do condado",
        "Código FIPS del condado de cinco dígitos, formado por el código del estado "
        "seguido del código del condado",
    ),
    "Former OTS docket number for thrift institutions": (
        "Antigo número de registro no OTS das instituições de poupança",
        "Antiguo número de registro en la OTS de las instituciones de ahorro",
    ),
    "Full item definition as published in the MDRM": (
        "Definição completa do item conforme publicada no MDRM",
        "Definición completa de la partida según se publica en el MDRM",
    ),
    "Geographic and assessment-area level the row totals over, from a single county up to a nationwide total": (
        "Nível geográfico e de área de avaliação sobre o qual a linha é totalizada, "
        "de um único condado até o total nacional",
        "Nivel geográfico y de área de evaluación sobre el que se totaliza la fila, "
        "desde un único condado hasta el total nacional",
    ),
    "Income group of the census tracts the lending is grouped into, as a share of area median family income": (
        "Faixa de renda dos setores censitários em que o crédito é agrupado, como "
        "proporção da renda familiar mediana da região",
        "Grupo de ingreso de los sectores censales en que se agrupa el crédito, como "
        "proporción del ingreso familiar mediano de la región",
    ),
    "Item name as published in the MDRM data dictionary": (
        "Nome do item conforme publicado no dicionário de dados MDRM",
        "Nombre de la partida según se publica en el diccionario de datos MDRM",
    ),
    "Last calendar day of the reporting quarter": (
        "Último dia civil do trimestre de referência",
        "Último día calendario del trimestre de referencia",
    ),
    "Last date on which the item was collected": (
        "Última data em que o item foi coletado",
        "Última fecha en que se recolectó la partida",
    ),
    "Last four characters of the identifier, naming the item itself; the same number carries the same meaning across reporting forms": (
        "Quatro últimos caracteres do identificador, que nomeiam o próprio item; o "
        "mesmo número tem o mesmo significado em todos os formulários",
        "Últimos cuatro caracteres del identificador, que nombran la partida en sí; "
        "el mismo número tiene el mismo significado en todos los formularios",
    ),
    "Legal name of the financial institution": (
        "Razão social da instituição financeira",
        "Razón social de la institución financiera",
    ),
    "Legal name of the holding company": (
        "Razão social da holding",
        "Razón social de la sociedad controladora",
    ),
    "Loan size or borrower revenue band the count and amount refer to": (
        "Faixa de valor do empréstimo ou de receita do tomador a que se referem a "
        "quantidade e o montante",
        "Rango de monto del préstamo o de ingresos del prestatario al que se "
        "refieren la cantidad y el monto",
    ),
    "MDRM glossary entry for the mnemonic": (
        "Verbete do glossário MDRM correspondente ao mnemônico",
        "Entrada del glosario MDRM correspondiente al mnemónico",
    ),
    "MDRM item type: F financial, D derived, R rate, P percentage, S structure, E examination, J projected": (
        "Tipo do item no MDRM: F financeiro, D derivado, R taxa, P percentual, "
        "S estrutura, E supervisão, J projetado",
        "Tipo de partida en el MDRM: F financiera, D derivada, R tasa, P porcentaje, "
        "S estructura, E supervisión, J proyectada",
    ),
    "Metropolitan statistical area or metropolitan division code as defined by OMB": (
        "Código da área estatística metropolitana ou da divisão metropolitana "
        "conforme definido pelo OMB",
        "Código del área estadística metropolitana o de la división metropolitana "
        "según la definición de la OMB",
    ),
    "NAICS code of the entity's primary activity, 551111 for a bank holding company": (
        "Código NAICS da atividade principal da entidade, 551111 para holdings "
        "bancárias",
        "Código NAICS de la actividad principal de la entidad, 551111 para "
        "sociedades controladoras de bancos",
    ),
    "Name of the reporting institution": (
        "Nome da instituição declarante",
        "Nombre de la institución declarante",
    ),
    "Nome da coluna": ("Nome da coluna", "Nombre de la columna"),
    "Nome da tabela": ("Nome da tabela", "Nombre de la tabla"),
    "Number of banks the holding company controls": (
        "Quantidade de bancos controlados pela holding",
        "Cantidad de bancos controlados por la sociedad controladora",
    ),
    "Number of loans in the band": (
        "Quantidade de empréstimos na faixa",
        "Cantidad de préstamos en el rango",
    ),
    "OCC charter number for nationally chartered banks": (
        "Número da carta patente no OCC dos bancos com carta federal",
        "Número de licencia de la OCC de los bancos con licencia federal",
    ),
    "OCC charter number, when the entity holds one": (
        "Número da carta patente no OCC, quando a entidade possui uma",
        "Número de licencia de la OCC, cuando la entidad posee una",
    ),
    "Organization type code assigned by the Federal Reserve": (
        "Código do tipo de organização atribuído pelo Federal Reserve",
        "Código del tipo de organización asignado por la Reserva Federal",
    ),
    "Primary ABA routing number": (
        "Número principal de roteamento ABA",
        "Número principal de enrutamiento ABA",
    ),
    "RSSD identifier assigned by the Federal Reserve, unique per institution and stable across name and charter changes; the join key across every table in this dataset and the correspondent of `cert` in us_fdic_bankfind": (
        "Identificador RSSD atribuído pelo Federal Reserve, único por instituição e "
        "estável diante de mudanças de nome e de carta patente; é a chave de junção "
        "entre todas as tabelas deste conjunto e o correspondente de `cert` em "
        "us_fdic_bankfind",
        "Identificador RSSD asignado por la Reserva Federal, único por institución y "
        "estable ante cambios de nombre y de licencia; es la clave de unión entre "
        "todas las tablas de este conjunto y el correspondiente de `cert` en "
        "us_fdic_bankfind",
    ),
    "RSSD identifier of the entity's head office, for subsidiaries of a larger group": (
        "Identificador RSSD da sede da entidade, no caso de subsidiárias de um grupo "
        "maior",
        "Identificador RSSD de la casa matriz de la entidad, en el caso de "
        "subsidiarias de un grupo mayor",
    ),
    "RSSD identifier of the reporting institution, taken from the CRA transmittal sheet for the same respondent, agency and year; the join key to institution, call_report_item and holding_company": (
        "Identificador RSSD da instituição declarante, obtido da folha de "
        "transmissão do CRA para o mesmo declarante, agência e ano; é a chave de "
        "junção com institution, call_report_item e holding_company",
        "Identificador RSSD de la institución declarante, obtenido de la hoja de "
        "transmisión del CRA para el mismo declarante, agencia y año; es la clave de "
        "unión con institution, call_report_item y holding_company",
    ),
    "Reporting forms that collect the item, separated by semicolons": (
        "Formulários que coletam o item, separados por ponto e vírgula",
        "Formularios que recolectan la partida, separados por punto y coma",
    ),
    "Respondent identifier assigned by the institution's supervisory agency; unique only together with agency and year, and not an RSSD": (
        "Identificador do declarante atribuído pela agência supervisora da "
        "instituição; é único apenas em conjunto com a agência e o ano, e não é um "
        "RSSD",
        "Identificador del declarante asignado por la agencia supervisora de la "
        "institución; es único solo junto con la agencia y el año, y no es un RSSD",
    ),
    "Street address of the holding company's physical location": (
        "Logradouro do endereço físico da holding",
        "Domicilio físico de la sociedad controladora",
    ),
    "Street address of the institution's main office": (
        "Logradouro da sede da instituição",
        "Domicilio de la casa matriz de la institución",
    ),
    "Street address of the reporting institution": (
        "Logradouro da instituição declarante",
        "Domicilio de la institución declarante",
    ),
    "Supervisory agency that assigned the respondent identifier: occ, frs, fdic, or ots for the Office of Thrift Supervision, which was abolished in 2011 and appears only up to 2010": (
        "Agência supervisora que atribuiu o identificador do declarante: occ, frs, "
        "fdic, ou ots para o Office of Thrift Supervision, extinto em 2011 e "
        "presente apenas até 2010",
        "Agencia supervisora que asignó el identificador del declarante: occ, frs, "
        "fdic, u ots para la Office of Thrift Supervision, suprimida en 2011 y "
        "presente solo hasta 2010",
    ),
    "Timestamp at which the institution last updated its submission for the quarter": (
        "Data e hora em que a instituição atualizou pela última vez sua entrega do "
        "trimestre",
        "Fecha y hora en que la institución actualizó por última vez su presentación "
        "del trimestre",
    ),
    "Total amount of the loans in the band": (
        "Montante total dos empréstimos na faixa",
        "Monto total de los préstamos en el rango",
    ),
    "Total assets reported on the prior year-end Call Report": (
        "Ativo total declarado no Call Report do encerramento do ano anterior",
        "Activo total declarado en el Call Report del cierre del año anterior",
    ),
    "Two-digit state FIPS code": (
        "Código FIPS do estado com dois dígitos",
        "Código FIPS del estado de dos dígitos",
    ),
    "Two-letter postal abbreviation of the institution's state": (
        "Sigla postal de duas letras do estado da instituição",
        "Sigla postal de dos letras del estado de la institución",
    ),
    "Two-letter postal abbreviation of the state of the main office": (
        "Sigla postal de duas letras do estado da sede",
        "Sigla postal de dos letras del estado de la casa matriz",
    ),
    "Two-letter postal abbreviation of the state of the physical location": (
        "Sigla postal de duas letras do estado do endereço físico",
        "Sigla postal de dos letras del estado del domicilio físico",
    ),
    "Unit of the values carried for this item in call_report_item and holding_company_item": (
        "Unidade dos valores deste item em call_report_item e holding_company_item",
        "Unidad de los valores de esta partida en call_report_item y "
        "holding_company_item",
    ),
    "Valor correspondente à chave": (
        "Valor correspondente à chave",
        "Valor correspondiente a la clave",
    ),
    "Value the holding company reported for the item": (
        "Valor declarado pela holding para o item",
        "Valor declarado por la sociedad controladora para la partida",
    ),
    "Value the institution reported for the item": (
        "Valor declarado pela instituição para o item",
        "Valor declarado por la institución para la partida",
    ),
    "Whether the MDRM marks the item as confidential and therefore not publicly disclosed, 1 for yes": (
        "Indica se o MDRM classifica o item como confidencial e portanto não "
        "divulgado publicamente, 1 para sim",
        "Indica si el MDRM clasifica la partida como confidencial y por lo tanto no "
        "divulgada públicamente, 1 para sí",
    ),
    "Whether the assessment area covers only part of the county, 1 for yes": (
        "Indica se a área de avaliação cobre apenas parte do condado, 1 para sim",
        "Indica si el área de evaluación cubre solo parte del condado, 1 para sí",
    ),
    "Whether the county has fewer or more than 500,000 residents": (
        "Indica se o condado tem menos ou mais de 500.000 habitantes",
        "Indica si el condado tiene menos o más de 500.000 habitantes",
    ),
    "Whether the county is split across more than one assessment area, 1 for yes": (
        "Indica se o condado está dividido entre mais de uma área de avaliação, 1 "
        "para sim",
        "Indica si el condado está dividido entre más de un área de evaluación, 1 "
        "para sí",
    ),
    "Whether the entity has elected financial holding company status, 1 for yes": (
        "Indica se a entidade optou pelo regime de holding financeira, 1 para sim",
        "Indica si la entidad optó por el régimen de sociedad controladora "
        "financiera, 1 para sí",
    ),
    "Whether the entity is a savings and loan holding company, 1 for yes": (
        "Indica se a entidade é uma holding de poupança e empréstimo, 1 para sim",
        "Indica si la entidad es una sociedad controladora de ahorro y préstamo, 1 "
        "para sí",
    ),
    "Whether the item is a yes/no answer or an indicator code rather than a magnitude, 1 for yes": (
        "Indica se o item é uma resposta de sim ou não ou um código indicador em vez "
        "de uma grandeza, 1 para sim",
        "Indica si la partida es una respuesta de sí o no o un código indicador en "
        "lugar de una magnitud, 1 para sí",
    ),
    "Whether the loans were originated by the reporting institution or purchased from another lender": (
        "Indica se os empréstimos foram originados pela instituição declarante ou "
        "adquiridos de outro credor",
        "Indica si los préstamos fueron originados por la institución declarante o "
        "adquiridos de otro prestamista",
    ),
    "Whether the row covers small business or small farm lending": (
        "Indica se a linha se refere a crédito para pequenas empresas ou para "
        "pequenos produtores rurais",
        "Indica si la fila se refiere a crédito para pequeñas empresas o para "
        "pequeños productores agrícolas",
    ),
    "Year of the reporting quarter": (
        "Ano do trimestre de referência",
        "Año del trimestre de referencia",
    ),
    "Year the assessment area was reported for": (
        "Ano a que se refere a área de avaliação declarada",
        "Año al que se refiere el área de evaluación declarada",
    ),
    "Year the institution reported CRA data for": (
        "Ano a que se referem os dados do CRA declarados pela instituição",
        "Año al que se refieren los datos del CRA declarados por la institución",
    ),
    "Year the lending activity was reported for": (
        "Ano a que se refere a atividade de crédito declarada",
        "Año al que se refiere la actividad crediticia declarada",
    ),
    "ZIP code of the holding company's physical location": (
        "Código postal do endereço físico da holding",
        "Código postal del domicilio físico de la sociedad controladora",
    ),
    "ZIP code of the institution's main office": (
        "Código postal da sede da instituição",
        "Código postal de la casa matriz de la institución",
    ),
    "ZIP code of the reporting institution": (
        "Código postal da instituição declarante",
        "Código postal de la institución declarante",
    ),
    "Name of the table the coded column belongs to": (
        "Nome da tabela a que pertence a coluna codificada",
        "Nombre de la tabla a la que pertenece la columna codificada",
    ),
    "Name of the coded column": (
        "Nome da coluna codificada",
        "Nombre de la columna codificada",
    ),
    "Value as stored in the column": (
        "Valor tal como armazenado na coluna",
        "Valor tal como está almacenado en la columna",
    ),
    "Years the key applies to, blank when it applies to the table's whole span": (
        "Anos a que a chave se aplica, em branco quando se aplica a todo o período "
        "da tabela",
        "Años a los que se aplica la clave, en blanco cuando se aplica a todo el "
        "período de la tabla",
    ),
    "Meaning of the key": ("Significado da chave", "Significado de la clave"),
}
