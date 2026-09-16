"""Emit the architecture CSVs + columns_json for us_epa_tri (BD source of truth).

Single spec, two outputs:
  * ``code/architecture/<table>.csv`` — the BD architecture table. The cleaning
    transform reads column ORDER from it and the dbt generator reads the types.
  * ``code/columns_json/<table>.json`` — trilingual column definitions used at
    metadata registration (``bulk_upsert_columns``).

The spec follows the EPA "TRI Basic Data Files Documentation" (August 2024),
field numbers 1-122. ``original_name`` carries the source header without its
sequence number.

Units. The source reports every quantity in pounds except dioxin and
dioxin-like compounds (``classification = Dioxin``), reported in grams. Every
quantity column here is in ONE unit: ``release.quantity_pounds`` and the
``form`` totals are pounds for every row (grams / 453.59237 for dioxins);
``release.quantity_grams`` carries the dioxin rows as reported.
"""

import csv
import json
from pathlib import Path

HERE = Path(__file__).parent
JSON_DIR = HERE / "columns_json"
ARCH_DIR = HERE / "architecture"

ARCH_HEADER = [
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

DIR_YEAR = "br_bd_diretorios_data_tempo.ano:ano"
DIR_STATE = "br_bd_diretorios_us.state:abbreviation"
DIR_COUNTY = "br_bd_diretorios_us.county:id_county"
DIR_SIC = "br_bd_diretorios_us.sic:id_sic"

# Observations are written once in English and translated here so every note
# ships in all three languages.
_OBS_I18N = {
    "Partition column": ("Coluna de partição", "Columna de partición"),
    "Derived: county FIPS code looked up by TRI facility ID in the Envirofacts "
    "TRI_FACILITY table (STATE_COUNTY_FIPS_CODE); the Basic file itself carries "
    "only the county name. The source's 00000 (unknown) is null. Connecticut "
    "facilities carry the legacy county codes 09001-09015, which the county "
    "directory (2022 planning regions) does not list": (
        "Derivada: código FIPS do condado obtido pelo identificador TRI da "
        "instalação na tabela TRI_FACILITY do Envirofacts "
        "(STATE_COUNTY_FIPS_CODE); o arquivo Basic traz apenas o nome do condado. "
        "O valor 00000 (desconhecido) da fonte é nulo. Instalações de Connecticut "
        "trazem os códigos de condado antigos 09001-09015, ausentes do diretório "
        "de condados (regiões de planejamento de 2022)",
        "Derivada: código FIPS del condado obtenido por el identificador TRI de "
        "la instalación en la tabla TRI_FACILITY de Envirofacts "
        "(STATE_COUNTY_FIPS_CODE); el archivo Basic trae solo el nombre del "
        "condado. El valor 00000 (desconocido) de la fuente es nulo. Las "
        "instalaciones de Connecticut traen los códigos de condado antiguos "
        "09001-09015, ausentes del directorio de condados (regiones de "
        "planificación de 2022)",
    ),
    "Includes the District of Columbia and the territories AS, GU, MP, PR and "
    "VI": (
        "Inclui o Distrito de Colúmbia e os territórios AS, GU, MP, PR e VI",
        "Incluye el Distrito de Columbia y los territorios AS, GU, MP, PR y VI",
    ),
    "Collected from facilities through RY 2004; obtained from the Facility "
    "Registry Service since RY 2005": (
        "Coletada das instalações até o ano de referência 2004; obtida do "
        "Facility Registry Service a partir de 2005",
        "Recogida de las instalaciones hasta el año de referencia 2004; "
        "obtenida del Facility Registry Service desde 2005",
    ),
    "Reported by facilities from RY 1987 through 2005; empty afterwards. The "
    "source's INVA (invalid) and NA are null": (
        "Informado pelas instalações de 1987 a 2005; vazio depois. Os valores "
        "INVA (inválido) e NA da fonte são nulos",
        "Informado por las instalaciones de 1987 a 2005; vacío después. Los "
        "valores INVA (inválido) y NA de la fuente son nulos",
    ),
    "Reported by facilities from RY 2006 on; assigned by EPA to 1987-2005 "
    "submissions (see Appendix D of the source documentation)": (
        "Informado pelas instalações a partir de 2006; atribuído pela EPA às "
        "submissões de 1987 a 2005 (ver Apêndice D da documentação da fonte)",
        "Informado por las instalaciones desde 2006; asignado por la EPA a las "
        "presentaciones de 1987 a 2005 (ver Apéndice D de la documentación de "
        "la fuente)",
    ),
    "Derived from the reporting year: 1987-2007 -> 2002, 2008-2012 -> 2007, "
    "2013-2016 -> 2012, 2017-2021 -> 2017, 2022 on -> 2022 (TRI adopts each "
    "NAICS revision the year after Census). EPA assigned the 1987-2005 codes "
    "in the 2002 vintage": (
        "Derivada do ano de referência: 1987-2007 -> 2002, 2008-2012 -> 2007, "
        "2013-2016 -> 2012, 2017-2021 -> 2017, 2022 em diante -> 2022 (o TRI "
        "adota cada revisão do NAICS no ano seguinte ao Census). A EPA atribuiu "
        "os códigos de 1987-2005 na versão 2002",
        "Derivada del año de referencia: 1987-2007 -> 2002, 2008-2012 -> 2007, "
        "2013-2016 -> 2012, 2017-2021 -> 2017, 2022 en adelante -> 2022 (el TRI "
        "adopta cada revisión del NAICS el año siguiente al Census). La EPA "
        "asignó los códigos de 1987-2005 en la versión 2002",
    ),
    "Collected since RY 2018": (
        "Coletado a partir do ano de referência 2018",
        "Recogido desde el año de referencia 2018",
    ),
    "The value 9999999999 marks sanitized trade-secret submissions": (
        "O valor 9999999999 marca submissões de segredo industrial "
        "descaracterizadas",
        "El valor 9999999999 marca presentaciones de secreto comercial "
        "anonimizadas",
    ),
    "As reported on the form; the canonical name per chemical ID is in the "
    "chemical table": (
        "Conforme informado no formulário; o nome canônico por identificador "
        "químico está na tabela chemical",
        "Según lo informado en el formulario; el nombre canónico por "
        "identificador químico está en la tabla chemical",
    ),
    "Unit the source used for this form: Pounds, or Grams for dioxin and "
    "dioxin-like compounds. Every quantity column in this dataset has already "
    "been put in a single unit": (
        "Unidade usada pela fonte neste formulário: Pounds (libras), ou Grams "
        "(gramas) para dioxinas e compostos similares. Toda coluna de "
        "quantidade deste conjunto já está em uma única unidade",
        "Unidad usada por la fuente en este formulario: Pounds (libras), o "
        "Grams (gramos) para dioxinas y compuestos similares. Toda columna de "
        "cantidad de este conjunto ya está en una única unidad",
    ),
    "Pounds for every row; dioxin quantities reported in grams were divided by "
    "453.59237": (
        "Libras em todas as linhas; quantidades de dioxinas informadas em "
        "gramas foram divididas por 453,59237",
        "Libras en todas las filas; cantidades de dioxinas informadas en gramos "
        "se dividieron por 453,59237",
    ),
    "Only dioxin and dioxin-like compounds (classification Dioxin), as "
    "reported; null for every other chemical": (
        "Apenas dioxinas e compostos similares (classificação Dioxin), conforme "
        "informado; nulo para os demais produtos químicos",
        "Solo dioxinas y compuestos similares (clasificación Dioxin), según lo "
        "informado; nulo para los demás productos químicos",
    ),
    "Sum of the on-site release categories (section 5); pounds": (
        "Soma das categorias de liberação no local (seção 5); libras",
        "Suma de las categorías de liberación en el sitio (sección 5); libras",
    ),
    "Reported from RY 1987 through 1995; replaced in 1996 by the Class I and "
    "Class II-V wells split": (
        "Informado de 1987 a 1995; substituído em 1996 pela divisão entre poços "
        "Classe I e Classe II-V",
        "Informado de 1987 a 1995; reemplazado en 1996 por la división entre "
        "pozos Clase I y Clase II-V",
    ),
    "Reported from RY 1987 through 1995; replaced in 1996 by the RCRA C and "
    "other landfills split": (
        "Informado de 1987 a 1995; substituído em 1996 pela divisão entre "
        "aterros RCRA C e outros aterros",
        "Informado de 1987 a 1995; reemplazado en 1996 por la división entre "
        "rellenos RCRA C y otros rellenos",
    ),
    "Reported from RY 1987 through 2002; replaced in 2003 by the RCRA C and "
    "other surface impoundments split": (
        "Informado de 1987 a 2002; substituído em 2003 pela divisão entre "
        "lagoas RCRA C e outras lagoas de contenção",
        "Informado de 1987 a 2002; reemplazado en 2003 por la división entre "
        "lagunas RCRA C y otras lagunas de contención",
    ),
    "Reported from RY 1987 through 2002; split into 8.1A-8.1D from 2003": (
        "Informado de 1987 a 2002; dividido em 8.1A-8.1D a partir de 2003",
        "Informado de 1987 a 2002; dividido en 8.1A-8.1D desde 2003",
    ),
    "Reported from RY 2003 on": (
        "Informado a partir de 2003",
        "Informado desde 2003",
    ),
    "Rows with a zero quantity are not stored: the source fills zeros where the "
    "facility reported NA, left the field blank, or filed a Form A": (
        "Linhas com quantidade zero não são armazenadas: a fonte preenche zeros "
        "onde a instalação informou NA, deixou o campo em branco ou entregou um "
        "Form A",
        "Las filas con cantidad cero no se almacenan: la fuente rellena ceros "
        "donde la instalación informó NA, dejó el campo en blanco o presentó un "
        "Form A",
    ),
    "Groups release categories the way the source's own totals do: on-site "
    "release, off-site release (disposal), recycling, energy recovery, "
    "treatment, unclassified": (
        "Agrupa as categorias de liberação como os totais da própria fonte: "
        "liberação no local, liberação fora do local (disposição), reciclagem, "
        "recuperação de energia, tratamento, não classificado",
        "Agrupa las categorías de liberación como los totales de la propia "
        "fuente: liberación en el sitio, liberación fuera del sitio "
        "(disposición), reciclaje, recuperación de energía, tratamiento, no "
        "clasificado",
    ),
    "Form R section and off-site management code (M-code) the quantity was "
    "reported under; categories are mutually exclusive, so summing quantity "
    "over a form reproduces the source's total transfers plus on-site "
    "releases": (
        "Seção do Form R e código de gestão fora do local (código M) sob o qual "
        "a quantidade foi informada; as categorias são mutuamente exclusivas, "
        "de modo que somar a quantidade de um formulário reproduz o total de "
        "transferências mais liberações no local da fonte",
        "Sección del Form R y código de gestión fuera del sitio (código M) bajo "
        "el cual se informó la cantidad; las categorías son mutuamente "
        "excluyentes, de modo que sumar la cantidad de un formulario reproduce "
        "el total de transferencias más liberaciones en el sitio de la fuente",
    ),
    "Unique within a reporting year": (
        "Único dentro de um ano de referência",
        "Único dentro de un año de referencia",
    ),
    "Codes 1-4 in the source documentation appear in the file as text labels "
    "(Elemental metals, Metal compound categories, ...)": (
        "Os códigos 1-4 da documentação da fonte aparecem no arquivo como "
        "rótulos de texto (Elemental metals, Metal compound categories, ...)",
        "Los códigos 1-4 de la documentación de la fuente aparecen en el "
        "archivo como etiquetas de texto (Elemental metals, Metal compound "
        "categories, ...)",
    ),
    "Attributes as published in each reporting year, so a chemical's "
    "classifications can change across years; the PFAS flag exists since RY 2020": (
        "Atributos conforme publicados em cada ano de referência, de modo que as "
        "classificações de um produto químico podem mudar entre anos; a marcação "
        "PFAS existe desde 2020",
        "Atributos según se publicaron en cada año de referencia, de modo que las "
        "clasificaciones de un producto químico pueden cambiar entre años; la "
        "marca PFAS existe desde 2020",
    ),
    "Unique identifier assigned by EPA to each submission; format TTYY plus a "
    "sequential number": (
        "Identificador único atribuído pela EPA a cada submissão; formato TTYY "
        "mais um número sequencial",
        "Identificador único asignado por la EPA a cada presentación; formato "
        "TTYY más un número secuencial",
    ),
}


def _obs(text: str) -> tuple[str, str, str]:
    if not text:
        return "", "", ""
    if text not in _OBS_I18N:
        raise SystemExit(f"Untranslated observation: {text!r}")
    pt, es = _OBS_I18N[text]
    return pt, text, es


def c(
    name,
    typ,
    pt,
    en,
    es,
    orig="",
    *,
    dic=False,
    dir_="",
    unit="",
    obs="",
    sensitive=False,
    coverage="",
):
    return {
        "name": name,
        "bigquery_type": typ,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "original_name": orig,
        "covered_by_dictionary": dic,
        "directory_column": dir_,
        "measurement_unit": unit,
        "observations": obs,
        "has_sensitive_data": sensitive,
        "temporal_coverage": coverage,
    }


YEAR = c(
    "year",
    "INT64",
    "Ano-calendário de referência das atividades informadas",
    "Calendar year in which the reported activities occurred",
    "Año calendario de referencia de las actividades informadas",
    "YEAR",
    dir_=DIR_YEAR,
    unit="year",
    obs="Partition column",
)
TRIFID = c(
    "tri_facility_id",
    "STRING",
    "Identificador TRI da instalação (TRIFID), 15 caracteres; identifica uma "
    "localização geográfica e não muda com trocas de proprietário",
    "TRI facility identification number (TRIFID), 15 characters; identifies a "
    "geographical location and does not change with ownership changes",
    "Identificador TRI de la instalación (TRIFID), 15 caracteres; identifica "
    "una ubicación geográfica y no cambia con cambios de propietario",
    "TRIFID",
)
DOC = c(
    "document_control_number",
    "STRING",
    "Número de controle do documento, identificador único da submissão (Form R "
    "ou Form A) atribuído pela EPA",
    "Document control number, the unique identifier EPA assigns to each "
    "submission (Form R or Form A)",
    "Número de control del documento, identificador único de la presentación "
    "(Form R o Form A) asignado por la EPA",
    "DOC_CTRL_NUM",
    obs="Unique identifier assigned by EPA to each submission; format TTYY plus "
    "a sequential number",
)
CHEM_ID = c(
    "tri_chemical_id",
    "STRING",
    "Identificador TRI do produto químico ou categoria de compostos: número CAS "
    "sem hífens com zeros à esquerda (10 dígitos) ou código Nnnn para categorias",
    "TRI chemical or compound-category identifier: the CAS number without "
    "dashes, zero-padded to 10 digits, or an Nnnn code for compound categories",
    "Identificador TRI del producto químico o categoría de compuestos: número "
    "CAS sin guiones con ceros a la izquierda (10 dígitos) o código Nnnn para "
    "categorías",
    "TRI CHEMICAL/COMPOUND ID",
    obs="The value 9999999999 marks sanitized trade-secret submissions",
)

FACILITY = [
    YEAR,
    TRIFID,
    c(
        "frs_id",
        "STRING",
        "Identificador da instalação no Facility Registry Service (FRS) da EPA, "
        "chave de ligação com outros programas da agência",
        "EPA Facility Registry Service (FRS) identifier, the key that links the "
        "facility to other EPA programs",
        "Identificador de la instalación en el Facility Registry Service (FRS) "
        "de la EPA, clave de enlace con otros programas de la agencia",
        "FRS ID",
    ),
    c(
        "facility_name",
        "STRING",
        "Nome da instalação declarante",
        "Name of the reporting facility",
        "Nombre de la instalación declarante",
        "FACILITY NAME",
    ),
    c(
        "street_address",
        "STRING",
        "Endereço (logradouro) da instalação",
        "Street address of the facility",
        "Dirección (calle) de la instalación",
        "STREET ADDRESS",
    ),
    c(
        "city",
        "STRING",
        "Cidade da instalação",
        "City of the facility",
        "Ciudad de la instalación",
        "CITY",
    ),
    c(
        "county_id",
        "STRING",
        "Código FIPS do condado (5 dígitos: estado + condado) da instalação",
        "County FIPS code (5 digits: state plus county) of the facility",
        "Código FIPS del condado (5 dígitos: estado más condado) de la "
        "instalación",
        "(Envirofacts TRI_FACILITY.STATE_COUNTY_FIPS_CODE)",
        dir_=DIR_COUNTY,
        obs="Derived: county FIPS code looked up by TRI facility ID in the "
        "Envirofacts TRI_FACILITY table (STATE_COUNTY_FIPS_CODE); the Basic "
        "file itself carries only the county name. The source's 00000 "
        "(unknown) is null. Connecticut facilities carry the legacy county "
        "codes 09001-09015, which the county directory (2022 planning regions) "
        "does not list",
    ),
    c(
        "county_name",
        "STRING",
        "Nome do condado da instalação, conforme informado",
        "County name of the facility, as reported",
        "Nombre del condado de la instalación, según lo informado",
        "COUNTY",
    ),
    c(
        "state",
        "STRING",
        "Sigla postal de duas letras do estado da instalação",
        "Two-letter postal abbreviation of the facility's state",
        "Abreviatura postal de dos letras del estado de la instalación",
        "STATE",
        dir_=DIR_STATE,
        obs="Includes the District of Columbia and the territories AS, GU, MP, "
        "PR and VI",
    ),
    c(
        "zip_code",
        "STRING",
        "Código postal (ZIP) da instalação, 5 ou 9 dígitos",
        "ZIP code of the facility, 5 or 9 digits",
        "Código postal (ZIP) de la instalación, 5 o 9 dígitos",
        "ZIP",
    ),
    c(
        "bia_code",
        "STRING",
        "Código de três letras do Bureau of Indian Affairs (BIA) da tribo em "
        "cuja terra a instalação está localizada",
        "Three-letter Bureau of Indian Affairs (BIA) code of the tribe on whose "
        "land the facility is located",
        "Código de tres letras del Bureau of Indian Affairs (BIA) de la tribu en "
        "cuya tierra se ubica la instalación",
        "BIA",
    ),
    c(
        "tribe_name",
        "STRING",
        "Nome da tribo em cuja terra a instalação está localizada",
        "Name of the tribe on whose land the facility is located",
        "Nombre de la tribu en cuya tierra se ubica la instalación",
        "TRIBE",
    ),
    c(
        "latitude",
        "FLOAT64",
        "Latitude que melhor representa a instalação, em graus decimais",
        "Latitude that best represents the facility, in decimal degrees",
        "Latitud que mejor representa la instalación, en grados decimales",
        "LATITUDE",
        unit="degree",
        obs="Collected from facilities through RY 2004; obtained from the "
        "Facility Registry Service since RY 2005",
    ),
    c(
        "longitude",
        "FLOAT64",
        "Longitude que melhor representa a instalação, em graus decimais",
        "Longitude that best represents the facility, in decimal degrees",
        "Longitud que mejor representa la instalación, en grados decimales",
        "LONGITUDE",
        unit="degree",
        obs="Collected from facilities through RY 2004; obtained from the "
        "Facility Registry Service since RY 2005",
    ),
    c(
        "parent_company_name",
        "STRING",
        "Nome da empresa controladora da instalação, conforme informado",
        "Name of the parent company that controls the facility, as reported",
        "Nombre de la empresa matriz que controla la instalación, según lo "
        "informado",
        "PARENT CO NAME",
    ),
    c(
        "parent_company_duns",
        "STRING",
        "Número Dun & Bradstreet (D-U-N-S) da empresa controladora",
        "Dun & Bradstreet (D-U-N-S) number of the parent company",
        "Número Dun & Bradstreet (D-U-N-S) de la empresa matriz",
        "PARENT CO DB NUM",
    ),
    c(
        "standardized_parent_company_name",
        "STRING",
        "Nome padronizado pela EPA da empresa controladora final nos Estados "
        "Unidos",
        "EPA-standardized name of the current ultimate U.S. parent company",
        "Nombre estandarizado por la EPA de la empresa matriz final en Estados "
        "Unidos",
        "STANDARD PARENT CO NAME",
    ),
    c(
        "foreign_parent_company_name",
        "STRING",
        "Nome da empresa controladora estrangeira, quando há uma controladora "
        "de nível superior fora dos Estados Unidos",
        "Name of the foreign parent company, when a higher-level parent exists "
        "outside the United States",
        "Nombre de la empresa matriz extranjera, cuando existe una matriz de "
        "nivel superior fuera de Estados Unidos",
        "FOREIGN PARENT CO NAME",
    ),
    c(
        "foreign_parent_company_duns",
        "STRING",
        "Número Dun & Bradstreet (D-U-N-S) da empresa controladora estrangeira",
        "Dun & Bradstreet (D-U-N-S) number of the foreign parent company",
        "Número Dun & Bradstreet (D-U-N-S) de la empresa matriz extranjera",
        "FOREIGN PARENT CO DB NUM",
    ),
    c(
        "standardized_foreign_parent_company_name",
        "STRING",
        "Nome padronizado pela EPA da empresa controladora estrangeira",
        "EPA-standardized name of the foreign parent company",
        "Nombre estandarizado por la EPA de la empresa matriz extranjera",
        "STANDARD FOREIGN PARENT CO NAME",
    ),
    c(
        "federal_facility",
        "STRING",
        "Indica se a instalação é de propriedade e operação do governo federal "
        "(YES ou NO)",
        "Whether the facility is federally owned and operated (YES or NO)",
        "Indica si la instalación es de propiedad y operación del gobierno "
        "federal (YES o NO)",
        "FEDERAL FACILITY",
    ),
]

CHEMICAL = [
    YEAR,
    CHEM_ID,
    c(
        "chemical_name",
        "STRING",
        "Nome do produto químico ou da categoria de compostos na lista TRI",
        "Name of the chemical or compound category on the TRI chemical list",
        "Nombre del producto químico o de la categoría de compuestos en la "
        "lista TRI",
        "CHEMICAL",
        obs="Attributes as published in each reporting year, so a chemical's "
        "classifications can change across years; the PFAS flag exists since "
        "RY 2020",
    ),
    c(
        "cas_number",
        "STRING",
        "Número de registro do Chemical Abstracts Service (CAS) com hífens, ou o "
        "código da categoria de compostos",
        "Chemical Abstracts Service (CAS) registry number with dashes, or the "
        "compound-category code",
        "Número de registro del Chemical Abstracts Service (CAS) con guiones, o "
        "el código de la categoría de compuestos",
        "CAS#",
    ),
    c(
        "srs_id",
        "STRING",
        "Identificador da substância no Substance Registry Services (SRS) da "
        "EPA",
        "EPA Substance Registry Services (SRS) identifier of the substance",
        "Identificador de la sustancia en el Substance Registry Services (SRS) "
        "de la EPA",
        "SRS ID",
    ),
    c(
        "clean_air_act_chemical",
        "STRING",
        "Indica se o produto químico é listado como poluente atmosférico "
        "perigoso pelo Clean Air Act (YES ou NO)",
        "Whether the chemical is listed as a hazardous air pollutant under the "
        "Clean Air Act (YES or NO)",
        "Indica si el producto químico está listado como contaminante "
        "atmosférico peligroso por el Clean Air Act (YES o NO)",
        "CLEAN AIR ACT CHEMICAL",
    ),
    c(
        "classification",
        "STRING",
        "Classificação do produto químico: TRI (substância geral da seção 313 do "
        "EPCRA), PBT (persistente, bioacumulativa e tóxica) ou Dioxin (dioxina "
        "ou composto similar)",
        "Chemical classification: TRI (general EPCRA section 313 chemical), PBT "
        "(persistent, bioaccumulative and toxic) or Dioxin (dioxin or "
        "dioxin-like compound)",
        "Clasificación del producto químico: TRI (sustancia general de la "
        "sección 313 del EPCRA), PBT (persistente, bioacumulativa y tóxica) o "
        "Dioxin (dioxina o compuesto similar)",
        "CLASSIFICATION",
        dic=True,
    ),
    c(
        "metal",
        "STRING",
        "Indica se o produto químico é um metal com restrições de declaração no "
        "TRI (YES ou NO)",
        "Whether the chemical is a metal with TRI reporting restrictions (YES or "
        "NO)",
        "Indica si el producto químico es un metal con restricciones de "
        "declaración en el TRI (YES o NO)",
        "METAL",
    ),
    c(
        "metal_category",
        "STRING",
        "Categoria de metal do produto químico, como rótulo de texto (por "
        "exemplo Elemental metals, Metal compound categories, Non_Metal)",
        "Metal category of the chemical, as a text label (for example Elemental "
        "metals, Metal compound categories, Non_Metal)",
        "Categoría de metal del producto químico, como etiqueta de texto (por "
        "ejemplo Elemental metals, Metal compound categories, Non_Metal)",
        "METAL CATEGORY",
        obs="Codes 1-4 in the source documentation appear in the file as text "
        "labels (Elemental metals, Metal compound categories, ...)",
    ),
    c(
        "carcinogen",
        "STRING",
        "Indica se o produto químico é classificado como carcinógeno pela OSHA "
        "(YES ou NO)",
        "Whether the chemical is classified as a carcinogen by OSHA (YES or NO)",
        "Indica si el producto químico está clasificado como carcinógeno por la "
        "OSHA (YES o NO)",
        "CARCINOGEN",
    ),
    c(
        "pbt",
        "STRING",
        "Indica se o produto químico é persistente, bioacumulativo e tóxico "
        "(PBT) (YES ou NO)",
        "Whether the chemical is persistent, bioaccumulative and toxic (PBT) "
        "(YES or NO)",
        "Indica si el producto químico es persistente, bioacumulativo y tóxico "
        "(PBT) (YES o NO)",
        "PBT",
    ),
    c(
        "pfas",
        "STRING",
        "Indica se o produto químico é uma substância per- e polifluoroalquil "
        "(PFAS) (YES ou NO)",
        "Whether the chemical is a per- and polyfluoroalkyl substance (PFAS) "
        "(YES or NO)",
        "Indica si el producto químico es una sustancia per- y "
        "polifluoroalquilada (PFAS) (YES o NO)",
        "PFAS",
    ),
]

_LB = "Pounds for every row; dioxin quantities reported in grams were divided by 453.59237"


def qty(name, pt, en, es, orig, obs=_LB, coverage=""):
    return c(
        name,
        "FLOAT64",
        pt,
        en,
        es,
        orig,
        unit="pound",
        obs=obs,
        coverage=coverage,
    )


def sic(n, orig, ord_pt, ord_en, ord_es):
    return c(
        f"sic_{n}",
        "STRING",
        f"{ord_pt} código SIC (Standard Industrial Classification) de quatro "
        "dígitos informado pela instalação",
        f"{ord_en} four-digit Standard Industrial Classification (SIC) code "
        "entered by the facility",
        f"{ord_es} código SIC (Standard Industrial Classification) de cuatro "
        "dígitos informado por la instalación",
        orig,
        obs="Reported by facilities from RY 1987 through 2005; empty afterwards. "
        "The source's INVA (invalid) and NA are null",
        coverage="1987(1)2005",
    )


def naics(n, orig, ord_pt, ord_en, ord_es):
    return c(
        f"naics_{n}",
        "STRING",
        f"{ord_pt} código NAICS (North American Industry Classification System) "
        "de seis dígitos informado pela instalação",
        f"{ord_en} six-digit North American Industry Classification System "
        "(NAICS) code entered by the facility",
        f"{ord_es} código NAICS (North American Industry Classification System) "
        "de seis dígitos informado por la instalación",
        orig,
        obs="Reported by facilities from RY 2006 on; assigned by EPA to "
        "1987-2005 submissions (see Appendix D of the source documentation)",
    )


ORD = [
    ("Segundo", "Second", "Segundo"),
    ("Terceiro", "Third", "Tercer"),
    ("Quarto", "Fourth", "Cuarto"),
    ("Quinto", "Fifth", "Quinto"),
    ("Sexto", "Sixth", "Sexto"),
]

FORM = [
    YEAR,
    TRIFID,
    DOC,
    CHEM_ID,
    c(
        "chemical_name",
        "STRING",
        "Nome do produto químico conforme informado no formulário (ou nome "
        "genérico, se declarado como segredo industrial)",
        "Chemical name as reported on the form (or a generic name if claimed as "
        "a trade secret)",
        "Nombre del producto químico según lo informado en el formulario (o "
        "nombre genérico, si se declaró como secreto comercial)",
        "CHEMICAL",
        obs="As reported on the form; the canonical name per chemical ID is in "
        "the chemical table",
    ),
    c(
        "form_type",
        "STRING",
        "Tipo de formulário entregue: R (Form R, relatório completo) ou A (Form "
        "A, declaração de certificação sem quantidades)",
        "Form submitted: R (Form R, full report) or A (Form A certification "
        "statement, without quantities)",
        "Tipo de formulario presentado: R (Form R, informe completo) o A (Form "
        "A, declaración de certificación sin cantidades)",
        "FORM TYPE",
        dic=True,
    ),
    c(
        "elemental_metal_included",
        "STRING",
        "Indica se o formulário combina um composto metálico e o metal elementar "
        "correspondente (YES ou NO)",
        "Whether the form combines a metal compound and the corresponding "
        "elemental metal (YES or NO)",
        "Indica si el formulario combina un compuesto metálico y el metal "
        "elemental correspondiente (YES o NO)",
        "ELEMENTAL METAL INCLUDED",
        obs="Collected since RY 2018",
        coverage="2018(1)",
    ),
    c(
        "unit_of_measure",
        "STRING",
        "Unidade em que a fonte informou as quantidades deste formulário: Pounds "
        "(libras) ou Grams (gramas, para dioxinas)",
        "Unit in which the source reported this form's quantities: Pounds or "
        "Grams (for dioxins)",
        "Unidad en que la fuente informó las cantidades de este formulario: "
        "Pounds (libras) o Grams (gramos, para dioxinas)",
        "UNIT OF MEASURE",
        obs="Unit the source used for this form: Pounds, or Grams for dioxin and "
        "dioxin-like compounds. Every quantity column in this dataset has "
        "already been put in a single unit",
    ),
    c(
        "industry_sector_code",
        "STRING",
        "Código NAICS (3 ou 4 dígitos) do setor industrial atribuído pela EPA "
        "para análise de tendências do TRI",
        "NAICS code (3 or 4 digits) of the industry sector EPA assigns for TRI "
        "trend analysis",
        "Código NAICS (3 o 4 dígitos) del sector industrial asignado por la EPA "
        "para el análisis de tendencias del TRI",
        "INDUSTRY SECTOR CODE",
    ),
    c(
        "industry_sector",
        "STRING",
        "Nome do setor industrial atribuído pela EPA (por exemplo Chemicals, "
        "Primary Metals)",
        "Name of the EPA-assigned industry sector (for example Chemicals, "
        "Primary Metals)",
        "Nombre del sector industrial asignado por la EPA (por ejemplo "
        "Chemicals, Primary Metals)",
        "INDUSTRY SECTOR",
    ),
    c(
        "primary_sic",
        "STRING",
        "Código SIC (Standard Industrial Classification) primário de quatro "
        "dígitos da instalação",
        "Primary four-digit Standard Industrial Classification (SIC) code of the "
        "facility",
        "Código SIC (Standard Industrial Classification) primario de cuatro "
        "dígitos de la instalación",
        "PRIMARY SIC",
        dir_=DIR_SIC,
        obs="Reported by facilities from RY 1987 through 2005; empty afterwards. "
        "The source's INVA (invalid) and NA are null",
        coverage="1987(1)2005",
    ),
    *[sic(i + 2, f"SIC {i + 2}", *ORD[i]) for i in range(5)],
    c(
        "primary_naics",
        "STRING",
        "Código NAICS (North American Industry Classification System) primário "
        "de seis dígitos, a principal atividade da instalação",
        "Primary six-digit North American Industry Classification System (NAICS) "
        "code, the facility's main business activity",
        "Código NAICS (North American Industry Classification System) primario "
        "de seis dígitos, la actividad principal de la instalación",
        "PRIMARY NAICS",
        obs="Reported by facilities from RY 2006 on; assigned by EPA to "
        "1987-2005 submissions (see Appendix D of the source documentation)",
    ),
    *[naics(i + 2, f"NAICS {i + 2}", *ORD[i]) for i in range(5)],
    c(
        "naics_version",
        "STRING",
        "Versão da classificação NAICS que os códigos seguem (2002, 2007, 2012, "
        "2017 ou 2022)",
        "NAICS classification vintage the codes follow (2002, 2007, 2012, 2017 "
        "or 2022)",
        "Versión de la clasificación NAICS que siguen los códigos (2002, 2007, "
        "2012, 2017 o 2022)",
        "(derived)",
        obs="Derived from the reporting year: 1987-2007 -> 2002, 2008-2012 -> "
        "2007, 2013-2016 -> 2012, 2017-2021 -> 2017, 2022 on -> 2022 (TRI "
        "adopts each NAICS revision the year after Census). EPA assigned the "
        "1987-2005 codes in the 2002 vintage",
    ),
    qty(
        "on_site_release_total",
        "Total liberado no local para ar, água e solo (soma da seção 5 do Form "
        "R)",
        "Total released on site to air, water and land (sum of Form R section "
        "5)",
        "Total liberado en el sitio al aire, agua y suelo (suma de la sección 5 "
        "del Form R)",
        "ON-SITE RELEASE TOTAL",
    ),
    qty(
        "potw_transfer_total",
        "Total transferido para estações públicas de tratamento de esgoto "
        "(POTW), seção 6.1",
        "Total transferred to publicly owned treatment works (POTW), section 6.1",
        "Total transferido a plantas públicas de tratamiento de aguas residuales "
        "(POTW), sección 6.1",
        "POTW - TOTAL TRANSFERS",
    ),
    qty(
        "off_site_release_total",
        "Total transferido para fora do local para liberação ou disposição, "
        "incluindo a parcela de POTW considerada liberada",
        "Total transferred off site for release or disposal, including the POTW "
        "share treated as released",
        "Total transferido fuera del sitio para liberación o disposición, "
        "incluida la parte de POTW considerada liberada",
        "OFF-SITE RELEASE TOTAL",
    ),
    qty(
        "off_site_recycling_total",
        "Total transferido para fora do local para reciclagem",
        "Total transferred off site for recycling",
        "Total transferido fuera del sitio para reciclaje",
        "OFF-SITE RECYCLED TOTAL",
    ),
    qty(
        "off_site_energy_recovery_total",
        "Total transferido para fora do local para recuperação de energia",
        "Total transferred off site for energy recovery",
        "Total transferido fuera del sitio para recuperación de energía",
        "OFF-SITE ENERGY RECOVERY TOTAL",
    ),
    qty(
        "off_site_treatment_total",
        "Total transferido para fora do local para tratamento, incluindo a "
        "parcela de POTW considerada tratada",
        "Total transferred off site for treatment, including the POTW share "
        "treated as treated",
        "Total transferido fuera del sitio para tratamiento, incluida la parte "
        "de POTW considerada tratada",
        "OFF-SITE TREATED TOTAL",
    ),
    qty(
        "total_transfer",
        "Total transferido para fora do local (seção 6.2): liberação, "
        "reciclagem, recuperação de energia, tratamento e não classificado",
        "Total transferred off site (section 6.2): release, recycling, energy "
        "recovery, treatment and unclassified",
        "Total transferido fuera del sitio (sección 6.2): liberación, "
        "reciclaje, recuperación de energía, tratamiento y no clasificado",
        "6.2 - TOTAL TRANSFER",
    ),
    qty(
        "total_releases",
        "Total de liberações no local e fora do local (seções 5 e 6 do Form R), "
        "o indicador principal do TRI",
        "Total on-site and off-site releases (Form R sections 5 and 6), the "
        "headline TRI figure",
        "Total de liberaciones en el sitio y fuera del sitio (secciones 5 y 6 "
        "del Form R), el indicador principal del TRI",
        "TOTAL RELEASES",
    ),
    qty(
        "waste_released",
        "Total de liberações no local e fora do local informado na seção 8.1 do "
        "Form R (resíduo relacionado à produção)",
        "Total on- and off-site releases reported in Form R section 8.1 "
        "(production-related waste)",
        "Total de liberaciones en el sitio y fuera del sitio informado en la "
        "sección 8.1 del Form R (residuo relacionado con la producción)",
        "8.1 - RELEASES",
        obs="Reported from RY 1987 through 2002; split into 8.1A-8.1D from 2003",
        coverage="1987(1)2002",
    ),
    qty(
        "waste_released_on_site_contained",
        "Seção 8.1A: disposição no local em poços de injeção Classe I, aterros "
        "RCRA Subtítulo C e outros aterros",
        "Section 8.1A: on-site disposal to Class I underground injection wells, "
        "RCRA Subtitle C landfills and other landfills",
        "Sección 8.1A: disposición en el sitio en pozos de inyección Clase I, "
        "rellenos RCRA Subtítulo C y otros rellenos",
        "8.1A - ON-SITE CONTAINED RELEASES",
        obs="Reported from RY 2003 on",
        coverage="2003(1)",
    ),
    qty(
        "waste_released_on_site_other",
        "Seção 8.1B: outras disposições ou liberações no local não cobertas em "
        "8.1A",
        "Section 8.1B: other on-site disposal or releases not covered in 8.1A",
        "Sección 8.1B: otras disposiciones o liberaciones en el sitio no "
        "cubiertas en 8.1A",
        "8.1B - ON-SITE OTHER RELEASES",
        obs="Reported from RY 2003 on",
        coverage="2003(1)",
    ),
    qty(
        "waste_released_off_site_contained",
        "Seção 8.1C: disposição fora do local em poços de injeção Classe I, "
        "aterros RCRA Subtítulo C e outros aterros",
        "Section 8.1C: off-site disposal to Class I underground injection "
        "wells, RCRA Subtitle C landfills and other landfills",
        "Sección 8.1C: disposición fuera del sitio en pozos de inyección Clase "
        "I, rellenos RCRA Subtítulo C y otros rellenos",
        "8.1C - OFF-SITE CONTAINED RELEASES",
        obs="Reported from RY 2003 on",
        coverage="2003(1)",
    ),
    qty(
        "waste_released_off_site_other",
        "Seção 8.1D: outras disposições ou liberações fora do local não "
        "cobertas em 8.1C",
        "Section 8.1D: other off-site disposal or releases not covered in 8.1C",
        "Sección 8.1D: otras disposiciones o liberaciones fuera del sitio no "
        "cubiertas en 8.1C",
        "8.1D - OFF-SITE OTHER RELEASES",
        obs="Reported from RY 2003 on",
        coverage="2003(1)",
    ),
    qty(
        "waste_energy_recovery_on_site",
        "Seção 8.2: quantidade queimada no local para recuperação de energia",
        "Section 8.2: quantity burned on site for energy recovery",
        "Sección 8.2: cantidad quemada en el sitio para recuperación de energía",
        "8.2 - ENERGY RECOVERY ON SITE",
    ),
    qty(
        "waste_energy_recovery_off_site",
        "Seção 8.3: quantidade enviada para fora do local para recuperação de "
        "energia",
        "Section 8.3: quantity sent off site for energy recovery",
        "Sección 8.3: cantidad enviada fuera del sitio para recuperación de "
        "energía",
        "8.3 - ENERGY RECOVERY OFF SITE",
    ),
    qty(
        "waste_recycled_on_site",
        "Seção 8.4: quantidade reciclada no local",
        "Section 8.4: quantity recycled on site",
        "Sección 8.4: cantidad reciclada en el sitio",
        "8.4 - RECYCLING ON SITE",
    ),
    qty(
        "waste_recycled_off_site",
        "Seção 8.5: quantidade enviada para fora do local para reciclagem",
        "Section 8.5: quantity sent off site for recycling",
        "Sección 8.5: cantidad enviada fuera del sitio para reciclaje",
        "8.5 - RECYCLING OFF SITE",
    ),
    qty(
        "waste_treated_on_site",
        "Seção 8.6: quantidade tratada no local",
        "Section 8.6: quantity treated on site",
        "Sección 8.6: cantidad tratada en el sitio",
        "8.6 - TREATMENT ON SITE",
    ),
    qty(
        "waste_treated_off_site",
        "Seção 8.7: quantidade enviada para fora do local para tratamento, "
        "incluindo transferências para POTW",
        "Section 8.7: quantity sent off site for treatment, including transfers "
        "to POTWs",
        "Sección 8.7: cantidad enviada fuera del sitio para tratamiento, "
        "incluidas las transferencias a POTW",
        "8.7 - TREATMENT OFF SITE",
    ),
    qty(
        "production_related_waste",
        "Total de resíduo relacionado à produção, soma das seções 8.1 a 8.7",
        "Total production-related waste, the sum of sections 8.1 through 8.7",
        "Total de residuo relacionado con la producción, suma de las secciones "
        "8.1 a 8.7",
        "PRODUCTION WSTE (8.1-8.7)",
    ),
    qty(
        "one_time_release",
        "Seção 8.8: quantidade liberada ou transferida por eventos não "
        "associados aos processos rotineiros de produção (acidentes, "
        "remediações)",
        "Section 8.8: quantity released or transferred due to events not "
        "associated with routine production processes (accidents, remediation)",
        "Sección 8.8: cantidad liberada o transferida por eventos no asociados "
        "a los procesos rutinarios de producción (accidentes, remediaciones)",
        "8.8 - ONE-TIME RELEASE",
    ),
    c(
        "production_ratio_type",
        "STRING",
        "Indica se o valor da seção 8.9 é uma razão de produção (PRODUCTION) ou "
        "um índice de atividade (ACTIVITY)",
        "Whether the section 8.9 value is a production ratio (PRODUCTION) or an "
        "activity index (ACTIVITY)",
        "Indica si el valor de la sección 8.9 es una razón de producción "
        "(PRODUCTION) o un índice de actividad (ACTIVITY)",
        "PROD_RATIO_OR_ACTIVITY",
    ),
    c(
        "production_ratio",
        "FLOAT64",
        "Seção 8.9: razão entre a produção ou atividade do ano de referência e a "
        "do ano anterior",
        "Section 8.9: ratio of production or activity in the reporting year to "
        "that of the previous year",
        "Sección 8.9: razón entre la producción o actividad del año de "
        "referencia y la del año anterior",
        "8.9 - PRODUCTION RATIO",
        unit="ratio",
    ),
]

RELEASE = [
    YEAR,
    TRIFID,
    DOC,
    CHEM_ID,
    c(
        "management_category",
        "STRING",
        "Grupo de gestão da quantidade: liberação no local, liberação fora do "
        "local (disposição), reciclagem, recuperação de energia, tratamento ou "
        "não classificado",
        "Management group of the quantity: on-site release, off-site release "
        "(disposal), recycling, energy recovery, treatment or unclassified",
        "Grupo de gestión de la cantidad: liberación en el sitio, liberación "
        "fuera del sitio (disposición), reciclaje, recuperación de energía, "
        "tratamiento o no clasificado",
        "(derived)",
        dic=True,
        obs="Groups release categories the way the source's own totals do: "
        "on-site release, off-site release (disposal), recycling, energy "
        "recovery, treatment, unclassified",
    ),
    c(
        "release_category",
        "STRING",
        "Categoria de liberação ou transferência: seção do Form R (5.1 ar "
        "fugitivo, 5.2 chaminé, 5.3 água, 5.4 injeção subterrânea, 5.5 solo, "
        "6.1 POTW) ou código M de gestão fora do local (6.2)",
        "Release or transfer category: Form R section (5.1 fugitive air, 5.2 "
        "stack air, 5.3 water, 5.4 underground injection, 5.5 land, 6.1 POTW) "
        "or off-site management M-code (6.2)",
        "Categoría de liberación o transferencia: sección del Form R (5.1 aire "
        "fugitivo, 5.2 chimenea, 5.3 agua, 5.4 inyección subterránea, 5.5 "
        "suelo, 6.1 POTW) o código M de gestión fuera del sitio (6.2)",
        "(source column name)",
        dic=True,
        obs="Form R section and off-site management code (M-code) the quantity "
        "was reported under; categories are mutually exclusive, so summing "
        "quantity over a form reproduces the source's total transfers plus "
        "on-site releases",
    ),
    c(
        "quantity_pounds",
        "FLOAT64",
        "Quantidade liberada ou transferida, em libras",
        "Quantity released or transferred, in pounds",
        "Cantidad liberada o transferida, en libras",
        "(quantity column)",
        unit="pound",
        obs="Pounds for every row; dioxin quantities reported in grams were "
        "divided by 453.59237",
    ),
    c(
        "quantity_grams",
        "FLOAT64",
        "Quantidade liberada ou transferida, em gramas, apenas para dioxinas e "
        "compostos similares, conforme informado",
        "Quantity released or transferred, in grams, for dioxin and dioxin-like "
        "compounds only, as reported",
        "Cantidad liberada o transferida, en gramos, solo para dioxinas y "
        "compuestos similares, según lo informado",
        "(quantity column)",
        unit="gram",
        obs="Only dioxin and dioxin-like compounds (classification Dioxin), as "
        "reported; null for every other chemical",
    ),
]

DICIONARIO = [
    c(
        "id_tabela",
        "STRING",
        "Nome da tabela",
        "Table name",
        "Nombre de la tabla",
    ),
    c(
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
    ),
    c(
        "chave",
        "STRING",
        "Valor codificado da coluna",
        "Coded value of the column",
        "Valor codificado de la columna",
    ),
    c(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal do código",
        "Temporal coverage of the code",
        "Cobertura temporal del código",
    ),
    c(
        "valor",
        "STRING",
        "Descrição legível do valor codificado",
        "Human-readable description of the coded value",
        "Descripción legible del valor codificado",
    ),
]

TABLES = {
    "facility": FACILITY,
    "chemical": CHEMICAL,
    "form": FORM,
    "release": RELEASE,
    "dicionario": DICIONARIO,
}


def _json_row(x: dict) -> dict:
    obs_pt, obs_en, obs_es = _obs(x["observations"])
    row = {
        "name": x["name"],
        "bigquery_type": x["bigquery_type"],
        "description_pt": x["description_pt"],
        "description_en": x["description_en"],
        "description_es": x["description_es"],
        "covered_by_dictionary": x["covered_by_dictionary"],
        "has_sensitive_data": x["has_sensitive_data"],
    }
    if x["directory_column"]:
        row["directory_column"] = x["directory_column"]
    if x["measurement_unit"]:
        row["measurement_unit"] = x["measurement_unit"]
    if obs_pt:
        row["observations_pt"] = obs_pt
        row["observations_en"] = obs_en
        row["observations_es"] = obs_es
    return row


def main():
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in TABLES.items():
        names = [x["name"] for x in cols]
        assert len(names) == len(set(names)), (
            f"{table}: duplicate column names"
        )
        for x in cols:
            for k in ("description_pt", "description_en", "description_es"):
                assert not x[k].endswith("."), (
                    f"{table}.{x['name']}: {k} ends with a period"
                )
            if x["bigquery_type"] in ("INT64", "FLOAT64"):
                assert x["measurement_unit"], (
                    f"{table}.{x['name']}: numeric without unit"
                )
        with open(JSON_DIR / f"{table}.json", "w", encoding="utf-8") as fh:
            json.dump(
                [_json_row(x) for x in cols], fh, ensure_ascii=False, indent=1
            )
            fh.write("\n")
        with open(
            ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            w = csv.writer(fh, lineterminator="\n")
            w.writerow(ARCH_HEADER)
            for x in cols:
                w.writerow(
                    [
                        x["name"],
                        x["bigquery_type"],
                        x["description_pt"],
                        x["temporal_coverage"],
                        "yes" if x["covered_by_dictionary"] else "no",
                        x["directory_column"],
                        x["measurement_unit"],
                        "yes" if x["has_sensitive_data"] else "no",
                        x["observations"],
                        x["original_name"],
                    ]
                )
        print(f"{table}: {len(cols)} columns")


if __name__ == "__main__":
    main()
