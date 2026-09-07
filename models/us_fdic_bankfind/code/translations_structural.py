"""Explicit PT/ES renderings of the structural column descriptions.

These are the columns a reader actually reads — the institution directory, the
indicator dictionary and the keys of the two financial tables — so they are
translated in full rather than by glossary substitution, which cannot get the
grammar right ("de o instituição").

Keyed by the English description exactly as it appears in the architecture CSVs.
"""

STRUCTURAL: dict[str, tuple[str, str]] = {
    # --- institution: identity ---
    "Date the institution directory was extracted from the FDIC API": (
        "Data em que o cadastro de instituições foi extraído da API do FDIC",
        "Fecha en que el directorio de instituciones fue extraído de la API de la FDIC",
    ),
    "FDIC certificate number identifying the institution": (
        "Número do certificado do FDIC que identifica a instituição",
        "Número de certificado de la FDIC que identifica a la institución",
    ),
    "Legal name of the institution": (
        "Razão social da instituição",
        "Razón social de la institución",
    ),
    "Federal Reserve RSSD identifier": (
        "Identificador RSSD no Federal Reserve",
        "Identificador RSSD en la Reserva Federal",
    ),
    "FDIC unique number for the institution": (
        "Número único atribuído pelo FDIC à instituição",
        "Número único asignado por la FDIC a la institución",
    ),
    "Legal Entity Identifier": (
        "Identificador de entidade jurídica (LEI)",
        "Identificador de entidad jurídica (LEI)",
    ),
    "Charter number assigned by the OCC": (
        "Número de charter atribuído pelo OCC",
        "Número de charter asignado por la OCC",
    ),
    "Docket number assigned by the former OTS": (
        "Número de processo atribuído pelo extinto OTS",
        "Número de expediente asignado por la extinta OTS",
    ),
    "Most recent previous name of the institution": (
        "Nome anterior mais recente da instituição",
        "Nombre anterior más reciente de la institución",
    ),
    # --- institution: ownership ---
    "RSSD identifier of the regulatory top holding company": (
        "Identificador RSSD da holding controladora final",
        "Identificador RSSD de la sociedad holding controladora final",
    ),
    "Name of the regulatory top holding company": (
        "Nome da holding controladora final",
        "Nombre de la sociedad holding controladora final",
    ),
    "City of the regulatory top holding company": (
        "Cidade da holding controladora final",
        "Ciudad de la sociedad holding controladora final",
    ),
    "State abbreviation of the regulatory top holding company": (
        "Sigla do estado da holding controladora final",
        "Sigla del estado de la sociedad holding controladora final",
    ),
    "Type of bank holding company": (
        "Tipo de holding bancária",
        "Tipo de sociedad holding bancaria",
    ),
    "FDIC certificate number of the bank that directly owns this one": (
        "Número do certificado do FDIC do banco que detém diretamente esta instituição",
        "Número de certificado de la FDIC del banco que posee directamente esta institución",
    ),
    "FDIC certificate number the institution ultimately maps to": (
        "Número do certificado do FDIC ao qual a instituição corresponde em última instância",
        "Número de certificado de la FDIC al que la institución corresponde en última instancia",
    ),
    "FDIC certificate number the institution became after a merger or conversion": (
        "Número do certificado do FDIC que a instituição passou a ter após fusão ou conversão",
        "Número de certificado de la FDIC que la institución pasó a tener tras una fusión o conversión",
    ),
    # --- institution: classification ---
    "Charter and supervisory class of the institution": (
        "Classe de charter e de supervisão da instituição",
        "Clase de charter y de supervisión de la institución",
    ),
    "Numeric subcategory of the institution class": (
        "Subcategoria numérica da classe da instituição",
        "Subcategoría numérica de la clase de la institución",
    ),
    "Agency that granted the institution's charter": (
        "Órgão que concedeu o charter da instituição",
        "Organismo que concedió el charter de la institución",
    ),
    "Primary federal regulator": (
        "Regulador federal primário",
        "Regulador federal primario",
    ),
    "Secondary regulator": ("Regulador secundário", "Regulador secundario"),
    "Whether the institution holds a state charter": (
        "Indica se a instituição possui charter estadual",
        "Indica si la institución posee charter estatal",
    ),
    "Whether the institution holds a federal charter": (
        "Indica se a instituição possui charter federal",
        "Indica si la institución posee charter federal",
    ),
    "Asset concentration group the institution falls into": (
        "Grupo de concentração de ativos em que a instituição se enquadra",
        "Grupo de concentración de activos en que se ubica la institución",
    ),
    "Whether the institution is stock-owned or mutually owned": (
        "Indica se a instituição é de capital acionário ou mútua",
        "Indica si la institución es de capital accionario o mutua",
    ),
    "Whether the institution is a Subchapter S corporation": (
        "Indica se a instituição é uma corporação Subchapter S",
        "Indica si la institución es una corporación Subchapter S",
    ),
    "Whether the institution meets the FDIC community bank definition": (
        "Indica se a instituição atende à definição de banco comunitário do FDIC",
        "Indica si la institución cumple la definición de banco comunitario de la FDIC",
    ),
    "Minority depository institution status": (
        "Situação da instituição como depositária de propriedade minoritária",
        "Situación de la institución como depositaria de propiedad minoritaria",
    ),
    "Trust powers granted to the institution": (
        "Poderes fiduciários concedidos à instituição",
        "Poderes fiduciarios concedidos a la institución",
    ),
    "Whether the institution is classified as an agricultural lender": (
        "Indica se a instituição é classificada como credora agrícola",
        "Indica si la institución se clasifica como prestamista agrícola",
    ),
    "Whether the institution is classified as a credit card institution": (
        "Indica se a instituição é classificada como instituição de cartão de crédito",
        "Indica si la institución se clasifica como institución de tarjeta de crédito",
    ),
    "Whether the institution is an insured office of a foreign bank": (
        "Indica se a instituição é uma agência segurada de banco estrangeiro",
        "Indica si la institución es una oficina asegurada de un banco extranjero",
    ),
    "Whether the institution files the FFIEC 031 Call Report": (
        "Indica se a instituição entrega o Call Report FFIEC 031",
        "Indica si la institución presenta el Call Report FFIEC 031",
    ),
    # --- institution: insurance ---
    "Whether the institution is insured by the FDIC": (
        "Indica se a instituição é segurada pelo FDIC",
        "Indica si la institución está asegurada por la FDIC",
    ),
    "Deposit insurance fund the institution belongs to": (
        "Fundo de seguro de depósito ao qual a instituição pertence",
        "Fondo de seguro de depósito al que pertenece la institución",
    ),
    "Date deposit insurance took effect": (
        "Data em que o seguro de depósito entrou em vigor",
        "Fecha en que entró en vigor el seguro de depósito",
    ),
    "Date deposit insurance ended": (
        "Data em que o seguro de depósito se encerrou",
        "Fecha en que terminó el seguro de depósito",
    ),
    "Whether the institution is an insured commercial bank": (
        "Indica se a instituição é um banco comercial segurado",
        "Indica si la institución es un banco comercial asegurado",
    ),
    "Whether the institution is an insured savings institution": (
        "Indica se a instituição é uma instituição de poupança segurada",
        "Indica si la institución es una institución de ahorro asegurada",
    ),
    # --- institution: status and dates ---
    "Whether the institution was operating at the extraction date": (
        "Indica se a instituição estava em operação na data de extração",
        "Indica si la institución estaba operando en la fecha de extracción",
    ),
    "Date the institution began operating": (
        "Data em que a instituição iniciou suas operações",
        "Fecha en que la institución inició sus operaciones",
    ),
    "Date the institution ceased to exist as a separate entity": (
        "Data em que a instituição deixou de existir como entidade separada",
        "Fecha en que la institución dejó de existir como entidad separada",
    ),
    "Effective date of the most recent structure change": (
        "Data de vigência da mudança de estrutura mais recente",
        "Fecha de vigencia del cambio de estructura más reciente",
    ),
    "Date the most recent structure change was processed": (
        "Data em que a mudança de estrutura mais recente foi processada",
        "Fecha en que se procesó el cambio de estructura más reciente",
    ),
    "Code of the most recent structure change": (
        "Código da mudança de estrutura mais recente",
        "Código del cambio de estructura más reciente",
    ),
    "Whether the institution is in conservatorship": (
        "Indica se a instituição está sob intervenção",
        "Indica si la institución está bajo intervención",
    ),
    "Whether the institution is newly chartered": (
        "Indica se a instituição foi recém-autorizada",
        "Indica si la institución fue recién autorizada",
    ),
    "Date the FDIC last updated the record": (
        "Data da última atualização do registro pelo FDIC",
        "Fecha de la última actualización del registro por la FDIC",
    ),
    # --- institution: location ---
    "Street address of the main office": (
        "Endereço da sede",
        "Dirección de la sede",
    ),
    "City of the main office": ("Cidade da sede", "Ciudad de la sede"),
    "County of the main office": ("Condado da sede", "Condado de la sede"),
    "Two-letter state abbreviation of the main office": (
        "Sigla de duas letras do estado da sede",
        "Sigla de dos letras del estado de la sede",
    ),
    "State name of the main office": (
        "Nome do estado da sede",
        "Nombre del estado de la sede",
    ),
    "FIPS state and county code of the main office": (
        "Código FIPS de estado e condado da sede",
        "Código FIPS de estado y condado de la sede",
    ),
    "ZIP code of the main office": (
        "Código postal (ZIP) da sede",
        "Código postal (ZIP) de la sede",
    ),
    "Latitude of the main office": ("Latitude da sede", "Latitud de la sede"),
    "Longitude of the main office": (
        "Longitude da sede",
        "Longitud de la sede",
    ),
    "Core Based Statistical Area code of the main office": (
        "Código da área estatística de núcleo urbano (CBSA) da sede",
        "Código del área estadística de núcleo urbano (CBSA) de la sede",
    ),
    "Core Based Statistical Area name of the main office": (
        "Nome da área estatística de núcleo urbano (CBSA) da sede",
        "Nombre del área estadística de núcleo urbano (CBSA) de la sede",
    ),
    "Combined Statistical Area code of the main office": (
        "Código da área estatística combinada (CSA) da sede",
        "Código del área estadística combinada (CSA) de la sede",
    ),
    "Combined Statistical Area name of the main office": (
        "Nome da área estatística combinada (CSA) da sede",
        "Nombre del área estadística combinada (CSA) de la sede",
    ),
    "FDIC geographic region": (
        "Região geográfica do FDIC",
        "Región geográfica de la FDIC",
    ),
    "FDIC supervisory region": (
        "Região de supervisão do FDIC",
        "Región de supervisión de la FDIC",
    ),
    "Federal Reserve district the institution belongs to": (
        "Distrito do Federal Reserve ao qual a instituição pertence",
        "Distrito de la Reserva Federal al que pertenece la institución",
    ),
    "OCC district the institution belongs to": (
        "Distrito do OCC ao qual a instituição pertence",
        "Distrito de la OCC al que pertenece la institución",
    ),
    "Primary internet address of the institution": (
        "Endereço eletrônico principal da instituição",
        "Dirección electrónica principal de la institución",
    ),
    # --- institution: financial snapshot ---
    "Quarter end the financial snapshot columns refer to": (
        "Fim de trimestre a que se referem as colunas do retrato financeiro",
        "Fin de trimestre al que se refieren las columnas del retrato financiero",
    ),
    "Total assets": ("Ativos totais", "Activos totales"),
    "Total deposits": ("Depósitos totais", "Depósitos totales"),
    "Deposits held in domestic offices": (
        "Depósitos mantidos em agências domésticas",
        "Depósitos mantenidos en oficinas domésticas",
    ),
    "Total equity capital": (
        "Patrimônio líquido total",
        "Patrimonio neto total",
    ),
    "Net income for the year to date": (
        "Lucro líquido acumulado no ano",
        "Utilidad neta acumulada del año",
    ),
    "Net income as a percent of average total assets": (
        "Lucro líquido como percentual dos ativos totais médios",
        "Utilidad neta como porcentaje de los activos totales promedio",
    ),
    "Net income as a percent of average equity": (
        "Lucro líquido como percentual do patrimônio líquido médio",
        "Utilidad neta como porcentaje del patrimonio neto promedio",
    ),
    "Number of domestic offices": (
        "Número de agências domésticas",
        "Número de oficinas domésticas",
    ),
    "Number of foreign offices": (
        "Número de agências no exterior",
        "Número de oficinas en el exterior",
    ),
    # --- indicator ---
    "FDIC mnemonic of the line item": (
        "Mnemônico do FDIC para a rubrica",
        "Mnemónico de la FDIC para la partida",
    ),
    "Readable name of the line item": (
        "Nome legível da rubrica",
        "Nombre legible de la partida",
    ),
    "Definition of the line item as published by the FDIC": (
        "Definição da rubrica conforme publicada pelo FDIC",
        "Definición de la partida según la publica la FDIC",
    ),
    "Unit the value is expressed in: USD, percent or unit": (
        "Unidade em que o valor é expresso: USD, percent ou unit",
        "Unidad en que se expresa el valor: USD, percent o unit",
    ),
    "Whether the line item is a ratio computed by the FDIC": (
        "Indica se a rubrica é um índice calculado pelo FDIC",
        "Indica si la partida es un índice calculado por la FDIC",
    ),
    "Whether the line item covers the quarter rather than the year to date": (
        "Indica se a rubrica se refere ao trimestre e não ao acumulado do ano",
        "Indica si la partida se refiere al trimestre y no al acumulado del año",
    ),
    "Whether the line item is a binary flag rather than a measure": (
        "Indica se a rubrica é um marcador binário e não uma medida",
        "Indica si la partida es un marcador binario y no una medida",
    ),
    "Name of the matching column in the financials table": (
        "Nome da coluna correspondente na tabela financials",
        "Nombre de la columna correspondiente en la tabla financials",
    ),
    # --- shared keys ---
    "Calendar year of the quarterly report": (
        "Ano civil do relatório trimestral",
        "Año calendario del informe trimestral",
    ),
    "Calendar quarter of the report, 1 to 4": (
        "Trimestre civil do relatório, de 1 a 4",
        "Trimestre calendario del informe, de 1 a 4",
    ),
    "Last day of the quarter the report covers": (
        "Último dia do trimestre a que o relatório se refere",
        "Último día del trimestre al que se refiere el informe",
    ),
    "FDIC mnemonic of the reported line item": (
        "Mnemônico do FDIC para a rubrica reportada",
        "Mnemónico de la FDIC para la partida reportada",
    ),
    "Reported value of the line item": (
        "Valor reportado da rubrica",
        "Valor reportado de la partida",
    ),
    "Federal Reserve RSSD identifier of the institution": (
        "Identificador RSSD da instituição no Federal Reserve",
        "Identificador RSSD de la institución en la Reserva Federal",
    ),
}
