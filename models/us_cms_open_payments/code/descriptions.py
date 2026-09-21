"""Trilingual column descriptions, keyed by (table family, column).

English follows the wording CMS publishes in its own Table Schema and in the
Open Payments Methodology Overview & Data Dictionary, trimmed to one clause.
Portuguese and Spanish are translations of that wording.

The same column name means different things in different tables -- ``city`` is
the investigator's business city in the child table and the profile city drawn
from the CMS Master Provider List in the entity tables -- so lookups resolve
(family, column) first and fall back to the shared ``ANY`` layer.
"""

FAMILY = {
    "research_principal_investigator": "pi",
    "covered_recipient_profile": "recipient_profile",
    "summary_physician": "recipient_profile",
    "teaching_hospital_profile": "hospital",
    "summary_teaching_hospital": "hospital",
    "reporting_entity_profile": "entity",
    "provider_profile_mapping": "mapping",
    "dicionario": "dicionario",
}


def family_of(table: str) -> str:
    if table in FAMILY:
        return FAMILY[table]
    return "summary" if table.startswith("summary_") else "detail"


def _series(
    template: dict[str, str], column: str, count: int
) -> dict[str, tuple[str, str, str]]:
    """Expand a numbered column family, e.g. product_name_1 .. product_name_5."""
    return {
        f"{column}_{i}": tuple(
            template[lang].format(i=i, n=count) for lang in ("pt", "en", "es")
        )
        for i in range(1, count + 1)
    }


# --- shared across every table ---------------------------------------------
ANY = {
    "year": (
        "Ano do programa a que o registro se refere",
        "Program year the record refers to",
        "Año del programa al que se refiere el registro",
    ),
    "record_id": (
        "Identificador único do registro de pagamento atribuído pelo sistema Open Payments",
        "System-assigned unique identifier of the payment record",
        "Identificador único del registro de pago asignado por el sistema Open Payments",
    ),
    "covered_recipient_profile_id": (
        "Identificador único do perfil do médico ou profissional não médico que recebeu o pagamento",
        "Unique identifier of the covered recipient physician or non-physician practitioner profile",
        "Identificador único del perfil del médico o profesional no médico que recibió el pago",
    ),
    "covered_recipient_npi": (
        "National Provider Identifier (NPI) do médico ou profissional não médico",
        "National Provider Identifier (NPI) of the covered recipient physician or non-physician practitioner",
        "National Provider Identifier (NPI) del médico o profesional no médico",
    ),
    "covered_recipient_type": (
        "Tipo de beneficiário: médico, profissional não médico, hospital universitário ou entidade não coberta",
        "Type of recipient: physician, non-physician practitioner, teaching hospital or non-covered entity",
        "Tipo de beneficiario: médico, profesional no médico, hospital universitario o entidad no cubierta",
    ),
    "teaching_hospital_ccn": (
        "CMS Certification Number (CCN) do hospital universitário",
        "CMS Certification Number (CCN) of the teaching hospital",
        "CMS Certification Number (CCN) del hospital universitario",
    ),
    "teaching_hospital_name": (
        "Nome do hospital universitário, conforme a lista de hospitais universitários do CMS",
        "Name of the teaching hospital, as listed in the CMS teaching hospital list",
        "Nombre del hospital universitario, según la lista de hospitales universitarios del CMS",
    ),
    "reporting_entity_id": (
        "Identificador único do fabricante ou GPO que declarou o pagamento",
        "Unique identifier of the applicable manufacturer or GPO that reported the payment",
        "Identificador único del fabricante o GPO que declaró el pago",
    ),
    "reporting_entity_name": (
        "Nome do fabricante ou GPO que efetuou o pagamento",
        "Name of the applicable manufacturer or GPO making the payment",
        "Nombre del fabricante o GPO que efectuó el pago",
    ),
    "reporting_entity_state": (
        "Sigla de duas letras do estado do fabricante ou GPO que efetuou o pagamento",
        "Two-letter state abbreviation of the applicable manufacturer or GPO making the payment",
        "Sigla de dos letras del estado del fabricante o GPO que efectuó el pago",
    ),
    "reporting_entity_country": (
        "País do fabricante ou GPO que efetuou o pagamento",
        "Country of the applicable manufacturer or GPO making the payment",
        "País del fabricante o GPO que efectuó el pago",
    ),
    "general_payment_amount_total": (
        "Valor total de pagamentos gerais, em dólares americanos",
        "Total amount of general payments, in US dollars",
        "Monto total de pagos generales, en dólares estadounidenses",
    ),
    "research_payment_amount_total": (
        "Valor total de pagamentos de pesquisa, em dólares americanos",
        "Total amount of research payments, in US dollars",
        "Monto total de pagos de investigación, en dólares estadounidenses",
    ),
    "general_transaction_count": (
        "Quantidade de transações de pagamento geral",
        "Number of general payment transactions",
        "Cantidad de transacciones de pago general",
    ),
    "research_transaction_count": (
        "Quantidade de transações de pagamento de pesquisa",
        "Number of research payment transactions",
        "Cantidad de transacciones de pago de investigación",
    ),
    "disputed_transaction_count": (
        "Quantidade de transações contestadas pelo beneficiário",
        "Number of transactions disputed by the recipient",
        "Cantidad de transacciones disputadas por el beneficiario",
    ),
    "undisputed_transaction_count": (
        "Quantidade de transações não contestadas",
        "Number of undisputed transactions",
        "Cantidad de transacciones no disputadas",
    ),
    "ownership_amount_invested_total": (
        "Valor total investido em participação societária, em dólares americanos",
        "Total amount invested in ownership or investment interest, in US dollars",
        "Monto total invertido en participación societaria, en dólares estadounidenses",
    ),
    "ownership_interest_value_total": (
        "Valor total da participação societária detida, em dólares americanos",
        "Total value of the ownership or investment interest held, in US dollars",
        "Valor total de la participación societaria mantenida, en dólares estadounidenses",
    ),
    "ownership_invested_transaction_count": (
        "Quantidade de registros de investimento em participação societária",
        "Number of ownership investment records",
        "Cantidad de registros de inversión en participación societaria",
    ),
    "ownership_interest_transaction_count": (
        "Quantidade de registros de participação societária detida",
        "Number of ownership interest records",
        "Cantidad de registros de participación societaria mantenida",
    ),
}


# --- payment detail tables --------------------------------------------------
_LICENSE = {
    "pt": "Sigla de duas letras do estado da licença profissional, licença {i} de {n}",
    "en": "Two-letter state abbreviation of the professional license, license {i} of {n}",
    "es": "Sigla de dos letras del estado de la licencia profesional, licencia {i} de {n}",
}
_PRIMARY_TYPE = {
    "pt": "Tipo principal de prática do beneficiário, tipo {i} de {n}",
    "en": "Primary type of medicine practiced by the covered recipient, type {i} of {n}",
    "es": "Tipo principal de práctica del beneficiario, tipo {i} de {n}",
}
_SPECIALTY = {
    "pt": "Especialidade do beneficiário na taxonomia padronizada de prestadores, especialidade {i} de {n}",
    "en": "Specialty of the covered recipient from the standardized provider taxonomy, specialty {i} of {n}",
    "es": "Especialidad del beneficiario en la taxonomía estandarizada de prestadores, especialidad {i} de {n}",
}

DETAIL = {
    "covered_recipient_first_name": (
        "Primeiro nome do beneficiário, conforme declarado pela entidade declarante",
        "First name of the covered recipient, as reported by the submitting entity",
        "Primer nombre del beneficiario, según lo declarado por la entidad declarante",
    ),
    "covered_recipient_middle_name": (
        "Nome do meio do beneficiário, conforme declarado pela entidade declarante",
        "Middle name of the covered recipient, as reported by the submitting entity",
        "Segundo nombre del beneficiario, según lo declarado por la entidad declarante",
    ),
    "covered_recipient_last_name": (
        "Sobrenome do beneficiário, conforme declarado pela entidade declarante",
        "Last name of the covered recipient, as reported by the submitting entity",
        "Apellido del beneficiario, según lo declarado por la entidad declarante",
    ),
    "covered_recipient_name_suffix": (
        "Sufixo do nome do beneficiário, conforme declarado pela entidade declarante",
        "Name suffix of the covered recipient, as reported by the submitting entity",
        "Sufijo del nombre del beneficiario, según lo declarado por la entidad declarante",
    ),
    "physician_profile_id": (
        "Identificador único do perfil do médico que recebeu o pagamento",
        "Unique identifier of the physician profile receiving the payment",
        "Identificador único del perfil del médico que recibió el pago",
    ),
    "physician_npi": (
        "National Provider Identifier (NPI) do médico",
        "National Provider Identifier (NPI) of the physician",
        "National Provider Identifier (NPI) del médico",
    ),
    "physician_first_name": (
        "Primeiro nome do médico, conforme declarado pela entidade declarante",
        "First name of the physician, as reported by the submitting entity",
        "Primer nombre del médico, según lo declarado por la entidad declarante",
    ),
    "physician_middle_name": (
        "Nome do meio do médico, conforme declarado pela entidade declarante",
        "Middle name of the physician, as reported by the submitting entity",
        "Segundo nombre del médico, según lo declarado por la entidad declarante",
    ),
    "physician_last_name": (
        "Sobrenome do médico, conforme declarado pela entidade declarante",
        "Last name of the physician, as reported by the submitting entity",
        "Apellido del médico, según lo declarado por la entidad declarante",
    ),
    "physician_name_suffix": (
        "Sufixo do nome do médico, conforme declarado pela entidade declarante",
        "Name suffix of the physician, as reported by the submitting entity",
        "Sufijo del nombre del médico, según lo declarado por la entidad declarante",
    ),
    "physician_primary_type": (
        "Tipo principal de prática do médico",
        "Primary type of medicine practiced by the physician",
        "Tipo principal de práctica del médico",
    ),
    "physician_specialty": (
        "Especialidade do médico na taxonomia padronizada de prestadores",
        "Specialty of the physician from the standardized provider taxonomy",
        "Especialidad del médico en la taxonomía estandarizada de prestadores",
    ),
    "teaching_hospital_id": (
        "Identificador único do hospital universitário atribuído pelo sistema Open Payments",
        "System-assigned unique identifier of the teaching hospital",
        "Identificador único del hospital universitario asignado por el sistema Open Payments",
    ),
    "recipient_address_line_1": (
        "Primeira linha do endereço comercial principal do beneficiário",
        "First line of the primary business street address of the covered recipient",
        "Primera línea de la dirección comercial principal del beneficiario",
    ),
    "recipient_address_line_2": (
        "Segunda linha do endereço comercial principal do beneficiário",
        "Second line of the primary business street address of the covered recipient",
        "Segunda línea de la dirección comercial principal del beneficiario",
    ),
    "recipient_city": (
        "Cidade do endereço comercial principal do beneficiário",
        "City of the primary business address of the covered recipient",
        "Ciudad de la dirección comercial principal del beneficiario",
    ),
    "recipient_state": (
        "Sigla de duas letras do estado do endereço comercial principal do beneficiário, quando nos Estados Unidos",
        "Two-letter state abbreviation of the covered recipient primary business address, when in the United States",
        "Sigla de dos letras del estado de la dirección comercial principal del beneficiario, cuando está en Estados Unidos",
    ),
    "recipient_zip_code": (
        "CEP de nove dígitos do endereço comercial principal do beneficiário, quando nos Estados Unidos",
        "Nine-digit ZIP code of the covered recipient primary business address, when in the United States",
        "Código postal de nueve dígitos de la dirección comercial principal del beneficiario, cuando está en Estados Unidos",
    ),
    "recipient_country": (
        "País do endereço comercial principal do beneficiário",
        "Country of the primary business address of the covered recipient",
        "País de la dirección comercial principal del beneficiario",
    ),
    "recipient_province": (
        "Província do endereço comercial principal do beneficiário, quando fora dos Estados Unidos",
        "Province of the covered recipient primary business address, when outside the United States",
        "Provincia de la dirección comercial principal del beneficiario, cuando está fuera de Estados Unidos",
    ),
    "recipient_postal_code": (
        "Código postal do endereço comercial principal do beneficiário, quando fora dos Estados Unidos",
        "Postal code of the covered recipient primary business address, when outside the United States",
        "Código postal de la dirección comercial principal del beneficiario, cuando está fuera de Estados Unidos",
    ),
    "submitting_entity_name": (
        "Nome do fabricante ou GPO que submeteu o registro, que pode diferir de quem efetuou o pagamento",
        "Name of the applicable manufacturer or GPO that submitted the record, which may differ from the entity making the payment",
        "Nombre del fabricante o GPO que presentó el registro, que puede diferir de quien efectuó el pago",
    ),
    "payment_amount_total": (
        "Valor total do pagamento ou transferência de valor ao beneficiário, em dólares americanos",
        "Total amount of the payment or other transfer of value to the recipient, in US dollars",
        "Monto total del pago o transferencia de valor al beneficiario, en dólares estadounidenses",
    ),
    "payment_date": (
        "Data em que o pagamento ou transferência de valor foi efetuado",
        "Date the payment or other transfer of value was made",
        "Fecha en que se efectuó el pago o transferencia de valor",
    ),
    "payment_count": (
        "Quantidade de pagamentos individuais agregados no valor total do registro",
        "Number of individual payments included in the record total amount",
        "Cantidad de pagos individuales agregados en el monto total del registro",
    ),
    "payment_form": (
        "Forma do pagamento ou transferência de valor, como dinheiro, ações ou itens em espécie",
        "Form of the payment or other transfer of value, such as cash, stock or in-kind items",
        "Forma del pago o transferencia de valor, como efectivo, acciones o artículos en especie",
    ),
    "payment_nature": (
        "Natureza do pagamento ou transferência de valor, como consultoria, alimentação, viagem ou honorários de palestra",
        "Nature of the payment or other transfer of value, such as consulting, food and beverage, travel or speaker fees",
        "Naturaleza del pago o transferencia de valor, como consultoría, alimentación, viaje u honorarios de conferencia",
    ),
    "travel_city": (
        "Cidade de destino, quando o pagamento se refere a viagem ou hospedagem",
        "Destination city, when the payment relates to travel or lodging",
        "Ciudad de destino, cuando el pago se refiere a viaje u hospedaje",
    ),
    "travel_state": (
        "Estado de destino, quando o pagamento se refere a viagem ou hospedagem",
        "Destination state, when the payment relates to travel or lodging",
        "Estado de destino, cuando el pago se refiere a viaje u hospedaje",
    ),
    "travel_country": (
        "País de destino, quando o pagamento se refere a viagem ou hospedagem",
        "Destination country, when the payment relates to travel or lodging",
        "País de destino, cuando el pago se refiere a viaje u hospedaje",
    ),
    "physician_ownership_indicator": (
        "Indica se o médico ou familiar imediato detém participação societária no fabricante ou GPO declarante",
        "Indicates whether the physician or an immediate family member holds an ownership interest in the reporting manufacturer or GPO",
        "Indica si el médico o un familiar inmediato posee participación societaria en el fabricante o GPO declarante",
    ),
    "third_party_payment_recipient_indicator": (
        "Indica se o pagamento foi feito a terceiro em nome do beneficiário",
        "Indicates whether the payment was made to a third party on behalf of the covered recipient",
        "Indica si el pago se hizo a un tercero en nombre del beneficiario",
    ),
    "third_party_entity_name": (
        "Nome da entidade terceira que recebeu o pagamento em nome do beneficiário",
        "Name of the third party entity that received the payment on behalf of the covered recipient",
        "Nombre de la entidad tercera que recibió el pago en nombre del beneficiario",
    ),
    "charity_indicator": (
        "Indica se a entidade terceira que recebeu o pagamento é uma instituição de caridade",
        "Indicates whether the third party entity that received the payment is a charity",
        "Indica si la entidad tercera que recibió el pago es una institución de caridad",
    ),
    "third_party_equals_covered_recipient_indicator": (
        "Indica que o pagamento a terceiro foi feito em nome do próprio beneficiário",
        "Indicates that the third party payment was made in the name of the covered recipient",
        "Indica que el pago a un tercero se hizo en nombre del propio beneficiario",
    ),
    "contextual_information": (
        "Texto livre fornecido pela entidade declarante sobre o pagamento",
        "Free text supplied by the reporting entity about the payment",
        "Texto libre proporcionado por la entidad declarante sobre el pago",
    ),
    "delay_in_publication_indicator": (
        "Indica que a entidade declarante solicitou adiamento da publicação do registro",
        "Indicates that the reporting entity requested a delay in publication of the record",
        "Indica que la entidad declarante solicitó aplazamiento de la publicación del registro",
    ),
    "dispute_status": (
        "Indica se o registro estava contestado no momento da publicação",
        "Indicates whether the record was under dispute at the time of publication",
        "Indica si el registro estaba en disputa al momento de la publicación",
    ),
    "change_type": (
        "Indica se o registro é novo, adicionado, alterado ou inalterado em relação à publicação anterior",
        "Indicates whether the record is new, added, changed or unchanged relative to the previous publication",
        "Indica si el registro es nuevo, añadido, modificado o sin cambios respecto a la publicación anterior",
    ),
    "publication_date": (
        "Data em que o registro foi publicado pelo Open Payments",
        "Date the record was published by Open Payments",
        "Fecha en que el registro fue publicado por Open Payments",
    ),
    "related_product_indicator": (
        "Indica se o pagamento está associado a um ou mais medicamentos, produtos biológicos, dispositivos ou insumos médicos",
        "Indicates whether the payment is associated with one or more drugs, biologicals, devices or medical supplies",
        "Indica si el pago está asociado a uno o más medicamentos, productos biológicos, dispositivos o insumos médicos",
    ),
    "noncovered_recipient_entity_name": (
        "Nome da entidade não coberta que recebeu o pagamento de pesquisa",
        "Name of the non-covered recipient entity that received the research payment",
        "Nombre de la entidad no cubierta que recibió el pago de investigación",
    ),
    "study_name": (
        "Nome do estudo de pesquisa a que o pagamento se refere",
        "Name of the research study the payment relates to",
        "Nombre del estudio de investigación al que se refiere el pago",
    ),
    "clinicaltrials_gov_id": (
        "Identificador do estudo no registro clinicaltrials.gov, quando registrado",
        "Identifier of the study in the clinicaltrials.gov registry, when registered",
        "Identificador del estudio en el registro clinicaltrials.gov, cuando está registrado",
    ),
    "research_information_link": (
        "Endereço eletrônico com informações adicionais sobre a pesquisa",
        "Web address with additional information about the research",
        "Dirección web con información adicional sobre la investigación",
    ),
    "research_context": (
        "Texto livre com o contexto da pesquisa fornecido pela entidade declarante",
        "Free text describing the context of the research, supplied by the reporting entity",
        "Texto libre con el contexto de la investigación proporcionado por la entidad declarante",
    ),
    "preclinical_research_indicator": (
        "Indica que o pagamento se refere a pesquisa pré-clínica, cujos detalhes de estudo não são publicados",
        "Indicates that the payment relates to preclinical research, for which study details are not published",
        "Indica que el pago se refiere a investigación preclínica, cuyos detalles de estudio no se publican",
    ),
    "amount_invested_total": (
        "Valor investido pelo médico ou familiar imediato no fabricante ou GPO durante o ano do programa, em dólares americanos",
        "Amount the physician or an immediate family member invested in the manufacturer or GPO during the program year, in US dollars",
        "Monto invertido por el médico o un familiar inmediato en el fabricante o GPO durante el año del programa, en dólares estadounidenses",
    ),
    "interest_value": (
        "Valor da participação societária detida, em dólares americanos",
        "Value of the ownership or investment interest held, in US dollars",
        "Valor de la participación societaria mantenida, en dólares estadounidenses",
    ),
    "interest_terms": (
        "Termos da participação societária, como ações ordinárias, opções ou dívida",
        "Terms of the ownership or investment interest, such as common stock, options or debt",
        "Términos de la participación societaria, como acciones ordinarias, opciones o deuda",
    ),
    "interest_held_by_physician_or_family": (
        "Indica se a participação é detida pelo próprio médico ou por familiar imediato",
        "Indicates whether the interest is held by the physician or by an immediate family member",
        "Indica si la participación es del propio médico o de un familiar inmediato",
    ),
    **_series(_LICENSE, "covered_recipient_license_state", 5),
    **_series(_LICENSE, "physician_license_state", 5),
    **_series(_PRIMARY_TYPE, "covered_recipient_primary_type", 6),
    **_series(_SPECIALTY, "covered_recipient_specialty", 6),
    **_series(
        {
            "pt": "Indica se o produto associado ao pagamento é coberto ou não coberto, produto {i} de {n}",
            "en": "Indicates whether the product associated with the payment is covered or non-covered, product {i} of {n}",
            "es": "Indica si el producto asociado al pago es cubierto o no cubierto, producto {i} de {n}",
        },
        "product_covered_indicator",
        5,
    ),
    **_series(
        {
            "pt": "Indica se o produto é medicamento, produto biológico, dispositivo ou insumo médico, produto {i} de {n}",
            "en": "Indicates whether the product is a drug, biological, device or medical supply, product {i} of {n}",
            "es": "Indica si el producto es medicamento, producto biológico, dispositivo o insumo médico, producto {i} de {n}",
        },
        "product_type",
        5,
    ),
    **_series(
        {
            "pt": "Categoria do produto ou área terapêutica, produto {i} de {n}",
            "en": "Product category or therapeutic area, product {i} of {n}",
            "es": "Categoría del producto o área terapéutica, producto {i} de {n}",
        },
        "product_category",
        5,
    ),
    **_series(
        {
            "pt": "Nome do medicamento, produto biológico, dispositivo ou insumo médico associado ao pagamento, produto {i} de {n}",
            "en": "Name of the drug, biological, device or medical supply associated with the payment, product {i} of {n}",
            "es": "Nombre del medicamento, producto biológico, dispositivo o insumo médico asociado al pago, producto {i} de {n}",
        },
        "product_name",
        5,
    ),
    **_series(
        {
            "pt": "National Drug Code (NDC) do medicamento ou produto biológico associado, produto {i} de {n}",
            "en": "National Drug Code (NDC) of the associated drug or biological, product {i} of {n}",
            "es": "National Drug Code (NDC) del medicamento o producto biológico asociado, producto {i} de {n}",
        },
        "product_ndc",
        5,
    ),
    **_series(
        {
            "pt": "Primary Device Identifier (PDI) do dispositivo ou insumo médico associado, produto {i} de {n}",
            "en": "Primary Device Identifier (PDI) of the associated device or medical supply, product {i} of {n}",
            "es": "Primary Device Identifier (PDI) del dispositivo o insumo médico asociado, producto {i} de {n}",
        },
        "product_pdi",
        5,
    ),
    **_series(
        {
            "pt": "Nome do medicamento ou produto biológico coberto associado ao pagamento, medicamento {i} de {n}",
            "en": "Name of the associated covered drug or biological, drug {i} of {n}",
            "es": "Nombre del medicamento o producto biológico cubierto asociado al pago, medicamento {i} de {n}",
        },
        "drug_name",
        5,
    ),
    **_series(
        {
            "pt": "National Drug Code (NDC) do medicamento ou produto biológico coberto associado, medicamento {i} de {n}",
            "en": "National Drug Code (NDC) of the associated covered drug or biological, drug {i} of {n}",
            "es": "National Drug Code (NDC) del medicamento o producto biológico cubierto asociado, medicamento {i} de {n}",
        },
        "drug_ndc",
        5,
    ),
    **_series(
        {
            "pt": "Nome do dispositivo ou insumo médico coberto associado ao pagamento, dispositivo {i} de {n}",
            "en": "Name of the associated covered device or medical supply, device {i} of {n}",
            "es": "Nombre del dispositivo o insumo médico cubierto asociado al pago, dispositivo {i} de {n}",
        },
        "device_name",
        5,
    ),
    **_series(
        {
            "pt": "Categoria de despesa do pagamento de pesquisa, categoria {i} de {n}",
            "en": "Expenditure category of the research payment, category {i} of {n}",
            "es": "Categoría de gasto del pago de investigación, categoría {i} de {n}",
        },
        "expenditure_category",
        6,
    ),
}


# --- principal investigator child table -------------------------------------
PI = {
    "principal_investigator_number": (
        "Posição do pesquisador principal no registro de origem, de 1 a 5",
        "Position of the principal investigator within the source record, 1 to 5",
        "Posición del investigador principal en el registro de origen, de 1 a 5",
    ),
    "first_name": (
        "Primeiro nome do pesquisador principal",
        "First name of the principal investigator",
        "Primer nombre del investigador principal",
    ),
    "middle_name": (
        "Nome do meio do pesquisador principal",
        "Middle name of the principal investigator",
        "Segundo nombre del investigador principal",
    ),
    "last_name": (
        "Sobrenome do pesquisador principal",
        "Last name of the principal investigator",
        "Apellido del investigador principal",
    ),
    "name_suffix": (
        "Sufixo do nome do pesquisador principal",
        "Name suffix of the principal investigator",
        "Sufijo del nombre del investigador principal",
    ),
    "address_line_1": (
        "Primeira linha do endereço comercial do pesquisador principal",
        "First line of the business street address of the principal investigator",
        "Primera línea de la dirección comercial del investigador principal",
    ),
    "address_line_2": (
        "Segunda linha do endereço comercial do pesquisador principal",
        "Second line of the business street address of the principal investigator",
        "Segunda línea de la dirección comercial del investigador principal",
    ),
    "city": (
        "Cidade do endereço comercial do pesquisador principal",
        "City of the business address of the principal investigator",
        "Ciudad de la dirección comercial del investigador principal",
    ),
    "state": (
        "Sigla de duas letras do estado do endereço comercial do pesquisador principal",
        "Two-letter state abbreviation of the principal investigator business address",
        "Sigla de dos letras del estado de la dirección comercial del investigador principal",
    ),
    "zip_code": (
        "CEP do endereço comercial do pesquisador principal",
        "ZIP code of the business address of the principal investigator",
        "Código postal de la dirección comercial del investigador principal",
    ),
    "country": (
        "País do endereço comercial do pesquisador principal",
        "Country of the business address of the principal investigator",
        "País de la dirección comercial del investigador principal",
    ),
    "province": (
        "Província do endereço comercial do pesquisador principal, quando fora dos Estados Unidos",
        "Province of the principal investigator business address, when outside the United States",
        "Provincia de la dirección comercial del investigador principal, cuando está fuera de Estados Unidos",
    ),
    "postal_code": (
        "Código postal do endereço comercial do pesquisador principal, quando fora dos Estados Unidos",
        "Postal code of the principal investigator business address, when outside the United States",
        "Código postal de la dirección comercial del investigador principal, cuando está fuera de Estados Unidos",
    ),
    "covered_recipient_type": (
        "Tipo de beneficiário do pesquisador principal, preenchido a partir do ano do programa 2016",
        "Covered recipient type of the principal investigator, populated from program year 2016 onwards",
        "Tipo de beneficiario del investigador principal, completado a partir del año del programa 2016",
    ),
    **_series(_LICENSE, "license_state", 5),
    **_series(_PRIMARY_TYPE, "primary_type", 6),
    **_series(_SPECIALTY, "specialty", 6),
}

# --- covered recipient profile ----------------------------------------------
_MPL = "lista mestra de prestadores (MPL) fornecida pelo CMS"
RECIPIENT_PROFILE = {
    "profile_type": (
        "Tipo do perfil: médico ou profissional não médico",
        "Type of profile: physician or non-physician practitioner",
        "Tipo de perfil: médico o profesional no médico",
    ),
    "associated_profile_id_1": (
        "Identificador de outro perfil associado ao mesmo profissional, associação 1 de 2",
        "Identifier of another profile associated with the same practitioner, association 1 of 2",
        "Identificador de otro perfil asociado al mismo profesional, asociación 1 de 2",
    ),
    "associated_profile_id_2": (
        "Identificador de outro perfil associado ao mesmo profissional, associação 2 de 2",
        "Identifier of another profile associated with the same practitioner, association 2 of 2",
        "Identificador de otro perfil asociado al mismo profesional, asociación 2 de 2",
    ),
    "first_name": (
        f"Primeiro nome do profissional conforme a {_MPL}",
        "First name of the practitioner as listed in the CMS master provider list",
        "Primer nombre del profesional según la lista maestra de prestadores del CMS",
    ),
    "middle_name": (
        f"Nome do meio do profissional conforme a {_MPL}",
        "Middle name of the practitioner as listed in the CMS master provider list",
        "Segundo nombre del profesional según la lista maestra de prestadores del CMS",
    ),
    "last_name": (
        f"Sobrenome do profissional conforme a {_MPL}",
        "Last name of the practitioner as listed in the CMS master provider list",
        "Apellido del profesional según la lista maestra de prestadores del CMS",
    ),
    "name_suffix": (
        f"Sufixo do nome do profissional conforme a {_MPL}",
        "Name suffix of the practitioner as listed in the CMS master provider list",
        "Sufijo del nombre del profesional según la lista maestra de prestadores del CMS",
    ),
    "alternate_first_name": (
        "Primeiro nome alternativo do profissional registrado na lista mestra de prestadores",
        "Alternate first name of the practitioner recorded in the master provider list",
        "Primer nombre alternativo del profesional registrado en la lista maestra de prestadores",
    ),
    "alternate_middle_name": (
        "Nome do meio alternativo do profissional registrado na lista mestra de prestadores",
        "Alternate middle name of the practitioner recorded in the master provider list",
        "Segundo nombre alternativo del profesional registrado en la lista maestra de prestadores",
    ),
    "alternate_last_name": (
        "Sobrenome alternativo do profissional registrado na lista mestra de prestadores",
        "Alternate last name of the practitioner recorded in the master provider list",
        "Apellido alternativo del profesional registrado en la lista maestra de prestadores",
    ),
    "alternate_name_suffix": (
        "Sufixo alternativo do nome do profissional registrado na lista mestra de prestadores",
        "Alternate name suffix of the practitioner recorded in the master provider list",
        "Sufijo alternativo del nombre del profesional registrado en la lista maestra de prestadores",
    ),
    "address_line_1": (
        "Primeira linha do endereço de prática do profissional conforme a lista mestra de prestadores",
        "First line of the practitioner business practice location address from the master provider list",
        "Primera línea de la dirección de práctica del profesional según la lista maestra de prestadores",
    ),
    "address_line_2": (
        "Segunda linha do endereço de prática do profissional conforme a lista mestra de prestadores",
        "Second line of the practitioner business practice location address from the master provider list",
        "Segunda línea de la dirección de práctica del profesional según la lista maestra de prestadores",
    ),
    "city": (
        "Cidade do endereço de prática do profissional conforme a lista mestra de prestadores",
        "City of the practitioner business practice location from the master provider list",
        "Ciudad de la dirección de práctica del profesional según la lista maestra de prestadores",
    ),
    "state": (
        "Sigla de duas letras do estado do endereço de prática do profissional",
        "Two-letter state abbreviation of the practitioner business practice location",
        "Sigla de dos letras del estado de la dirección de práctica del profesional",
    ),
    "zip_code": (
        "CEP do endereço de prática do profissional",
        "ZIP code of the practitioner business practice location",
        "Código postal de la dirección de práctica del profesional",
    ),
    "country": (
        "País do endereço de prática do profissional",
        "Country of the practitioner business practice location",
        "País de la dirección de práctica del profesional",
    ),
    "province": (
        "Província do endereço de prática do profissional, quando fora dos Estados Unidos",
        "Province of the practitioner business practice location, when outside the United States",
        "Provincia de la dirección de práctica del profesional, cuando está fuera de Estados Unidos",
    ),
    "primary_specialty": (
        "Especialidade principal do profissional na taxonomia padronizada de prestadores",
        "Primary specialty of the practitioner from the standardized provider taxonomy",
        "Especialidad principal del profesional en la taxonomía estandarizada de prestadores",
    ),
    "has_multiple_ids": (
        "Indica se o profissional possui mais de um identificador de perfil no Open Payments",
        "Indicates whether the practitioner has more than one Open Payments profile identifier",
        "Indica si el profesional tiene más de un identificador de perfil en Open Payments",
    ),
    "associated_research_payment_amount_total": (
        "Valor total de pagamentos de pesquisa em que o profissional consta como pesquisador principal, em dólares americanos",
        "Total amount of research payments on which the practitioner appears as principal investigator, in US dollars",
        "Monto total de pagos de investigación en los que el profesional consta como investigador principal, en dólares estadounidenses",
    ),
    "associated_research_transaction_count": (
        "Quantidade de transações de pesquisa em que o profissional consta como pesquisador principal",
        "Number of research transactions on which the practitioner appears as principal investigator",
        "Cantidad de transacciones de investigación en las que el profesional consta como investigador principal",
    ),
    **_series(
        {
            "pt": "Código de taxonomia de prestador do Open Payments, taxonomia {i} de {n}",
            "en": "Open Payments provider taxonomy code, taxonomy {i} of {n}",
            "es": "Código de taxonomía de prestador de Open Payments, taxonomía {i} de {n}",
        },
        "taxonomy",
        6,
    ),
    **_series(_LICENSE, "license_state", 5),
}

# --- teaching hospital ------------------------------------------------------
HOSPITAL = {
    "name": (
        "Nome do hospital universitário, conforme a lista de hospitais universitários do CMS",
        "Name of the teaching hospital, as listed in the CMS teaching hospital list",
        "Nombre del hospital universitario, según la lista de hospitales universitarios del CMS",
    ),
    "address_line_1": (
        "Primeira linha do endereço do hospital universitário",
        "First line of the teaching hospital street address",
        "Primera línea de la dirección del hospital universitario",
    ),
    "address_line_2": (
        "Segunda linha do endereço do hospital universitário",
        "Second line of the teaching hospital street address",
        "Segunda línea de la dirección del hospital universitario",
    ),
    "city": (
        "Cidade do hospital universitário",
        "City of the teaching hospital",
        "Ciudad del hospital universitario",
    ),
    "state": (
        "Sigla de duas letras do estado do hospital universitário",
        "Two-letter state abbreviation of the teaching hospital",
        "Sigla de dos letras del estado del hospital universitario",
    ),
    "zip_code": (
        "CEP do hospital universitário",
        "ZIP code of the teaching hospital",
        "Código postal del hospital universitario",
    ),
    **_series(
        {
            "pt": "Nome alternativo pelo qual o hospital universitário também é registrado, nome {i} de {n}",
            "en": "Alternate name under which the teaching hospital is also recorded, name {i} of {n}",
            "es": "Nombre alternativo bajo el cual el hospital universitario también está registrado, nombre {i} de {n}",
        },
        "alternate_name",
        5,
    ),
}

# --- reporting entity -------------------------------------------------------
ENTITY = {
    "name": (
        "Nome do fabricante ou GPO declarante",
        "Name of the applicable manufacturer or GPO",
        "Nombre del fabricante o GPO declarante",
    ),
    "state": (
        "Sigla de duas letras do estado do fabricante ou GPO declarante",
        "Two-letter state abbreviation of the applicable manufacturer or GPO",
        "Sigla de dos letras del estado del fabricante o GPO declarante",
    ),
    "country": (
        "País do fabricante ou GPO declarante",
        "Country of the applicable manufacturer or GPO",
        "País del fabricante o GPO declarante",
    ),
    **_series(
        {
            "pt": "Nome alternativo pelo qual o fabricante ou GPO também é registrado, nome {i} de {n}",
            "en": "Alternate name under which the manufacturer or GPO is also recorded, name {i} of {n}",
            "es": "Nombre alternativo bajo el cual el fabricante o GPO también está registrado, nombre {i} de {n}",
        },
        "alternate_name",
        5,
    ),
}

MAPPING = {
    "primary_profile_id": (
        "Identificador de perfil principal do prestador, para o qual os perfis secundários são consolidados",
        "Primary provider profile identifier, into which secondary profiles are consolidated",
        "Identificador de perfil principal del prestador, en el cual se consolidan los perfiles secundarios",
    ),
    "secondary_profile_id": (
        "Identificador de perfil secundário do prestador, que remete ao perfil principal",
        "Secondary provider profile identifier, which resolves to the primary profile",
        "Identificador de perfil secundario del prestador, que remite al perfil principal",
    ),
}

DICIONARIO = {
    "id_tabela": (
        "Nome da tabela à qual a chave e o valor se referem",
        "Name of the table the key and value refer to",
        "Nombre de la tabla a la que se refieren la clave y el valor",
    ),
    "nome_coluna": (
        "Nome da coluna à qual a chave e o valor se referem",
        "Name of the column the key and value refer to",
        "Nombre de la columna a la que se refieren la clave y el valor",
    ),
    "chave": (
        "Código presente nos dados da coluna",
        "Code present in the column data",
        "Código presente en los datos de la columna",
    ),
    "cobertura_temporal": (
        "Cobertura temporal a que se aplica o par chave-valor",
        "Temporal coverage the key-value pair applies to",
        "Cobertura temporal a la que se aplica el par clave-valor",
    ),
    "valor": (
        "Rótulo correspondente ao código na coluna",
        "Label corresponding to the code in the column",
        "Etiqueta correspondiente al código en la columna",
    ),
}


# --- summary reports --------------------------------------------------------
# The summary family repeats one statistic across three recipient types.
_RECIPIENT_KIND = {
    "physician": ("médicos", "physicians", "médicos"),
    "non_physician_practitioner": (
        "profissionais não médicos",
        "non-physician practitioners",
        "profesionales no médicos",
    ),
    "teaching_hospital": (
        "hospitais universitários",
        "teaching hospitals",
        "hospitales universitarios",
    ),
}

_STATISTIC = {
    "payment_amount_total": (
        "Valor total pago a {pt}, em dólares americanos",
        "Total amount paid to {en}, in US dollars",
        "Monto total pagado a {es}, en dólares estadounidenses",
    ),
    "payment_amount_mean": (
        "Valor médio pago a {pt}, em dólares americanos",
        "Mean amount paid to {en}, in US dollars",
        "Monto medio pagado a {es}, en dólares estadounidenses",
    ),
    "payment_amount_median": (
        "Valor mediano pago a {pt}, em dólares americanos",
        "Median amount paid to {en}, in US dollars",
        "Monto mediano pagado a {es}, en dólares estadounidenses",
    ),
    "payment_count_total": (
        "Quantidade total de pagamentos a {pt}",
        "Total number of payments to {en}",
        "Cantidad total de pagos a {es}",
    ),
    "payment_count_mean": (
        "Quantidade média de pagamentos por beneficiário entre {pt}",
        "Mean number of payments per recipient among {en}",
        "Cantidad media de pagos por beneficiario entre {es}",
    ),
    "payment_count_median": (
        "Quantidade mediana de pagamentos por beneficiário entre {pt}",
        "Median number of payments per recipient among {en}",
        "Cantidad mediana de pagos por beneficiario entre {es}",
    ),
    "general_transaction_count": (
        "Quantidade de transações de pagamento geral com {pt}",
        "Number of general payment transactions with {en}",
        "Cantidad de transacciones de pago general con {es}",
    ),
    "research_transaction_count": (
        "Quantidade de transações de pagamento de pesquisa com {pt}",
        "Number of research payment transactions with {en}",
        "Cantidad de transacciones de pago de investigación con {es}",
    ),
}


def _by_recipient_kind() -> dict[str, tuple[str, str, str]]:
    out = {}
    for stat, template in _STATISTIC.items():
        for kind, words in _RECIPIENT_KIND.items():
            out[f"{stat}_{kind}"] = tuple(
                template[i].format(pt=words[0], en=words[1], es=words[2])
                for i in range(3)
            )
    return out


SUMMARY = {
    "recipient_id": (
        "Identificador do beneficiário: perfil do profissional ou CCN do hospital universitário",
        "Recipient identifier: the practitioner profile identifier or the teaching hospital CCN",
        "Identificador del beneficiario: perfil del profesional o CCN del hospital universitario",
    ),
    "recipient_type": (
        "Tipo de beneficiário: médico, profissional não médico ou hospital universitário",
        "Type of recipient: physician, non-physician practitioner or teaching hospital",
        "Tipo de beneficiario: médico, profesional no médico u hospital universitario",
    ),
    "recipient_name": (
        "Nome do beneficiário agregado no registro",
        "Name of the recipient aggregated in the record",
        "Nombre del beneficiario agregado en el registro",
    ),
    "first_name": (
        "Primeiro nome do profissional beneficiário",
        "First name of the recipient practitioner",
        "Primer nombre del profesional beneficiario",
    ),
    "middle_name": (
        "Nome do meio do profissional beneficiário",
        "Middle name of the recipient practitioner",
        "Segundo nombre del profesional beneficiario",
    ),
    "last_name": (
        "Sobrenome do profissional beneficiário",
        "Last name of the recipient practitioner",
        "Apellido del profesional beneficiario",
    ),
    "payment_type": (
        "Categoria de pagamento agregada: geral, pesquisa ou participação societária",
        "Payment category aggregated: general, research or ownership",
        "Categoría de pago agregada: general, investigación o participación societaria",
    ),
    "payment_nature": (
        "Natureza do pagamento ou transferência de valor",
        "Nature of the payment or other transfer of value",
        "Naturaleza del pago o transferencia de valor",
    ),
    "payment_nature_code": (
        "Código da natureza do pagamento ou transferência de valor",
        "Code of the nature of the payment or other transfer of value",
        "Código de la naturaleza del pago o transferencia de valor",
    ),
    "transaction_count": (
        "Quantidade de transações agregadas no registro",
        "Number of transactions aggregated in the record",
        "Cantidad de transacciones agregadas en el registro",
    ),
    "amount_total": (
        "Valor total agregado no registro, em dólares americanos",
        "Total amount aggregated in the record, in US dollars",
        "Monto total agregado en el registro, en dólares estadounidenses",
    ),
    "metric_level": (
        "Nível de agregação da métrica",
        "Aggregation level of the metric",
        "Nivel de agregación de la métrica",
    ),
    "country_code": (
        "Código do país do beneficiário",
        "Country code of the recipient",
        "Código del país del beneficiario",
    ),
    "country_name": (
        "Nome do país do beneficiário",
        "Country name of the recipient",
        "Nombre del país del beneficiario",
    ),
    "state_code": (
        "Sigla de duas letras do estado do beneficiário",
        "Two-letter state abbreviation of the recipient",
        "Sigla de dos letras del estado del beneficiario",
    ),
    "state_name": (
        "Nome do estado do beneficiário",
        "State name of the recipient",
        "Nombre del estado del beneficiario",
    ),
    "taxonomy_code": (
        "Código de taxonomia de prestador que identifica a especialidade agregada",
        "Provider taxonomy code identifying the aggregated specialty",
        "Código de taxonomía de prestador que identifica la especialidad agregada",
    ),
    "provider_type_description": (
        "Descrição do grupo de prestadores na taxonomia",
        "Description of the provider grouping in the taxonomy",
        "Descripción del grupo de prestadores en la taxonomía",
    ),
    "classification": (
        "Classificação da especialidade na taxonomia de prestadores",
        "Classification of the specialty within the provider taxonomy",
        "Clasificación de la especialidad en la taxonomía de prestadores",
    ),
    "specialization": (
        "Especialização dentro da classificação da taxonomia de prestadores",
        "Specialization within the provider taxonomy classification",
        "Especialización dentro de la clasificación de la taxonomía de prestadores",
    ),
    "physician_count": (
        "Quantidade de médicos distintos que receberam pagamentos",
        "Number of distinct physicians that received payments",
        "Cantidad de médicos distintos que recibieron pagos",
    ),
    "non_physician_practitioner_count": (
        "Quantidade de profissionais não médicos distintos que receberam pagamentos",
        "Number of distinct non-physician practitioners that received payments",
        "Cantidad de profesionales no médicos distintos que recibieron pagos",
    ),
    "teaching_hospital_count": (
        "Quantidade de hospitais universitários distintos que receberam pagamentos",
        "Number of distinct teaching hospitals that received payments",
        "Cantidad de hospitales universitarios distintos que recibieron pagos",
    ),
    "dashboard_row_number": (
        "Número da linha da métrica no painel resumo publicado pelo CMS",
        "Row number of the metric in the summary dashboard published by CMS",
        "Número de fila de la métrica en el panel resumen publicado por el CMS",
    ),
    "metric": (
        "Nome da métrica agregada do painel resumo",
        "Name of the aggregated metric in the summary dashboard",
        "Nombre de la métrica agregada del panel resumen",
    ),
    "value": (
        "Valor da métrica no ano do programa",
        "Value of the metric in the program year",
        "Valor de la métrica en el año del programa",
    ),
    **_by_recipient_kind(),
}

LAYERS = {
    "detail": DETAIL,
    "pi": PI,
    "recipient_profile": RECIPIENT_PROFILE,
    "hospital": HOSPITAL,
    "entity": ENTITY,
    "mapping": MAPPING,
    "summary": SUMMARY,
    "dicionario": DICIONARIO,
}


def describe(table: str, column: str) -> tuple[str, str, str]:
    """Portuguese, English and Spanish description of one column."""
    layer = LAYERS[family_of(table)]
    if column in layer:
        return layer[column]
    if column in ANY:
        return ANY[column]
    raise KeyError(f"no description for {table}.{column}")
