"""Write the architecture CSVs for world_iati_activities.

The architecture CSVs under ``architecture/`` are the single source of truth for
column names, order, types and the raw -> clean name mapping. Both the one-shot
bootstrap (``clean.py``) and the recurring pipeline
(``pipelines/datasets/world_iati_activities/utils.py``) read them; nothing else
declares a schema.

Column names are English, because the data and its metadata are English
(.claude/rules/data-basis-style.md). Identifiers take the ``_id`` suffix;
``_code`` is reserved for IATI codelist values, each of which ships with a
sibling ``_name`` column carrying the label IATI Tables already resolved — which
is why no column is ``covered_by_dictionary`` and the dataset has no
``dicionario`` table.

Run: ``python gen_architecture.py``
"""

import csv

from common import ARCH_DIR

HEADER = [
    "name",
    "bigquery_type",
    "description_pt",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations_pt",
    "observations_en",
    "observations_es",
    "original_name",
]


def c(
    name,
    raw,
    bq_type,
    pt,
    en,
    es,
    unit="",
    directory="",
    obs=("", "", ""),
):
    """One architecture row. Descriptions are capitalised and carry no trailing
    period, per .claude/rules/data-basis-style.md."""
    for text in (pt, en, es):
        assert (text and text[0].isupper()) or text[0].isdigit(), text
        assert not text.endswith("."), text
    return {
        "name": name,
        "bigquery_type": bq_type,
        "description_pt": pt,
        "description_en": en,
        "description_es": es,
        "temporal_coverage": "",
        "covered_by_dictionary": "no",
        "directory_column": directory,
        "measurement_unit": unit,
        "has_sensitive_data": "no",
        "observations_pt": obs[0],
        "observations_en": obs[1],
        "observations_es": obs[2],
        "original_name": raw,
    }


# --- Building blocks shared across tables ---------------------------------
# Every IATI Tables row carries the registry dataset it came from (`dataset`),
# the publisher that registered it (`prefix`), the activity's globally unique
# identifier and the reporting organisation's ref. licence_id is ours, joined
# from the Bulk Data Service dataset index.


def year_col():
    return c(
        "year",
        "",
        "INT64",
        "Ano da data de referência da linha, derivado pela Data Basis para particionar a tabela",
        "Year of the row's reference date, derived by Data Basis to partition the table",
        "Año de la fecha de referencia de la fila, derivado por Data Basis para particionar la tabla",
        unit="year",
        obs=(
            "Coluna de particionamento. Não existe na fonte: é extraída da coluna de data indicada na descrição da tabela",
            "Partition column. Absent from the source: it is extracted from the date column named in the table description",
            "Columna de particionamiento. No existe en la fuente: se extrae de la columna de fecha indicada en la descripción de la tabla",
        ),
    )


def registry_dataset_id():
    return c(
        "registry_dataset_id",
        "dataset",
        "STRING",
        "Identificador do conjunto de dados no Registro IATI de onde a linha veio",
        "Identifier of the dataset in the IATI Registry the row came from",
        "Identificador del conjunto de datos en el Registro IATI del que proviene la fila",
        directory="world_iati_activities.registry_dataset:registry_dataset_id",
        obs=(
            "Resolve para https://www.iatiregistry.org/dataset/<valor>",
            "Resolves to https://www.iatiregistry.org/dataset/<value>",
            "Resuelve a https://www.iatiregistry.org/dataset/<valor>",
        ),
    )


def publisher_id():
    return c(
        "publisher_id",
        "prefix",
        "STRING",
        "Identificador da organização publicadora no Registro IATI",
        "Identifier of the publishing organisation in the IATI Registry",
        "Identificador de la organización publicadora en el Registro IATI",
        obs=(
            "Resolve para https://www.iatiregistry.org/publisher/<valor>. É quem registrou o arquivo, que nem sempre é a organização que reporta a atividade (reporting_org_id)",
            "Resolves to https://www.iatiregistry.org/publisher/<value>. This is who registered the file, which is not always the organisation reporting the activity (reporting_org_id)",
            "Resuelve a https://www.iatiregistry.org/publisher/<valor>. Es quien registró el archivo, que no siempre es la organización que reporta la actividad (reporting_org_id)",
        ),
    )


def licence_id():
    return c(
        "licence_id",
        "",
        "STRING",
        "Licença sob a qual a organização publicadora liberou o conjunto de dados de origem",
        "Licence under which the publishing organisation released the source dataset",
        "Licencia bajo la cual la organización publicadora liberó el conjunto de datos de origen",
        obs=(
            "Não existe no IATI Tables: é unido pela Data Basis a partir do índice do Bulk Data Service (short_name). Os dados IATI são licenciados por publicador, não por corpus; os 16 conjuntos não comerciais (cc-nc, other-nc) foram removidos. Vazio ou 'notspecified' significa que o publicador não declarou licença",
            "Absent from IATI Tables: joined by Data Basis from the Bulk Data Service index (short_name). IATI data is licensed per publisher, not per corpus; the 16 non-commercial datasets (cc-nc, other-nc) were dropped. Blank or 'notspecified' means the publisher declared no licence",
            "No existe en IATI Tables: es unido por Data Basis a partir del índice del Bulk Data Service (short_name). Los datos IATI se licencian por publicador, no por corpus; los 16 conjuntos no comerciales (cc-nc, other-nc) fueron eliminados. Vacío o 'notspecified' significa que el publicador no declaró licencia",
        ),
    )


def activity_id(raw="_link_activity", primary=False):
    return c(
        "activity_id",
        raw,
        "STRING",
        (
            "Identificador único da atividade nesta base"
            if primary
            else "Identificador da atividade a que a linha pertence"
        ),
        (
            "Unique identifier of the activity in this database"
            if primary
            else "Identifier of the activity the row belongs to"
        ),
        (
            "Identificador único de la actividad en esta base"
            if primary
            else "Identificador de la actividad a la que pertenece la fila"
        ),
        directory=(
            "" if primary else "world_iati_activities.activity:activity_id"
        ),
        obs=(
            "Chave sintética atribuída pelo IATI Tables (_link). Não é estável entre execuções: use iati_identifier para acompanhar uma atividade ao longo do tempo",
            "Synthetic key assigned by IATI Tables (_link). It is not stable across runs: use iati_identifier to follow an activity over time",
            "Clave sintética asignada por IATI Tables (_link). No es estable entre ejecuciones: use iati_identifier para seguir una actividad a lo largo del tiempo",
        ),
    )


def iati_identifier():
    return c(
        "iati_identifier",
        "iatiidentifier",
        "STRING",
        "Identificador global e único da atividade, atribuído pela organização que a reporta",
        "Globally unique identifier of the activity, assigned by the organisation reporting it",
        "Identificador global y único de la actividad, asignado por la organización que la reporta",
        obs=(
            "Chave estável entre execuções, ao contrário de activity_id",
            "Stable across runs, unlike activity_id",
            "Clave estable entre ejecuciones, a diferencia de activity_id",
        ),
    )


def reporting_org_id():
    return c(
        "reporting_org_id",
        "reportingorg_ref",
        "STRING",
        "Identificador IATI da organização que reporta a atividade",
        "IATI identifier of the organisation reporting the activity",
        "Identificador IATI de la organización que reporta la actividad",
    )


def _code_pair(prefix_clean, prefix_raw, pt, en, es, codelist):
    """A codelist code and the label IATI Tables resolved for it."""
    obs = (
        f"Valor da lista de códigos {codelist} do padrão IATI",
        f"Value from the IATI standard's {codelist} codelist",
        f"Valor de la lista de códigos {codelist} del estándar IATI",
    )
    return [
        c(
            f"{prefix_clean}_code",
            f"{prefix_raw}_code",
            "STRING",
            f"Código {pt}",
            f"Code {en}",
            f"Código {es}",
            obs=obs,
        ),
        c(
            f"{prefix_clean}_name",
            f"{prefix_raw}_codename",
            "STRING",
            f"Nome {pt}",
            f"Name {en}",
            f"Nombre {es}",
            obs=(
                "Rótulo resolvido pelo IATI Tables a partir do código irmão",
                "Label resolved by IATI Tables from the sibling code",
                "Etiqueta resuelta por IATI Tables a partir del código hermano",
            ),
        ),
    ]


def value_cols(
    raw_value="value",
    raw_currency="value_currency",
    raw_currency_name="value_currencyname",
    raw_valuedate="value_valuedate",
    raw_usd="value_usd",
    what_pt="da transação",
    what_en="of the transaction",
    what_es="de la transacción",
):
    cols = [
        c(
            "value",
            raw_value,
            "FLOAT64",
            f"Valor monetário {what_pt}, na moeda declarada em currency_code",
            f"Monetary value {what_en}, in the currency declared in currency_code",
            f"Valor monetario {what_es}, en la moneda declarada en currency_code",
            unit="currency",
            obs=(
                "Quando currency_code é nulo, vale a moeda padrão da atividade (activity.default_currency_code)",
                "When currency_code is null, the activity's default currency applies (activity.default_currency_code)",
                "Cuando currency_code es nulo, se aplica la moneda por defecto de la actividad (activity.default_currency_code)",
            ),
        ),
        c(
            "currency_code",
            raw_currency,
            "STRING",
            "Código ISO 4217 da moeda do valor",
            "ISO 4217 code of the value's currency",
            "Código ISO 4217 de la moneda del valor",
        ),
        c(
            "currency_name",
            raw_currency_name,
            "STRING",
            "Nome da moeda do valor",
            "Name of the value's currency",
            "Nombre de la moneda del valor",
        ),
        c(
            "value_date",
            raw_valuedate,
            "DATE",
            "Data à qual o valor se refere, usada para a conversão cambial",
            "Date the value refers to, used for the currency conversion",
            "Fecha a la que se refiere el valor, usada para la conversión cambiaria",
        ),
    ]
    if raw_usd:
        cols.append(
            c(
                "value_usd",
                raw_usd,
                "FLOAT64",
                "Valor convertido para dólares dos Estados Unidos",
                "Value converted to United States dollars",
                "Valor convertido a dólares de los Estados Unidos",
                unit="USD",
                obs=(
                    "Conversão feita pelo IATI Tables usando as taxas do FMI publicadas pelo Code for IATI, na data de value_date",
                    "Converted by IATI Tables using the IMF rates published by Code for IATI, at value_date",
                    "Conversión hecha por IATI Tables usando las tasas del FMI publicadas por Code for IATI, en la fecha de value_date",
                ),
            )
        )
    return cols


def narrative(name, raw, pt, en, es):
    return c(
        name,
        raw,
        "STRING",
        pt,
        en,
        es,
        obs=(
            "O IATI permite o mesmo texto em vários idiomas; o IATI Tables achata todos em uma única cadeia, no formato 'texto, FR: texte, ES: texto'",
            "IATI allows the same text in several languages; IATI Tables flattens them all into a single string, formatted 'text, FR: texte, ES: texto'",
            "IATI permite el mismo texto en varios idiomas; IATI Tables aplana todos en una única cadena, con el formato 'texto, FR: texte, ES: texto'",
        ),
    )


def percentage(name, raw, pt, en, es):
    return c(
        name,
        raw,
        "FLOAT64",
        pt,
        en,
        es,
        unit="percent",
        obs=(
            "Declarado pelo publicador; a soma dentro de uma atividade nem sempre fecha em 100",
            "Declared by the publisher; the sum within an activity does not always add to 100",
            "Declarado por el publicador; la suma dentro de una actividad no siempre suma 100",
        ),
    )


def vocabulary_cols(what_pt, what_en, what_es):
    return [
        c(
            "vocabulary_code",
            "vocabulary",
            "STRING",
            f"Código do vocabulário {what_pt}",
            f"Code of the {what_en} vocabulary",
            f"Código del vocabulario {what_es}",
        ),
        c(
            "vocabulary_name",
            "vocabularyname",
            "STRING",
            f"Nome do vocabulário {what_pt}",
            f"Name of the {what_en} vocabulary",
            f"Nombre del vocabulario {what_es}",
        ),
        c(
            "vocabulary_uri",
            "vocabularyuri",
            "STRING",
            "Endereço da definição do vocabulário, quando ele não é um dos padronizados pelo IATI",
            "Address of the vocabulary definition, when it is not one of those standardised by IATI",
            "Dirección de la definición del vocabulario, cuando no es uno de los estandarizados por IATI",
        ),
    ]


def pair(clean, raw_code, raw_name, pt, en, es, codelist):
    """A code/label pair whose raw names do not follow the _code/_codename shape."""
    obs = (
        f"Valor da lista de códigos {codelist} do padrão IATI",
        f"Value from the IATI standard's {codelist} codelist",
        f"Valor de la lista de códigos {codelist} del estándar IATI",
    )
    return [
        c(
            f"{clean}_code",
            raw_code,
            "STRING",
            f"Código {pt}",
            f"Code {en}",
            f"Código {es}",
            obs=obs,
        ),
        c(
            f"{clean}_name",
            raw_name,
            "STRING",
            f"Nome {pt}",
            f"Name {en}",
            f"Nombre {es}",
            obs=(
                "Rótulo resolvido pelo IATI Tables a partir do código irmão",
                "Label resolved by IATI Tables from the sibling code",
                "Etiqueta resuelta por IATI Tables a partir del código hermano",
            ),
        ),
    ]


def org_cols(side, raw_prefix, raw_activity_id, pt_role, en_role, es_role):
    """The provider/receiver organisation block shared by transaction and
    planned_disbursement."""
    return [
        c(
            f"{side}_org_id",
            f"{raw_prefix}_ref",
            "STRING",
            f"Identificador IATI da organização {pt_role}",
            f"IATI identifier of the {en_role} organisation",
            f"Identificador IATI de la organización {es_role}",
        ),
        c(
            f"{side}_activity_id",
            raw_activity_id,
            "STRING",
            f"Identificador IATI da atividade correspondente na base da organização {pt_role}",
            f"IATI identifier of the corresponding activity in the {en_role} organisation's own data",
            f"Identificador IATI de la actividad correspondiente en la base de la organización {es_role}",
            obs=(
                "É o elo de rastreabilidade entre publicadores: permite seguir o mesmo recurso da atividade do financiador até a do executor",
                "This is the traceability link between publishers: it lets the same resource be followed from the funder's activity to the implementer's",
                "Es el enlace de trazabilidad entre publicadores: permite seguir el mismo recurso desde la actividad del financiador hasta la del ejecutor",
            ),
        ),
        *pair(
            f"{side}_org_type",
            f"{raw_prefix}_type",
            f"{raw_prefix}_typename",
            f"do tipo da organização {pt_role}",
            f"of the {en_role} organisation's type",
            f"del tipo de la organización {es_role}",
            "OrganisationType",
        ),
        narrative(
            f"{side}_org_name",
            f"{raw_prefix}_narrative",
            f"Nome da organização {pt_role}",
            f"Name of the {en_role} organisation",
            f"Nombre de la organización {es_role}",
        ),
    ]


def child_head(entity, raw_link="_link"):
    """The identifier block every activity-child table opens with."""
    return [
        registry_dataset_id(),
        publisher_id(),
        c(
            f"{entity}_id",
            raw_link,
            "STRING",
            "Identificador único da linha nesta tabela",
            "Unique identifier of the row in this table",
            "Identificador único de la fila en esta tabla",
            obs=(
                "Chave sintética atribuída pelo IATI Tables (_link); não é estável entre execuções",
                "Synthetic key assigned by IATI Tables (_link); it is not stable across runs",
                "Clave sintética asignada por IATI Tables (_link); no es estable entre ejecuciones",
            ),
        ),
        activity_id(),
        iati_identifier(),
        reporting_org_id(),
        licence_id(),
    ]


# --- Table specs ----------------------------------------------------------
# Column order follows .claude/rules/data-basis-style.md: partition columns
# first, then identifiers, then descriptive columns.

SPEC = {}

SPEC["registry_dataset"] = [
    c(
        "registry_dataset_id",
        "short_name",
        "STRING",
        "Identificador do conjunto de dados no Registro IATI",
        "Identifier of the dataset in the IATI Registry",
        "Identificador del conjunto de datos en el Registro IATI",
    ),
    c(
        "publisher_id",
        "reporting_org_short_name",
        "STRING",
        "Identificador da organização publicadora no Registro IATI",
        "Identifier of the publishing organisation in the IATI Registry",
        "Identificador de la organización publicadora en el Registro IATI",
    ),
    licence_id(),
    c(
        "source_url",
        "source_url",
        "STRING",
        "Endereço de onde o publicador serve o arquivo XML original",
        "Address where the publisher serves the original XML file",
        "Dirección desde donde el publicador sirve el archivo XML original",
    ),
    c(
        "cached_xml_url",
        "last_known_good_dataset.cached_dataset_url_xml",
        "STRING",
        "Endereço da última cópia íntegra do arquivo mantida pelo Bulk Data Service",
        "Address of the last known good copy of the file held by the Bulk Data Service",
        "Dirección de la última copia íntegra del archivo mantenida por el Bulk Data Service",
    ),
    c(
        "is_downloaded",
        "",
        "BOOLEAN",
        "Indica se o Bulk Data Service conseguiu baixar o arquivo do publicador",
        "Whether the Bulk Data Service managed to download the file from the publisher",
        "Indica si el Bulk Data Service logró descargar el archivo del publicador",
        obs=(
            "Derivado pela Data Basis: verdadeiro quando cached_xml_url está preenchido. Falso em 748 dos 13.901 conjuntos, cujo endereço de origem não respondeu; esses conjuntos não têm linhas nas demais tabelas",
            "Derived by Data Basis: true when cached_xml_url is filled. False for 748 of the 13,901 datasets, whose source address did not respond; those datasets have no rows in the other tables",
            "Derivado por Data Basis: verdadero cuando cached_xml_url está lleno. Falso en 748 de los 13.901 conjuntos, cuya dirección de origen no respondió; esos conjuntos no tienen filas en las demás tablas",
        ),
    ),
    c(
        "last_update_check",
        "last_update_check",
        "DATETIME",
        "Momento em que o Bulk Data Service verificou o arquivo pela última vez",
        "Time the Bulk Data Service last checked the file",
        "Momento en que el Bulk Data Service verificó el archivo por última vez",
    ),
]

SPEC["activity"] = [
    registry_dataset_id(),
    publisher_id(),
    activity_id(raw="_link_activity", primary=True),
    iati_identifier(),
    reporting_org_id(),
    licence_id(),
    narrative(
        "title",
        "title_narrative",
        "Título da atividade",
        "Title of the activity",
        "Título de la actividad",
    ),
    narrative(
        "reporting_org_name",
        "reportingorg_narrative",
        "Nome da organização que reporta a atividade",
        "Name of the organisation reporting the activity",
        "Nombre de la organización que reporta la actividad",
    ),
    *pair(
        "reporting_org_type",
        "reportingorg_type",
        "reportingorg_typename",
        "do tipo da organização que reporta a atividade",
        "of the type of the organisation reporting the activity",
        "del tipo de la organización que reporta la actividad",
        "OrganisationType",
    ),
    c(
        "reporting_org_is_secondary",
        "reportingorg_secondaryreporter",
        "BOOLEAN",
        "Indica que a organização está reproduzindo dados publicados por outra, e não reportando os próprios",
        "Whether the organisation is republishing data published by another, rather than reporting its own",
        "Indica que la organización está reproduciendo datos publicados por otra, y no reportando los propios",
    ),
    *_code_pair(
        "activity_status",
        "activitystatus",
        "da situação da atividade",
        "of the activity's status",
        "de la situación de la actividad",
        "ActivityStatus",
    ),
    *_code_pair(
        "activity_scope",
        "activityscope",
        "do alcance geográfico da atividade",
        "of the activity's geographic scope",
        "del alcance geográfico de la actividad",
        "ActivityScope",
    ),
    c(
        "planned_start_date",
        "plannedstart",
        "DATE",
        "Data planejada de início da atividade",
        "Planned start date of the activity",
        "Fecha planificada de inicio de la actividad",
    ),
    c(
        "actual_start_date",
        "actualstart",
        "DATE",
        "Data efetiva de início da atividade",
        "Actual start date of the activity",
        "Fecha efectiva de inicio de la actividad",
    ),
    c(
        "planned_end_date",
        "plannedend",
        "DATE",
        "Data planejada de término da atividade",
        "Planned end date of the activity",
        "Fecha planificada de término de la actividad",
    ),
    c(
        "actual_end_date",
        "actualend",
        "DATE",
        "Data efetiva de término da atividade",
        "Actual end date of the activity",
        "Fecha efectiva de término de la actividad",
    ),
    *_code_pair(
        "collaboration_type",
        "collaborationtype",
        "do tipo de colaboração, na classificação do CAD/OCDE",
        "of the collaboration type, in the OECD DAC classification",
        "del tipo de colaboración, en la clasificación del CAD/OCDE",
        "CollaborationType",
    ),
    *_code_pair(
        "default_flow_type",
        "defaultflowtype",
        "padrão do tipo de fluxo financeiro da atividade",
        "default for the activity's financial flow type",
        "por defecto del tipo de flujo financiero de la actividad",
        "FlowType",
    ),
    *_code_pair(
        "default_finance_type",
        "defaultfinancetype",
        "padrão do instrumento financeiro da atividade",
        "default for the activity's financial instrument",
        "por defecto del instrumento financiero de la actividad",
        "FinanceType",
    ),
    *_code_pair(
        "default_tied_status",
        "defaulttiedstatus",
        "padrão da condicionalidade de aquisição da atividade",
        "default for the activity's procurement tying status",
        "por defecto de la condicionalidad de adquisición de la actividad",
        "TiedStatus",
    ),
    c(
        "default_currency_code",
        "defaultcurrency",
        "STRING",
        "Código ISO 4217 da moeda padrão da atividade",
        "ISO 4217 code of the activity's default currency",
        "Código ISO 4217 de la moneda por defecto de la actividad",
    ),
    c(
        "default_currency_name",
        "defaultcurrencyname",
        "STRING",
        "Nome da moeda padrão da atividade",
        "Name of the activity's default currency",
        "Nombre de la moneda por defecto de la actividad",
    ),
    percentage(
        "capital_spend_percentage",
        "capitalspend_percentage",
        "Parcela do orçamento da atividade destinada a despesa de capital",
        "Share of the activity's budget going to capital expenditure",
        "Parte del presupuesto de la actividad destinada a gasto de capital",
    ),
    c(
        "has_conditions_attached",
        "conditions_attached",
        "BOOLEAN",
        "Indica que a atividade tem condições associadas ao financiamento",
        "Whether the activity has conditions attached to its funding",
        "Indica que la actividad tiene condiciones asociadas al financiamiento",
    ),
    c(
        "is_humanitarian",
        "humanitarian",
        "BOOLEAN",
        "Indica que a atividade é classificada como humanitária pelo publicador",
        "Whether the activity is classified as humanitarian by the publisher",
        "Indica que la actividad es clasificada como humanitaria por el publicador",
    ),
    c(
        "hierarchy_level",
        "hierarchy",
        "STRING",
        "Nível da atividade na hierarquia declarada pelo publicador, de programa a subprojeto",
        "Level of the activity in the hierarchy declared by the publisher, from programme to sub-project",
        "Nivel de la actividad en la jerarquía declarada por el publicador, de programa a subproyecto",
        obs=(
            "Código de nível, não uma quantidade: 1 é o nível mais alto. Publicadores que não usam hierarquia deixam a coluna vazia",
            "A level code, not a quantity: 1 is the highest level. Publishers that do not use hierarchy leave the column empty",
            "Código de nivel, no una cantidad: 1 es el nivel más alto. Publicadores que no usan jerarquía dejan la columna vacía",
        ),
    ),
    c(
        "budget_not_provided_code",
        "budgetnotprovided",
        "STRING",
        "Código da razão pela qual a atividade não declara orçamento",
        "Code of the reason the activity declares no budget",
        "Código de la razón por la cual la actividad no declara presupuesto",
    ),
    c(
        "budget_not_provided_name",
        "budgetnotprovidedname",
        "STRING",
        "Nome da razão pela qual a atividade não declara orçamento",
        "Name of the reason the activity declares no budget",
        "Nombre de la razón por la cual la actividad no declara presupuesto",
    ),
    c(
        "crs_channel_code",
        "crsadd_channelcode",
        "STRING",
        "Código do canal de entrega na classificação CRS do CAD/OCDE",
        "Code of the delivery channel in the OECD DAC CRS classification",
        "Código del canal de entrega en la clasificación CRS del CAD/OCDE",
    ),
    c(
        "country_budget_items_vocabulary_code",
        "countrybudgetitems_vocabulary",
        "STRING",
        "Código do vocabulário usado para alinhar a atividade às rubricas orçamentárias do país receptor",
        "Code of the vocabulary used to align the activity to the recipient country's budget lines",
        "Código del vocabulario usado para alinear la actividad a las partidas presupuestarias del país receptor",
    ),
    c(
        "country_budget_items_vocabulary_name",
        "countrybudgetitems_vocabularyname",
        "STRING",
        "Nome do vocabulário usado para alinhar a atividade às rubricas orçamentárias do país receptor",
        "Name of the vocabulary used to align the activity to the recipient country's budget lines",
        "Nombre del vocabulario usado para alinear la actividad a las partidas presupuestarias del país receptor",
    ),
    c(
        "language_code",
        "lang",
        "STRING",
        "Código do idioma padrão dos textos da atividade",
        "Code of the default language of the activity's texts",
        "Código del idioma por defecto de los textos de la actividad",
    ),
    c(
        "linked_data_uri",
        "linkeddatauri",
        "STRING",
        "Endereço de dados ligados publicado pela organização para esta atividade",
        "Linked data address published by the organisation for this activity",
        "Dirección de datos enlazados publicada por la organización para esta actividad",
    ),
    c(
        "last_updated_datetime",
        "lastupdateddatetime",
        "DATETIME",
        "Momento da última alteração da atividade declarado pelo publicador",
        "Time of the activity's last change as declared by the publisher",
        "Momento de la última modificación de la actividad declarado por el publicador",
        obs=(
            "Declarado, não medido: cabe ao publicador mantê-lo correto",
            "Declared, not measured: it is up to the publisher to keep it correct",
            "Declarado, no medido: corresponde al publicador mantenerlo correcto",
        ),
    ),
]


SPEC["transaction"] = [
    year_col(),
    *child_head("transaction"),
    c(
        "transaction_ref",
        "ref",
        "STRING",
        "Referência da transação atribuída pela organização que a reporta",
        "Reference of the transaction assigned by the organisation reporting it",
        "Referencia de la transacción asignada por la organización que la reporta",
    ),
    *_code_pair(
        "transaction_type",
        "transactiontype",
        "do tipo de transação, que distingue compromisso, desembolso, gasto e reembolso",
        "of the transaction type, which distinguishes commitment, disbursement, expenditure and refund",
        "del tipo de transacción, que distingue compromiso, desembolso, gasto y reembolso",
        "TransactionType",
    ),
    c(
        "transaction_date",
        "transactiondate_isodate",
        "DATE",
        "Data em que a transação ocorreu",
        "Date the transaction took place",
        "Fecha en que ocurrió la transacción",
        obs=(
            "Origem da coluna de particionamento year",
            "Source of the year partition column",
            "Origen de la columna de particionamiento year",
        ),
    ),
    *value_cols(),
    narrative(
        "description",
        "description_narrative",
        "Descrição da transação",
        "Description of the transaction",
        "Descripción de la transacción",
    ),
    *org_cols(
        "provider",
        "providerorg",
        "providerorg_provideractivityid",
        "de onde o recurso saiu",
        "the resource came from",
        "de donde salió el recurso",
    ),
    *org_cols(
        "receiver",
        "receiverorg",
        "receiverorg_receiveractivityid",
        "que recebeu o recurso",
        "that received the resource",
        "que recibió el recurso",
    ),
    *_code_pair(
        "disbursement_channel",
        "disbursementchannel",
        "do canal pelo qual o recurso foi desembolsado",
        "of the channel the resource was disbursed through",
        "del canal por el cual se desembolsó el recurso",
        "DisbursementChannel",
    ),
    *_code_pair(
        "sector",
        "sector",
        "do setor principal da transação",
        "of the transaction's main sector",
        "del sector principal de la transacción",
        "Sector",
    ),
    *_code_pair(
        "recipient_country",
        "recipientcountry",
        "do país receptor da transação",
        "of the transaction's recipient country",
        "del país receptor de la transacción",
        "Country",
    ),
    *_code_pair(
        "recipient_region",
        "recipientregion",
        "da região receptora da transação",
        "of the transaction's recipient region",
        "de la región receptora de la transacción",
        "Region",
    ),
    c(
        "recipient_region_vocabulary_code",
        "recipientregion_vocabulary",
        "STRING",
        "Código do vocabulário de regiões usado",
        "Code of the region vocabulary used",
        "Código del vocabulario de regiones usado",
    ),
    c(
        "recipient_region_vocabulary_name",
        "recipientregion_vocabularyname",
        "STRING",
        "Nome do vocabulário de regiões usado",
        "Name of the region vocabulary used",
        "Nombre del vocabulario de regiones usado",
    ),
    *_code_pair(
        "flow_type",
        "flowtype",
        "do tipo de fluxo financeiro, que distingue ajuda oficial de outros fluxos",
        "of the financial flow type, which distinguishes official aid from other flows",
        "del tipo de flujo financiero, que distingue ayuda oficial de otros flujos",
        "FlowType",
    ),
    *_code_pair(
        "finance_type",
        "financetype",
        "do instrumento financeiro, que distingue doação de empréstimo",
        "of the financial instrument, which distinguishes grant from loan",
        "del instrumento financiero, que distingue donación de préstamo",
        "FinanceType",
    ),
    *_code_pair(
        "tied_status",
        "tiedstatus",
        "da condicionalidade de aquisição do recurso",
        "of the resource's procurement tying status",
        "de la condicionalidad de adquisición del recurso",
        "TiedStatus",
    ),
    c(
        "is_humanitarian",
        "humanitarian",
        "BOOLEAN",
        "Indica que a transação é classificada como humanitária pelo publicador",
        "Whether the transaction is classified as humanitarian by the publisher",
        "Indica que la transacción es clasificada como humanitaria por el publicador",
    ),
]

SPEC["transaction_breakdown"] = [
    year_col(),
    c(
        "registry_dataset_id",
        "transaction.dataset",
        "STRING",
        "Identificador do conjunto de dados no Registro IATI de onde a linha veio",
        "Identifier of the dataset in the IATI Registry the row came from",
        "Identificador del conjunto de datos en el Registro IATI del que proviene la fila",
        directory="world_iati_activities.registry_dataset:registry_dataset_id",
        obs=(
            "Esta tabela não traz a coluna dataset na fonte; ela é obtida pela Data Basis unindo transaction_id à tabela transaction",
            "The source table carries no dataset column; Data Basis obtains it by joining transaction_id to the transaction table",
            "La tabla de origen no trae la columna dataset; Data Basis la obtiene uniendo transaction_id a la tabla transaction",
        ),
    ),
    publisher_id(),
    c(
        "transaction_id",
        "_link_transaction",
        "STRING",
        "Identificador da transação que esta linha decompõe",
        "Identifier of the transaction this row breaks down",
        "Identificador de la transacción que esta fila descompone",
        directory="world_iati_activities.transaction:transaction_id",
    ),
    activity_id(),
    iati_identifier(),
    reporting_org_id(),
    licence_id(),
    *_code_pair(
        "transaction_type",
        "transactiontype",
        "do tipo de transação",
        "of the transaction type",
        "del tipo de transacción",
        "TransactionType",
    ),
    c(
        "transaction_date",
        "transactiondate_isodate",
        "DATE",
        "Data em que a transação ocorreu",
        "Date the transaction took place",
        "Fecha en que ocurrió la transacción",
        obs=(
            "Origem da coluna de particionamento year",
            "Source of the year partition column",
            "Origen de la columna de particionamiento year",
        ),
    ),
    *_code_pair(
        "sector",
        "sector",
        "do setor a que esta parcela foi atribuída",
        "of the sector this share was attributed to",
        "del sector al que se atribuyó esta parte",
        "Sector",
    ),
    *_code_pair(
        "recipient_country",
        "recipientcountry",
        "do país a que esta parcela foi atribuída",
        "of the country this share was attributed to",
        "del país al que se atribuyó esta parte",
        "Country",
    ),
    *_code_pair(
        "recipient_region",
        "recipientregion",
        "da região a que esta parcela foi atribuída",
        "of the region this share was attributed to",
        "de la región a la que se atribuyó esta parte",
        "Region",
    ),
    *[
        col
        for col in value_cols()
        if col["name"] != "currency_name"  # absent from this source table
    ],
    percentage(
        "percentage_used",
        "percentage_used",
        "Parcela do valor da transação atribuída a esta combinação de setor e destino",
        "Share of the transaction's value attributed to this combination of sector and destination",
        "Parte del valor de la transacción atribuida a esta combinación de sector y destino",
    ),
]

SPEC["transaction_sector"] = [
    *child_head("transaction_sector"),
    c(
        "transaction_id",
        "_link_transaction",
        "STRING",
        "Identificador da transação a que o setor foi atribuído",
        "Identifier of the transaction the sector was attributed to",
        "Identificador de la transacción a la que se atribuyó el sector",
        directory="world_iati_activities.transaction:transaction_id",
    ),
    *vocabulary_cols("de setores usado", "sector", "de sectores usado"),
    *pair(
        "sector",
        "code",
        "codename",
        "do setor atribuído à transação",
        "of the sector attributed to the transaction",
        "del sector atribuido a la transacción",
        "Sector",
    ),
    narrative(
        "sector_narrative",
        "narrative",
        "Nome do setor conforme escrito pelo publicador",
        "Name of the sector as written by the publisher",
        "Nombre del sector según lo escrito por el publicador",
    ),
]

SPEC["budget"] = [
    year_col(),
    *child_head("budget"),
    *pair(
        "budget_type",
        "type",
        "typename",
        "do tipo de orçamento, que distingue original de revisado",
        "of the budget type, which distinguishes original from revised",
        "del tipo de presupuesto, que distingue original de revisado",
        "BudgetType",
    ),
    *pair(
        "budget_status",
        "status",
        "statusname",
        "da situação do orçamento, que distingue indicativo de comprometido",
        "of the budget status, which distinguishes indicative from committed",
        "de la situación del presupuesto, que distingue indicativo de comprometido",
        "BudgetStatus",
    ),
    c(
        "period_start_date",
        "periodstart_isodate",
        "DATE",
        "Data de início do período orçamentário",
        "Start date of the budget period",
        "Fecha de inicio del período presupuestario",
        obs=(
            "Origem da coluna de particionamento year",
            "Source of the year partition column",
            "Origen de la columna de particionamiento year",
        ),
    ),
    c(
        "period_end_date",
        "periodend_isodate",
        "DATE",
        "Data de término do período orçamentário",
        "End date of the budget period",
        "Fecha de término del período presupuestario",
    ),
    *value_cols(
        raw_usd="",
        what_pt="do orçamento",
        what_en="of the budget",
        what_es="del presupuesto",
    ),
]

SPEC["planned_disbursement"] = [
    year_col(),
    *child_head("planned_disbursement"),
    *pair(
        "planned_disbursement_type",
        "type",
        "typename",
        "do tipo de desembolso planejado, que distingue original de revisado",
        "of the planned disbursement type, which distinguishes original from revised",
        "del tipo de desembolso planificado, que distingue original de revisado",
        "BudgetType",
    ),
    c(
        "period_start_date",
        "periodstart_isodate",
        "DATE",
        "Data de início do período do desembolso planejado",
        "Start date of the planned disbursement period",
        "Fecha de inicio del período del desembolso planificado",
        obs=(
            "Origem da coluna de particionamento year",
            "Source of the year partition column",
            "Origen de la columna de particionamiento year",
        ),
    ),
    c(
        "period_end_date",
        "periodend_isodate",
        "DATE",
        "Data de término do período do desembolso planejado",
        "End date of the planned disbursement period",
        "Fecha de término del período del desembolso planificado",
    ),
    *value_cols(
        raw_usd="",
        what_pt="do desembolso planejado",
        what_en="of the planned disbursement",
        what_es="del desembolso planificado",
    ),
    *org_cols(
        "provider",
        "providerorg",
        "providerorg_provideractivityid",
        "de onde o recurso deve sair",
        "the resource is due to come from",
        "de donde debe salir el recurso",
    ),
    *org_cols(
        "receiver",
        "receiverorg",
        "receiverorg_receiveractivityid",
        "que deve receber o recurso",
        "due to receive the resource",
        "que debe recibir el recurso",
    ),
]

SPEC["sector"] = [
    *child_head("activity_sector"),
    *vocabulary_cols("de setores usado", "sector", "de sectores usado"),
    *pair(
        "sector",
        "code",
        "codename",
        "do setor atribuído à atividade",
        "of the sector attributed to the activity",
        "del sector atribuido a la actividad",
        "Sector",
    ),
    percentage(
        "percentage",
        "percentage",
        "Parcela da atividade atribuída a este setor",
        "Share of the activity attributed to this sector",
        "Parte de la actividad atribuida a este sector",
    ),
    narrative(
        "sector_narrative",
        "narrative",
        "Nome do setor conforme escrito pelo publicador",
        "Name of the sector as written by the publisher",
        "Nombre del sector según lo escrito por el publicador",
    ),
]

SPEC["recipient_country"] = [
    *child_head("activity_recipient_country"),
    *pair(
        "recipient_country",
        "code",
        "codename",
        "do país receptor da atividade",
        "of the activity's recipient country",
        "del país receptor de la actividad",
        "Country",
    ),
    percentage(
        "percentage",
        "percentage",
        "Parcela da atividade atribuída a este país",
        "Share of the activity attributed to this country",
        "Parte de la actividad atribuida a este país",
    ),
    narrative(
        "recipient_country_narrative",
        "narrative",
        "Nome do país conforme escrito pelo publicador",
        "Name of the country as written by the publisher",
        "Nombre del país según lo escrito por el publicador",
    ),
]

SPEC["recipient_region"] = [
    *child_head("activity_recipient_region"),
    *pair(
        "recipient_region",
        "code",
        "codename",
        "da região receptora da atividade",
        "of the activity's recipient region",
        "de la región receptora de la actividad",
        "Region",
    ),
    *vocabulary_cols("de regiões usado", "region", "de regiones usado"),
    percentage(
        "percentage",
        "percentage",
        "Parcela da atividade atribuída a esta região",
        "Share of the activity attributed to this region",
        "Parte de la actividad atribuida a esta región",
    ),
    narrative(
        "recipient_region_narrative",
        "narrative",
        "Nome da região conforme escrito pelo publicador",
        "Name of the region as written by the publisher",
        "Nombre de la región según lo escrito por el publicador",
    ),
]

SPEC["participating_org"] = [
    *child_head("participating_org"),
    c(
        "org_id",
        "ref",
        "STRING",
        "Identificador IATI da organização participante",
        "IATI identifier of the participating organisation",
        "Identificador IATI de la organización participante",
    ),
    c(
        "org_activity_id",
        "activityid",
        "STRING",
        "Identificador IATI da atividade correspondente na base da organização participante",
        "IATI identifier of the corresponding activity in the participating organisation's own data",
        "Identificador IATI de la actividad correspondiente en la base de la organización participante",
    ),
    *pair(
        "role",
        "role",
        "rolename",
        "do papel da organização, que distingue financiador, responsável, extensor e executor",
        "of the organisation's role, which distinguishes funding, accountable, extending and implementing",
        "del papel de la organización, que distingue financiador, responsable, extensor y ejecutor",
        "OrganisationRole",
    ),
    *pair(
        "org_type",
        "type",
        "typename",
        "do tipo da organização participante",
        "of the participating organisation's type",
        "del tipo de la organización participante",
        "OrganisationType",
    ),
    *pair(
        "crs_channel",
        "crschannelcode",
        "crschannelcodename",
        "do canal de entrega na classificação CRS do CAD/OCDE",
        "of the delivery channel in the OECD DAC CRS classification",
        "del canal de entrega en la clasificación CRS del CAD/OCDE",
        "CRSChannelCode",
    ),
    narrative(
        "org_name",
        "narrative",
        "Nome da organização participante",
        "Name of the participating organisation",
        "Nombre de la organización participante",
    ),
]

SPEC["related_activity"] = [
    *child_head("related_activity"),
    c(
        "related_iati_identifier",
        "ref",
        "STRING",
        "Identificador IATI da atividade relacionada",
        "IATI identifier of the related activity",
        "Identificador IATI de la actividad relacionada",
        obs=(
            "A atividade referenciada pode não estar nesta base: o publicador pode apontar para uma atividade que ninguém publicou no IATI",
            "The referenced activity may not be in this database: a publisher can point at an activity nobody published to IATI",
            "La actividad referenciada puede no estar en esta base: el publicador puede apuntar a una actividad que nadie publicó en IATI",
        ),
    ),
    *pair(
        "relation_type",
        "type",
        "typename",
        "do tipo de relação, que distingue atividade-mãe, filha, irmã e cofinanciada",
        "of the relation type, which distinguishes parent, child, sibling and co-funded activity",
        "del tipo de relación, que distingue actividad madre, hija, hermana y cofinanciada",
        "RelatedActivityType",
    ),
]

SPEC["policy_marker"] = [
    *child_head("policy_marker"),
    *vocabulary_cols(
        "de marcadores de política usado",
        "policy marker",
        "de marcadores de política usado",
    ),
    *pair(
        "policy_marker",
        "code",
        "codename",
        "do marcador de política, como igualdade de gênero ou mitigação climática",
        "of the policy marker, such as gender equality or climate mitigation",
        "del marcador de política, como igualdad de género o mitigación climática",
        "PolicyMarker",
    ),
    *pair(
        "significance",
        "significance",
        "significancename",
        "do grau em que o marcador é objetivo da atividade, de não orientado a principal",
        "of the degree to which the marker is an objective of the activity, from not targeted to principal",
        "del grado en que el marcador es objetivo de la actividad, de no orientado a principal",
        "PolicySignificance",
    ),
    narrative(
        "policy_marker_narrative",
        "narrative",
        "Nome do marcador conforme escrito pelo publicador",
        "Name of the marker as written by the publisher",
        "Nombre del marcador según lo escrito por el publicador",
    ),
]

SPEC["document_link"] = [
    *child_head("document_link"),
    c(
        "url",
        "url",
        "STRING",
        "Endereço do documento",
        "Address of the document",
        "Dirección del documento",
        obs=(
            "Endereço declarado pelo publicador, não verificado pelo IATI nem pela Data Basis",
            "Address declared by the publisher, verified neither by IATI nor by Data Basis",
            "Dirección declarada por el publicador, no verificada por IATI ni por Data Basis",
        ),
    ),
    *pair(
        "format",
        "format",
        "formatname",
        "do formato do arquivo, no padrão de tipos de mídia da IANA",
        "of the file format, in the IANA media type standard",
        "del formato del archivo, en el estándar de tipos de medios de la IANA",
        "FileFormat",
    ),
    narrative(
        "title",
        "title_narrative",
        "Título do documento",
        "Title of the document",
        "Título del documento",
    ),
    narrative(
        "description",
        "description_narrative",
        "Descrição do documento",
        "Description of the document",
        "Descripción del documento",
    ),
    c(
        "document_date",
        "documentdate_isodate",
        "DATE",
        "Data do documento declarada pelo publicador",
        "Date of the document as declared by the publisher",
        "Fecha del documento declarada por el publicador",
    ),
]

SPEC["location"] = [
    *child_head("location"),
    c(
        "location_ref",
        "ref",
        "STRING",
        "Referência do local atribuída pela organização que reporta a atividade",
        "Reference of the location assigned by the organisation reporting the activity",
        "Referencia del lugar asignada por la organización que reporta la actividad",
    ),
    narrative(
        "name",
        "name_narrative",
        "Nome do local",
        "Name of the location",
        "Nombre del lugar",
    ),
    narrative(
        "description",
        "description_narrative",
        "Descrição do local",
        "Description of the location",
        "Descripción del lugar",
    ),
    narrative(
        "activity_description",
        "activitydescription_narrative",
        "Descrição do que a atividade faz neste local",
        "Description of what the activity does at this location",
        "Descripción de lo que la actividad hace en este lugar",
    ),
    c(
        "point_position",
        "point_pos",
        "STRING",
        "Coordenadas do local, como latitude e longitude separadas por espaço",
        "Coordinates of the location, as latitude and longitude separated by a space",
        "Coordenadas del lugar, como latitud y longitud separadas por un espacio",
        obs=(
            "Mantido como texto e não convertido para GEOGRAPHY: o publicador declara o sistema de referência em point_srs_name e nem todos usam WGS 84",
            "Kept as text rather than converted to GEOGRAPHY: the publisher declares the reference system in point_srs_name and not all use WGS 84",
            "Mantenido como texto y no convertido a GEOGRAPHY: el publicador declara el sistema de referencia en point_srs_name y no todos usan WGS 84",
        ),
    ),
    c(
        "point_srs_name",
        "point_srsName",
        "STRING",
        "Sistema de referência espacial em que as coordenadas foram declaradas",
        "Spatial reference system the coordinates were declared in",
        "Sistema de referencia espacial en que se declararon las coordenadas",
    ),
    *_code_pair(
        "location_reach",
        "locationreach",
        "do alcance do local, que distingue onde a atividade ocorre de onde ela produz efeito",
        "of the location's reach, which distinguishes where the activity happens from where it has effect",
        "del alcance del lugar, que distingue dónde ocurre la actividad de dónde produce efecto",
        "GeographicLocationReach",
    ),
    *_code_pair(
        "exactness",
        "exactness",
        "da exatidão das coordenadas, que distingue ponto exato de aproximação",
        "of the coordinates' exactness, which distinguishes an exact point from an approximation",
        "de la exactitud de las coordenadas, que distingue punto exacto de aproximación",
        "GeographicExactness",
    ),
    *_code_pair(
        "location_class",
        "locationclass",
        "da classe do local, que distingue feição administrativa, povoado e estrutura",
        "of the location class, which distinguishes administrative region, populated place and structure",
        "de la clase del lugar, que distingue rasgo administrativo, poblado y estructura",
        "GeographicLocationClass",
    ),
    *_code_pair(
        "feature_designation",
        "featuredesignation",
        "da designação da feição geográfica, no vocabulário do GeoNames",
        "of the geographic feature designation, in the GeoNames vocabulary",
        "de la designación del rasgo geográfico, en el vocabulario de GeoNames",
        "LocationType",
    ),
]

SPEC["result"] = [
    *child_head("result"),
    *pair(
        "result_type",
        "type",
        "typename",
        "do tipo de resultado, que distingue produto, efeito e impacto",
        "of the result type, which distinguishes output, outcome and impact",
        "del tipo de resultado, que distingue producto, efecto e impacto",
        "ResultType",
    ),
    c(
        "is_aggregation_status",
        "aggregationstatus",
        "BOOLEAN",
        "Indica que os valores do resultado podem ser somados a outros da mesma organização",
        "Whether the result's values can be summed with others from the same organisation",
        "Indica que los valores del resultado pueden sumarse a otros de la misma organización",
    ),
    narrative(
        "title",
        "title_narrative",
        "Título do resultado",
        "Title of the result",
        "Título del resultado",
    ),
    narrative(
        "description",
        "description_narrative",
        "Descrição do resultado",
        "Description of the result",
        "Descripción del resultado",
    ),
]

SPEC["result_indicator"] = [
    *child_head("result_indicator"),
    c(
        "result_id",
        "_link_result",
        "STRING",
        "Identificador do resultado a que o indicador pertence",
        "Identifier of the result the indicator belongs to",
        "Identificador del resultado al que pertenece el indicador",
        directory="world_iati_activities.result:result_id",
    ),
    *pair(
        "measure",
        "measure",
        "measurename",
        "da unidade de medida do indicador, que distingue unidade, percentual e qualitativo",
        "of the indicator's measure, which distinguishes unit, percentage and qualitative",
        "de la unidad de medida del indicador, que distingue unidad, porcentaje y cualitativo",
        "IndicatorMeasure",
    ),
    c(
        "is_ascending",
        "ascending",
        "BOOLEAN",
        "Indica que valores maiores do indicador representam progresso",
        "Whether higher values of the indicator represent progress",
        "Indica que valores mayores del indicador representan progreso",
    ),
    c(
        "is_aggregation_status",
        "aggregationstatus",
        "BOOLEAN",
        "Indica que os valores do indicador podem ser somados a outros da mesma organização",
        "Whether the indicator's values can be summed with others from the same organisation",
        "Indica que los valores del indicador pueden sumarse a otros de la misma organización",
    ),
    narrative(
        "title",
        "title_narrative",
        "Título do indicador",
        "Title of the indicator",
        "Título del indicador",
    ),
    narrative(
        "description",
        "description_narrative",
        "Descrição do indicador",
        "Description of the indicator",
        "Descripción del indicador",
    ),
]

SPEC["result_indicator_period"] = [
    year_col(),
    *child_head("result_indicator_period"),
    c(
        "result_id",
        "_link_result",
        "STRING",
        "Identificador do resultado a que o período pertence",
        "Identifier of the result the period belongs to",
        "Identificador del resultado al que pertenece el período",
        directory="world_iati_activities.result:result_id",
    ),
    c(
        "result_indicator_id",
        "_link_result_indicator",
        "STRING",
        "Identificador do indicador a que o período pertence",
        "Identifier of the indicator the period belongs to",
        "Identificador del indicador al que pertenece el período",
        directory="world_iati_activities.result_indicator:result_indicator_id",
    ),
    c(
        "period_start_date",
        "periodstart_isodate",
        "DATE",
        "Data de início do período de medição",
        "Start date of the measurement period",
        "Fecha de inicio del período de medición",
        obs=(
            "Origem da coluna de particionamento year",
            "Source of the year partition column",
            "Origen de la columna de particionamiento year",
        ),
    ),
    c(
        "period_end_date",
        "periodend_isodate",
        "DATE",
        "Data de término do período de medição",
        "End date of the measurement period",
        "Fecha de término del período de medición",
    ),
    c(
        "target_value",
        "target",
        "STRING",
        "Valor que o indicador deveria atingir no período",
        "Value the indicator was meant to reach in the period",
        "Valor que el indicador debería alcanzar en el período",
        obs=(
            "Mantido como texto porque o padrão IATI permite indicadores qualitativos: a coluna contém tanto números quanto descrições",
            "Kept as text because the IATI standard allows qualitative indicators: the column holds both numbers and descriptions",
            "Mantenido como texto porque el estándar IATI permite indicadores cualitativos: la columna contiene tanto números como descripciones",
        ),
    ),
    c(
        "actual_value",
        "actual",
        "STRING",
        "Valor que o indicador efetivamente atingiu no período",
        "Value the indicator actually reached in the period",
        "Valor que el indicador efectivamente alcanzó en el período",
        obs=(
            "Mantido como texto pela mesma razão que target_value",
            "Kept as text for the same reason as target_value",
            "Mantenido como texto por la misma razón que target_value",
        ),
    ),
]

SPEC["organisation"] = [
    c(
        "registry_dataset_id",
        "dataset",
        "STRING",
        "Identificador do conjunto de dados no Registro IATI de onde a linha veio",
        "Identifier of the dataset in the IATI Registry the row came from",
        "Identificador del conjunto de datos en el Registro IATI del que proviene la fila",
        directory="world_iati_activities.registry_dataset:registry_dataset_id",
    ),
    publisher_id(),
    c(
        "organisation_id",
        "_link_organisation",
        "STRING",
        "Identificador único da organização nesta tabela",
        "Unique identifier of the organisation in this table",
        "Identificador único de la organización en esta tabla",
        obs=(
            "Chave sintética atribuída pelo IATI Tables; não é estável entre execuções. Use organisation_identifier",
            "Synthetic key assigned by IATI Tables; it is not stable across runs. Use organisation_identifier",
            "Clave sintética asignada por IATI Tables; no es estable entre ejecuciones. Use organisation_identifier",
        ),
    ),
    c(
        "organisation_identifier",
        "organisationidentifier",
        "STRING",
        "Identificador IATI da organização, estável entre execuções",
        "IATI identifier of the organisation, stable across runs",
        "Identificador IATI de la organización, estable entre ejecuciones",
    ),
    reporting_org_id(),
    licence_id(),
    narrative(
        "name",
        "name_narrative",
        "Nome da organização",
        "Name of the organisation",
        "Nombre de la organización",
    ),
    narrative(
        "reporting_org_name",
        "reportingorg_narrative",
        "Nome da organização que reporta o arquivo",
        "Name of the organisation reporting the file",
        "Nombre de la organización que reporta el archivo",
    ),
    *pair(
        "reporting_org_type",
        "reportingorg_type",
        "reportingorg_typename",
        "do tipo da organização que reporta o arquivo",
        "of the type of the organisation reporting the file",
        "del tipo de la organización que reporta el archivo",
        "OrganisationType",
    ),
    c(
        "reporting_org_is_secondary",
        "reportingorg_secondaryreporter",
        "BOOLEAN",
        "Indica que a organização está reproduzindo dados publicados por outra",
        "Whether the organisation is republishing data published by another",
        "Indica que la organización está reproduciendo datos publicados por otra",
    ),
    c(
        "default_currency_code",
        "defaultcurrency",
        "STRING",
        "Código ISO 4217 da moeda padrão da organização",
        "ISO 4217 code of the organisation's default currency",
        "Código ISO 4217 de la moneda por defecto de la organización",
    ),
    c(
        "default_currency_name",
        "defaultcurrencyname",
        "STRING",
        "Nome da moeda padrão da organização",
        "Name of the organisation's default currency",
        "Nombre de la moneda por defecto de la organización",
    ),
    c(
        "language_code",
        "lang",
        "STRING",
        "Código do idioma padrão dos textos da organização",
        "Code of the default language of the organisation's texts",
        "Código del idioma por defecto de los textos de la organización",
    ),
    c(
        "last_updated_datetime",
        "lastupdateddatetime",
        "DATETIME",
        "Momento da última alteração do arquivo declarado pelo publicador",
        "Time of the file's last change as declared by the publisher",
        "Momento de la última modificación del archivo declarado por el publicador",
    ),
]


def main():
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    for table, cols in SPEC.items():
        names = [x["name"] for x in cols]
        assert len(names) == len(set(names)), (
            f"{table}: duplicate column names "
            f"{[n for n in names if names.count(n) > 1]}"
        )
        path = ARCH_DIR / f"sheet_{table}.csv"
        with path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=HEADER)
            writer.writeheader()
            writer.writerows(cols)
        print(f"{path.name:45s} {len(cols):>3} columns")


if __name__ == "__main__":
    main()
