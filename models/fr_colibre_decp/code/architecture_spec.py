"""Column specification for fr_colibre_decp, in Portuguese, English and Spanish.

``build_architecture.py`` writes the architecture CSVs from this module, and the
metadata registration reads the English and Spanish descriptions from it, so the
three languages are kept in one place. The CSVs under ``architecture/`` remain the
artefact the cleaning code and dbt models follow.

Terminology held constant throughout:

| Portuguese | English | Spanish | French source term |
|---|---|---|---|
| contrato | contract | contrato | marché |
| comprador | buyer | comprador | acheteur |
| contratado | awardee | adjudicatario | titulaire |
| modificação | amendment | modificación | modification / avenant |
| notificação | notification | notificación | notification |

Proper nouns stay untranslated in all three: DECP, SIRET, SIREN, CPV, CCAG, NAF,
INSEE, Licence Ouverte.
"""

from __future__ import annotations

from typing import NamedTuple

ANO = "br_bd_diretorios_data_tempo.ano:ano"
MES = "br_bd_diretorios_data_tempo.mes:mes"
COMMUNE = "br_bd_diretorios_fr.commune:id_comuna"
DEPARTEMENT = "br_bd_diretorios_fr.departement:id_departamento"
REGION = "br_bd_diretorios_fr.region:id_regiao"
NAF = "br_bd_diretorios_fr.naf_rev2:naf_rev2"

PARTITION_OBS = (
    "Coluna de partição (ano) ou de agrupamento (mês), derivada da data de notificação da versão inicial do "
    "contrato (menor id_modification). Contratos sem data de notificação válida, "
    "ou notificados antes de 2014, não entram na base"
)
LABEL_OBS = (
    "Rótulo legível publicado pela fonte. Variantes de grafia e de caixa da mesma "
    "categoria foram unificadas na limpeza"
)
FLAG_OBS = "Valores: true / false. Nulo quando a fonte não informa"
GEO_OBS = (
    "Ponto construído no modelo dbt a partir da latitude e longitude publicadas "
    "pela fonte, que as obtém pela geolocalização do endereço no SIRENE"
)
PERCENT_OBS = (
    "A fonte publica uma proporção entre 0 e 1; multiplicada por 100 na limpeza. "
    "Há valores acima de 100 na fonte, mantidos como publicados"
)


class Col(NamedTuple):
    name: str
    bigquery_type: str
    pt: str
    en: str
    es: str
    measurement_unit: str = ""
    directory_column: str = ""
    observations: str = ""
    original_name: str = ""
    covered_by_dictionary: str = "no"
    has_sensitive_data: str = "no"


def _partition() -> list[Col]:
    return [
        Col(
            "ano",
            "INT64",
            "Ano de notificação da versão inicial do contrato",
            "Year in which the initial version of the contract was notified",
            "Año de notificación de la versión inicial del contrato",
            "year",
            ANO,
            PARTITION_OBS,
            "dateNotification",
        ),
        Col(
            "mes",
            "INT64",
            "Mês de notificação da versão inicial do contrato",
            "Month in which the initial version of the contract was notified",
            "Mes de notificación de la versión inicial del contrato",
            "month",
            MES,
            PARTITION_OBS,
            "dateNotification",
        ),
    ]


ID_MARCHE = Col(
    "id_marche",
    "STRING",
    "Identificador único do contrato na DECP",
    "Unique contract identifier in the DECP",
    "Identificador único del contrato en la DECP",
    observations=(
        "Concatenação, feita pela fonte, do SIRET do comprador, do identificador "
        "interno do contrato e do código CPV"
    ),
    original_name="uid",
)

ID_MODIFICATION = Col(
    "id_modification",
    "STRING",
    "Número sequencial da versão do contrato; 0 corresponde à atribuição inicial",
    "Sequence number of the contract version; 0 is the initial award",
    "Número secuencial de la versión del contrato; 0 es la adjudicación inicial",
    observations=(
        "Gerado pela fonte a partir da ordem das datas de notificação. 10.074 "
        "contratos não têm versão 0 na fonte"
    ),
    original_name="modification_id",
)

DATE_NOTIFICATION = Col(
    "date_notification",
    "DATE",
    "Data em que o contrato ou a modificação foi notificado ao contratado",
    "Date the contract or amendment was notified to the awardee",
    "Fecha en que el contrato o la modificación fue notificado al adjudicatario",
    original_name="dateNotification",
)
DATE_PUBLICATION = Col(
    "date_publication_donnees",
    "DATE",
    "Data em que os dados do contrato ou da modificação foram publicados",
    "Date the contract or amendment data were published",
    "Fecha en que se publicaron los datos del contrato o de la modificación",
    original_name="datePublicationDonnees",
)
DUREE = Col(
    "duree_mois",
    "INT64",
    "Duração do contrato em meses",
    "Contract duration in months",
    "Duración del contrato en meses",
    "month",
    observations="Mantido como publicado; a fonte contém valores negativos e extremos",
    original_name="dureeMois",
)
MONTANT = Col(
    "montant",
    "FLOAT64",
    "Valor fixo ou valor máximo estimado do contrato, sem impostos",
    "Fixed amount or estimated maximum amount of the contract, excluding tax",
    "Monto fijo o monto máximo estimado del contrato, sin impuestos",
    "eur",
    observations=(
        "Mantido como declarado pelo comprador, inclusive valores aberrantes; ver "
        "montant_rationalise e montant_anomalie"
    ),
    original_name="montant",
)
MONTANT_RATIONALISE = Col(
    "montant_rationalise",
    "FLOAT64",
    "Valor do contrato com os montantes classificados como aberrantes substituídos",
    "Contract amount with amounts classified as aberrant replaced",
    "Monto del contrato con los montos clasificados como aberrantes sustituidos",
    "eur",
    observations=(
        "Igual a montant, exceto quando montant_anomalie = aberrant, caso em que a "
        "fonte o substitui por uma estimativa a partir de contratos semelhantes"
    ),
    original_name="montant_rationalise",
)
MONTANT_ANOMALIE = Col(
    "montant_anomalie",
    "STRING",
    "Classificação do valor declarado em comparação com contratos semelhantes",
    "Classification of the declared amount compared with similar contracts",
    "Clasificación del monto declarado en comparación con contratos similares",
    observations="Valores: suspect, aberrant. Nulo quando o valor não é anômalo",
    original_name="montant_anomalie",
)
MONTANT_ANOMALIE_RAISONS = Col(
    "montant_anomalie_raisons",
    "STRING",
    "Critério que levou à classificação do valor como anômalo",
    "Criterion that led to the amount being classified as anomalous",
    "Critério que llevó a clasificar el monto como anómalo",
    original_name="montant_anomalie_raisons",
)


def _geo(role_pt: str, role_en: str, role_es: str, src: str) -> list[Col]:
    return [
        Col(
            f"code_commune_{src}",
            "STRING",
            f"Código INSEE da comuna do {role_pt}",
            f"INSEE code of the {role_en}'s commune",
            f"Código INSEE de la comuna del {role_es}",
            directory_column=COMMUNE,
            original_name=f"{src}_commune_code",
        ),
        Col(
            f"code_departement_{src}",
            "STRING",
            f"Código do departamento do {role_pt}",
            f"Code of the {role_en}'s department",
            f"Código del departamento del {role_es}",
            directory_column=DEPARTEMENT,
            observations=(
                "Inclui códigos de coletividades de ultramar (por exemplo 975, 977, "
                "978) que não constam do Code Officiel Géographique"
            ),
            original_name=f"{src}_departement_code",
        ),
        Col(
            f"code_region_{src}",
            "STRING",
            f"Código da região do {role_pt}",
            f"Code of the {role_en}'s region",
            f"Código de la región del {role_es}",
            directory_column=REGION,
            observations=(
                "Inclui códigos de coletividades de ultramar (por exemplo 975, 977, "
                "978) que não são regiões no Code Officiel Géographique"
            ),
            original_name=f"{src}_region_code",
        ),
        Col(
            f"geometria_{src}",
            "GEOGRAPHY",
            f"Localização geográfica do {role_pt}",
            f"Geographic location of the {role_en}",
            f"Ubicación geográfica del {role_es}",
            observations=GEO_OBS,
            original_name=f"{src}_latitude, {src}_longitude",
        ),
    ]


MARCHE = [
    *_partition(),
    ID_MARCHE,
    Col(
        "id_marche_acheteur",
        "STRING",
        "Identificador interno atribuído pelo comprador ao contrato",
        "Internal identifier the buyer assigned to the contract",
        "Identificador interno que el comprador asignó al contrato",
        observations="Deveria ser único entre os contratos de um mesmo comprador",
        original_name="id",
    ),
    Col(
        "id_accord_cadre",
        "STRING",
        "Identificador interno do acordo-quadro ao qual o contrato subsequente se vincula",
        "Internal identifier of the framework agreement a subsequent contract belongs to",
        "Identificador interno del acuerdo marco al que pertenece el contrato subsecuente",
        original_name="idAccordCadre",
    ),
    Col(
        "siret_acheteur",
        "STRING",
        "SIRET do estabelecimento comprador",
        "SIRET of the buying establishment",
        "SIRET del establecimiento comprador",
        observations="Vincula-se a fr_insee_sirene.etablissement",
        original_name="acheteur_id",
    ),
    Col(
        "siren_acheteur",
        "STRING",
        "SIREN da unidade legal compradora",
        "SIREN of the buying legal unit",
        "SIREN de la unidad legal compradora",
        observations=(
            "Derivado na limpeza: os 9 primeiros dígitos de siret_acheteur. Vincula-se "
            "a fr_insee_sirene.unite_legale"
        ),
        original_name="acheteur_id",
    ),
    Col(
        "nom_acheteur",
        "STRING",
        "Nome do comprador segundo o SIRENE",
        "Name of the buyer according to SIRENE",
        "Nombre del comprador según SIRENE",
        original_name="acheteur_nom",
    ),
    Col(
        "categorie_acheteur",
        "STRING",
        "Categoria do comprador segundo sua categoria jurídica no INSEE",
        "Buyer category according to its INSEE legal category",
        "Categoría del comprador según su categoría jurídica en el INSEE",
        observations="Rótulo atribuído pela fonte, por exemplo Commune, Département, EPIC",
        original_name="acheteur_categorie",
    ),
    Col(
        "labels_acheteur",
        "STRING",
        "Selos e certificações do comprador, separados por vírgula",
        "Labels and certifications held by the buyer, comma-separated",
        "Sellos y certificaciones del comprador, separados por coma",
        original_name="acheteur_labels",
    ),
    *_geo("comprador", "buyer", "comprador", "acheteur"),
    Col(
        "nature",
        "STRING",
        "Natureza do contrato",
        "Nature of the contract",
        "Naturaleza del contrato",
        observations=LABEL_OBS
        + ". Valores: Marché, Accord-cadre, Marché subséquent, Marché de partenariat, "
        "Marché de défense ou de sécurité e casos raros de concessão",
        original_name="nature",
    ),
    Col(
        "objet",
        "STRING",
        "Objeto do contrato",
        "Purpose of the contract",
        "Objeto del contrato",
        observations="Pode ter sido truncado pelo produtor em 256 ou 1.000 caracteres",
        original_name="objet",
    ),
    Col(
        "type_marche",
        "STRING",
        "Tipo de contrato: fornecimentos, serviços ou obras",
        "Contract type: supplies, services or works",
        "Tipo de contrato: suministros, servicios u obras",
        observations="Derivado pela fonte a partir do código CPV",
        original_name="type",
    ),
    Col(
        "code_cpv",
        "STRING",
        "Código CPV (Common Procurement Vocabulary) do objeto do contrato",
        "CPV (Common Procurement Vocabulary) code of the contract's purpose",
        "Código CPV (Common Procurement Vocabulary) del objeto del contrato",
        original_name="codeCPV",
    ),
    Col(
        "procedure",
        "STRING",
        "Procedimento de contratação utilizado",
        "Award procedure used",
        "Procedimiento de contratación utilizado",
        observations=LABEL_OBS,
        original_name="procedure",
    ),
    Col(
        "techniques",
        "STRING",
        "Técnicas de compra utilizadas, separadas por vírgula",
        "Purchasing techniques used, comma-separated",
        "Técnicas de compra utilizadas, separadas por coma",
        original_name="techniques",
    ),
    Col(
        "modalites_execution",
        "STRING",
        "Modalidades de execução do contrato, separadas por vírgula",
        "Contract execution arrangements, comma-separated",
        "Modalidades de ejecución del contrato, separadas por coma",
        original_name="modalitesExecution",
    ),
    DATE_NOTIFICATION,
    DATE_PUBLICATION,
    DUREE,
    MONTANT,
    MONTANT_RATIONALISE,
    MONTANT_ANOMALIE,
    MONTANT_ANOMALIE_RAISONS,
    Col(
        "nombre_offres",
        "INT64",
        "Número de propostas recebidas, incluindo as irregulares ou inaceitáveis",
        "Number of bids received, including irregular or unacceptable ones",
        "Número de ofertas recibidas, incluidas las irregulares o inaceptables",
        observations=(
            "Contagem de propostas, sem unidade de medida correspondente no catálogo. "
            "Mantido como publicado; a fonte contém valores negativos e extremos"
        ),
        original_name="offresRecues",
    ),
    Col(
        "forme_prix",
        "STRING",
        "Forma do preço do contrato",
        "Price form of the contract",
        "Forma del precio del contrato",
        original_name="formePrix",
    ),
    Col(
        "types_prix",
        "STRING",
        "Tipos de preço do contrato, separados por vírgula",
        "Price types of the contract, comma-separated",
        "Tipos de precio del contrato, separados por coma",
        original_name="typesPrix",
    ),
    Col(
        "attribution_avance",
        "STRING",
        "Indica se foi concedido adiantamento ao contratado",
        "Whether an advance payment was granted to the awardee",
        "Indica si se concedió un anticipo al adjudicatario",
        observations=FLAG_OBS,
        original_name="attributionAvance",
    ),
    Col(
        "taux_avance",
        "FLOAT64",
        "Percentual do adiantamento concedido em relação ao valor do contrato",
        "Advance payment as a percentage of the contract amount",
        "Porcentaje del anticipo concedido respecto del monto del contrato",
        "percent",
        observations=PERCENT_OBS,
        original_name="tauxAvance",
    ),
    Col(
        "marche_innovant",
        "STRING",
        "Indica se o contrato inclui obras, serviços ou fornecimentos inovadores",
        "Whether the contract includes innovative works, services or supplies",
        "Indica si el contrato incluye obras, servicios o suministros innovadores",
        observations=FLAG_OBS,
        original_name="marcheInnovant",
    ),
    Col(
        "considerations_sociales",
        "STRING",
        "Considerações sociais previstas no contrato, separadas por vírgula",
        "Social considerations included in the contract, comma-separated",
        "Consideraciones sociales previstas en el contrato, separadas por coma",
        original_name="considerationsSociales",
    ),
    Col(
        "considerations_environnementales",
        "STRING",
        "Considerações ambientais previstas no contrato, separadas por vírgula",
        "Environmental considerations included in the contract, comma-separated",
        "Consideraciones ambientales previstas en el contrato, separadas por coma",
        original_name="considerationsEnvironnementales",
    ),
    Col(
        "ccag",
        "STRING",
        "Caderno de cláusulas administrativas gerais (CCAG) aplicado ao contrato",
        "General administrative clauses (CCAG) applied to the contract",
        "Pliego de cláusulas administrativas generales (CCAG) aplicado al contrato",
        observations=LABEL_OBS,
        original_name="ccag",
    ),
    Col(
        "sous_traitance_declaree",
        "STRING",
        "Indica se os contratados declararam recorrer a subcontratação na notificação",
        "Whether the awardees declared subcontracting at notification",
        "Indica si los adjudicatarios declararon subcontratación en la notificación",
        observations=FLAG_OBS,
        original_name="sousTraitanceDeclaree",
    ),
    Col(
        "type_groupement_operateurs",
        "STRING",
        "Tipo de consórcio de operadores econômicos",
        "Type of grouping of economic operators",
        "Tipo de agrupación de operadores económicos",
        original_name="typeGroupementOperateurs",
    ),
    Col(
        "origine_ue",
        "FLOAT64",
        "Percentual do valor de produtos originários da União Europeia",
        "Percentage of the value of products originating in the European Union",
        "Porcentaje del valor de productos originários de la Unión Europea",
        "percent",
        observations=PERCENT_OBS
        + ". Informado apenas para alimentos, veículos, produtos de saúde e similares",
        original_name="origineUE",
    ),
    Col(
        "origine_france",
        "FLOAT64",
        "Percentual do valor de produtos originários da França",
        "Percentage of the value of products originating in France",
        "Porcentaje del valor de productos originários de Francia",
        "percent",
        observations=PERCENT_OBS
        + ". Informado apenas para alimentos, veículos, produtos de saúde e similares",
        original_name="origineFrance",
    ),
    Col(
        "code_lieu_execution",
        "STRING",
        "Código do local de execução do contrato",
        "Code of the place where the contract is performed",
        "Código del lugar de ejecución del contrato",
        observations="O tipo de código está em type_code_lieu_execution",
        original_name="lieuExecution_code",
    ),
    Col(
        "type_code_lieu_execution",
        "STRING",
        "Tipo do código do local de execução",
        "Type of the place-of-performance code",
        "Tipo del código del lugar de ejecución",
        observations=LABEL_OBS
        + ". Valores: Code postal, Code commune, Code département, Code région, Code pays, Code arrondissement, Code canton",
        original_name="lieuExecution_typeCode",
    ),
    Col(
        "source_donnees",
        "STRING",
        "Código do conjunto de dados de origem da linha na consolidação",
        "Code of the source dataset the row comes from in the consolidation",
        "Código del conjunto de datos de origen de la fila en la consolidación",
        original_name="sourceDataset",
    ),
    Col(
        "fichier_source",
        "STRING",
        "Endereço do arquivo de dados abertos de onde a linha foi extraída",
        "URL of the open-data file the row was taken from",
        "Dirección del archivo de datos abiertos del que se extrajo la fila",
        original_name="sourceFile",
    ),
]

MODIFICATION = [
    *_partition(),
    ID_MARCHE,
    ID_MODIFICATION,
    DATE_NOTIFICATION,
    DATE_PUBLICATION,
    DUREE,
    MONTANT,
    MONTANT_RATIONALISE,
    MONTANT_ANOMALIE,
    MONTANT_ANOMALIE_RAISONS,
    Col(
        "donnees_actuelles",
        "STRING",
        "Indica se a linha corresponde à versão mais recente do contrato",
        "Whether the row is the most recent version of the contract",
        "Indica si la fila corresponde a la versión más reciente del contrato",
        observations=FLAG_OBS,
        original_name="donneesActuelles",
    ),
]

TITULAIRE = [
    *_partition(),
    ID_MARCHE,
    ID_MODIFICATION,
    Col(
        "id_titulaire",
        "STRING",
        "Identificador do contratado, no referencial indicado em type_identifiant_titulaire",
        "Awardee identifier, in the register given by type_identifiant_titulaire",
        "Identificador del adjudicatario, en el registro indicado en type_identifiant_titulaire",
        observations="Para SIRET, vincula-se a fr_insee_sirene.etablissement",
        original_name="titulaire_id",
    ),
    Col(
        "type_identifiant_titulaire",
        "STRING",
        "Referencial do identificador do contratado",
        "Register the awardee identifier belongs to",
        "Registro al que pertenece el identificador del adjudicatario",
        observations=LABEL_OBS
        + ". Valores: SIRET, TVA, HORS-UE, UE, IREP, RIDET, TAHITI, FRWF, Autre",
        original_name="titulaire_typeIdentifiant",
    ),
    Col(
        "siren_titulaire",
        "STRING",
        "SIREN da unidade legal contratada",
        "SIREN of the awarded legal unit",
        "SIREN de la unidad legal adjudicataria",
        observations=(
            "Derivado na limpeza: os 9 primeiros dígitos de id_titulaire, apenas quando "
            "é um SIRET de 14 dígitos. Vincula-se a fr_insee_sirene.unite_legale"
        ),
        original_name="titulaire_id",
    ),
    Col(
        "nom_titulaire",
        "STRING",
        "Nome do contratado; segundo o SIRENE quando o identificador é um SIRET",
        "Awardee name; as in SIRENE when the identifier is a SIRET",
        "Nombre del adjudicatario; según SIRENE cuando el identificador es un SIRET",
        original_name="titulaire_nom",
    ),
    Col(
        "categorie_titulaire",
        "STRING",
        "Categoria de empresa do contratado segundo o INSEE",
        "Awardee company category according to INSEE",
        "Categoría de empresa del adjudicatario según el INSEE",
        observations="Valores: PME, ETI, GE",
        original_name="titulaire_categorie",
    ),
    Col(
        "labels_titulaire",
        "STRING",
        "Selos e certificações do contratado, separados por vírgula",
        "Labels and certifications held by the awardee, comma-separated",
        "Sellos y certificaciones del adjudicatario, separados por coma",
        original_name="titulaire_labels",
    ),
    Col(
        "code_activite_titulaire",
        "STRING",
        "Código da atividade principal (NAF rev. 2) do estabelecimento contratado",
        "Main activity code (NAF rev. 2) of the awarded establishment",
        "Código de la actividad principal (NAF rev. 2) del establecimiento adjudicatario",
        directory_column=NAF,
        original_name="titulaire_activite_code",
    ),
    *_geo("contratado", "awardee", "adjudicatario", "titulaire"),
    Col(
        "distance_acheteur",
        "INT64",
        "Distância entre o endereço do comprador e o do contratado",
        "Distance between the buyer's and the awardee's addresses",
        "Distancia entre la dirección del comprador y la del adjudicatario",
        "kilometer",
        original_name="titulaire_distance",
    ),
]

TABLES: dict[str, list[Col]] = {
    "marche": MARCHE,
    "modification": MODIFICATION,
    "titulaire": TITULAIRE,
}
