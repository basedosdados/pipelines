"""English and Spanish renderings of the column observations in the architecture.

The architecture CSVs carry Portuguese observations. register_metadata.py looks
each one up here and fails rather than registering a Portuguese-only note, so an
observation edited in architecture_spec.py must be edited here too.
"""

from __future__ import annotations

_VALUES_LABEL = (
    "Rótulo legível publicado pela fonte. Variantes de grafia e de caixa da mesma "
    "categoria foram unificadas na limpeza"
)
_LABEL_EN = (
    "Readable label published by the source. Spelling and case variants of the "
    "same category were unified during cleaning"
)
_LABEL_ES = (
    "Etiqueta legible publicada por la fuente. Las variantes de grafía y de "
    "mayúsculas de una misma categoría se unificaron en la limpieza"
)
_PERCENT = (
    "A fonte publica uma proporção entre 0 e 1; multiplicada por 100 na limpeza. "
    "Há valores acima de 100 na fonte, mantidos como publicados"
)
_PERCENT_EN = (
    "The source publishes a proportion between 0 and 1; multiplied by 100 during "
    "cleaning. The source has values above 100, kept as published"
)
_PERCENT_ES = (
    "La fuente publica una proporción entre 0 y 1; multiplicada por 100 en la "
    "limpieza. La fuente tiene valores superiores a 100, mantenidos como se publicaron"
)
_ORIGIN = (
    "Informado apenas para alimentos, veículos, produtos de saúde e similares"
)
_ORIGIN_EN = "Reported only for food, vehicles, health products and similar"
_ORIGIN_ES = (
    "Informado solo para alimentos, vehículos, productos de salud y similares"
)
_RAW = "Mantido como publicado; a fonte contém valores negativos e extremos"
_RAW_EN = "Kept as published; the source contains negative and extreme values"
_RAW_ES = "Mantenido como se publicó; la fuente contiene valores negativos y extremos"

# Portuguese -> (English, Spanish)
OBSERVATIONS: dict[str, tuple[str, str]] = {
    (
        "Coluna de partição (ano) ou de agrupamento (mês), derivada da data de "
        "notificação da versão inicial do contrato (menor id_modification). "
        "Contratos sem data de notificação válida, ou notificados antes de 2014, "
        "não entram na base"
    ): (
        "Partition (year) or clustering (month) column, derived from the "
        "notification date of the contract's initial version (lowest "
        "id_modification). Contracts with no valid notification date, or notified "
        "before 2014, are excluded",
        "Columna de partición (año) o de agrupamiento (mes), derivada de la fecha "
        "de notificación de la versión inicial del contrato (menor "
        "id_modification). Los contratos sin fecha de notificación válida, o "
        "notificados antes de 2014, quedan excluidos",
    ),
    (
        "Concatenação, feita pela fonte, do SIRET do comprador, do identificador "
        "interno do contrato e do código CPV"
    ): (
        "Concatenation, made by the source, of the buyer's SIRET, the contract's "
        "internal identifier and the CPV code",
        "Concatenación, hecha por la fuente, del SIRET del comprador, el "
        "identificador interno del contrato y el código CPV",
    ),
    "Deveria ser único entre os contratos de um mesmo comprador": (
        "Should be unique among the contracts of a given buyer",
        "Debería ser único entre los contratos de un mismo comprador",
    ),
    "Vincula-se a fr_insee_sirene.etablissement": (
        "Links to fr_insee_sirene.etablissement",
        "Se vincula a fr_insee_sirene.etablissement",
    ),
    (
        "Derivado na limpeza: os 9 primeiros dígitos de siret_acheteur. "
        "Vincula-se a fr_insee_sirene.unite_legale"
    ): (
        "Derived during cleaning: the first 9 digits of siret_acheteur. Links to "
        "fr_insee_sirene.unite_legale",
        "Derivado en la limpieza: los 9 primeros dígitos de siret_acheteur. Se "
        "vincula a fr_insee_sirene.unite_legale",
    ),
    "Rótulo atribuído pela fonte, por exemplo Commune, Département, EPIC": (
        "Label assigned by the source, for example Commune, Département, EPIC",
        "Etiqueta asignada por la fuente, por ejemplo Commune, Département, EPIC",
    ),
    (
        "Inclui códigos de coletividades de ultramar (por exemplo 975, 977, 978) "
        "que não constam do Code Officiel Géographique"
    ): (
        "Includes codes of overseas collectivities (for example 975, 977, 978) "
        "that are not in the Code Officiel Géographique",
        "Incluye códigos de colectividades de ultramar (por ejemplo 975, 977, 978) "
        "que no figuran en el Code Officiel Géographique",
    ),
    (
        "Inclui códigos de coletividades de ultramar (por exemplo 975, 977, 978) "
        "que não são regiões no Code Officiel Géographique"
    ): (
        "Includes codes of overseas collectivities (for example 975, 977, 978) "
        "that are not regions in the Code Officiel Géographique",
        "Incluye códigos de colectividades de ultramar (por ejemplo 975, 977, 978) "
        "que no son regiones en el Code Officiel Géographique",
    ),
    (
        "Ponto construído no modelo dbt a partir da latitude e longitude "
        "publicadas pela fonte, que as obtém pela geolocalização do endereço no "
        "SIRENE"
    ): (
        "Point built in the dbt model from the latitude and longitude published by "
        "the source, which obtains them by geocoding the SIRENE address",
        "Punto construido en el modelo dbt a partir de la latitud y longitud "
        "publicadas por la fuente, que las obtiene geolocalizando la dirección en "
        "SIRENE",
    ),
    (
        _VALUES_LABEL + ". Valores: Marché, Accord-cadre, Marché subséquent, "
        "Marché de partenariat, Marché de défense ou de sécurité e casos raros de "
        "concessão"
    ): (
        _LABEL_EN
        + ". Values: Marché, Accord-cadre, Marché subséquent, Marché de "
        "partenariat, Marché de défense ou de sécurité and rare concession cases",
        _LABEL_ES
        + ". Valores: Marché, Accord-cadre, Marché subséquent, Marché de "
        "partenariat, Marché de défense ou de sécurité y casos raros de concesión",
    ),
    "Pode ter sido truncado pelo produtor em 256 ou 1.000 caracteres": (
        "May have been truncated by the publisher at 256 or 1,000 characters",
        "Puede haber sido truncado por el productor en 256 o 1.000 caracteres",
    ),
    "Derivado pela fonte a partir do código CPV": (
        "Derived by the source from the CPV code",
        "Derivado por la fuente a partir del código CPV",
    ),
    _VALUES_LABEL: (_LABEL_EN, _LABEL_ES),
    _RAW: (_RAW_EN, _RAW_ES),
    (
        "Mantido como declarado pelo comprador, inclusive valores aberrantes; ver "
        "montant_rationalise e montant_anomalie"
    ): (
        "Kept as declared by the buyer, including aberrant values; see "
        "montant_rationalise and montant_anomalie",
        "Mantenido como lo declaró el comprador, incluidos los valores aberrantes; "
        "ver montant_rationalise y montant_anomalie",
    ),
    (
        "Igual a montant, exceto quando montant_anomalie = aberrant, caso em que a "
        "fonte o substitui por uma estimativa a partir de contratos semelhantes"
    ): (
        "Equal to montant, except when montant_anomalie = aberrant, in which case "
        "the source replaces it with an estimate based on similar contracts",
        "Igual a montant, salvo cuando montant_anomalie = aberrant, caso en que la "
        "fuente lo sustituye por una estimación a partir de contratos similares",
    ),
    "Valores: suspect, aberrant. Nulo quando o valor não é anômalo": (
        "Values: suspect, aberrant. Null when the amount is not anomalous",
        "Valores: suspect, aberrant. Nulo cuando el monto no es anómalo",
    ),
    (
        "Contagem de propostas, sem unidade de medida correspondente no catálogo. "
        + _RAW
    ): (
        "Count of bids, with no matching measurement unit in the catalogue. "
        + _RAW_EN,
        "Recuento de ofertas, sin unidad de medida correspondiente en el catálogo. "
        + _RAW_ES,
    ),
    "Valores: true / false. Nulo quando a fonte não informa": (
        "Values: true / false. Null when the source does not report it",
        "Valores: true / false. Nulo cuando la fuente no lo informa",
    ),
    _PERCENT: (_PERCENT_EN, _PERCENT_ES),
    _PERCENT + ". " + _ORIGIN: (
        _PERCENT_EN + ". " + _ORIGIN_EN,
        _PERCENT_ES + ". " + _ORIGIN_ES,
    ),
    "O tipo de código está em type_code_lieu_execution": (
        "The code type is in type_code_lieu_execution",
        "El tipo de código está en type_code_lieu_execution",
    ),
    (
        _VALUES_LABEL
        + ". Valores: Code postal, Code commune, Code département, "
        "Code région, Code pays, Code arrondissement, Code canton"
    ): (
        _LABEL_EN
        + ". Values: Code postal, Code commune, Code département, Code "
        "région, Code pays, Code arrondissement, Code canton",
        _LABEL_ES
        + ". Valores: Code postal, Code commune, Code département, Code "
        "région, Code pays, Code arrondissement, Code canton",
    ),
    (
        "Gerado pela fonte a partir da ordem das datas de notificação. 10.074 "
        "contratos não têm versão 0 na fonte"
    ): (
        "Generated by the source from the order of the notification dates. "
        "10,074 contracts have no version 0 at source",
        "Generado por la fuente a partir del orden de las fechas de notificación. "
        "10.074 contratos no tienen versión 0 en la fuente",
    ),
    "Para SIRET, vincula-se a fr_insee_sirene.etablissement": (
        "For a SIRET, links to fr_insee_sirene.etablissement",
        "Para un SIRET, se vincula a fr_insee_sirene.etablissement",
    ),
    (
        _VALUES_LABEL
        + ". Valores: SIRET, TVA, TVA intracommunautaire, HORS-UE, "
        "UE, IREP, RIDET, TAHITI, FRW, FRWF, RCI, Autre"
    ): (
        _LABEL_EN
        + ". Values: SIRET, TVA, TVA intracommunautaire, HORS-UE, UE, "
        "IREP, RIDET, TAHITI, FRW, FRWF, RCI, Autre",
        _LABEL_ES
        + ". Valores: SIRET, TVA, TVA intracommunautaire, HORS-UE, UE, "
        "IREP, RIDET, TAHITI, FRW, FRWF, RCI, Autre",
    ),
    (
        "Derivado na limpeza: os 9 primeiros dígitos de id_titulaire, apenas "
        "quando é um SIRET de 14 dígitos. Vincula-se a fr_insee_sirene.unite_legale"
    ): (
        "Derived during cleaning: the first 9 digits of id_titulaire, only when it "
        "is a 14-digit SIRET. Links to fr_insee_sirene.unite_legale",
        "Derivado en la limpieza: los 9 primeros dígitos de id_titulaire, solo "
        "cuando es un SIRET de 14 dígitos. Se vincula a fr_insee_sirene.unite_legale",
    ),
    "Valores: PME, ETI, GE": (
        "Values: PME, ETI, GE",
        "Valores: PME, ETI, GE",
    ),
}
