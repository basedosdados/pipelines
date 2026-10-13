"""Portuguese and English renderings of the architecture's Spanish text.

The architecture CSVs of br_bd_diretorios_ar are written in Spanish, the
language of the data (per the Data Basis style manual). The backend stores every
description and observation in three languages, so each Spanish string is keyed
here to its (pt, en) pair. register_metadata.py fails if a CSV string is missing
from these maps rather than registering a Portuguese-only column.
"""

# Spanish description -> (Portuguese, English)
DESCRIPTIONS: dict[str, tuple[str, str]] = {
    "Código de la jurisdicción, de dos dígitos": (
        "Código da jurisdição, de dois dígitos",
        "Two-digit code of the jurisdiction",
    ),
    "Nombre de la jurisdicción": (
        "Nome da jurisdição",
        "Name of the jurisdiction",
    ),
    "Nombre oficial completo de la jurisdicción": (
        "Nome oficial completo da jurisdição",
        "Full official name of the jurisdiction",
    ),
    "Código de subdivisión de una letra según la norma ISO 3166-2:AR": (
        "Código de subdivisão de uma letra segundo a norma ISO 3166-2:AR",
        "One-letter subdivision code under ISO 3166-2:AR",
    ),
    "Código del departamento, de cinco dígitos": (
        "Código do departamento, de cinco dígitos",
        "Five-digit code of the department",
    ),
    "Código de la jurisdicción a la que pertenece el departamento": (
        "Código da jurisdição à qual pertence o departamento",
        "Code of the jurisdiction the department belongs to",
    ),
    "Nombre del departamento": (
        "Nome do departamento",
        "Name of the department",
    ),
    "Código del gobierno local, de seis dígitos": (
        "Código do governo local, de seis dígitos",
        "Six-digit code of the local government",
    ),
    "Código de la jurisdicción a la que pertenece el gobierno local": (
        "Código da jurisdição à qual pertence o governo local",
        "Code of the jurisdiction the local government belongs to",
    ),
    "Nombre del gobierno local": (
        "Nome do governo local",
        "Name of the local government",
    ),
    (
        "Categoría del gobierno local según el régimen municipal de cada "
        "jurisdicción"
    ): (
        "Categoria do governo local segundo o regime municipal de cada "
        "jurisdição",
        "Category of the local government under each jurisdiction's municipal "
        "regime",
    ),
    "Código del aglomerado, de cuatro dígitos": (
        "Código do aglomerado, de quatro dígitos",
        "Four-digit code of the agglomeration",
    ),
    "Nombre del aglomerado": (
        "Nome do aglomerado",
        "Name of the agglomeration",
    ),
    "Código de la localidad censal, de ocho dígitos": (
        "Código da localidade censitária, de oito dígitos",
        "Eight-digit code of the census locality",
    ),
    "Código del departamento al que pertenece la localidad": (
        "Código do departamento ao qual pertence a localidade",
        "Code of the department the locality belongs to",
    ),
    "Código de la jurisdicción a la que pertenece la localidad": (
        "Código da jurisdição à qual pertence a localidade",
        "Code of the jurisdiction the locality belongs to",
    ),
    "Código del gobierno local al que pertenece la localidad": (
        "Código do governo local ao qual pertence a localidade",
        "Code of the local government the locality belongs to",
    ),
    "Código del aglomerado al que pertenece la localidad": (
        "Código do aglomerado ao qual pertence a localidade",
        "Code of the agglomeration the locality belongs to",
    ),
    "Nombre de la localidad censal": (
        "Nome da localidade censitária",
        "Name of the census locality",
    ),
    "Tipo de localidad": ("Tipo de localidade", "Type of locality"),
    "Nombre de la tabla que contiene la columna descrita": (
        "Nome da tabela que contém a coluna descrita",
        "Name of the table holding the described column",
    ),
    "Nombre de la columna descrita": (
        "Nome da coluna descrita",
        "Name of the described column",
    ),
    "Valor codificado almacenado en la columna": (
        "Valor codificado armazenado na coluna",
        "Coded value stored in the column",
    ),
    "Cobertura temporal del par clave-valor": (
        "Cobertura temporal do par chave-valor",
        "Temporal coverage of the key-value pair",
    ),
    "Significado del valor codificado": (
        "Significado do valor codificado",
        "Meaning of the coded value",
    ),
}

# Spanish observations -> (Portuguese, English)
OBSERVATIONS: dict[str, tuple[str, str]] = {
    (
        "Llave primaria. Los códigos no son contiguos: van de 02 a 94 y cubren "
        "las 23 provincias y la Ciudad Autónoma de Buenos Aires"
    ): (
        "Chave primária. Os códigos não são contíguos: vão de 02 a 94 e cobrem "
        "as 23 províncias e a Cidade Autônoma de Buenos Aires",
        "Primary key. The codes are not contiguous: they run from 02 to 94 and "
        "cover the 23 provinces and the Autonomous City of Buenos Aires",
    ),
    (
        "Agregado por Data Basis. Derivado como «Provincia de» o «Provincia "
        "del» más el nombre, según el artículo que fija cada constitución "
        "provincial; la Ciudad Autónoma de Buenos Aires no es una provincia y "
        "conserva su nombre"
    ): (
        "Adicionado pela Data Basis. Derivado como «Provincia de» ou "
        "«Provincia del» mais o nome, conforme o artigo fixado por cada "
        "constituição provincial; a Cidade Autônoma de Buenos Aires não é uma "
        "província e conserva seu nome",
        "Added by Data Basis. Derived as «Provincia de» or «Provincia del» "
        "plus the name, following the article each provincial constitution "
        "sets; the Autonomous City of Buenos Aires is not a province and keeps "
        "its own name",
    ),
    (
        "Agregado por Data Basis, sin el prefijo «AR-». No forma parte del "
        "archivo del INDEC"
    ): (
        "Adicionado pela Data Basis, sem o prefixo «AR-». Não faz parte do "
        "arquivo do INDEC",
        "Added by Data Basis, without the «AR-» prefix. Not part of the INDEC "
        "file",
    ),
    "Llave primaria. Los dos primeros dígitos corresponden a la jurisdicción": (
        "Chave primária. Os dois primeiros dígitos correspondem à jurisdição",
        "Primary key. The first two digits are the jurisdiction",
    ),
    (
        "La unidad se denomina departamento en 21 provincias, partido en la "
        "provincia de Buenos Aires y comuna en la Ciudad Autónoma de Buenos "
        "Aires"
    ): (
        "A unidade chama-se departamento em 21 províncias, partido na "
        "província de Buenos Aires e comuna na Cidade Autônoma de Buenos Aires",
        "The unit is called departamento in 21 provinces, partido in Buenos "
        "Aires province and comuna in the Autonomous City of Buenos Aires",
    ),
    (
        "Llave primaria. Los dos primeros dígitos corresponden a la "
        "jurisdicción. Codificación establecida por la Resolución INDEC 144/2022"
    ): (
        "Chave primária. Os dois primeiros dígitos correspondem à jurisdição. "
        "Codificação estabelecida pela Resolução INDEC 144/2022",
        "Primary key. The first two digits are the jurisdiction. Coding set by "
        "INDEC Resolution 144/2022",
    ),
    (
        "El gobierno local no anida en el departamento: 24 gobiernos locales "
        "abarcan más de un departamento, por lo que la tabla no lleva esa llave"
    ): (
        "O governo local não se aninha no departamento: 24 governos locais "
        "abrangem mais de um departamento, de modo que a tabela não carrega "
        "essa chave",
        "The local government does not nest in the department: 24 local "
        "governments span more than one department, so the table carries no "
        "such key",
    ),
    (
        "Incluye 11 registros «Sin Gobierno Local», para áreas censadas que "
        "dependen solo de la jurisdicción provincial, y 24 «Indeterminado», "
        "para áreas cuyos límites no pueden compatibilizarse con las áreas "
        "operativas censales"
    ): (
        "Inclui 11 registros «Sin Gobierno Local», para áreas censadas que "
        "dependem apenas da jurisdição provincial, e 24 «Indeterminado», para "
        "áreas cujos limites não podem ser compatibilizados com as áreas "
        "operacionais censitárias",
        "Includes 11 «Sin Gobierno Local» records, for censused areas answering "
        "only to the provincial jurisdiction, and 24 «Indeterminado», for areas "
        "whose boundaries cannot be reconciled with the census operational areas",
    ),
    (
        "Nula en los 35 registros «Sin Gobierno Local» e «Indeterminado». El "
        "INDEC define 20 categorías; 18 aparecen en el Censo 2022"
    ): (
        "Nula nos 35 registros «Sin Gobierno Local» e «Indeterminado». O INDEC "
        "define 20 categorias; 18 aparecem no Censo 2022",
        "Null in the 35 «Sin Gobierno Local» and «Indeterminado» records. INDEC "
        "defines 20 categories; 18 appear in the 2022 census",
    ),
    "Llave primaria": ("Chave primária", "Primary key"),
    (
        "Para los 119 aglomerados que agrupan más de una localidad es la "
        "etiqueta publicada por el INDEC; para los 3.587 aglomerados de una "
        "sola localidad, que el INDEC no etiqueta, es el nombre de esa "
        "localidad. La tabla no lleva llaves geográficas porque 14 aglomerados "
        "abarcan más de una jurisdicción y 62 más de un departamento"
    ): (
        "Para os 119 aglomerados que agrupam mais de uma localidade é a "
        "etiqueta publicada pelo INDEC; para os 3.587 aglomerados de uma só "
        "localidade, que o INDEC não etiqueta, é o nome dessa localidade. A "
        "tabela não carrega chaves geográficas porque 14 aglomerados abrangem "
        "mais de uma jurisdição e 62 mais de um departamento",
        "For the 119 agglomerations grouping more than one locality it is the "
        "label INDEC publishes; for the 3,587 single-locality agglomerations, "
        "which INDEC does not label, it is that locality's name. The table "
        "carries no geographic keys because 14 agglomerations span more than "
        "one jurisdiction and 62 more than one department",
    ),
    "Llave primaria. Los cinco primeros dígitos corresponden al departamento": (
        "Chave primária. Os cinco primeiros dígitos correspondem ao departamento",
        "Primary key. The first five digits are the department",
    ),
    (
        "Redundante respecto de id_departamento, se incluye para evitar un "
        "cruce adicional en los análisis por jurisdicción"
    ): (
        "Redundante em relação a id_departamento, é incluída para evitar um "
        "cruzamento adicional nas análises por jurisdição",
        "Redundant with id_departamento, included to save an extra join in "
        "analyses by jurisdiction",
    ),
    (
        "Toda localidad integra exactamente un aglomerado; 3.587 son "
        "aglomerados de una sola localidad"
    ): (
        "Toda localidade integra exatamente um aglomerado; 3.587 são "
        "aglomerados de uma só localidade",
        "Every locality belongs to exactly one agglomeration; 3,587 are "
        "single-locality agglomerations",
    ),
    (
        "No es único dentro del departamento: 11 localidades comparten nombre "
        "y departamento con otra"
    ): (
        "Não é único dentro do departamento: 11 localidades compartilham nome "
        "e departamento com outra",
        "Not unique within the department: 11 localities share a name and "
        "department with another",
    ),
    (
        "LS para localidad simple y CA para componente de aglomerado. Equivale "
        "a la cardinalidad del aglomerado: las 436 localidades CA integran los "
        "119 aglomerados de más de una localidad y las 3.587 LS son "
        "aglomerados de una sola localidad"
    ): (
        "LS para localidade simples e CA para componente de aglomerado. "
        "Equivale à cardinalidade do aglomerado: as 436 localidades CA "
        "integram os 119 aglomerados de mais de uma localidade e as 3.587 LS "
        "são aglomerados de uma só localidade",
        "LS for a simple locality and CA for an agglomeration component. It is "
        "equivalent to the agglomeration's cardinality: the 436 CA localities "
        "make up the 119 multi-locality agglomerations and the 3,587 LS are "
        "single-locality agglomerations",
    ),
    (
        "Vacía en todas las filas: los códigos del Censo 2022 no varían dentro "
        "del directorio"
    ): (
        "Vazia em todas as linhas: os códigos do Censo 2022 não variam dentro "
        "do diretório",
        "Empty in every row: the 2022 census codes do not vary within the "
        "directory",
    ),
}
