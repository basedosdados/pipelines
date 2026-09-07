"""Architecture for the dicionario table."""

from arch_common import c

DICIONARIO = [
    c(
        "id_tabela",
        "STRING",
        "Nome da tabela à qual a chave se refere",
        en="Name of the table the key belongs to",
        es="Nombre de la tabla a la que se refiere la clave",
    ),
    c(
        "nome_coluna",
        "STRING",
        "Nome da coluna à qual a chave se refere",
        en="Name of the column the key belongs to",
        es="Nombre de la columna a la que se refiere la clave",
    ),
    c(
        "chave",
        "STRING",
        "Valor codificado presente na coluna",
        en="Coded value present in the column",
        es="Valor codificado presente en la columna",
    ),
    c(
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal em que a chave é válida",
        en="Temporal coverage over which the key is valid",
        es="Cobertura temporal en que la clave es válida",
    ),
    c(
        "valor",
        "STRING",
        "Significado da chave",
        en="Meaning of the key",
        es="Significado de la clave",
    ),
]

TABLES = {"dicionario": DICIONARIO}
