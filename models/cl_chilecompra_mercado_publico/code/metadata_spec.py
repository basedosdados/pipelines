"""Every string and structural fact the backend registration needs, in one place.

Kept separate from the driver so the copy that goes to prod is provably the copy
that was reviewed on staging: `register_metadata.py --env prod` re-runs the same
spec against the other backend rather than repeating a hand-made sequence of
calls.
"""

from __future__ import annotations

DATASET_SLUG = "cl_chilecompra_mercado_publico"
GCP_DATASET_ID = "cl_chilecompra_mercado_publico"
ORGANIZATION = "chilecompra"
THEMES = ["economics", "government"]
TAGS = [
    "administracao_publica",
    "compra",
    "concorrencia",
    "contrato",
    "empresa",
    "financas_publicas",
    "gasto",
    "licitacao",
]
AREA = "cl"
LICENSE = "libre_uso_cl"
AVAILABILITY = "online"

# The load covers 2007-01 .. 2026-08 in every table.
COVERAGE_START = (2007, 1)
COVERAGE_END = (2026, 8)
# ChileCompra republishes the running month continuously, so the dataset refreshes
# monthly or more often and takes the house BD Pro rolling window: the most recent
# six months are Pro, everything older is free. free_end is inclusive, so the pro
# range starts the month after it.
FREE_LAG_MONTHS = 6

# Auxiliary bundles are served from basedosdados-public, which is NOT requester-pays.
# The two data-lake buckets (basedosdados, basedosdados-dev) are, so a link served from
# either returns HTTP 400 UserProjectMissing to an anonymous visitor -- which is why
# every one of the 84 production tables using this field today has a dead link. The
# convention change is pipelines#1928; this dataset follows the working bucket rather
# than shipping three more dead links.
AUXILIARY_FILES_BUCKET = "basedosdados-public"


def auxiliary_files_url(table_slug: str) -> str:
    return (
        f"https://storage.googleapis.com/{AUXILIARY_FILES_BUCKET}/auxiliary_files/"
        f"{GCP_DATASET_ID}/{table_slug}/auxiliary_files.zip"
    )


DATASET = {
    "name_pt": "Compras Públicas (Mercado Público)",
    "name_en": "Public Procurement (Mercado Público)",
    "name_es": "Compras Públicas (Mercado Público)",
    "description_pt": (
        "Compras públicas do Chile realizadas pela plataforma Mercado Público, no "
        "nível de linha de item, de janeiro de 2007 em diante. Cobre as ordens de "
        "compra emitidas pelos organismos do Estado regidos pela Lei N° 19.886 e as "
        "licitações que as originam, incluindo todas as propostas recebidas em cada "
        "licitação e não apenas a vencedora. Conforme a própria ChileCompra, o arquivo "
        "de descarga massiva inclui ordens de compra excluídas das estatísticas "
        "oficiais por conterem erros de valor ou de tipo de moeda, de modo que "
        "agregados calculados aqui não coincidem com as cifras publicadas pelo órgão."
    ),
    "description_en": (
        "Chilean public procurement carried out through the Mercado Público platform, "
        "at item-line level, from January 2007 onward. It covers the purchase orders "
        "issued by State bodies governed by Law No. 19.886 and the tenders that "
        "originate them, including every bid received on a tender rather than only the "
        "winning one. As ChileCompra itself states, the bulk download includes purchase "
        "orders excluded from its official statistics for carrying errors in amount or "
        "currency type, so aggregates computed here do not match the figures the agency "
        "publishes."
    ),
    "description_es": (
        "Compras públicas de Chile realizadas a través de la plataforma Mercado "
        "Público, a nivel de línea de ítem, desde enero de 2007 en adelante. Cubre las "
        "órdenes de compra emitidas por los organismos del Estado regidos por la Ley N° "
        "19.886 y las licitaciones que las originan, incluyendo todas las ofertas "
        "recibidas en cada licitación y no solo la adjudicada. Según la propia "
        "ChileCompra, la descarga masiva incluye órdenes de compra excluidas de sus "
        "cifras oficiales por tener errores en los montos o en el tipo de moneda, por "
        "lo que los agregados calculados aquí no coinciden con las cifras que publica "
        "el organismo."
    ),
}

RAW_SOURCES = {
    "oc": {
        "name_pt": "Descarga massiva de ordens de compra",
        "name_en": "Bulk download of purchase orders",
        "name_es": "Descarga masiva de órdenes de compra",
        "url": "https://datos-abiertos.chilecompra.cl/descargas",
        "description_pt": (
            "Arquivos ZIP mensais com as ordens de compra no nível de linha de item, "
            "publicados em https://transparenciachc.blob.core.windows.net/oc-da/."
        ),
        "description_en": (
            "Monthly ZIP files carrying the purchase orders at item-line level, "
            "published at https://transparenciachc.blob.core.windows.net/oc-da/."
        ),
        "description_es": (
            "Archivos ZIP mensuales con las órdenes de compra a nivel de línea de "
            "ítem, publicados en https://transparenciachc.blob.core.windows.net/oc-da/."
        ),
    },
    "lic": {
        "name_pt": "Descarga massiva de licitações",
        "name_en": "Bulk download of tenders",
        "name_es": "Descarga masiva de licitaciones",
        "url": "https://datos-abiertos.chilecompra.cl/descargas",
        "description_pt": (
            "Arquivos ZIP mensais com as licitações e as propostas recebidas, "
            "publicados em https://transparenciachc.blob.core.windows.net/lic-da/."
        ),
        "description_en": (
            "Monthly ZIP files carrying the tenders and the bids received on them, "
            "published at https://transparenciachc.blob.core.windows.net/lic-da/."
        ),
        "description_es": (
            "Archivos ZIP mensuales con las licitaciones y las ofertas recibidas, "
            "publicados en https://transparenciachc.blob.core.windows.net/lic-da/."
        ),
    },
}

# Ordered as the dataset should display them.
TABLES = [
    {
        "slug": "orden_compra_item",
        "auxiliary_files": True,
        "raw_source": "oc",
        "name_pt": "Ordens de compra (item)",
        "name_en": "Purchase orders (item)",
        "name_es": "Órdenes de compra (ítem)",
        "description_pt": (
            "Ordens de compra do Mercado Público do Chile no nível de linha de item, de "
            "janeiro de 2007 em diante. Cada linha é um item de uma ordem de compra "
            "emitida por um organismo público a um fornecedor; a chave lógica é "
            "(codigo_orden_compra, id_item). Conforme a própria ChileCompra, o arquivo "
            "de descarga massiva inclui ordens de compra excluídas das estatísticas "
            "oficiais por conterem erros de valor ou de tipo de moeda, de modo que "
            "agregados calculados aqui não coincidem com as cifras publicadas pelo órgão."
        ),
        "description_en": (
            "Purchase orders from Chile's Mercado Público at item-line level, from "
            "January 2007 onward. Each row is one item of a purchase order issued by a "
            "public agency to a supplier; the logical key is (codigo_orden_compra, "
            "id_item). As ChileCompra itself states, the bulk download includes purchase "
            "orders excluded from its official statistics for carrying errors in amount "
            "or currency type, so aggregates computed here do not match the figures the "
            "agency publishes."
        ),
        "description_es": (
            "Órdenes de compra del Mercado Público de Chile a nivel de línea de ítem, "
            "desde enero de 2007 en adelante. Cada fila es un ítem de una orden de "
            "compra emitida por un organismo público a un proveedor; la llave lógica es "
            "(codigo_orden_compra, id_item). Según la propia ChileCompra, la descarga "
            "masiva incluye órdenes de compra excluidas de sus cifras oficiales por "
            "tener errores en los montos o en el tipo de moneda, por lo que los "
            "agregados calculados aquí no coinciden con las cifras que publica el "
            "organismo."
        ),
        "observation_levels": ["year", "month", "procurement", "item"],
        "observation_level_columns": {
            "ano": "year",
            "mes": "month",
            "codigo_orden_compra": "procurement",
            "id_item": "item",
        },
    },
    {
        "slug": "licitacion_item",
        "auxiliary_files": True,
        "raw_source": "lic",
        "name_pt": "Licitações (item)",
        "name_en": "Tenders (item)",
        "name_es": "Licitaciones (ítem)",
        "description_pt": (
            "Licitações do Mercado Público do Chile no nível de linha de item, de "
            "janeiro de 2007 em diante. Cada linha é um item de uma licitação, com "
            "todos os atributos do processo licitatório; a chave lógica é "
            "(codigo_licitacion, codigo_item). As propostas recebidas em cada item "
            "estão na tabela licitacion_oferta. Abril de 2014 não existe na fonte: o "
            "arquivo publicado nesse mês contém os dados de março, portanto a série não "
            "cobre esse mês."
        ),
        "description_en": (
            "Tenders from Chile's Mercado Público at item-line level, from January 2007 "
            "onward. Each row is one item of a tender, carrying every attribute of the "
            "tendering process; the logical key is (codigo_licitacion, codigo_item). The "
            "bids received on each item are in the licitacion_oferta table. April 2014 "
            "does not exist at source: the file published for that month contains "
            "March's data, so the series does not cover it."
        ),
        "description_es": (
            "Licitaciones del Mercado Público de Chile a nivel de línea de ítem, desde "
            "enero de 2007 en adelante. Cada fila es un ítem de una licitación, con "
            "todos los atributos del proceso licitatorio; la llave lógica es "
            "(codigo_licitacion, codigo_item). Las ofertas recibidas en cada ítem están "
            "en la tabla licitacion_oferta. Abril de 2014 no existe en la fuente: el "
            "archivo publicado para ese mes contiene los datos de marzo, por lo que la "
            "serie no cubre ese mes."
        ),
        "observation_levels": ["year", "month", "procurement", "item"],
        "observation_level_columns": {
            "ano": "year",
            "mes": "month",
            "codigo_licitacion": "procurement",
            "codigo_item": "item",
        },
    },
    {
        "slug": "licitacion_oferta",
        "auxiliary_files": True,
        "raw_source": "lic",
        "name_pt": "Licitações (proposta)",
        "name_en": "Tenders (bid)",
        "name_es": "Licitaciones (oferta)",
        "description_pt": (
            "Propostas recebidas nas licitações do Mercado Público do Chile, de janeiro "
            "de 2007 em diante. Cada linha é a proposta de um fornecedor para uma linha "
            "de item de uma licitação, incluindo as propostas não vencedoras; a chave "
            "lógica é (codigo_licitacion, codigo_item, codigo_proveedor, nombre_oferta). "
            "Os atributos do processo licitatório estão na tabela licitacion_item. Nas "
            "1.450.638 linhas provenientes de março de 2014 o fornecedor é identificado "
            "por rut_proveedor, e não por codigo_proveedor, que vem vazio. Abril de 2014 "
            "não existe na fonte."
        ),
        "description_en": (
            "Bids received on tenders in Chile's Mercado Público, from January 2007 "
            "onward. Each row is one supplier's bid on one item line of a tender, losing "
            "bids included; the logical key is (codigo_licitacion, codigo_item, "
            "codigo_proveedor, nombre_oferta). The attributes of the tendering process "
            "are in the licitacion_item table. In the 1,450,638 rows coming from March "
            "2014 the supplier is identified by rut_proveedor rather than "
            "codigo_proveedor, which is empty there. April 2014 does not exist at source."
        ),
        "description_es": (
            "Ofertas recibidas en las licitaciones del Mercado Público de Chile, desde "
            "enero de 2007 en adelante. Cada fila es la oferta de un proveedor para una "
            "línea de ítem de una licitación, incluidas las ofertas no adjudicadas; la "
            "llave lógica es (codigo_licitacion, codigo_item, codigo_proveedor, "
            "nombre_oferta). Los atributos del proceso licitatorio están en la tabla "
            "licitacion_item. En las 1.450.638 filas provenientes de marzo de 2014 el "
            "proveedor se identifica por rut_proveedor y no por codigo_proveedor, que "
            "viene vacío. Abril de 2014 no existe en la fuente."
        ),
        "observation_levels": [
            "year",
            "month",
            "procurement",
            "item",
            "company",
        ],
        "observation_level_columns": {
            "ano": "year",
            "mes": "month",
            "codigo_licitacion": "procurement",
            "codigo_item": "item",
            "codigo_proveedor": "company",
        },
    },
    {
        "slug": "dicionario",
        "auxiliary_files": False,
        "raw_source": None,
        "name_pt": "Dicionário",
        "name_en": "Dictionary",
        "name_es": "Diccionario",
        "description_pt": (
            "Dicionário de valores codificados das tabelas do conjunto, com a tradução "
            "de cada chave para o rótulo correspondente. Os rótulos são derivados dos "
            "próprios dados, já que a maioria das colunas codificadas vem acompanhada da "
            "sua coluna de rótulo na mesma linha, e complementados pelas definições que "
            "a ChileCompra publica apenas na sua página de Definiciones."
        ),
        "description_en": (
            "Dictionary of the coded values used in this dataset's tables, mapping each "
            "key to its label. The labels are derived from the data itself, since most "
            "coded columns ship alongside their own label column on the same row, and "
            "supplemented with the definitions ChileCompra publishes only on its "
            "Definiciones page."
        ),
        "description_es": (
            "Diccionario de los valores codificados de las tablas de este conjunto, con "
            "la traducción de cada llave a su rótulo correspondiente. Los rótulos se "
            "derivan de los propios datos, ya que la mayoría de las columnas codificadas "
            "vienen acompañadas de su columna de rótulo en la misma fila, y se "
            "complementan con las definiciones que ChileCompra publica únicamente en su "
            "página de Definiciones."
        ),
        "observation_levels": [],
        "observation_level_columns": {},
    },
]

# ``ano`` and ``mes`` are the hive partitions of the staging parquet and the columns
# a reader filters on to prune; both carry the flag.
PARTITION_COLUMNS = {"ano", "mes"}

DICIONARIO_COLUMNS = [
    {
        "name": "id_tabela",
        "bigquery_type": "STRING",
        "description_pt": "Nome da tabela a que se refere a chave",
        "description_en": "Name of the table the key belongs to",
        "description_es": "Nombre de la tabla a la que pertenece la clave",
    },
    {
        "name": "nome_coluna",
        "bigquery_type": "STRING",
        "description_pt": "Nome da coluna a que se refere a chave",
        "description_en": "Name of the column the key belongs to",
        "description_es": "Nombre de la columna a la que pertenece la clave",
    },
    {
        "name": "chave",
        "bigquery_type": "STRING",
        "description_pt": "Valor codificado tal como aparece na coluna",
        "description_en": "Coded value as it appears in the column",
        "description_es": "Valor codificado tal como aparece en la columna",
    },
    {
        "name": "cobertura_temporal",
        "bigquery_type": "STRING",
        "description_pt": "Cobertura temporal da chave",
        "description_en": "Temporal coverage of the key",
        "description_es": "Cobertura temporal de la clave",
    },
    {
        "name": "valor",
        "bigquery_type": "STRING",
        "description_pt": "Rótulo correspondente à chave",
        "description_en": "Label the key stands for",
        "description_es": "Rótulo correspondiente a la clave",
    },
]
