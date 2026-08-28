"""Trilingual table metadata and observation levels for the backend registration.

The Portuguese table descriptions live in dbt_spec (they are also the dbt model
descriptions); this module adds the names, the English and Spanish renderings,
the observation levels and the temporal coverage the backend needs.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class TableMeta:
    name_pt: str
    name_en: str
    name_es: str
    description_en: str
    description_es: str
    #: entity slug -> the column that identifies that level, or "" if none
    observation_levels: dict[str, str] = field(default_factory=dict)
    #: inclusive coverage as (start_year, start_month, end_year, end_month);
    #: months are None for annual grain
    coverage: tuple[int, int | None, int, int | None] | None = None


# Registries carry no temporal series of their own -- they are a snapshot of the
# current state of the register, stamped with the extraction date.
SNAPSHOT_COVERAGE = (2026, 8, 2026, 8)

TABLES: dict[str, TableMeta] = {
    "contratacao": TableMeta(
        "Contratação",
        "Procurement",
        "Contratación",
        "Public procurements run under Law 14,133/2021 and published on PNCP through "
        "Compras.gov.br, from 2021 onwards. One row per procurement, covering all three levels "
        "of government",
        "Contrataciones públicas realizadas bajo la Ley 14.133/2021 y divulgadas en el PNCP a "
        "través de Compras.gov.br, desde 2021. Una fila por contratación, abarcando los tres "
        "niveles de gobierno",
        {"procurement": "numero_controle_pncp", "year": "ano"},
        (2021, 1, 2026, 7),
    ),
    "contratacao_item": TableMeta(
        "Item da contratação",
        "Procurement item",
        "Ítem de la contratación",
        "Items of the procurements run under Law 14,133/2021. One row per item, with quantity, "
        "estimated value and, once determined, the result",
        "Ítems de las contrataciones realizadas bajo la Ley 14.133/2021. Una fila por ítem, con "
        "cantidad, valor estimado y, cuando ya está determinado, el resultado",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        (2021, 1, 2026, 7),
    ),
    "contratacao_item_resultado": TableMeta(
        "Resultado do item da contratação",
        "Procurement item result",
        "Resultado del ítem de la contratación",
        "Results of the items of Law 14,133/2021 procurements. One row per supplier ranked on "
        "each item, with the quantity and value awarded",
        "Resultados de los ítems de las contrataciones de la Ley 14.133/2021. Una fila por "
        "proveedor clasificado en cada ítem, con la cantidad y el valor homologados",
        {"item": "id_compra_item", "company": "id_fornecedor", "year": "ano"},
        (2021, 1, 2026, 7),
    ),
    "ata_registro_preco": TableMeta(
        "Ata de registro de preços",
        "Price record",
        "Acta de registro de precios",
        "Price records established from Law 14,133/2021 procurements. One row per price record",
        "Actas de registro de precios establecidas a partir de contrataciones de la Ley "
        "14.133/2021. Una fila por acta",
        {"agreement": "numero_controle_pncp_ata", "year": "ano"},
        (2023, 1, 2027, 12),
    ),
    "ata_registro_preco_item": TableMeta(
        "Item da ata de registro de preços",
        "Price record item",
        "Ítem del acta de registro de precios",
        "Items of the price records, with the registered supplier, the unit price and the "
        "piggyback limit. One row per supplier ranked on each item",
        "Ítems de las actas de registro de precios, con el proveedor registrado, el precio "
        "unitario y el límite de adhesión. Una fila por proveedor clasificado en cada ítem",
        {
            "agreement": "numero_controle_pncp_ata",
            "item": "numero_item",
            "year": "ano",
        },
        (2023, 1, 2027, 12),
    ),
    "contrato": TableMeta(
        "Contrato",
        "Contract",
        "Contrato",
        "Administrative contracts recorded in SIASG, from 2010 onwards. One row per contract, "
        "with its term, supplier and values",
        "Contratos administrativos registrados en el SIASG, desde 2010. Una fila por contrato, "
        "con su vigencia, proveedor y valores",
        {"contract": "numero_contrato", "year": "ano"},
        (2010, 1, 2026, 7),
    ),
    "contrato_item": TableMeta(
        "Item do contrato",
        "Contract item",
        "Ítem del contrato",
        "Items of the administrative contracts recorded in SIASG. One row per contracted item, "
        "with quantity and unit and total values",
        "Ítems de los contratos administrativos registrados en el SIASG. Una fila por ítem "
        "contratado, con cantidad y valores unitario y total",
        {"contract": "numero_contrato", "item": "numero_item", "year": "ano"},
        (2010, 1, 2026, 7),
    ),
    "licitacao": TableMeta(
        "Licitação",
        "Tender",
        "Licitación",
        "Tenders run under Law 8,666/1993 and earlier legislation, from 1997 to 2025. One row "
        "per tender, across every modality",
        "Licitaciones realizadas bajo la Ley 8.666/1993 y legislación anterior, de 1997 a 2025. "
        "Una fila por licitación, en todas las modalidades",
        {"procurement": "id_compra", "year": "ano"},
        (1997, 1, 2025, 12),
    ),
    "licitacao_pregao": TableMeta(
        "Pregão",
        "Reverse auction",
        "Pregón",
        "Procedural detail of the reverse auctions run under Law 8,666/1993, including the "
        "appointing order, status and the closing and result dates. One row per reverse auction",
        "Detalle procesal de los pregones realizados bajo la Ley 8.666/1993, incluyendo "
        "resolución, situación y fechas de cierre y resultado. Una fila por pregón",
        {"procurement": "id_compra", "year": "ano"},
        (2000, 1, 2023, 12),
    ),
    "licitacao_item": TableMeta(
        "Item da licitação",
        "Tender item",
        "Ítem de la licitación",
        "Items of the tenders run under Law 8,666/1993. One row per tendered item, with "
        "quantity, estimated value and the winning supplier",
        "Ítems de las licitaciones realizadas bajo la Ley 8.666/1993. Una fila por ítem "
        "licitado, con cantidad, valor estimado y proveedor adjudicatario",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        (1997, 1, 2025, 12),
    ),
    "licitacao_item_pregao": TableMeta(
        "Item do pregão",
        "Reverse auction item",
        "Ítem del pregón",
        "Result of the items of Law 8,666/1993 reverse auctions, with the lowest bid, the "
        "negotiated value and the awarded value. One row per awarded item",
        "Resultado de los ítems de los pregones de la Ley 8.666/1993, con la menor oferta, el "
        "valor negociado y el valor homologado. Una fila por ítem homologado",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        (2000, 1, 2025, 12),
    ),
    "compra_sem_licitacao": TableMeta(
        "Contratação direta",
        "Direct contracting",
        "Contratación directa",
        "Waivers and non-enforceability of tender under Law 8,666/1993, from 1997 to 2024. One "
        "row per direct contracting, with its legal basis and justification",
        "Dispensas e inexigibilidades de licitación bajo la Ley 8.666/1993, de 1997 a 2024. Una "
        "fila por contratación directa, con su fundamento legal y justificación",
        {"procurement": "id_compra", "year": "ano"},
        (1997, 1, 2024, 12),
    ),
    "compra_sem_licitacao_item": TableMeta(
        "Item da contratação direta",
        "Direct contracting item",
        "Ítem de la contratación directa",
        "Items of the waivers and non-enforceability of tender under Law 8,666/1993. One row "
        "per item, with the winning supplier and the estimated value",
        "Ítems de las dispensas e inexigibilidades de licitación bajo la Ley 8.666/1993. Una "
        "fila por ítem, con el proveedor adjudicatario y el valor estimado",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        (1997, 1, 2024, 12),
    ),
    "orgao": TableMeta(
        "Órgão",
        "Government body",
        "Órgano",
        "Register of the bodies recorded in SIASG, with their administrative hierarchy, "
        "federative sphere and branch of government. One row per body in each extraction",
        "Registro de los órganos inscritos en el SIASG, con su jerarquía administrativa, esfera "
        "federativa y poder. Una fila por órgano en cada extracción",
        {"agency": "codigo_orgao"},
        SNAPSHOT_COVERAGE,
    ),
    "unidade_administrativa": TableMeta(
        "Unidade administrativa",
        "Administrative unit",
        "Unidad administrativa",
        "Register of the administrative purchasing units (UASG), the units that actually buy. "
        "One row per unit in each extraction",
        "Registro de las Unidades Administrativas de Servicios Generales (UASG), las unidades "
        "que efectivamente compran. Una fila por unidad en cada extracción",
        {"agency": "codigo_uasg"},
        SNAPSHOT_COVERAGE,
    ),
    "fornecedor": TableMeta(
        "Fornecedor",
        "Supplier",
        "Proveedor",
        "Register of SICAF suppliers, with their economic activity, size band and legal nature. "
        "One row per supplier in each extraction",
        "Registro de proveedores del SICAF, con su actividad económica, tamaño y naturaleza "
        "jurídica. Una fila por proveedor en cada extracción",
        {"company": "cnpj"},
        SNAPSHOT_COVERAGE,
    ),
    "catalogo_material": TableMeta(
        "Catálogo de materiais",
        "Material catalogue",
        "Catálogo de materiales",
        "Material catalogue (CATMAT), with the hierarchy of group, class and descriptive "
        "standard. One row per material item",
        "Catálogo de Materiales (CATMAT), con la jerarquía de grupo, clase y patrón "
        "descriptivo. Una fila por artículo de material",
        {"product": "codigo_item"},
        SNAPSHOT_COVERAGE,
    ),
    "catalogo_servico": TableMeta(
        "Catálogo de serviços",
        "Service catalogue",
        "Catálogo de servicios",
        "Service catalogue (CATSER), with the hierarchy of section, division, group and class. "
        "One row per service item",
        "Catálogo de Servicios (CATSER), con la jerarquía de sección, división, grupo y clase. "
        "Una fila por artículo de servicio",
        {"product": "codigo_servico"},
        SNAPSHOT_COVERAGE,
    ),
    "dicionario": TableMeta(
        "Dicionário",
        "Dictionary",
        "Diccionario",
        "Dictionary of the dataset's coded columns, mapping each key to its meaning",
        "Diccionario de las columnas codificadas del conjunto, relacionando cada clave con su "
        "significado",
        {},
        None,
    ),
}

DATASET = {
    "slug": "compras_publicas",
    "name_pt": "Compras públicas",
    "name_en": "Public procurement",
    "name_es": "Compras públicas",
    "description_pt": (
        "Compras públicas brasileiras registradas no Compras.gov.br, abrangendo os dois regimes "
        "de contratação: as contratações realizadas sob a Lei 14.133/2021 e divulgadas no PNCP, "
        "de 2021 em diante, e as licitações e contratações diretas sob a Lei 8.666/1993, de "
        "1997 a 2025. Inclui itens, resultados, atas de registro de preços, contratos e os "
        "cadastros de órgãos, unidades compradoras, fornecedores e catálogos de materiais e "
        "serviços. Apesar do nome do portal, cobre os três níveis de governo: 43% das "
        "contratações são federais, 31% estaduais e 25% municipais."
    ),
    "description_en": (
        "Brazilian public procurement recorded in Compras.gov.br, covering both contracting "
        "regimes: procurements run under Law 14,133/2021 and published on PNCP from 2021 "
        "onwards, and tenders and direct contracting under Law 8,666/1993 from 1997 to 2025. "
        "Includes items, results, price records, contracts and the registers of bodies, "
        "purchasing units, suppliers and the material and service catalogues. Despite the "
        "portal's name it covers all three levels of government: 43% of procurements are "
        "federal, 31% state and 25% municipal."
    ),
    "description_es": (
        "Compras públicas brasileñas registradas en Compras.gov.br, abarcando los dos regímenes "
        "de contratación: las contrataciones realizadas bajo la Ley 14.133/2021 y divulgadas en "
        "el PNCP, desde 2021, y las licitaciones y contrataciones directas bajo la Ley "
        "8.666/1993, de 1997 a 2025. Incluye ítems, resultados, actas de registro de precios, "
        "contratos y los registros de órganos, unidades compradoras, proveedores y los "
        "catálogos de materiales y servicios. Pese al nombre del portal, cubre los tres niveles "
        "de gobierno: 43% de las contrataciones son federales, 31% estatales y 25% municipales."
    ),
    "themes": ["government", "economics"],
    "tags": [
        "licitacao",
        "contrato",
        "compra",
        "administracao_publica",
        "gasto",
        "transparencia",
        "preco",
        "empresa",
    ],
}

#: Order the tables are presented on the site: the 14.133 regime first, then the
#: legado series, then the registers the two share.
TABLE_ORDER = [
    "contratacao",
    "contratacao_item",
    "contratacao_item_resultado",
    "ata_registro_preco",
    "ata_registro_preco_item",
    "contrato",
    "contrato_item",
    "licitacao",
    "licitacao_pregao",
    "licitacao_item",
    "licitacao_item_pregao",
    "compra_sem_licitacao",
    "compra_sem_licitacao_item",
    "orgao",
    "unidade_administrativa",
    "fornecedor",
    "catalogo_material",
    "catalogo_servico",
    "dicionario",
]
