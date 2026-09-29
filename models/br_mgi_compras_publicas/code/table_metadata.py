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
        "Contratação - Item",
        "Procurement - Item",
        "Contratación - Ítem",
        "Items of the procurements run under Law 14,133/2021. One row per item, with quantity, "
        "estimated value and, once determined, the result",
        "Ítems de las contrataciones realizadas bajo la Ley 14.133/2021. Una fila por ítem, con "
        "cantidad, valor estimado y, cuando ya está determinado, el resultado",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        (2021, 1, 2026, 7),
    ),
    "contratacao_item_resultado": TableMeta(
        "Contratação - Item - Resultado",
        "Procurement - Item - Result",
        "Contratación - Ítem - Resultado",
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
        "Price records established from Law 14,133/2021 procurements. One row per price "
        "record, in the most recent state reported by the source",
        "Actas de registro de precios establecidas a partir de contrataciones de la Ley "
        "14.133/2021. Una fila por acta, en el estado más reciente informado por la fuente",
        {"agreement": "numero_controle_pncp_ata", "year": "ano"},
        (2023, 1, 2027, 12),
    ),
    "ata_registro_preco_item": TableMeta(
        "Ata de registro de preços - Item",
        "Price record - Item",
        "Acta de registro de precios - Ítem",
        "Items of the price records, with the registered supplier, the unit price and the "
        "piggyback limit. One row per supplier ranked on each item, in the most recent state "
        "reported by the source",
        "Ítems de las actas de registro de precios, con el proveedor registrado, el precio "
        "unitario y el límite de adhesión. Una fila por proveedor clasificado en cada ítem, en "
        "el estado más reciente informado por la fuente",
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
        "with its term, supplier and values. A management unit reuses a contract number across "
        "procurements, so the key includes the originating procurement",
        "Contratos administrativos registrados en el SIASG, desde 2010. Una fila por contrato, "
        "con su vigencia, proveedor y valores. Una unidad gestora reutiliza el número del "
        "contrato entre contrataciones, por lo que la clave incluye la compra de origen",
        {"contract": "numero_contrato", "year": "ano"},
        # data_vigencia_inicial, non-null on every row: 2010-01-01 to 2027-01-01.
        # The end is genuinely in the future -- contracts are registered with a
        # start date ahead of today, tapering from 15,629 rows in 2026-08 to a
        # single one in 2027-01. The previous 2026-07 was the refresh date, not
        # the data.
        (2010, 1, 2027, 1),
    ),
    "contrato_item": TableMeta(
        "Contrato - Item",
        "Contract - Item",
        "Contrato - Ítem",
        "Items of the administrative contracts recorded in SIASG. One row per contracted item, "
        "with quantity and unit and total values, in the most recent state reported by the "
        "source",
        "Ítems de los contratos administrativos registrados en el SIASG. Una fila por ítem "
        "contratado, con cantidad y valores unitario y total, en el estado más reciente "
        "informado por la fuente",
        {"contract": "numero_contrato", "item": "numero_item", "year": "ano"},
        # Same basis and the same single forward-dated row as contrato:
        # data_vigencia_inicial, non-null throughout, 2010-01-01 to 2027-01-01.
        (2010, 1, 2027, 1),
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
        "Licitação - Pregão",
        "Tender - Reverse auction",
        "Licitación - Pregón",
        "Procedural detail of the reverse auctions run under Law 8,666/1993, including the "
        "appointing order, status and the closing and result dates. One row per reverse auction",
        "Detalle procesal de los pregones realizados bajo la Ley 8.666/1993, incluyendo "
        "resolución, situación y fechas de cierre y resultado. Una fila por pregón",
        {"procurement": "id_compra", "year": "ano"},
        # data_edital, which is non-null on every row: 2000-12-29 to 2024-07-12.
        (2000, 12, 2024, 7),
    ),
    "licitacao_item": TableMeta(
        "Licitação - Item",
        "Tender - Item",
        "Licitación - Ítem",
        "Items of the tenders run under Law 8,666/1993. One row per tendered item, with "
        "quantity, estimated value and the winning supplier. 58,890 items (1.23%) are "
        "missing because the source does not serve them: 23,690 from Convite (0.7% of the "
        "modality), 22,183 from Concorrência (6.0%), and the whole of Concorrência "
        "Internacional (7,366) and Concurso (5,651), which therefore do not appear in this "
        "table at all. Items of the Pregão, Dispensa and Inexigibilidade modalities are in "
        "the licitacao_item_pregao and compra_sem_licitacao_item tables",
        "Ítems de las licitaciones realizadas bajo la Ley 8.666/1993. Una fila por ítem "
        "licitado, con cantidad, valor estimado y proveedor adjudicatario. Faltan 58.890 "
        "ítems (1,23%) que la fuente no entrega: 23.690 de Convite (0,7% de la modalidad), "
        "22.183 de Concorrência (6,0%), y la totalidad de Concorrência Internacional "
        "(7.366) y de Concurso (5.651), que por lo tanto no aparecen en esta tabla. Los "
        "ítems de las modalidades Pregón, Dispensa e Inexigibilidad están en las tablas "
        "licitacao_item_pregao y compra_sem_licitacao_item",
        {"procurement": "id_compra", "item": "id_compra_item", "year": "ano"},
        # Year grain: the source publishes no date for the item, so `ano` is
        # derived from the last four digits of id_compra and is all we know.
        # 1997 and 2023 are the verifiable bounds, not the raw min and max of
        # `ano` (1990-2024). All 3,275 rows below 1997 are orphans with no
        # parent licitacao, and the single 2024 row's own parent says 2023 --
        # both tails are artefacts of the id derivation, not coverage.
        (1997, None, 2023, None),
    ),
    "licitacao_item_pregao": TableMeta(
        "Licitação - Pregão - Item",
        "Tender - Reverse auction - Item",
        "Licitación - Pregón - Ítem",
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
        "Waivers and non-enforceability of tender under Law 8,666/1993, from 1997 to 2025. One "
        "row per direct contracting, with its legal basis and justification",
        "Dispensas e inexigibilidades de licitación bajo la Ley 8.666/1993, de 1997 a 2025. Una "
        "fila por contratación directa, con su fundamento legal y justificación",
        {"procurement": "id_compra", "year": "ano"},
        # Year grain: the only trustworthy temporal field is `ano`
        # (dt_ano_aviso, given by the source), 1997-2025. The date columns are
        # not usable as a basis -- data_publicacao is null on 99.5% of rows and
        # data_declaracao_dispensa reaches back to 1979 on 24 of them.
        (1997, None, 2025, None),
    ),
    "compra_sem_licitacao_item": TableMeta(
        "Contratação direta - Item",
        "Direct contracting - Item",
        "Contratación directa - Ítem",
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
        "Catálogo - Materiais",
        "Catalogue - Materials",
        "Catálogo - Materiales",
        "Material catalogue (CATMAT), with the hierarchy of group, class and descriptive "
        "standard. One row per material item",
        "Catálogo de Materiales (CATMAT), con la jerarquía de grupo, clase y patrón "
        "descriptivo. Una fila por artículo de material",
        {"product": "codigo_item"},
        SNAPSHOT_COVERAGE,
    ),
    "catalogo_servico": TableMeta(
        "Catálogo - Serviços",
        "Catalogue - Services",
        "Catálogo - Servicios",
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
    # The two ComprasNet legado tables. Scraped from the award decisions and the
    # result-by-supplier pages, which no Compras.gov.br API exposes, so their
    # coverage ends with the legado system rather than with the Lei 8.666 series.
    "pregao_item_oferta": TableMeta(
        "Licitação - Pregão - Item - Proposta vencedora",
        "Tender - Reverse auction - Item - Winning offer",
        "Licitación - Pregón - Ítem - Propuesta ganadora",
        "Winning offers by item for the electronic reverse auctions run under Law 8.666, with "
        "the brand, manufacturer, model and detailed description of the object actually offered "
        "by the supplier, plus the unit and total price. One row per item and winning supplier. "
        "The data come from ComprasNet's result-by-supplier pages and exist in no "
        "Compras.gov.br API. Joins to licitacao_pregao and licitacao_item_pregao on id_compra. "
        "Coverage runs from 2004 to 2024: for earlier reverse auctions the source answers that "
        "no result is published, and from 2005 the page is filled in for most of them.",
        "Propuestas ganadoras por ítem de los pregones electrónicos realizados bajo la Ley "
        "8.666, con marca, fabricante, modelo y la descripción detallada del objeto "
        "efectivamente ofrecido por el proveedor, además del valor unitario y del valor global. "
        "Una fila por ítem y proveedor ganador. Los datos provienen de las páginas de resultado "
        "por proveedor de ComprasNet y no existen en ninguna API de Compras.gov.br. Se une a "
        "licitacao_pregao y licitacao_item_pregao por la columna id_compra. Cobertura de 2004 a "
        "2024: para pregones anteriores la fuente responde que no existe resultado publicado, y "
        "a partir de 2005 la página está completa en la mayoría de ellos.",
        {
            "procurement": "id_compra",
            "item": "numero_item",
            "company": "cnpj_cpf_fornecedor",
            "year": "ano",
        },
        (2004, None, 2024, None),
    ),
    "pregao_item_evento": TableMeta(
        "Licitação - Pregão - Item - Evento",
        "Tender - Reverse auction - Item - Event",
        "Licitación - Pregón - Ítem - Evento",
        "Event timeline by item for the electronic reverse auctions run under Law 8.666, with "
        "the date, time and responsible public official for each award, homologation, "
        "cancellation and return to a previous phase. One row per item event. It includes items "
        "that were cancelled, deserted or declared unsuccessful, which licitacao_item_pregao "
        "does not cover because its source keys on the homologation date. The data come from "
        "ComprasNet's award decisions and exist in no Compras.gov.br API. Joins to "
        "licitacao_pregao and licitacao_item_pregao on id_compra. Coverage runs from 2002 to "
        "2024: the 2001 reverse auctions have no published award decision.",
        "Línea de tiempo de eventos por ítem de los pregones electrónicos realizados bajo la "
        "Ley 8.666, con fecha, hora y agente público responsable de cada adjudicación, "
        "homologación, cancelación y vuelta de fase. Una fila por evento de ítem. Incluye ítems "
        "cancelados, desiertos y fracasados, que la tabla licitacao_item_pregao no cubre porque "
        "su fuente depende de la fecha de homologación. Los datos provienen de las actas de "
        "homologación de ComprasNet y no existen en ninguna API de Compras.gov.br. Se une a "
        "licitacao_pregao y licitacao_item_pregao por la columna id_compra. Cobertura de 2002 a "
        "2024: los pregones de 2001 no tienen acta de homologación publicada.",
        {
            "procurement": "id_compra",
            "item": "numero_item",
            "act": "ordem_evento",
            "year": "ano",
        },
        (2002, None, 2024, None),
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
    # Staging carries Portuguese tag slugs while prod carries English ones, so
    # the same concepts resolve under different names. Keyed by environment
    # rather than translated at the call site, because a missing tag aborts
    # registration outright.
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
    "tags_prod": [
        "public_procurement",
        "contract",
        "purchase",
        "public_administration",
        "spending",
        "transparency",
        "price",
        "company",
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
    "licitacao_item",
    "licitacao_pregao",
    "licitacao_item_pregao",
    # The two ComprasNet legado tables sit with the pregao they describe, not at
    # the end where they were first appended: they join licitacao_pregao and
    # licitacao_item_pregao on id_compra and are read alongside them.
    "pregao_item_oferta",
    "pregao_item_evento",
    "compra_sem_licitacao",
    "compra_sem_licitacao_item",
    "orgao",
    "unidade_administrativa",
    "fornecedor",
    "catalogo_material",
    "catalogo_servico",
    "dicionario",
]

#: table -> (entity slug, frequency) for the table-anchored Update record, i.e.
#: how often *we* refresh the table, not how often the source publishes.
#:
#: Daily for the Lei 14.133 modules and the contract register, which the
#: `br_mgi_compras_publicas.diario` flow refreshes (cron 37 5 * * *). Weekly for
#: the registries and the dicionario, which `…semanal` re-snapshots
#: (cron 12 4 * * 0).
#:
#: The eight archive tables carry NO frequency. They were recorded as weekly on
#: the assumption that a flow would later refresh them, but the Lei 8.666 legado
#: stopped publishing in mid-2025 and there will be no refresh to declare:
#:
#:   /modulo-legado/1_consultarLicitacao, by publication month
#:     2025-01: 1,631   2025-03: 1,566   2025-05: 951
#:     2025-07: 0       2025-09: 0       every month of 2026: 0
#:   /modulo-legado/5_consultarComprasSemLicitacao, whole year
#:     2025: 24,382     2026: 0
#:
#: A weekly claim on the site would also be expensive to honour rather than
#: merely wrong: /modulo-legado/2_consultarItemLicitacao, which feeds
#: licitacao_item, has no date filter at all -- modalidade=5 alone returns
#: 32,397,345 records -- so a refresh must re-read tens of millions of rows to
#: discover nothing changed. The same applies to the two ComprasNet tables,
#: scraped from the legacy web UI and ending in 2024.
#:
#: `frequency=None` is "no declared cadence", which is what a closed archive
#: has. `latest` still means what it always did -- when we last refreshed the
#: table -- so the record stays useful. Prod already holds 37 such Updates
#: (censo_demografico). The MCP types `frequency` as `int`, but the backend
#: field is nullable and accepts None; verified against staging and prod.
UPDATE_CADENCE: dict[str, tuple[str, int | None]] = {
    "contratacao": ("day", 1),
    "contratacao_item": ("day", 1),
    "contratacao_item_resultado": ("day", 1),
    "ata_registro_preco": ("day", 1),
    "ata_registro_preco_item": ("day", 1),
    "contrato": ("day", 1),
    "contrato_item": ("day", 1),
    "orgao": ("week", 1),
    "unidade_administrativa": ("week", 1),
    "fornecedor": ("week", 1),
    "catalogo_material": ("week", 1),
    "catalogo_servico": ("week", 1),
    "dicionario": ("week", 1),
    "licitacao": ("year", None),
    "licitacao_item": ("year", None),
    "licitacao_pregao": ("year", None),
    "licitacao_item_pregao": ("year", None),
    "compra_sem_licitacao": ("year", None),
    "compra_sem_licitacao_item": ("year", None),
    "pregao_item_oferta": ("year", None),
    "pregao_item_evento": ("year", None),
}
