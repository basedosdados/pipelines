"""
Architecture (single source of truth) for br_sfb_sicar (Cadastro Ambiental Rural).

Drives: the cleaning transform, the dbt models + schema.yml, and the backend
metadata column registration. Nine spatial theme tables + one dictionary.

Snapshot-stacked design:
  - partition column `data` = per-UF snapshot (disponibilização) date, DATE.
  - cluster by sigla_uf.
  - geometry stored as GEOGRAPHY (reprojected SIRGAS 2000 -> WGS84, make_valid).

Each column entry:
  name, src (raw shapefile column or a derived token), type,
  desc_pt, desc_en, desc_es, unit, directory, dict (covered_by_dictionary),
  partition (bool)

Derived tokens (src):
  __snapshot__   -> per-UF release date (data)
  __uf_cod__     -> from cod_estado (area_imovel) — 2-letter UF
  __uf_split__   -> sigla from cod_imovel prefix (overlays lack cod_estado)
  __muni_split__ -> IBGE code from cod_imovel: split('-')[1]
  __wkt__        -> geometry -> WGS84 WKT
"""

# ---- reusable column builders ---------------------------------------------

DIR_UF = "br_bd_diretorios_brasil.uf:sigla"
DIR_MUNI = "br_bd_diretorios_brasil.municipio:id_municipio"
DIR_DATA = "br_bd_diretorios_data_tempo.data:data"


def c(
    name,
    src,
    type,
    desc_pt,
    desc_en,
    desc_es,
    unit="",
    directory="",
    dict=False,
    partition=False,
):
    return dict_entry(
        name,
        src,
        type,
        desc_pt,
        desc_en,
        desc_es,
        unit,
        directory,
        dict,
        partition,
    )


def dict_entry(
    name,
    src,
    type,
    desc_pt,
    desc_en,
    desc_es,
    unit,
    directory,
    covered,
    partition,
):
    return {
        "name": name,
        "src": src,
        "type": type,
        "desc_pt": desc_pt,
        "desc_en": desc_en,
        "desc_es": desc_es,
        "unit": unit,
        "directory": directory,
        "covered_by_dictionary": "yes" if covered else "no",
        "is_partition": partition,
    }


COL_DATA = c(
    "data",
    "__snapshot__",
    "DATE",
    "Data de disponibilização (snapshot) da base do CAR para a Unidade da Federação",
    "Release (snapshot) date of the CAR database for the state",
    "Fecha de disponibilización (snapshot) de la base del CAR para la unidad federativa",
    directory=DIR_DATA,
    partition=True,
)

# sigla_uf is derived from the cod_imovel prefix everywhere (authoritative CAR
# key: `UF-IBGE7-HASH`), so it stays internally consistent with id_municipio.
COL_UF_IMOVEL = c(
    "sigla_uf",
    "__uf_split__",
    "STRING",
    "Sigla da Unidade da Federação onde se localiza o imóvel",
    "Abbreviation of the state where the property is located",
    "Sigla de la unidad federativa donde se ubica el inmueble",
    directory=DIR_UF,
)

COL_UF_OVERLAY = COL_UF_IMOVEL

COL_MUNI = c(
    "id_municipio",
    "__muni_split__",
    "STRING",
    "Identificador do município (IBGE, 7 dígitos) onde se localiza o imóvel",
    "Municipality identifier (IBGE, 7 digits) where the property is located",
    "Identificador del municipio (IBGE, 7 dígitos) donde se ubica el inmueble",
    directory=DIR_MUNI,
)

COL_ID_IMOVEL = c(
    "id_imovel",
    "cod_imovel",
    "STRING",
    "Código de inscrição do imóvel no Cadastro Ambiental Rural (CAR)",
    "Registration code of the property in the Rural Environmental Registry (CAR)",
    "Código de inscripción del inmueble en el Catastro Ambiental Rural (CAR)",
)

COL_STATUS = c(
    "status",
    "ind_status",
    "STRING",
    "Situação do cadastro no CAR (AT Ativo, PE Pendente, SU Suspenso, CA Cancelado)",
    "Registration status in the CAR (AT Active, PE Pending, SU Suspended, CA Cancelled)",
    "Situación del registro en el CAR (AT Activo, PE Pendiente, SU Suspendido, CA Cancelado)",
    dict=True,
)

COL_CONDICAO = c(
    "condicao",
    "des_condic",
    "STRING",
    "Condição atual da análise do registro no fluxo de validação pelo órgão competente",
    "Current condition of the record analysis in the validation workflow by the competent agency",
    "Condición actual del análisis del registro en el flujo de validación por el órgano competente",
)

COL_GEO = c(
    "geometria",
    "__wkt__",
    "GEOGRAPHY",
    "Geometria do polígono em coordenadas geográficas (WGS84)",
    "Polygon geometry in geographic coordinates (WGS84)",
    "Geometría del polígono en coordenadas geográficas (WGS84)",
)


def area_col(desc_pt, desc_en, desc_es):
    return c(
        "area",
        "num_area",
        "FLOAT64",
        desc_pt,
        desc_en,
        desc_es,
        unit="hectare",
    )


def tipo_col(desc_pt, desc_en, desc_es, src="cod_tema"):
    return c("tipo", src, "STRING", desc_pt, desc_en, desc_es, dict=True)


# ---- table definitions -----------------------------------------------------

AREA_IMOVEL = [
    COL_DATA,
    COL_UF_IMOVEL,
    COL_MUNI,
    COL_ID_IMOVEL,
    tipo_col(
        "Tipo do imóvel rural (IRU Imóvel Rural, AST Assentamento de Reforma Agrária, PCT Povos e Comunidades Tradicionais)",
        "Type of rural property (IRU Rural Property, AST Agrarian Reform Settlement, PCT Traditional Peoples and Communities)",
        "Tipo del inmueble rural (IRU Inmueble Rural, AST Asentamiento de Reforma Agraria, PCT Pueblos y Comunidades Tradicionales)",
        src="ind_tipo",
    ),
    COL_STATUS,
    COL_CONDICAO,
    area_col(
        "Área total do imóvel declarada no CAR",
        "Total property area declared in the CAR",
        "Área total del inmueble declarada en el CAR",
    ),
    c(
        "modulos_fiscais",
        "mod_fiscal",
        "FLOAT64",
        "Tamanho do imóvel expresso em módulos fiscais",
        "Property size expressed in fiscal modules",
        "Tamaño del inmueble expresado en módulos fiscales",
        unit="modulo fiscal",
    ),
    c(
        "data_criacao",
        "dat_criaca",
        "DATE",
        "Data de inscrição do imóvel no CAR",
        "Date the property was registered in the CAR",
        "Fecha de inscripción del inmueble en el CAR",
    ),
    c(
        "data_atualizacao",
        "dat_atuali",
        "DATE",
        "Data da última modificação dos dados do imóvel no CAR",
        "Date of the last modification of the property data in the CAR",
        "Fecha de la última modificación de los datos del inmueble en el CAR",
    ),
    COL_GEO,
]


def overlay(tipo_pt, tipo_en, tipo_es, with_area=True):
    cols = [
        COL_DATA,
        COL_UF_OVERLAY,
        COL_MUNI,
        COL_ID_IMOVEL,
        tipo_col(tipo_pt, tipo_en, tipo_es),
        COL_STATUS,
        COL_CONDICAO,
    ]
    if with_area:
        cols.append(
            area_col(
                "Área do polígono do tema",
                "Area of the theme polygon",
                "Área del polígono del tema",
            )
        )
    cols.append(COL_GEO)
    return cols


TABLES = {
    "area_imovel": AREA_IMOVEL,
    "app": overlay(
        "Tipo de Área de Preservação Permanente (APP)",
        "Type of Permanent Preservation Area (APP)",
        "Tipo de Área de Preservación Permanente (APP)",
    ),
    "reserva_legal": overlay(
        "Situação da Reserva Legal (proposta, averbada, aprovada e não averbada)",
        "Legal Reserve status (proposed, recorded, approved and not recorded)",
        "Situación de la Reserva Legal (propuesta, registrada, aprobada y no registrada)",
    ),
    "vegetacao_nativa": overlay(
        "Tipo de remanescente de vegetação nativa",
        "Type of native vegetation remnant",
        "Tipo de remanente de vegetación nativa",
    ),
    "area_consolidada": overlay(
        "Tipo de área rural consolidada",
        "Type of consolidated rural area",
        "Tipo de área rural consolidada",
    ),
    "area_pousio": overlay(
        "Tipo de área de pousio",
        "Type of fallow area",
        "Tipo de área de barbecho",
    ),
    "uso_restrito": overlay(
        "Tipo de área de uso restrito",
        "Type of restricted-use area",
        "Tipo de área de uso restringido",
    ),
    "servidao_administrativa": overlay(
        "Tipo de servidão administrativa",
        "Type of administrative easement",
        "Tipo de servidumbre administrativa",
    ),
    "hidrografia": overlay(
        "Tipo de corpo hídrico (curso d'água, lago, reservatório)",
        "Type of water body (watercourse, lake, reservoir)",
        "Tipo de cuerpo hídrico (curso de agua, lago, embalse)",
        with_area=False,
    ),
}

DICIONARIO = [
    c(
        "id_tabela",
        "id_tabela",
        "STRING",
        "Nome da tabela",
        "Table name",
        "Nombre de la tabla",
    ),
    c(
        "nome_coluna",
        "nome_coluna",
        "STRING",
        "Nome da coluna",
        "Column name",
        "Nombre de la columna",
    ),
    c(
        "chave",
        "chave",
        "STRING",
        "Chave (valor codificado)",
        "Key (coded value)",
        "Clave (valor codificado)",
    ),
    c(
        "cobertura_temporal",
        "cobertura_temporal",
        "STRING",
        "Cobertura temporal",
        "Temporal coverage",
        "Cobertura temporal",
    ),
    c(
        "valor",
        "valor",
        "STRING",
        "Valor traduzido da chave",
        "Translated value of the key",
        "Valor traducido de la clave",
    ),
]

# Themes with no num_area column in the source shapefile.
NO_AREA_THEMES = {"hidrografia"}

# Polygon enum value (tipoBase) per output table.
THEME_POLYGON = {
    "area_imovel": "AREA_IMOVEL",
    "app": "APPS",
    "reserva_legal": "RESERVA_LEGAL",
    "vegetacao_nativa": "VEGETACAO_NATIVA",
    "area_consolidada": "AREA_CONSOLIDADA",
    "area_pousio": "AREA_POUSIO",
    "uso_restrito": "USO_RESTRITO",
    "servidao_administrativa": "SERVIDAO_ADMINISTRATIVA",
    "hidrografia": "HIDROGRAFIA",
}
