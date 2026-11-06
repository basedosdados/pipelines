"""Schema specification for br_ufmg_censo_demografico_1872.

Single source of truth for the mapping from the Pop-72 Access database
(``Pop-1872-Brasil_versao1_0.mdb``) to Data Basis tables.

The Access file holds each census cross-tabulation twice: as published by the
Diretoria Geral de Estatistica in 1872 (``T0``-``T7``) and as corrected by
NPHED/Cedeplar (``T0c``-``T8c``, documented in their *Relatorio critico*).
Every data table is a complete cross of 1,440 parishes by N categories; the
category axis is table-specific and resolved through ``dicionario``.

Each source table is published at three geographic levels (parish, municipality,
province). Municipality and province figures are sums over the parishes they
contain -- the 5-digit parish id encodes the hierarchy exactly
(``id_paroquia // 100 == id_municipio_1872``, ``id_municipio_1872 // 100 ==
id_provincia``), which is verified in ``clean.py``.
"""

from __future__ import annotations

ANO = 1872

# Geographic levels. Ordered coarse-last so aggregation can reuse the previous
# level's frame. `key` lists the geography columns present at that level.
LEVELS = {
    "paroquia": ["id_provincia", "id_municipio_1872", "id_paroquia"],
    "municipio": ["id_provincia", "id_municipio_1872"],
    "provincia": ["id_provincia"],
}

# --------------------------------------------------------------------------
# Measure column renames, per source table.
#
# Source abbreviations: h/m = homem/mulher; br/pa/pr/ca = branco/pardo/preto/
# caboclo; li/ei = livre/escravizado; s/c/v = solteiro/casado/viuvo;
# cat/ac = catolico/acatolico; et = estrangeiro; sinf = sem informacao.
#
# The 1872 source says "escravos"; the published columns use "escravizado",
# the current standard in Brazilian scholarship. `original_name` in the
# architecture preserves the source spelling.
# --------------------------------------------------------------------------

_DOMICILIO = {
    "C_HABIT": "casas_habitadas",
    "C_DESAB": "casas_desabitadas",
    "FOGOS": "fogos",
}

# T1 / T1c / T8c share this shape: sex x free/enslaved, plus totals.
_SEXO_CONDICAO_ORIG = {
    "H Livres": "homens_livres",
    "M Livres": "mulheres_livres",
    "Soma L": "total_livres",
    "H Escravos": "homens_escravizados",
    "M Escravas": "mulheres_escravizadas",
    "Soma E": "total_escravizados",
    "Soma G": "total",
}
_SEXO_CONDICAO_CORR = {
    "H_Livres": "homens_livres",
    "M_Livres": "mulheres_livres",
    "Soma_L": "total_livres",
    "H_Escrav": "homens_escravizados",
    "M_Escrav": "mulheres_escravizadas",
    "Soma_E": "total_escravizados",
    "Soma_G": "total",
}
_SEXO_CONDICAO_T8C = {
    "H_LIVRES": "homens_livres",
    "M_LIVRES": "mulheres_livres",
    "SOMA_L": "total_livres",
    "H_ESCR": "homens_escravizados",
    "M_ESCR": "mulheres_escravizadas",
    "SOMA_E": "total_escravizados",
    "SOMA_G": "total",
}

# T2 / T3 (published) and T2c / T3c / T2e3c (corrected): sex x colour x condition.
_COR_ORIG = {
    "H Branco L": "homens_brancos_livres",
    "H Pardo L": "homens_pardos_livres",
    "H Preto L": "homens_pretos_livres",
    "H Caboclo L": "homens_caboclos_livres",
    "H Pardo E": "homens_pardos_escravizados",
    "H Preto E": "homens_pretos_escravizados",
    "M Branca L": "mulheres_brancas_livres",
    "M Parda L": "mulheres_pardas_livres",
    "M Preta L": "mulheres_pretas_livres",
    "M Cabocla L": "mulheres_caboclas_livres",
    "M Parda E": "mulheres_pardas_escravizadas",
    "M Preta E": "mulheres_pretas_escravizadas",
    "Soma": "total",
}
_COR_CORR = {
    "h_br_li": "homens_brancos_livres",
    "h_pa_li": "homens_pardos_livres",
    "h_pr_li": "homens_pretos_livres",
    "h_ca_li": "homens_caboclos_livres",
    "h_pa_ei": "homens_pardos_escravizados",
    "h_pr_ei": "homens_pretos_escravizados",
    "m_br_li": "mulheres_brancas_livres",
    "m_pa_li": "mulheres_pardas_livres",
    "m_pr_li": "mulheres_pretas_livres",
    "m_ca_li": "mulheres_caboclas_livres",
    "m_pa_ei": "mulheres_pardas_escravizadas",
    "m_pr_ei": "mulheres_pretas_escravizadas",
    "Soma": "total",
}

# T4 / T5 count men and women respectively, but the sex lives in the table
# rather than in the column: the published T4 and T5 share one column set, and
# the corrected T4c / T5c differ only by a leading sex letter. Published names
# therefore take the gender of the table, so a column in the women's table is
# never spelled in the masculine.
_ORIGEM_GENDER = {
    "h": dict(
        s="solteiros",
        c="casados",
        v="viuvos",
        br="brancos",
        pa="pardos",
        pr="pretos",
        ca="caboclos",
        li="livres",
        ei="escravizados",
    ),
    "m": dict(
        s="solteiras",
        c="casadas",
        v="viuvas",
        br="brancas",
        pa="pardas",
        pr="pretas",
        ca="caboclas",
        li="livres",
        ei="escravizadas",
    ),
}


def _origem_names(p: str) -> dict[str, str]:
    """Published column names for the origin tables, in the table's gender."""
    g = _ORIGEM_GENDER[p]
    names = {}
    for civil in ("s", "c", "v"):
        for cor in ("br", "pa", "pr", "ca"):
            names[f"{civil}{cor}li"] = f"{g[civil]}_{g[cor]}_{g['li']}"
        for cor in ("pa", "pr"):
            names[f"{civil}{cor}ei"] = f"{g[civil]}_{g[cor]}_{g['ei']}"
    names["l_sinf"] = f"{g['li']}_sem_informacao"
    names["e_sinf"] = f"{g['ei']}_sem_informacao"
    return names


def _origem_orig(p: str) -> dict[str, str]:
    """Published T4 (p="h") / T5 (p="m") column map. No "sem informacao"."""
    n = _origem_names(p)
    return {
        "S Branc L": n["sbrli"],
        "S Pard L": n["spali"],
        "S Pret L": n["sprli"],
        "S Cabocl L": n["scali"],
        "Cas Branc L": n["cbrli"],
        "Cas Pard L": n["cpali"],
        "Cas Pret L": n["cprli"],
        "Cas Cabocl L": n["ccali"],
        "Viu Branc L": n["vbrli"],
        "Viu Pard L": n["vpali"],
        "Viu Pret L": n["vprli"],
        "Viu Cab L": n["vcali"],
        "S Pard E": n["spaei"],
        "S Pret E": n["sprei"],
        "C Pard E": n["cpaei"],
        "C Pret E": n["cprei"],
        "V Pard E": n["vpaei"],
        "V Pret E": n["vprei"],
    }


def _origem_corr(p: str) -> dict[str, str]:
    """Corrected T4c (p="h") / T5c (p="m") column map.

    Both use the same positional scheme; only the leading sex letter differs.
    """
    n = _origem_names(p)
    return {
        f"{p}brsli": n["sbrli"],
        f"{p}pasli": n["spali"],
        f"{p}prsli": n["sprli"],
        f"{p}casli": n["scali"],
        f"{p}brcli": n["cbrli"],
        f"{p}pacli": n["cpali"],
        f"{p}prcli": n["cprli"],
        f"{p}cacli": n["ccali"],
        f"{p}brvli": n["vbrli"],
        f"{p}pavli": n["vpali"],
        f"{p}prvli": n["vprli"],
        f"{p}cavli": n["vcali"],
        f"{p}_l_sinf": n["l_sinf"],
        f"{p}pasei": n["spaei"],
        f"{p}prsei": n["sprei"],
        f"{p}pacei": n["cpaei"],
        f"{p}prcei": n["cprei"],
        f"{p}pavei": n["vpaei"],
        f"{p}prvei": n["vprei"],
        f"{p}_e_sinf": n["e_sinf"],
    }


# T6 / T6c: religion x marital status x sex, for foreign residents.
_ESTRANGEIRO_ORIG = {
    "H Cat Solt": "homens_catolicos_solteiros",
    "H Cat Cas": "homens_catolicos_casados",
    "H Cat Viuv": "homens_catolicos_viuvos",
    "H Acat Solt": "homens_acatolicos_solteiros",
    "H Acat Cas": "homens_acatolicos_casados",
    "H Acat Viuv": "homens_acatolicos_viuvos",
    "M Cat Solt": "mulheres_catolicas_solteiras",
    "M Cat Cas": "mulheres_catolicas_casadas",
    "M Cat Viu": "mulheres_catolicas_viuvas",
    "M Acat Solt": "mulheres_acatolicas_solteiras",
    "M Acat Cas": "mulheres_acatolicas_casadas",
    "M Acat Viu": "mulheres_acatolicas_viuvas",
    "SOMA": "total",
}
_ESTRANGEIRO_CORR = {
    "hcasli": "homens_catolicos_solteiros",
    "hcacli": "homens_catolicos_casados",
    "hcavli": "homens_catolicos_viuvos",
    "hacsli": "homens_acatolicos_solteiros",
    "haccli": "homens_acatolicos_casados",
    "hacvli": "homens_acatolicos_viuvos",
    "h_sinf": "homens_sem_informacao",
    "mcasli": "mulheres_catolicas_solteiras",
    "mcacli": "mulheres_catolicas_casadas",
    "mcavli": "mulheres_catolicas_viuvas",
    "macsli": "mulheres_acatolicas_solteiras",
    "maccli": "mulheres_acatolicas_casadas",
    "macvli": "mulheres_acatolicas_viuvas",
    "m_sinf": "mulheres_sem_informacao",
    "SOMA": "total",
}

# T7 / T7c: nationality x marital status x sex, by occupation.
_PROFISSAO_ORIG = {
    "H Brs L Solt": "homens_brasileiros_solteiros",
    "H Brs L Cas": "homens_brasileiros_casados",
    "H Brs L Viu": "homens_brasileiros_viuvos",
    "M Brs L Solt": "mulheres_brasileiras_solteiras",
    "M Brs L Cas": "mulheres_brasileiras_casadas",
    "M Brs L Viu": "mulheres_brasileiras_viuvas",
    "H Estr L Solt": "homens_estrangeiros_solteiros",
    "H Estr L Cas": "homens_estrangeiros_casados",
    "H Estr L Viu": "homens_estrangeiros_viuvos",
    "M Estr L Solt": "mulheres_estrangeiras_solteiras",
    "M Estr L Cas": "mulheres_estrangeiras_casadas",
    "M Estr L Viu": "mulheres_estrangeiras_viuvas",
    "Homens Escr": "homens_escravizados",
    "Mulheres Escr": "mulheres_escravizadas",
    "Soma": "total",
}
_PROFISSAO_CORR = {
    "hbrsli": "homens_brasileiros_solteiros",
    "hbrcli": "homens_brasileiros_casados",
    "hbrvli": "homens_brasileiros_viuvos",
    "hbrlsinf": "homens_brasileiros_sem_informacao",
    "mbrsli": "mulheres_brasileiras_solteiras",
    "mbrcli": "mulheres_brasileiras_casadas",
    "mbrvli": "mulheres_brasileiras_viuvas",
    "mbrlsinf": "mulheres_brasileiras_sem_informacao",
    "hetsli": "homens_estrangeiros_solteiros",
    "hetcli": "homens_estrangeiros_casados",
    "hetvli": "homens_estrangeiros_viuvos",
    "hetlsinf": "homens_estrangeiros_sem_informacao",
    "metsli": "mulheres_estrangeiras_solteiras",
    "metcli": "mulheres_estrangeiras_casadas",
    "metvli": "mulheres_estrangeiras_viuvas",
    "metlsinf": "mulheres_estrangeiras_sem_informacao",
    "h_esci": "homens_escravizados",
    "m_esci": "mulheres_escravizadas",
    "soma": "total",
}


# --------------------------------------------------------------------------
# Source table registry.
#
#   stem          published table name, before the version and level suffixes
#   versao        "original" (as printed in 1872) | "corrigido" (NPHED)
#   local_col     source column holding the 5-digit parish id
#   categoria_col source column holding the category code; None for T0
#   dicionario    the Cod_categorias_* table that decodes `categoria_col`
#   measures      source column -> published column
# --------------------------------------------------------------------------

TABLES: dict[str, dict] = {
    "T0": dict(
        stem="domicilio",
        versao="original",
        local_col="LOCAL",
        categoria_col=None,
        dicionario=None,
        measures=_DOMICILIO,
    ),
    "T0c": dict(
        stem="domicilio",
        versao="corrigido",
        local_col="LOCAL",
        categoria_col=None,
        dicionario=None,
        measures=_DOMICILIO,
    ),
    "T1": dict(
        stem="populacao_geral",
        versao="original",
        local_col="cod_local",
        categoria_col="cod_categ",
        dicionario="Cod_categorias_Tab_1",
        measures=_SEXO_CONDICAO_ORIG,
    ),
    "T1c": dict(
        stem="populacao_geral",
        versao="corrigido",
        local_col="Local",
        categoria_col="categ",
        dicionario="Cod_categorias_Tab_1c",
        measures=_SEXO_CONDICAO_CORR,
    ),
    "T2": dict(
        stem="populacao_presente_idade",
        versao="original",
        local_col="Localidade",
        categoria_col="Categoria",
        dicionario="Cod_categorias_Tab_2e3",
        measures=_COR_ORIG,
    ),
    "T2c": dict(
        stem="populacao_presente_idade",
        versao="corrigido",
        local_col="Local",
        categoria_col="Categ",
        dicionario="Cod_categorias_Tab_2e3c",
        measures=_COR_CORR,
    ),
    "T3": dict(
        stem="populacao_ausente_idade",
        versao="original",
        local_col="Localidade",
        categoria_col="Categoria",
        dicionario="Cod_categorias_Tab_2e3",
        measures=_COR_ORIG,
    ),
    "T3c": dict(
        stem="populacao_ausente_idade",
        versao="corrigido",
        local_col="Local",
        categoria_col="Categ",
        dicionario="Cod_categorias_Tab_2e3c",
        measures=_COR_CORR,
    ),
    "T2e3c": dict(
        stem="populacao_total_idade",
        versao="corrigido",
        local_col="Local",
        categoria_col="Categ",
        dicionario="Cod_categorias_Tab_2e3c",
        measures=_COR_CORR,
    ),
    "T4": dict(
        stem="homem_origem_brasileira",
        versao="original",
        local_col="cod_local",
        categoria_col="categoria",
        dicionario="Cod_categorias_Tab_4e5",
        measures=_origem_orig("h"),
    ),
    "T4c": dict(
        stem="homem_origem_brasileira",
        versao="corrigido",
        local_col="local",
        categoria_col="categ",
        dicionario="Cod_categorias_Tab_4e5c",
        measures=_origem_corr("h"),
    ),
    "T5": dict(
        stem="mulher_origem_brasileira",
        versao="original",
        local_col="cod_local",
        categoria_col="categoria",
        dicionario="Cod_categorias_Tab_4e5",
        measures=_origem_orig("m"),
    ),
    "T5c": dict(
        stem="mulher_origem_brasileira",
        versao="corrigido",
        local_col="local",
        categoria_col="categ",
        dicionario="Cod_categorias_Tab_4e5c",
        measures=_origem_corr("m"),
    ),
    "T6": dict(
        stem="estrangeiro_nacionalidade",
        versao="original",
        local_col="cod_local",
        categoria_col="cod_categ",
        dicionario="Cod_categorias_Tab_6",
        measures=_ESTRANGEIRO_ORIG,
    ),
    "T6c": dict(
        stem="estrangeiro_nacionalidade",
        versao="corrigido",
        local_col="Local",
        categoria_col="categ",
        dicionario="Cod_categorias_Tab_6c",
        measures=_ESTRANGEIRO_CORR,
    ),
    "T7": dict(
        stem="profissao",
        versao="original",
        local_col="Localidade",
        categoria_col="Categoria",
        dicionario="Cod_categorias_Tab_7",
        measures=_PROFISSAO_ORIG,
    ),
    "T7c": dict(
        stem="profissao",
        versao="corrigido",
        local_col="Local",
        categoria_col="Categ",
        dicionario="Cod_categorias_Tab_7c",
        measures=_PROFISSAO_CORR,
    ),
    "T8c": dict(
        stem="resumo_geral",
        versao="corrigido",
        local_col="LOCAL",
        categoria_col="CAT_T8",
        dicionario="Cod_categorias_Tab_8c",
        measures=_SEXO_CONDICAO_T8C,
    ),
}


def table_slug(source: str, level: str) -> str:
    """Published table slug, e.g. ``populacao_geral_corrigido_municipio``."""
    t = TABLES[source]
    return f"{t['stem']}_{t['versao']}_{level}"


# --------------------------------------------------------------------------
# Category dictionaries: how to read each Cod_categorias_* table.
#   key    the column holding the code that appears in the data tables
#   grupo  the column holding the grouping label (None when there is none)
#   desc   the column holding the category label
# --------------------------------------------------------------------------

DICIONARIOS: dict[str, dict[str, str | None]] = {
    "Cod_categorias_Tab_1": dict(
        key="Cod_categoria", grupo="Grupos", desc="Categorias"
    ),
    "Cod_categorias_Tab_1c": dict(
        key="Cod_categoria", grupo="Grupos", desc="Categorias"
    ),
    "Cod_categorias_Tab_2e3": dict(
        key="Categorias", grupo="Faixa", desc="idade"
    ),
    "Cod_categorias_Tab_2e3c": dict(
        key="Categorias", grupo="Faixa", desc="idade"
    ),
    "Cod_categorias_Tab_4e5": dict(key="Item", grupo=None, desc="Descrição"),
    "Cod_categorias_Tab_4e5c": dict(key="Item", grupo=None, desc="Descrição"),
    "Cod_categorias_Tab_6": dict(key="Item", grupo=None, desc="Descrição"),
    "Cod_categorias_Tab_6c": dict(key="Item", grupo=None, desc="Descrição"),
    "Cod_categorias_Tab_7": dict(
        key="cod_ocup", grupo="Grupo_ocup", desc="Profissao"
    ),
    "Cod_categorias_Tab_7c": dict(
        key="cod_ocup", grupo="Grupo_ocup", desc="Profissao"
    ),
    "Cod_categorias_Tab_8c": dict(
        key="Categ", grupo="Grupo", desc="Descrição"
    ),
}


# Geography lookup tables built from cod_prov_id / cod_municipios / cod_paroquias.
GEOGRAFIA = {
    "provincia": ["id_provincia", "nome_provincia"],
    "municipio": ["id_provincia", "id_municipio_1872", "nome_municipio"],
    "paroquia": [
        "id_provincia",
        "id_municipio_1872",
        "id_paroquia",
        "nome_paroquia",
    ],
}
