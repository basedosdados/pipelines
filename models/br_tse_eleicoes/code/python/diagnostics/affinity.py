"""Affinity between official TSE variable names and BD output column names.

Used by tier 3 to judge whether the official column at position N is a
plausible source for the BD column the code maps ``vN`` to. Matching is
token-based after canonicalizing TSE prefixes/abbreviations, plus an explicit
whitelist for non-obvious pairs (e.g. ``tipo_eleicao <- DS_ELEICAO``).
"""

from __future__ import annotations

import re
import unicodedata

# canonical token mapping: TSE prefixes/abbreviations and BD equivalents
_SYNONYMS = {
    "SG": "SIGLA",
    "NR": "NUMERO",
    "NUM": "NUMERO",
    "NM": "NOME",
    "NO": "NOME",
    "DT": "DATA",
    "VR": "VALOR",
    "QT": "QT",
    "QTD": "QT",
    "QTDE": "QT",
    "DS": "DESCRICAO",
    "DESC": "DESCRICAO",
    "CD": "CODIGO",
    "COD": "CODIGO",
    "ID": "CODIGO",
    "SQ": "SEQUENCIAL",
    "SEQ": "SEQUENCIAL",
    "AA": "ANO",
    "HH": "HORA",
    "TP": "TIPO",
    "PART": "PARTIDO",
    "CAND": "CANDIDATO",
    "ELEICOES": "ELEICAO",
    "COLIGACAO": "LEGENDA",
    "AGREMIACAO": "LEGENDA",
    "BEM": "ITEM",
    "BENS": "ITEM",
    "ELEITOR": "ELEITORES",
    "VOTO": "VOTOS",
    "VAGA": "VAGAS",
    "FUNCION": "FUNCIONAMENTO",
    "ANULADOS": "NULOS",
    "LEG": "LEGENDA",
    "BIOMETRICA": "BIOMETRIA",
    "MUN": "MUNICIPIO",
    "SITU": "SITUACAO",
    "CONTA": "CONTAS",
    "PARTIDARIA": "PARTIDO",
    "ESCOLARIDADE": "INSTRUCAO",
    "FEDERACACAO": "FEDERACAO",  # typo kept in the published BD column name
    "ESP": "ESPECIE",
    "ADM": "ADMINISTRADOR",
    "DESP": "DESPESA",
    "DOC": "DOCUMENTO",
    "FOR": "FORNECEDOR",
    "DOA": "DOADOR",
    "CGC": "CNPJ",
    "SUP": "SUPERIOR",
    "ABSTENCAO": "ABSTENCOES",
    "SIT": "SITUACAO",
    "APU": "APURADO",
    "SEP": "SEPARADO",
    "TOT": "TOTALIZADAS",
    "INC": "INCLUSAO",
    "NM_SOCIAL": "NOME_SOCIAL",
}

# tokens carrying no signal for matching
_STOPWORDS = {
    "DE", "DA", "DO", "DAS", "DOS", "E", "EM", "A", "O", "POR", "PARA",
    "TSE", "RF", "RFB", "ORIG", "2",
}  # fmt: skip

# explicit (bd_column, tse_variable) pairs that token matching cannot infer.
# tse side is matched after canonicalization, as a normalized string.
_WHITELIST: set[tuple[str, str]] = {
    # election descriptors
    ("tipo_eleicao", "DESCRICAO ELEICAO"),
    ("tipo_eleicao", "NOME TIPO ELEICAO"),
    ("tipo_eleicao", "CODIGO TIPO ELEICAO"),
    ("id_eleicao", "CODIGO ELEICAO"),
    # unidade eleitoral == municipality code in municipal elections
    ("id_municipio_tse", "SIGLA UE"),
    ("id_municipio_tse", "CODIGO MUNICIPIO"),
    ("id_municipio_tse", "NUMERO UE"),
    ("sigla_uf", "UF"),
    # candidate status / result
    ("resultado", "DESCRICAO SITUACAO TOTALIZADAS TURNO"),
    ("resultado", "DESCRICAO SITUACAO CANDIDATO TOTALIZADAS"),
    ("situacao", "DESCRICAO SITUACAO CANDIDATURA"),
    ("situacao", "DESCRICAO DETALHE SITUACAO CANDIDATO"),
    ("situacao", "DESCRICAO SITUACAO CANDIDATO PLEITO"),
    ("situacao", "DESCRICAO SITUACAO CANDIDATO URNA"),
    # education / demographics
    ("instrucao", "DESCRICAO GRAU INSTRUCAO"),
    ("grupo_idade", "CODIGO FAIXA ETARIA"),
    ("raca", "DESCRICAO COR RACA"),
    ("idade", "NUMERO IDADE DATA POSSE"),
    # campaign finance
    ("especie_recurso", "DESCRICAO TIPO RECURSO"),
    ("fonte_recurso", "DESCRICAO FONTE RECURSO"),
    ("entrega_conjunto", "ENTREGA CONJUNTO"),
    ("cpf_cnpj_doador", "CODIGO CPF CNPJ"),  # CD_CPF_CGC (CGC -> CNPJ synonym)
    ("cpf_cnpj_fornecedor", "CODIGO CPF CNPJ"),
    # 2002-2016 prestacao headers use idiosyncratic names (Stata-validated):
    # TSE "Tipo receita" == BD origem_receita, "Fonte recurso" == fonte_receita,
    # "Espécie recurso" == natureza_receita, etc.
    ("especie_receita", "TIPO RECURSO"),
    ("especie_receita", "DESCRICAO ESPECIE RECURSO"),
    ("especie_receita", "DESCRICAO TIPO RECURSO"),
    ("especie_recurso", "DESCRICAO FORMA PAGAMENTO"),
    ("origem_receita", "DESCRICAO TITULO"),
    ("origem_receita", "TIPO RECEITA"),
    ("fonte_receita", "FONTE RECURSO"),
    ("natureza_receita", "ESPECIE RECURSO"),
    ("tipo_despesa", "DESCRICAO TITULO"),
    ("situacao_receita", "SITUACAO CADASTRAL"),
    ("situacao_receita", "SITUACAOCADASTRAL"),
    ("cnpj_candidato", "CODIGO NUMERO CNPJ"),
    ("sigla_uf", "SIGLA UE SUPERIOR"),
    ("sigla_uf", "SIGLA UE"),  # UE == UF in federal/state-level files
    ("sigla_uf", "UNIDADE ELEITORAL CANDIDATO"),
    ("sigla_uf_doador", "SIGLA UE SUPERIOR 1"),
    ("sigla_uf_doador", "SIGLA UE DOADOR"),
    ("sigla_uf_doador", "UNIDADE ELEITORAL DOADOR"),
    ("id_municipio_tse_doador", "SIGLA UE 1"),
    ("cnae_2_doador", "CODIGO SETOR ECONOMICO DOADOR"),
    ("cnae_2_fornecedor", "CODIGO SETOR ECONOMICO FORNECEDOR"),
    ("descricao_cnae_2_fornecedor", "SETOR ECONOMICO FORNECEDOR"),
    ("descricao_cnae_2_doador", "SETOR ECONOMICO DOADOR"),
    ("descricao_cnae_2_doador_orig", "SETOR ECONOMICO DOADOR ORIGINARIO"),
    ("situacao_biometria", "CODIGO MUNICIPIO SITUACAO BIOMETRICA"),
    # historical: "não nominais" == legend votes
    ("votos_nao_nominais", "QT VOTOS LEGENDA"),
    # consulta_coligacao: DS_SITUACAO is the legend's situation
    ("situacao_legenda", "DESCRICAO SITUACAO"),
    # polling place
    ("numero", "NUMERO LOCAL VOTACAO"),
    ("tipo", "TIPO LOCAL"),
    ("tipo", "TIPO LOCAL VOTACAO"),
    ("nome", "NOME LOCAL VOTACAO"),
    ("situacao", "SITUACAO LOCAL VOTACAO"),
    ("situacao", "DESCRICAO SITUACAO LOCAL VOTACAO"),
}


def _strip_accents(s: str) -> str:
    return "".join(
        c
        for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def tokens(name: str) -> list[str]:
    """Canonical token list for a TSE variable or BD column name."""
    raw = re.split(r"[^A-Za-z0-9]+", _strip_accents(name).upper())
    out = []
    for t in raw:
        if not t or t in _STOPWORDS:
            continue
        out.append(_SYNONYMS.get(t, t))
    return out


def normalized(name: str) -> str:
    return " ".join(tokens(name))


# BD prefix tokens that describe the *kind* of value, not its identity —
# they may be absent from the TSE name (e.g. bd `cargo` <- DS_CARGO)
_KIND_TOKENS = {
    "DESCRICAO",
    "CODIGO",
    "NUMERO",
    "NOME",
    "SIGLA",
    "DATA",
    "QT",
    "VALOR",
    "TIPO",
}


def compatible(bd_column: str, tse_variable: str) -> bool:
    """True when the TSE variable is a plausible source for the BD column."""
    # some old headers carry raw SQL export artifacts (RTRIM(...), DECODE(...),
    # RV_MEANING) — uninformative names that cannot be judged
    if re.search(r"[()]|RTRIM|DECODE|RV_MEANING", tse_variable):
        return True
    tse_norm = normalized(tse_variable)
    if (bd_column, tse_norm) in _WHITELIST:
        return True
    bd = set(tokens(bd_column))
    tse = set(tse_norm.split())
    # content tokens of the BD name must all appear in the TSE name
    content = bd - _KIND_TOKENS or bd
    if not content <= tse:
        return False
    # qualifier guard: a *_legenda/_nominais/... BD column must not match a
    # TSE column carrying a DIFFERENT vote qualifier, and vice versa
    qualifiers = {"LEGENDA", "NOMINAIS", "BRANCOS", "NULOS", "VALIDOS"}
    bd_q = bd & qualifiers
    tse_q = tse & qualifiers
    if bd_q and tse_q and not bd_q <= tse_q:
        return False
    # plain `votos` must not silently read legenda/brancos/nulos counts
    return not (
        "VOTOS" in bd
        and bd_q == set()
        and bool(tse_q - {"NOMINAIS", "VALIDOS"})
    )
