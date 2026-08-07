"""
Senado Federal — extract + clean transforms for br_senado_dados_abertos (T1).

Pure functions. Each `clean_<table>()` returns a pandas DataFrame whose columns
and order match the architecture sheet. Values are ALL strings (staging is
all-STRING by house convention; the dbt model `safe_cast`s every column to its
final type). NULLs are real None (never the literal "nan").

Dates are normalized to 'YYYY-MM-DD' and datetimes to 'YYYY-MM-DD HH:MM:SS' so
the dbt `safe_cast(... as date/datetime)` is trivial.
"""

from __future__ import annotations

import re

import pandas as pd

from pipelines.datasets.br_senado_dados_abertos.senado_api import (
    _as_list,
    dig,
    get_json,
)


def _current_legislature() -> int:
    """Legislature number covering today (57 = 2023-2027, four-year terms).

    Over-estimating by one at a term boundary is harmless: the extra
    per-legislature query just returns an empty roster.
    """
    year = pd.Timestamp.today().year
    return 57 + max(0, year - 2023) // 4


CURRENT_LEG = _current_legislature()
LEG_START = 30
VOTACAO_START = 1991
ORIENT_START = 1991
PROCESSO_START = 1900


# ----------------------------------------------------------------------------- helpers
def s(x) -> str | None:
    """To clean string or None (handles NaN/empty/whitespace)."""
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    out = str(x).strip()
    return out or None


def norm_date(x) -> str | None:
    v = s(x)
    if not v:
        return None
    v = v.replace("T", " ")
    m = re.match(r"(\d{4}-\d{2}-\d{2})", v)
    return m.group(1) if m else None


def norm_datetime(x) -> str | None:
    v = s(x)
    if not v:
        return None
    v = v.replace("T", " ")
    m = re.match(r"(\d{4}-\d{2}-\d{2})[ ]?(\d{2}:\d{2}:\d{2})?", v)
    if not m:
        return None
    return f"{m.group(1)} {m.group(2)}" if m.group(2) else m.group(1)


def _frame(rows: list[dict], columns: list[str]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    for c in columns:
        if c not in df.columns:
            df[c] = None
    df = df[columns]
    # pyrefly: ignore [bad-argument-type]
    return df.astype(object).where(pd.notna(df), None)


# ----------------------------------------------------------------------------- 6. partido
PARTIDO_COLS = [
    "id_partido",
    "sigla_partido",
    "nome_partido",
    "data_criacao",
    "data_extincao",
]


def clean_partido() -> pd.DataFrame:
    d = get_json("/composicao/lista/partidos")
    rows = [
        {
            "id_partido": s(p.get("Codigo")),
            "sigla_partido": s(p.get("Sigla")),
            "nome_partido": s(p.get("Nome")),
            "data_criacao": norm_date(p.get("DataCriacao")),
            "data_extincao": norm_date(p.get("DataExtincao")),
        }
        for p in _as_list(dig(d, "ListaPartidos", "Partidos", "Partido"))
    ]
    return _frame(rows, PARTIDO_COLS).drop_duplicates(subset=["id_partido"])


# ----------------------------------------------------------------------------- 7. bloco
BLOCO_COLS = [
    "id_bloco",
    "nome_bloco",
    "nome_apelido",
    "data_criacao",
    "data_extincao",
]


def clean_bloco() -> pd.DataFrame:
    d = get_json("/composicao/lista/blocos")
    rows = [
        {
            "id_bloco": s(b.get("CodigoBloco")),
            "nome_bloco": s(b.get("NomeBloco")),
            "nome_apelido": s(b.get("NomeApelido")),
            "data_criacao": norm_date(b.get("DataCriacao")),
            "data_extincao": norm_date(b.get("DataExtincao")),
        }
        for b in _as_list(dig(d, "ListaBlocoParlamentar", "Blocos", "Bloco"))
    ]
    return _frame(rows, BLOCO_COLS).drop_duplicates(subset=["id_bloco"])


# ----------------------------------------------------------------------------- 8. lideranca
LIDERANCA_COLS = [
    "id_lideranca",
    "casa",
    "id_senador",
    "nome_parlamentar",
    "sigla_tipo_lideranca",
    "descricao_tipo_lideranca",
    "sigla_tipo_unidade_lideranca",
    "descricao_tipo_unidade_lideranca",
    "id_partido_filiacao",
    "sigla_partido_filiacao",
    "nome_partido_filiacao",
    "id_bloco",
    "sigla_bloco",
    "nome_bloco",
    "numero_ordem_vice_lider",
    "data_designacao",
]


def clean_lideranca() -> pd.DataFrame:
    d = _as_list(get_json("/composicao/lideranca"))
    rows = [
        {
            "id_lideranca": s(x.get("codigo")),
            "casa": s(x.get("casa")),
            "id_senador": s(x.get("codigoParlamentar")),
            "nome_parlamentar": s(x.get("nomeParlamentar")),
            "sigla_tipo_lideranca": s(x.get("siglaTipoLideranca")),
            "descricao_tipo_lideranca": s(x.get("descricaoTipoLideranca")),
            "sigla_tipo_unidade_lideranca": s(
                x.get("siglaTipoUnidadeLideranca")
            ),
            "descricao_tipo_unidade_lideranca": s(
                x.get("descricaoTipoUnidadeLideranca")
            ),
            "id_partido_filiacao": s(x.get("codigoPartidoFiliacao")),
            "sigla_partido_filiacao": s(x.get("siglaPartidoFiliacao")),
            "nome_partido_filiacao": s(x.get("nomePartidoFiliacao")),
            "id_bloco": s(x.get("codigoBloco")),
            "sigla_bloco": s(x.get("siglaBloco")),
            "nome_bloco": s(x.get("nomeBloco")),
            "numero_ordem_vice_lider": s(x.get("numeroOrdemViceLider")),
            "data_designacao": norm_date(x.get("dataDesignacao")),
        }
        for x in d
    ]
    return _frame(rows, LIDERANCA_COLS).drop_duplicates(
        subset=["id_lideranca"]
    )


# ----------------------------------------------------------------------------- 9. comissao
COMISSAO_COLS = [
    "id_comissao",
    "sigla_comissao",
    "nome_comissao",
    "sigla_casa",
    "id_tipo_colegiado",
    "sigla_tipo_colegiado",
    "descricao_tipo_colegiado",
    "publica",
    "data_inicio",
]


def clean_comissao() -> pd.DataFrame:
    d = get_json("/comissao/lista/colegiados")
    rows = [
        {
            "id_comissao": s(c.get("Codigo")),
            "sigla_comissao": s(c.get("Sigla")),
            "nome_comissao": s(c.get("Nome")),
            "sigla_casa": s(c.get("SiglaCasa")),
            "id_tipo_colegiado": s(c.get("CodigoTipoColegiado")),
            "sigla_tipo_colegiado": s(c.get("SiglaTipoColegiado")),
            "descricao_tipo_colegiado": s(c.get("DescricaoTipoColegiado")),
            "publica": s(c.get("Publica")),
            "data_inicio": norm_date(c.get("DataInicio")),
        }
        for c in _as_list(dig(d, "ListaColegiados", "Colegiados", "Colegiado"))
    ]
    return _frame(rows, COMISSAO_COLS).drop_duplicates(subset=["id_comissao"])


# ----------------------------------------------------------------------------- 10. mesa
MESA_COLS = [
    "id_colegiado",
    "sigla_colegiado",
    "nome_colegiado",
    "cargo",
    "id_senador",
    "nome_parlamentar",
    "bancada",
    "ordem",
    "origem",
]


def _mesa_from(endpoint: str) -> list[dict]:
    d = get_json(endpoint)
    root = "MesaSenado" if "SF" in endpoint else "MesaCongresso"
    rows = []
    for c in _as_list(dig(d, root, "Colegiados", "Colegiado")):
        for cg in _as_list(dig(c, "Cargos", "Cargo")):
            nome = s(cg.get("NomeParlamentar"))
            if nome:
                nome = re.sub(r"^Senador[a]?\s+", "", nome)
            rows.append(
                {
                    "id_colegiado": s(c.get("CodigoColegiado")),
                    "sigla_colegiado": s(c.get("SiglaColegiado")),
                    "nome_colegiado": s(c.get("NomeColegiado")),
                    "cargo": s((_as_list(cg.get("Cargo")) or [None])[0]),
                    "id_senador": s(cg.get("Http")),
                    "nome_parlamentar": nome,
                    "bancada": s(cg.get("Bancada")),
                    "ordem": s(cg.get("NumeroOrdemImpressao")),
                    "origem": s(cg.get("Origem")),
                }
            )
    return rows


def clean_mesa() -> pd.DataFrame:
    rows = _mesa_from("/composicao/mesaSF")
    return _frame(rows, MESA_COLS)


# ----------------------------------------------------------------------------- 1. senador
SENADOR_COLS = [
    "id_senador",
    "nome",
    "nome_completo",
    "sexo",
    "forma_tratamento",
    "sigla_partido",
    "sigla_uf",
    "email",
    "url_foto",
    "url_pagina",
    "url_pagina_particular",
    "id_publico_legislatura_atual",
]


def clean_senador() -> pd.DataFrame:
    rows: list[dict] = []
    for leg in range(LEG_START, CURRENT_LEG + 1):
        d = get_json(f"/senador/lista/legislatura/{leg}")
        for p in _as_list(
            dig(
                d,
                "ListaParlamentarLegislatura",
                "Parlamentares",
                "Parlamentar",
            )
        ):
            ip = p.get("IdentificacaoParlamentar", {}) or {}
            ft = s(ip.get("FormaTratamento"))
            rows.append(
                {
                    "id_senador": s(ip.get("CodigoParlamentar")),
                    "nome": s(ip.get("NomeParlamentar")),
                    "nome_completo": s(ip.get("NomeCompletoParlamentar")),
                    "sexo": s(ip.get("SexoParlamentar")),
                    "forma_tratamento": ft.strip() if ft else None,
                    "sigla_partido": s(ip.get("SiglaPartidoParlamentar")),
                    "sigla_uf": s(ip.get("UfParlamentar")),
                    "email": s(ip.get("EmailParlamentar")),
                    "url_foto": s(ip.get("UrlFotoParlamentar")),
                    "url_pagina": s(ip.get("UrlPaginaParlamentar")),
                    "url_pagina_particular": s(ip.get("UrlPaginaParticular")),
                    "id_publico_legislatura_atual": s(
                        ip.get("CodigoPublicoNaLegAtual")
                    ),
                    "_leg": leg,
                }
            )
    df = pd.DataFrame(rows)
    df = df[df["id_senador"].notna()].copy()
    # A senator recurs across legislatures; keep one row deterministically:
    # most complete (non-null count), tie-broken by the most recent
    # legislature, then id — reproducible regardless of API row order.
    df["_score"] = df[SENADOR_COLS].notna().sum(axis=1)
    df = (
        df.sort_values(
            ["id_senador", "_score", "_leg"],
            ascending=[True, False, False],
            kind="mergesort",
        )
        .drop_duplicates(subset=["id_senador"], keep="first")
        .reset_index(drop=True)
    )
    return _frame(df.to_dict("records"), SENADOR_COLS)


# ----------------------------------------------------------------------------- 2/3. votacao (+ parlamentar)
VOTO_MAP = {
    "Sim": "voto_sim",
    "Não": "voto_nao",
    "Nao": "voto_nao",
    "Abstenção": "voto_abstencao",
}
VOTACAO_COLS = [
    "ano",
    "id_votacao",
    "data_sessao",
    "casa",
    "id_processo",
    "codigo_materia",
    "identificacao_materia",
    "sigla_materia",
    "numero_materia",
    "ano_materia",
    "id_sessao",
    "id_sessao_legislativa",
    "numero_sessao",
    "sequencial_sessao",
    "sequencial_votacao",
    "sigla_tipo_sessao",
    "descricao_votacao",
    "ementa",
    "resultado_votacao",
    "votacao_secreta",
    "data_apresentacao",
    "sigla_colegiado",
    "nome_colegiado",
    "voto_sim",
    "voto_nao",
    "voto_abstencao",
    "voto_outro",
]
# nome/sexo are static senator attributes (see the senador table); only the
# vote-time political context (party, uf) and the vote itself are kept here.
VOTACAO_PARLAMENTAR_COLS = [
    "ano",
    "id_votacao",
    "data_sessao",
    "id_senador",
    "sigla_partido",
    "sigla_uf",
    "voto",
    "descricao_voto",
]


def _votacao_year(year: int) -> tuple[list[dict], list[dict]]:
    d = _as_list(
        get_json(
            "/votacao",
            {"dataInicio": f"{year}-01-01", "dataFim": f"{year}-12-31"},
        )
    )
    vrows, prows = [], []
    for v in d:
        data_sessao = norm_date(v.get("dataSessao"))
        ano = data_sessao[:4] if data_sessao else str(year)
        id_votacao = s(v.get("codigoSessaoVotacao"))
        tally = {
            "voto_sim": 0,
            "voto_nao": 0,
            "voto_abstencao": 0,
            "voto_outro": 0,
        }
        for voto in _as_list(v.get("votos")):
            key = VOTO_MAP.get(
                s(voto.get("siglaVotoParlamentar")) or "", "voto_outro"
            )
            tally[key] += 1
            prows.append(
                {
                    "ano": ano,
                    "id_votacao": id_votacao,
                    "data_sessao": data_sessao,
                    "id_senador": s(voto.get("codigoParlamentar")),
                    "sigla_partido": s(voto.get("siglaPartidoParlamentar")),
                    "sigla_uf": s(voto.get("siglaUFParlamentar")),
                    "voto": s(voto.get("siglaVotoParlamentar")),
                    "descricao_voto": s(voto.get("descricaoVotoParlamentar")),
                }
            )
        il = v.get("informeLegislativo") or {}
        vrows.append(
            {
                "ano": ano,
                "id_votacao": id_votacao,
                "data_sessao": data_sessao,
                "casa": s(v.get("casaSessao")),
                "id_processo": s(v.get("idProcesso")),
                "codigo_materia": s(v.get("codigoMateria")),
                "identificacao_materia": s(v.get("identificacao")),
                "sigla_materia": s(v.get("sigla")),
                "numero_materia": s(v.get("numero")),
                "ano_materia": s(v.get("ano")),
                "id_sessao": s(v.get("codigoSessao")),
                "id_sessao_legislativa": s(v.get("codigoSessaoLegislativa")),
                "numero_sessao": s(v.get("numeroSessao")),
                "sequencial_sessao": s(v.get("sequencialSessao")),
                "sequencial_votacao": s(v.get("sequencialVotacao")),
                "sigla_tipo_sessao": s(v.get("siglaTipoSessao")),
                "descricao_votacao": s(v.get("descricaoVotacao")),
                "ementa": s(v.get("ementa")),
                "resultado_votacao": s(v.get("resultadoVotacao")),
                "votacao_secreta": s(v.get("votacaoSecreta")),
                "data_apresentacao": norm_date(v.get("dataApresentacao")),
                "sigla_colegiado": s(il.get("siglaColegiado")),
                "nome_colegiado": s(il.get("nomeColegiado")),
                **{k: str(val) for k, val in tally.items()},
            }
        )
    return vrows, prows


def clean_votacao(years: range) -> tuple[pd.DataFrame, pd.DataFrame]:
    vall, pall = [], []
    for y in years:
        vr, pr = _votacao_year(y)
        vall += vr
        pall += pr
        print(f"    votacao {y}: {len(vr)} votes, {len(pr)} individual")
    return _frame(vall, VOTACAO_COLS), _frame(pall, VOTACAO_PARLAMENTAR_COLS)


# ----------------------------------------------------------------------------- 4. orientacao bancada
ORIENT_COLS = [
    "ano",
    "id_votacao_sve",
    "sequencial_votacao",
    "data_votacao",
    "sigla_materia",
    "numero_materia",
    "ano_materia",
    "bancada",
    "orientacao",
    "data_hora_orientacao",
]


def clean_orientacao(years: range) -> pd.DataFrame:
    rows = []
    for y in years:
        d = get_json(f"/plenario/votacao/orientacaoBancada/{y}0101/{y}1231")
        vots = (
            _as_list(dig(d, "votacoes"))
            if isinstance(d, dict)
            else _as_list(d)
        )
        n = 0
        for v in vots:
            dvot = norm_date(v.get("dataInicioVotacao"))
            ano = dvot[:4] if dvot else str(y)
            for o in _as_list(v.get("orientacoesLideranca")):
                rows.append(
                    {
                        "ano": ano,
                        "id_votacao_sve": s(v.get("codigoVotacaoSve")),
                        "sequencial_votacao": s(v.get("sequencialVotacao")),
                        "data_votacao": dvot,
                        "sigla_materia": s(v.get("siglaTipoMateria")),
                        "numero_materia": s(v.get("numeroMateria")),
                        "ano_materia": s(v.get("anoMateria")),
                        "bancada": s(o.get("partido")),
                        "orientacao": s(o.get("voto")),
                        "data_hora_orientacao": norm_datetime(
                            o.get("dataHora")
                        ),
                    }
                )
                n += 1
        print(f"    orientacao {y}: {n} rows")
    return _frame(rows, ORIENT_COLS)


# ----------------------------------------------------------------------------- 5. processo
PROCESSO_COLS = [
    "ano",
    "id_processo",
    "codigo_materia",
    "identificacao",
    "sigla",
    "numero",
    "autoria",
    "ementa",
    "objetivo",
    "tipo_documento",
    "tipo_conteudo",
    "situacao_atual",
    "sigla_tipo_deliberacao",
    "ente_identificador",
    "casa_identificadora",
    "norma_gerada",
    "apelido",
    "tramitando",
    "data_apresentacao",
    "data_deliberacao",
    "data_situacao_atual",
    "data_ultima_atualizacao",
    "ultima_informacao_atualizada",
    "url_documento",
]
# "<SIGLA> <NUMERO>/<ANO>", ignoring any trailing suffix such as " - CCJ"
_IDENT_RE = re.compile(r"^\s*(\S+)\s+([\w.]+)/(\d{4})")


def _parse_ident(
    ident: str | None,
) -> tuple[str | None, str | None, str | None]:
    if not ident:
        return None, None, None
    m = _IDENT_RE.match(ident)
    if m:
        return m.group(1), m.group(2), m.group(3)
    return None, None, None


def clean_processo(years: range) -> pd.DataFrame:
    rows = []
    for y in years:
        d = _as_list(get_json("/processo", {"ano": y}))
        for p in d:
            ident = s(p.get("identificacao"))
            sigla, numero, ano_id = _parse_ident(ident)
            rows.append(
                {
                    "ano": ano_id or str(y),
                    "id_processo": s(p.get("id")),
                    "codigo_materia": s(p.get("codigoMateria")),
                    "identificacao": ident,
                    "sigla": sigla,
                    "numero": numero,
                    "autoria": s(p.get("autoria")),
                    "ementa": s(p.get("ementa")),
                    "objetivo": s(p.get("objetivo")),
                    "tipo_documento": s(p.get("tipoDocumento")),
                    "tipo_conteudo": s(p.get("tipoConteudo")),
                    "situacao_atual": s(p.get("situacaoAtual")),
                    "sigla_tipo_deliberacao": s(p.get("siglaTipoDeliberacao")),
                    "ente_identificador": s(p.get("enteIdentificador")),
                    "casa_identificadora": s(p.get("casaIdentificadora")),
                    "norma_gerada": s(p.get("normaGerada")),
                    "apelido": s(p.get("apelido")),
                    "tramitando": s(p.get("tramitando")),
                    "data_apresentacao": norm_date(p.get("dataApresentacao")),
                    "data_deliberacao": norm_date(p.get("dataDeliberacao")),
                    "data_situacao_atual": norm_date(
                        p.get("dataSituacaoAtual")
                    ),
                    "data_ultima_atualizacao": norm_datetime(
                        p.get("dataUltimaAtualizacao")
                    ),
                    "ultima_informacao_atualizada": s(
                        p.get("ultimaInformacaoAtualizada")
                    ),
                    "url_documento": s(p.get("urlDocumento")),
                }
            )
        if d:
            print(f"    processo {y}: {len(d)} records")
    return _frame(rows, PROCESSO_COLS).drop_duplicates(subset=["id_processo"])
