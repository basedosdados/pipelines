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

import calendar
import re
from concurrent.futures import ThreadPoolExecutor

import pandas as pd

from pipelines.datasets.br_senado_dados_abertos.senado_api import (
    _as_list,
    dig,
    get_json,
    get_json_safe,
    get_xml_records,
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


# The per-entity fan-out (per senator, per committee, per month) dominates the
# extraction wall-clock and is network-bound, so fetch concurrently. Kept modest
# to stay gentle on the public API; `get_json_safe` still retries/among each call
# and returns None on persistent failure so one bad entity is skipped, not fatal.
_FETCH_WORKERS = 8


def _fetch_many(
    paths: list[str], workers: int = _FETCH_WORKERS, retries: int = 6
) -> list:
    """Concurrently GET each API path, preserving input order (None on failure)."""
    with ThreadPoolExecutor(max_workers=workers) as ex:
        return list(ex.map(lambda p: get_json_safe(p, retries=retries), paths))


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


def _legislatura_parlamentares(leg: int) -> list[dict]:
    """Return every parlamentar of one legislature.

    Read as XML rather than JSON: `senador/lista/legislatura/{leg}` lost its
    `ListaParlamentarLegislatura` envelope some time between 2026-08-13 and
    2026-08-20, and its JSON form now collapses the whole roster into a single
    `Parlamentar` object. See `get_xml_records`.

    `get_xml_records` parses the enveloped form too, so a restored endpoint needs
    no change here. The JSON call is not attempted first: it cannot distinguish a
    lost envelope from an empty roster, and every legislature that is legitimately
    empty would pay a second full retry-and-backoff cycle (~30s) for nothing.

    Args:
        leg: Legislature number.

    Returns:
        One dict per parlamentar; empty when the legislature has no roster.
    """
    return get_xml_records(
        f"/senador/lista/legislatura/{leg}", record_tag="Parlamentar"
    )


def clean_senador() -> pd.DataFrame:
    rows: list[dict] = []
    for leg in range(LEG_START, CURRENT_LEG + 1):
        for p in _legislatura_parlamentares(leg):
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
    if not rows:
        # Fail loudly. The 2026-08 upstream regression made the JSON form of this
        # endpoint return 1 of 245 senators, so an empty or thin roster must never
        # be written as if it were the real one.
        raise RuntimeError(
            "senador: no parlamentares returned for legislaturas "
            f"{LEG_START}..{CURRENT_LEG} — the dados-abertos roster endpoint "
            "changed shape again; inspect /senador/lista/legislatura/{leg}"
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


# =============================================================================
# T2 — legislative sub-tables
# =============================================================================
RELATORIA_START = 2015  # /processo/relatoria only returns data from ~2015 on
DISCURSO_START = 1997


def _parl(d, root: str) -> dict:
    """The `Parlamentar` node inside a per-senator envelope, or {}."""
    return dig(d, root, "Parlamentar") or {}


# ----------------------------------------------------------------------------- senador_mandato
SENADOR_MANDATO_COLS = [
    "id_senador",
    "id_mandato",
    "sigla_uf",
    "participacao",
    "numero_legislatura_1",
    "data_inicio_legislatura_1",
    "data_fim_legislatura_1",
    "numero_legislatura_2",
    "data_inicio_legislatura_2",
    "data_fim_legislatura_2",
]


def clean_senador_mandato(codes: list[str]) -> pd.DataFrame:
    rows = []
    docs = _fetch_many([f"/senador/{c}/mandatos" for c in codes])
    for cod, d in zip(codes, docs, strict=True):
        par = _parl(d, "MandatoParlamentar")
        sid = s(par.get("Codigo")) or s(cod)
        for m in _as_list(dig(par, "Mandatos", "Mandato")):
            l1 = m.get("PrimeiraLegislaturaDoMandato") or {}
            l2 = m.get("SegundaLegislaturaDoMandato") or {}
            rows.append(
                {
                    "id_senador": sid,
                    "id_mandato": s(m.get("CodigoMandato")),
                    "sigla_uf": s(m.get("UfParlamentar")),
                    "participacao": s(m.get("DescricaoParticipacao")),
                    "numero_legislatura_1": s(l1.get("NumeroLegislatura")),
                    "data_inicio_legislatura_1": norm_date(
                        l1.get("DataInicio")
                    ),
                    "data_fim_legislatura_1": norm_date(l1.get("DataFim")),
                    "numero_legislatura_2": s(l2.get("NumeroLegislatura")),
                    "data_inicio_legislatura_2": norm_date(
                        l2.get("DataInicio")
                    ),
                    "data_fim_legislatura_2": norm_date(l2.get("DataFim")),
                }
            )
    return _frame(rows, SENADOR_MANDATO_COLS).drop_duplicates(
        subset=["id_senador", "id_mandato"]
    )


# ----------------------------------------------------------------------------- senador_filiacao
SENADOR_FILIACAO_COLS = [
    "id_senador",
    "id_partido",
    "sigla_partido",
    "data_filiacao",
    "data_desfiliacao",
]


def clean_senador_filiacao(codes: list[str]) -> pd.DataFrame:
    rows = []
    docs = _fetch_many([f"/senador/{c}/filiacoes" for c in codes])
    for cod, d in zip(codes, docs, strict=True):
        par = _parl(d, "FiliacaoParlamentar")
        sid = s(par.get("Codigo")) or s(cod)
        for f in _as_list(dig(par, "Filiacoes", "Filiacao")):
            p = f.get("Partido") or {}
            rows.append(
                {
                    "id_senador": sid,
                    "id_partido": s(p.get("CodigoPartido")),
                    "sigla_partido": s(p.get("SiglaPartido")),
                    "data_filiacao": norm_date(f.get("DataFiliacao")),
                    "data_desfiliacao": norm_date(f.get("DataDesfiliacao")),
                }
            )
    return _frame(rows, SENADOR_FILIACAO_COLS).drop_duplicates(
        subset=["id_senador", "id_partido", "data_filiacao"]
    )


# ----------------------------------------------------------------------------- senador_comissao
SENADOR_COMISSAO_COLS = [
    "id_senador",
    "id_comissao",
    "sigla_comissao",
    "sigla_casa",
    "participacao",
    "data_inicio",
    "data_fim",
]


def clean_senador_comissao(codes: list[str]) -> pd.DataFrame:
    rows = []
    docs = _fetch_many([f"/senador/{c}/comissoes" for c in codes])
    for cod, d in zip(codes, docs, strict=True):
        par = _parl(d, "MembroComissaoParlamentar")
        sid = s(par.get("Codigo")) or s(cod)
        for c in _as_list(dig(par, "MembroComissoes", "Comissao")):
            ic = c.get("IdentificacaoComissao") or {}
            rows.append(
                {
                    "id_senador": sid,
                    "id_comissao": s(ic.get("CodigoComissao")),
                    "sigla_comissao": s(ic.get("SiglaComissao")),
                    "sigla_casa": s(ic.get("SiglaCasaComissao")),
                    "participacao": s(c.get("DescricaoParticipacao")),
                    "data_inicio": norm_date(c.get("DataInicio")),
                    "data_fim": norm_date(c.get("DataFim")),
                }
            )
    return _frame(rows, SENADOR_COMISSAO_COLS).drop_duplicates(
        subset=["id_senador", "id_comissao", "data_inicio"]
    )


# ----------------------------------------------------------------------------- senador_cargo
SENADOR_CARGO_COLS = [
    "id_senador",
    "id_comissao",
    "sigla_comissao",
    "id_cargo",
    "descricao_cargo",
    "data_inicio",
    "data_fim",
]


def clean_senador_cargo(codes: list[str]) -> pd.DataFrame:
    rows = []
    docs = _fetch_many([f"/senador/{c}/cargos" for c in codes])
    for cod, d in zip(codes, docs, strict=True):
        par = _parl(d, "CargoParlamentar")
        sid = s(par.get("Codigo")) or s(cod)
        for c in _as_list(dig(par, "Cargos", "Cargo")):
            ic = c.get("IdentificacaoComissao") or {}
            rows.append(
                {
                    "id_senador": sid,
                    "id_comissao": s(ic.get("CodigoComissao")),
                    "sigla_comissao": s(ic.get("SiglaComissao")),
                    "id_cargo": s(c.get("CodigoCargo")),
                    "descricao_cargo": s(c.get("DescricaoCargo")),
                    "data_inicio": norm_date(c.get("DataInicio")),
                    "data_fim": norm_date(c.get("DataFim")),
                }
            )
    return _frame(rows, SENADOR_CARGO_COLS).drop_duplicates(
        subset=["id_senador", "id_comissao", "id_cargo", "data_inicio"]
    )


# ----------------------------------------------------------------------------- relatoria
RELATORIA_COLS = [
    "ano",
    "id_relatoria",
    "id_processo",
    "codigo_materia",
    "identificacao_processo",
    "id_senador",
    "sigla_casa_relator",
    "id_colegiado",
    "sigla_colegiado",
    "nome_colegiado",
    "id_tipo_colegiado",
    "tipo_relator",
    "id_tipo_relator",
    "numero_autuacao",
    "tipo_encerramento",
    "data_designacao",
    "data_destituicao",
    "tramitando",
]


def clean_relatoria(years: range) -> pd.DataFrame:
    rows = []
    for y in years:
        d = _as_list(get_json("/processo/relatoria", {"ano": y}))
        for r in d:
            dd = norm_date(r.get("dataDesignacao"))
            rows.append(
                {
                    # The `ano` query param filters by the process's presentation
                    # year, so partition by it (matches the `processo` table);
                    # `data_designacao` carries the actual designation event.
                    "ano": str(y),
                    "id_relatoria": s(r.get("id")),
                    "id_processo": s(r.get("idProcesso")),
                    "codigo_materia": s(r.get("codigoMateria")),
                    "identificacao_processo": s(
                        r.get("identificacaoProcesso")
                    ),
                    "id_senador": s(r.get("codigoParlamentar")),
                    "sigla_casa_relator": s(r.get("casaRelator"))
                    or s(r.get("siglaCasa")),
                    "id_colegiado": s(r.get("codigoColegiado")),
                    "sigla_colegiado": s(r.get("siglaColegiado")),
                    "nome_colegiado": s(r.get("nomeColegiado")),
                    "id_tipo_colegiado": s(r.get("codigoTipoColegiado")),
                    "tipo_relator": s(r.get("descricaoTipoRelator")),
                    "id_tipo_relator": s(r.get("idTipoRelator")),
                    "numero_autuacao": s(r.get("numeroAutuacao")),
                    "tipo_encerramento": s(r.get("descricaoTipoEncerramento")),
                    "data_designacao": dd,
                    "data_destituicao": norm_date(r.get("dataDestituicao")),
                    "tramitando": s(r.get("tramitando")),
                }
            )
        if d:
            print(f"    relatoria {y}: {len(d)} rows")
    return _frame(rows, RELATORIA_COLS).drop_duplicates(
        subset=["id_relatoria"]
    )


# ----------------------------------------------------------------------------- votacao_comissao (+ parlamentar)
VOTACAO_COMISSAO_COLS = [
    "ano",
    "id_votacao",
    "id_comissao",
    "sigla_colegiado",
    "nome_colegiado",
    "sigla_casa_colegiado",
    "id_reuniao",
    "numero_reuniao",
    "tipo_reuniao",
    "data_reuniao",
    "identificacao_materia",
    "sigla_materia",
    "numero_materia",
    "ano_materia",
    "descricao",
    "id_senador_presidente",
    "voto_sim",
    "voto_nao",
    "voto_abstencao",
]
VOTACAO_COMISSAO_PARLAMENTAR_COLS = [
    "ano",
    "id_votacao",
    "data_reuniao",
    "id_senador",
    "sigla_partido",
    "sigla_casa",
    "voto",
    "voto_presidente",
]


def clean_votacao_comissao(
    siglas: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    hrows, prows = [], []
    docs = _fetch_many([f"/votacaoComissao/comissao/{sig}" for sig in siglas])
    for sig, d in zip(siglas, docs, strict=True):
        vots = _as_list(dig(d, "VotacoesComissao", "Votacoes", "Votacao"))
        for v in vots:
            dr = norm_date(v.get("DataHoraInicioReuniao"))
            if not dr:  # cannot place in an ano= partition without a date
                continue
            ano = dr[:4]
            idv = s(v.get("CodigoVotacao"))
            ident = s(v.get("IdentificacaoMateria"))
            sg, num, anom = _parse_ident(ident)
            hrows.append(
                {
                    "ano": ano,
                    "id_votacao": idv,
                    "id_comissao": s(v.get("CodigoColegiado")),
                    "sigla_colegiado": s(v.get("SiglaColegiado")),
                    "nome_colegiado": s(v.get("NomeColegiado")),
                    "sigla_casa_colegiado": s(v.get("SiglaCasaColegiado")),
                    "id_reuniao": s(v.get("CodigoReuniao")),
                    "numero_reuniao": s(v.get("NumeroReuniaoColegiado")),
                    "tipo_reuniao": s(v.get("TipoReuniao")),
                    "data_reuniao": dr,
                    "identificacao_materia": ident,
                    "sigla_materia": sg,
                    "numero_materia": num,
                    "ano_materia": anom,
                    "descricao": s(v.get("DescricaoVotacao")),
                    "id_senador_presidente": s(
                        v.get("CodigoParlamentarPresidente")
                    ),
                    "voto_sim": s(v.get("TotalVotosSim")),
                    "voto_nao": s(v.get("TotalVotosNao")),
                    "voto_abstencao": s(v.get("TotalVotosAbstencao")),
                }
            )
            for vt in _as_list(dig(v, "Votos", "Voto")):
                prows.append(
                    {
                        "ano": ano,
                        "id_votacao": idv,
                        "data_reuniao": dr,
                        "id_senador": s(vt.get("CodigoParlamentar")),
                        "sigla_partido": s(vt.get("SiglaPartidoParlamentar")),
                        "sigla_casa": s(vt.get("SiglaCasaParlamentar")),
                        "voto": s(vt.get("QualidadeVoto")),
                        "voto_presidente": s(vt.get("VotoPresidente")),
                    }
                )
        if vots:
            print(f"    votacao_comissao {sig}: {len(vots)} votes")
    hdf = _frame(hrows, VOTACAO_COMISSAO_COLS).drop_duplicates(
        subset=["id_votacao"]
    )
    pdf = _frame(prows, VOTACAO_COMISSAO_PARLAMENTAR_COLS).drop_duplicates(
        subset=["id_votacao", "id_senador"]
    )
    return hdf, pdf


# ----------------------------------------------------------------------------- discurso
DISCURSO_COLS = [
    "ano",
    "id_pronunciamento",
    "id_sessao",
    "data_sessao",
    "sigla_casa",
    "sigla_tipo_sessao",
    "numero_sessao",
    "id_senador",
    "tipo_autor",
    "sigla_partido",
    "sigla_uf",
    "sigla_tipo_uso_palavra",
    "descricao_tipo_uso_palavra",
    "resumo",
    "indexacao",
    "url_texto",
]


def _discurso_rows(ses: dict) -> list[dict]:
    ds = norm_date(ses.get("DataSessao"))
    ano = ds[:4] if ds else None
    out = []
    for pr in _as_list(dig(ses, "Pronunciamentos", "Pronunciamento")):
        tu = pr.get("TipoUsoPalavra") or {}
        out.append(
            {
                "ano": ano,
                "id_pronunciamento": s(pr.get("CodigoPronunciamento"))
                or s(pr.get("id")),
                "id_sessao": s(ses.get("CodigoSessao")),
                "data_sessao": ds,
                "sigla_casa": s(ses.get("SiglaCasa")),
                "sigla_tipo_sessao": s(ses.get("TipoSessao")),
                "numero_sessao": s(ses.get("NumeroSessao")),
                "id_senador": s(pr.get("CodigoParlamentar")),
                "tipo_autor": s(pr.get("TipoAutor")),
                "sigla_partido": s(pr.get("Partido")),
                "sigla_uf": s(pr.get("UF")),
                "sigla_tipo_uso_palavra": s(tu.get("Sigla")),
                "descricao_tipo_uso_palavra": s(tu.get("Descricao")),
                "resumo": s(pr.get("Resumo")),
                "indexacao": s(pr.get("Indexacao")),
                "url_texto": s(pr.get("TextoIntegralTxt")),
            }
        )
    return out


def clean_discurso(years: range) -> pd.DataFrame:
    """Pronouncements per year, fetched month by month, concurrently.

    The endpoint caps the date span at ~1 month (a wider range returns HTTP
    400), so each year is split into twelve month windows; future or empty
    months just return nothing.
    """
    paths = []
    for y in years:
        for m in range(1, 13):
            last = calendar.monthrange(y, m)[1]
            paths.append(
                f"/plenario/lista/discursos/{y}{m:02d}01/{y}{m:02d}{last:02d}"
            )
    docs = _fetch_many(paths, retries=3)  # future months 400 → skip fast
    rows = []
    for d in docs:
        for ses in _as_list(dig(d, "DiscursosSessao", "Sessoes", "Sessao")):
            rows.extend(_discurso_rows(ses))
    out = _frame(rows, DISCURSO_COLS).drop_duplicates(
        subset=["id_pronunciamento"]
    )
    print(f"    discurso: {len(out)} pronouncements")
    return out
