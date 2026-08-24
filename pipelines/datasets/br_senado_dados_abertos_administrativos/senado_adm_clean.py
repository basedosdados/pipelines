"""
Senado Federal Administrative Open Data — cleaning transform.

Pure functions (no Prefect). One ``build_*`` per table, each returning a list of
row dicts in the column order declared by
``models/br_senado_dados_abertos_administrativos/code/architecture_spec.py``,
which is the architecture's single source of truth.

Shared by the one-shot onboarding under ``models/.../code/`` and by the
recurring pipeline, so the transform has exactly one definition.

Source quirks handled here rather than downstream:

* Dates arrive in two formats — ISO (``2024-11-05``) from the contratações and
  supridos endpoints, ``dd/mm/aaaa`` from the servidores and senadores ones —
  and ``---`` is used as a null sentinel.
* Money arrives either as a JSON number or as a Brazilian-formatted string
  (``16.368,74``). Both normalise to a float.
* Booleans arrive as JSON ``true``/``false`` and as ``S``/``N``; both normalise
  to ``sim``/``nao``, recorded in the dicionário.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from typing import Any

from pipelines.datasets.br_senado_dados_abertos_administrativos import (
    senado_adm_api as api,
)

DATE_ISO = re.compile(r"^\d{4}-\d{2}-\d{2}")
DATE_BR = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")
MES_ANO_BR = re.compile(r"^(\d{2})/(\d{4})$")
HORAS = re.compile(r"^(\d+)h(\d{2})$")
NULLS = {"", "-", "---", "N/A", "NAO INFORMADO", "NÃO INFORMADO"}


# ------------------------------------------------------------------ helpers


def s(value: Any) -> str | None:
    """Normalize to a trimmed string, mapping the source's null sentinels."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text.upper() in NULLS else text


def date(value: Any) -> str | None:
    """Parse an ISO or dd/mm/aaaa date into ISO ``aaaa-mm-dd``."""
    text = s(value)
    if text is None:
        return None
    if DATE_ISO.match(text):
        return text[:10]
    match = DATE_BR.match(text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return None


def datetime_(value: Any) -> str | None:
    """Parse a ``aaaa-mm-dd hh:mm:ss`` timestamp, tolerating a missing time."""
    text = s(value)
    if text is None or not DATE_ISO.match(text):
        return None
    return text.replace("T", " ")[:19]


def num(value: Any) -> float | None:
    """Parse a JSON number or a Brazilian-formatted numeric string."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = s(value)
    if text is None:
        return None
    text = text.replace("R$", "").strip()
    # 16.368,74 -> 16368.74 ; 1234.56 stays as is
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def integer(value: Any) -> int | None:
    parsed = num(value)
    return None if parsed is None else int(parsed)


def flag(value: Any) -> str | None:
    """Normalize the source's several boolean encodings to sim / nao."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "sim" if value else "nao"
    text = s(value)
    if text is None:
        return None
    upper = text.upper()
    if upper in {"S", "SIM", "TRUE", "1"}:
        return "sim"
    if upper in {"N", "NAO", "NÃO", "FALSE", "0"}:
        return "nao"
    return text


def hours(value: Any) -> float | None:
    """Parse the ``02h00`` duration the horas-extras endpoint reports."""
    text = s(value)
    if text is None:
        return None
    match = HORAS.match(text)
    if match:
        hh, mm = match.groups()
        return int(hh) + int(mm) / 60
    return num(text)


def jornada(value: Any) -> int | None:
    """Parse the ``40 Horas`` weekly-hours string."""
    text = s(value)
    if text is None:
        return None
    digits = re.match(r"^(\d+)", text)
    return int(digits.group(1)) if digits else None


def g(node: Any, *path: str) -> Any:
    """Read a nested key, tolerating a missing or null intermediate object."""
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    return node


def mes_ano(value: Any) -> tuple[int | None, int | None]:
    """Split the ``MM/AAAA`` the horas-extras endpoint reports into (ano, mes)."""
    text = s(value)
    if text is None:
        return None, None
    match = MES_ANO_BR.match(text)
    if not match:
        return None, None
    month, year = match.groups()
    return int(year), int(month)


def dedupe(rows: list[dict], keys: Iterable[str]) -> list[dict]:
    """Drop rows repeating a key tuple, keeping the first occurrence.

    Used where the source repeats a child across parents — see
    ``build_contratacao_documento_fiscal``.
    """
    seen: set[tuple] = set()
    out: list[dict] = []
    for row in rows:
        key = tuple(row.get(k) for k in keys)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
    return out


# ================================================== A. Senadores ============


def build_despesa_ceaps(years: Iterable[int]) -> list[dict]:
    rows = []
    for year in years:
        for r in api.fetch(f"/senadores/despesas_ceaps/{year}"):
            rows.append(
                {
                    "ano": integer(r.get("ano")) or year,
                    "mes": integer(r.get("mes")),
                    "id_despesa": s(r.get("id")),
                    "id_senador": s(r.get("codSenador")),
                    "nome_senador": s(r.get("nomeSenador")),
                    "tipo_despesa": s(r.get("tipoDespesa")),
                    "tipo_documento": s(r.get("tipoDocumento")),
                    "documento": s(r.get("documento")),
                    "data": date(r.get("data")),
                    "cpf_cnpj_fornecedor": s(r.get("cpfCnpj")),
                    "nome_fornecedor": s(r.get("fornecedor")),
                    "detalhamento": s(r.get("detalhamento")),
                    "valor_reembolsado": num(r.get("valorReembolsado")),
                }
            )
    return rows


def build_senador_gabinete(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "nome_parlamentar": s(r.get("nomeParlamentar")),
            "sigla_uf": s(r.get("uf")),
            "sigla_partido": s(r.get("partido")),
            "titular_suplente": s(r.get("titularSuplente")),
            "mandato": s(r.get("mandato")),
            "data_nascimento": date(r.get("dataNascimento")),
            "endereco": s(r.get("endereco")),
            "telefones": s(r.get("telefones")),
            "fax": s(r.get("fax")),
            "email": s(r.get("email")),
            "chefe_gabinete": s(r.get("chefeGabinete")),
        }
        for r in api.fetch("/senadores")
    ]


def build_senador_escritorio_apoio(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "nome_parlamentar": s(g(r, "parlamentar", "nome")),
            "sigla_uf": s(g(r, "parlamentar", "estado")),
            "sigla_partido": s(g(r, "parlamentar", "partido")),
            "nome_escritorio": s(g(r, "setor", "nome")),
            "sigla_setor": s(g(r, "setor", "sigla")),
            "endereco": s(g(r, "setor", "endereco")),
            "telefone": s(g(r, "setor", "telefone")),
        }
        for r in api.fetch("/senadores/escritorios-apoio")
    ]


def build_senador_auxilio_moradia(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "nome_parlamentar": s(r.get("nomeParlamentar")),
            "sigla_uf": s(r.get("estadoEleito")),
            "sigla_partido": s(r.get("partidoEleito")),
            "indicador_auxilio_moradia": flag(r.get("auxilioMoradia")),
            "indicador_imovel_funcional": flag(r.get("imovelFuncional")),
        }
        for r in api.fetch("/senadores/auxilio-moradia-imoveis-funcionais")
    ]


def build_senador_aposentado_pensionista(extracted_at: str) -> list[dict]:
    """Stack the four benefit endpoints, which share a subject but not a shape.

    ``/senadores/aposentados`` is the only one carrying a remuneration figure;
    the IPC and PSSC lists carry a start date but no amount, and the pensioner
    list additionally carries an end date. `regime` records which endpoint the
    row came from, since that is the only place the source states it.
    """
    rows = []
    for r in api.fetch("/senadores/aposentados"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": s(r.get("nome")),
                "tipo_beneficio": "aposentadoria",
                "regime": "geral",
                "tipo": s(r.get("tipo")),
                "data_inicio": date(r.get("dataInicial")),
                "data_fim": None,
                "valor_remuneracao": num(r.get("remuneracao")),
            }
        )
    for path, regime in (
        ("/senadores/aposentados-ipc", "ipc"),
        ("/senadores/aposentados-pssc", "pssc"),
    ):
        for r in api.fetch(path):
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "nome": s(r.get("nome")),
                    "tipo_beneficio": "aposentadoria",
                    "regime": regime,
                    "tipo": s(r.get("tipo")),
                    "data_inicio": date(r.get("dataAposentadoria")),
                    "data_fim": None,
                    "valor_remuneracao": None,
                }
            )
    for r in api.fetch("/senadores/pensionistas-ipc"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": s(r.get("nome")),
                "tipo_beneficio": "pensao",
                "regime": "ipc",
                "tipo": s(r.get("tipo")),
                "data_inicio": date(r.get("dataInicio")),
                "data_fim": date(r.get("dataFim")),
                "valor_remuneracao": None,
            }
        )
    return rows


# ================================================== B. Servidores ===========


def build_servidor(extracted_at: str) -> list[dict]:
    """The full roster.

    ``/servidores/servidores`` is deliberately the only endpoint read here: the
    ativos, inativos, efetivos and comissionados views are `situacaoEquals` and
    `tipoVinculoEquals` filters over this same list, and both discriminators are
    already columns on it.
    """
    return [
        {
            "data_extracao": extracted_at,
            "id_servidor": s(r.get("sequencial")),
            "nome": s(r.get("nome")),
            "vinculo": s(r.get("vinculo")),
            "situacao": s(r.get("situacao")),
            "cargo": s(g(r, "cargo", "nome")),
            "especialidade": s(r.get("especialidade")),
            "padrao": s(r.get("padrao")),
            "codigo_categoria": s(g(r, "categoria", "codigo")),
            "categoria": s(g(r, "categoria", "nome")),
            "codigo_funcao": s(g(r, "funcao", "codigo")),
            "funcao": s(g(r, "funcao", "nome")),
            "sigla_lotacao": s(g(r, "lotacao", "sigla")),
            "lotacao": s(g(r, "lotacao", "nome")),
            "tipo_cessao": s(g(r, "cedido", "tipo_cessao")),
            "orgao_origem": s(g(r, "cedido", "orgao_origem")),
            "orgao_destino": s(g(r, "cedido", "orgao_destino")),
            "ano_admissao": integer(r.get("ano_admissao")),
        }
        for r in api.fetch("/servidores/servidores")
    ]


def build_servidor_ativo(extracted_at: str) -> list[dict]:
    """Active staff, with the fields `/servidores/servidores` does not carry.

    ``hierarquiaCompleta`` is a root-to-unit path; the deepest entry is the
    actual assignment unit. Each entry reads ``SIGLA - Nome``, so the sigla is
    split off and the whole path is kept as a slash-separated string.
    """
    rows = []
    for r in api.fetch("/servidores"):
        chain = r.get("hierarquiaCompleta") or []
        deepest = chain[-1] if chain else {}
        label = s(deepest.get("nome")) or ""
        sigla, _, nome = label.partition(" - ")
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": s(r.get("nome")),
                "tipo_vinculo": s(r.get("tipoVinculo")),
                "cargo": s(r.get("cargo")),
                "categoria": s(r.get("categoria")),
                "funcao": s(r.get("funcao")),
                "data_admissao": date(r.get("dataAdmissao")),
                "jornada_semanal_horas": jornada(r.get("jornadaSemanal")),
                "afastamento": s(r.get("afastamento")),
                "isencao_ponto": s(r.get("isencaoPonto")),
                "sigla_lotacao": s(sigla) if nome else None,
                "lotacao": s(nome) if nome else s(label),
                "nivel_lotacao": integer(deepest.get("nivel")),
                "hierarquia_lotacao": " / ".join(
                    x for x in (s(h.get("nome")) for h in chain) if x
                )
                or None,
            }
        )
    return rows


def build_servidor_remuneracao(
    periods: Iterable[tuple[int, int]],
) -> list[dict]:
    """Monthly payroll.

    ``/servidores/pensionistas/remuneracoes`` is not read: it returns a
    byte-identical payload to this endpoint, so pensioner payroll is not in fact
    exposed by the API.
    """
    rows = []
    for year, month in periods:
        for r in api.fetch(f"/servidores/remuneracoes/{year}/{month}"):
            rows.append(
                {
                    "ano": integer(r.get("ano")) or year,
                    "mes": integer(r.get("mes")) or month,
                    "id_remuneracao": s(r.get("sequencial")),
                    "nome": s(r.get("nome")),
                    "tipo_folha": s(r.get("tipo_folha")),
                    "remuneracao_basica": num(r.get("remuneracao_basica")),
                    "vantagens_pessoais": num(r.get("vantagens_pessoais")),
                    "funcao_comissionada": num(r.get("funcao_comissionada")),
                    "gratificacao_natalina": num(
                        r.get("gratificacao_natalina")
                    ),
                    "horas_extras": num(r.get("horas_extras")),
                    "auxilios": num(r.get("auxilios")),
                    "diarias": num(r.get("diarias")),
                    "vantagens_indenizatorias": num(
                        r.get("vantagens_indenizatorias")
                    ),
                    "outras_eventuais": num(r.get("outras_eventuais")),
                    "abono_permanencia": num(r.get("abono_permanencia")),
                    "faltas": num(r.get("faltas")),
                    "previdencia": num(r.get("previdencia")),
                    "imposto_renda": num(r.get("imposto_renda")),
                    "reversao_teto_constitucional": num(
                        r.get("reversao_teto_constitucional")
                    ),
                    "remuneracao_liquida": num(r.get("remuneracao_liquida")),
                }
            )
    # `sequencial` identifies the servidor-month, not the row: the same code
    # carries a Normal and a Suplementar folha for one person and month, so
    # tipo_folha completes the key. A few rows are then still repeated verbatim
    # by the source (389 in 1.9M) and are dropped.
    return dedupe(rows, ["ano", "mes", "id_remuneracao", "tipo_folha"])


def build_servidor_hora_extra(
    periods: Iterable[tuple[int, int]],
) -> tuple[list[dict], list[dict]]:
    """Overtime, split into the monthly parent and its daily detail.

    Returned as a pair because the source nests them in one payload. Keeping
    them apart matters: ``valorTotal`` is a per-month figure, so flattening to
    day grain would repeat a monetary value once per day and invite double
    counting.
    """
    parents: list[dict] = []
    days: list[dict] = []
    for year, month in periods:
        for r in api.fetch(f"/servidores/horas-extras/{year}/{month}"):
            record_id = s(r.get("sequencial"))
            prestacao = s(r.get("mes_ano_prestacao"))
            parents.append(
                {
                    "ano": year,
                    "mes": month,
                    "id_hora_extra": record_id,
                    "nome": s(r.get("nome")),
                    "mes_ano_prestacao": prestacao,
                    "mes_ano_pagamento": s(r.get("mes_ano_pagamento")),
                    "valor_total": num(r.get("valorTotal")),
                }
            )
            for d in r.get("horas_extras") or []:
                days.append(
                    {
                        "ano": year,
                        "mes": month,
                        "id_hora_extra": record_id,
                        "mes_ano_prestacao": prestacao,
                        "data": date(d.get("dia")),
                        "quantidade_horas": hours(d.get("quantidade")),
                        "sigla_setor_prestacao": s(
                            g(d, "setor_prestacao", "sigla")
                        ),
                        "setor_prestacao": s(g(d, "setor_prestacao", "nome")),
                    }
                )
    # One payment month can settle several months worked, and the source reuses
    # the same `sequencial` for each — so the month worked is part of the key,
    # and the day rows carry it too or they cannot be joined unambiguously.
    parents = dedupe(
        parents, ["ano", "mes", "id_hora_extra", "mes_ano_prestacao"]
    )
    days = dedupe(
        days,
        [
            "ano",
            "id_hora_extra",
            "mes_ano_prestacao",
            "data",
            "sigla_setor_prestacao",
        ],
    )
    return parents, days


def build_servidor_aposentado(extracted_at: str) -> list[dict]:
    rows = []
    for path, quadro in (
        ("/servidores/aposentados-efetivos", "efetivo"),
        ("/servidores/aposentados-comissionados", "comissionado"),
    ):
        for r in api.fetch(path):
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "matricula": s(r.get("matricula")),
                    "tipo_quadro": quadro,
                    "nome": s(r.get("nome")),
                    "categoria": s(r.get("categoria")),
                    "cargo": s(r.get("cargo")),
                    "data_aposentadoria": date(r.get("dataAposentadoria")),
                    "tipo_aposentadoria": s(r.get("tipoAposentadoria")),
                }
            )
    return rows


def build_servidor_exonerado(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "matricula": s(r.get("matricula")),
            "nome": s(r.get("nome")),
            "forma_vacancia": s(r.get("formaVacancia")),
            "data_exercicio": date(r.get("dataExercicio")),
            "data_vacancia": date(r.get("dataVacancia")),
        }
        for r in api.fetch("/servidores/exonerados")
    ]


SENADO = "SENADO FEDERAL"

# Each cessão endpoint reports a different subset of fields, and none of them
# states the direction of the secondment — that is carried only by which
# endpoint answered. The origin and destination are therefore reconstructed
# here: `orgaoOrigem` is the counterpart for inbound secondments, `orgao` is the
# counterpart for outbound ones.
CESSOES = (
    (
        "/servidores/cedidos/para-senado",
        "cedido para o senado",
        "in",
        "orgaoOrigem",
    ),
    ("/servidores/cedidos/pelo-senado", "cedido pelo senado", "out", "orgao"),
    (
        "/servidores/cedidos/infraero-para-senado",
        "cedido da infraero",
        "in",
        None,
    ),
    (
        "/servidores/exercicio-provisorio",
        "exercicio provisorio",
        "in",
        "orgaoOrigem",
    ),
)


def build_servidor_cedido(extracted_at: str) -> list[dict]:
    rows = []
    for path, tipo, direction, counterpart_key in CESSOES:
        for r in api.fetch(path):
            counterpart = (
                s(r.get(counterpart_key)) if counterpart_key else "INFRAERO"
            )
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "matricula": s(r.get("matricula")),
                    "tipo_cessao": tipo,
                    "nome": s(r.get("nome")),
                    "orgao_origem": counterpart
                    if direction == "in"
                    else SENADO,
                    "orgao_destino": SENADO
                    if direction == "in"
                    else counterpart,
                    "cargo": s(r.get("cargo")),
                    "categoria": s(r.get("categoria")),
                    "lotacao": s(r.get("lotacao")),
                    "data_exercicio": date(r.get("dataExercicio")),
                }
            )
    return rows


def build_pensionista(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "id_pensionista": s(r.get("sequencial")),
            "nome": s(r.get("nome")),
            "vinculo": s(r.get("vinculo")),
            "fundamento": s(r.get("fundamento")),
            "nome_instituidor": s(r.get("nome_instituidor")),
            "codigo_categoria": s(g(r, "categoria", "codigo")),
            "categoria": s(g(r, "categoria", "nome")),
            "cargo": s(g(r, "cargo", "nome")),
            "codigo_funcao": s(g(r, "funcao", "codigo")),
            "funcao": s(g(r, "funcao", "nome")),
            "ano_exercicio": integer(r.get("ano_exercicio")),
            "data_obito": date(r.get("data_obito")),
            "data_inicio_pensao": date(r.get("data_inicio_pensao")),
        }
        for r in api.fetch("/servidores/pensionistas")
    ]


# ================================================ C. Contratações ===========


def build_contratacao(raw: list[dict], extracted_at: str) -> list[dict]:
    """Flatten the contratações fetched by ``api.fetch_contratacoes``.

    ``raw`` is passed in rather than fetched here because every child builder in
    this group needs the same parent list, and refetching it would repeat the
    status fan-out four times.
    """
    return [
        {
            "data_extracao": extracted_at,
            "tipo_contratacao": s(r.get("tipo_contratacao")),
            "id_contratacao": s(r.get("id")),
            "numero": s(r.get("numero")),
            "numero_formatado": s(r.get("numero_formatado")),
            "status": s(r.get("status")),
            "sigla_sub_especie": s(g(r, "sub_especie", "sigla")),
            "sub_especie": s(g(r, "sub_especie", "nome")),
            "objeto": s(r.get("objeto")),
            "cpf_cnpj_empresa": s(g(r, "empresa", "cpf_cnpj")),
            "nome_empresa": s(g(r, "empresa", "nome")),
            "numero_licitacao": s(g(r, "licitacao", "numero")),
            "modalidade_licitacao": s(g(r, "licitacao", "modalidade")),
            "sigla_unidade_gestora": s(g(r, "unidade_gestora", "sigla")),
            "unidade_gestora": s(g(r, "unidade_gestora", "nome")),
            "fundamentacao_legal": s(r.get("fundamentacao_legal")),
            "processo_principal": s(r.get("processo_principal")),
            "indicador_mao_de_obra": flag(r.get("ind_mao_de_obra")),
            "data_assinatura": date(r.get("data_assinatura")),
            "data_publicacao": date(r.get("data_publicacao")),
            "data_inicio_vigencia": date(r.get("data_inicio_vigencia")),
            "data_fim_vigencia": date(r.get("data_fim_vigencia")),
            "data_ultima_atualizacao": date(r.get("data_ultima_atualizacao")),
        }
        for r in raw
    ]


def build_contratacao_orgao_gestor(
    raw: list[dict], extracted_at: str
) -> list[dict]:
    """Explode the ``orgaos_gestores`` already nested in the parent payload.

    The source's ``id`` for the órgão is not kept: in half the rows it simply
    repeats the contratação's own code (contratação 2412 lists two different
    órgãos, COPEGE and NGCIC, both under id 2412), so it identifies nothing. The
    órgão is identified by its sigla instead, and the residual verbatim repeats
    the source emits are dropped.
    """
    rows = []
    for r in raw:
        for o in r.get("orgaos_gestores") or []:
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "tipo_contratacao": s(r.get("tipo_contratacao")),
                    "id_contratacao": s(r.get("id")),
                    "sigla_orgao_gestor": s(o.get("sigla")),
                    "orgao_gestor": s(o.get("nome")),
                    "tipo_gestao": s(o.get("tipo_gestao")),
                }
            )
    return dedupe(
        rows,
        [
            "data_extracao",
            "tipo_contratacao",
            "id_contratacao",
            "sigla_orgao_gestor",
            "tipo_gestao",
        ],
    )


def build_contratacao_item(raw: list[dict], extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "tipo_contratacao": s(r["tipo_contratacao"]),
            "id_contratacao": s(r["id_contratacao"]),
            "id_item": s(r.get("id")),
            "numero_item": s(r.get("numero")),
            "descricao": s(r.get("descricao")),
            "quantidade": num(r.get("quantidade")),
            "data_atualizacao": date(r.get("data_atualizacao")),
        }
        for r in api.fetch_sub_resource(raw, "itens")
    ]


def build_contratacao_garantia(
    raw: list[dict], extracted_at: str
) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "tipo_contratacao": s(r["tipo_contratacao"]),
            "id_contratacao": s(r["id_contratacao"]),
            "id_garantia": s(r.get("id")),
            "tipo": s(r.get("tipo")),
            "valor": num(r.get("valor")),
            "data_vencimento": date(r.get("data_vencimento")),
        }
        for r in api.fetch_sub_resource(raw, "garantias")
    ]


def build_contratacao_pagamento(
    raw: list[dict], extracted_at: str
) -> tuple[list[dict], list[dict], list[dict]]:
    """Payments, their fiscal documents, and the empenhos backing them.

    Returns three tables from one crawl. The fiscal documents come free, nested
    in the payment payload — but the source repeats the *contract's* entire
    document list on every one of its payments (contract 2280 returns the same
    five document ids on all four of its payments), so they are deduplicated to
    contract grain. Empenhos genuinely differ per payment and keep their own
    fan-out. The dedicated ``/pagamentos/{id}/documentos_fiscais`` endpoint is
    not read: it returns zero rows even where the nested list is populated.
    """
    pagamentos: list[dict] = []
    documentos: list[dict] = []
    fanout: list[dict] = []

    for r in api.fetch_sub_resource(raw, "pagamentos"):
        tipo, parent = r["tipo_contratacao"], r["id_contratacao"]
        pagamento_id = s(r.get("id"))
        pagamentos.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": s(tipo),
                "id_contratacao": s(parent),
                "id_pagamento": pagamento_id,
                "descricao_despesa": s(r.get("descricao_despesa")),
                "valor_cobrado": num(r.get("valor_cobrado")),
                "multa": num(r.get("multa")),
                "glosa": num(r.get("glosa")),
                "observacao": s(r.get("observacao")),
            }
        )
        fanout.append(
            {
                "tipo_contratacao": tipo,
                "id_contratacao": parent,
                "id": r.get("id"),
            }
        )
        for d in r.get("documentos_fiscais") or []:
            documentos.append(
                {
                    "data_extracao": extracted_at,
                    "tipo_contratacao": s(tipo),
                    "id_contratacao": s(parent),
                    "id_documento_fiscal": s(d.get("id")),
                    "numero": s(d.get("numero")),
                    "data_emissao": date(d.get("data_emissao")),
                    "data_vencimento": date(d.get("data_vencimento")),
                }
            )

    documentos = dedupe(
        documentos,
        ["tipo_contratacao", "id_contratacao", "id_documento_fiscal"],
    )

    empenhos = [
        {
            "data_extracao": extracted_at,
            "tipo_contratacao": s(e["tipo_contratacao"]),
            "id_contratacao": s(e["id_contratacao"]),
            "id_pagamento": s(e["id_pagamento"]),
            "id_empenho": s(e.get("id")),
            "natureza_despesa": s(e.get("natureza_despesa")),
            "valor_empenhado": num(e.get("valor_empenhado")),
            "valor_liquidado": num(e.get("valor_liquidado")),
            "saldo": num(e.get("saldo")),
        }
        for e in api.fetch_pagamento_empenhos(fanout)
    ]
    return pagamentos, documentos, empenhos


def build_contrato_aditivo(raw: list[dict], extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "id_contratacao": s(r["id_contratacao"]),
            "id_aditivo": s(r.get("id")),
            "numero": s(r.get("numero")),
            "valor": num(r.get("valor")),
            "data_assinatura": date(r.get("data_assinatura")),
            "data_publicacao": date(r.get("data_publicacao")),
            "data_atualizacao": date(r.get("data_atualizacao")),
        }
        for r in api.fetch_sub_resource(raw, "aditivos", tipos=("contratos",))
    ]


def build_ata_acionamento(raw: list[dict], extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "id_contratacao": s(r["id_contratacao"]),
            "id_acionamento": s(r.get("id")),
            "numero": s(r.get("numero")),
            "objeto": s(r.get("objeto")),
        }
        for r in api.fetch_sub_resource(
            raw, "acionamentos", tipos=("atas_registro_preco",)
        )
    ]


def build_licitacao(extracted_at: str) -> tuple[list[dict], list[dict]]:
    """Tenders and their details.

    ``detalhamentos`` arrives nested in the list response, so the per-tender
    endpoint is never called — 2,743 requests avoided.
    """
    licitacoes: list[dict] = []
    detalhamentos: list[dict] = []
    for r in api.fetch("/contratacoes/licitacoes"):
        licitacao_id = s(r.get("id"))
        licitacoes.append(
            {
                "data_extracao": extracted_at,
                "id_licitacao": licitacao_id,
                "numero": s(r.get("numero")),
                "modalidade": s(r.get("modalidade")),
                "situacao": s(r.get("situacao")),
                "objeto": s(r.get("objeto")),
                "data_abertura": date(r.get("abertura")),
                "indicador_registro_preco": flag(r.get("registro_preco")),
                "orgao_origem": s(r.get("orgao_origem")),
                "edital": s(r.get("edital")),
            }
        )
        for d in r.get("detalhamentos") or []:
            detalhamentos.append(
                {
                    "data_extracao": extracted_at,
                    "id_licitacao": licitacao_id,
                    "id_detalhamento": s(d.get("id")),
                    "tipo": s(d.get("tipo")),
                    "descricao": s(d.get("descricao")),
                    "data_criacao": date(d.get("criacao")),
                    "link": s(d.get("link")),
                }
            )
    return licitacoes, detalhamentos


def build_empresa(extracted_at: str) -> list[dict]:
    """The company dimension.

    Only the identity fields are taken. The nested ``contratos``,
    ``notas_empenho`` and ``atas_registro_preco`` keys all return the same
    combined list, and it drops ``numero_formatado`` and ``unidade_gestora``,
    so the contratações themselves come from their own endpoints instead.
    """
    rows = [
        {
            "data_extracao": extracted_at,
            "id_empresa": s(r.get("id")),
            "cpf_cnpj": s(r.get("cpf_cnpj")),
            "nome": s(r.get("nome")),
        }
        for r in api.fetch("/contratacoes/empresas")
    ]
    # The bare listing repeats a handful of ids.
    return dedupe(rows, ["id_empresa"])


# =============================================== D. Colaboradores ===========


def build_terceirizado(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "id_terceirizado": s(r.get("id")),
            "cpf": s(r.get("cpf")),
            "nome": s(r.get("nome")),
            "situacao": s(r.get("situacao")),
            "empresa": s(r.get("empresa")),
            "id_contrato": s(r.get("codContrato")),
            "numero_contrato": s(r.get("numeroContrato")),
            "id_item_contrato": s(r.get("codItem")),
            "sigla_lotacao": s(g(r, "lotacao", "sigla")),
            "lotacao": s(g(r, "lotacao", "nome")),
        }
        for r in api.fetch("/contratacoes/terceirizados")
    ]


def build_menor_aprendiz(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "cpf": s(r.get("cpf")),
            "nome": s(r.get("nome")),
            "situacao": s(r.get("situacao")),
            "fornecedor": s(r.get("fornecedor")),
            "sigla_orgao": s(g(r, "orgao", "sigla")),
            "orgao": s(g(r, "orgao", "nome")),
        }
        for r in api.fetch("/contratacoes/menores_aprendizes")
    ]


def build_estagiario(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "nome": s(r.get("nome")),
            "curso": s(r.get("curso")),
            "sigla_orgao": s(r.get("siglaOrgao")),
            "orgao": s(r.get("nomeOrgao")),
        }
        for r in api.fetch("/colaboradores/estagiarios")
    ]


# ==================================================== E. Supridos ===========


def build_supridos(years: Iterable[int]) -> dict[str, list[dict]]:
    """Every supridos table, from one request per year.

    ``/supridos/{ano}`` nests the whole tree — suprido, atos de concessão, and
    each ato's empenhos, transações and movimentações. The four flat endpoints
    return the identical sets (verified for 2024), so reading them as well would
    quadruple the request count for nothing.
    """
    atos: list[dict] = []
    empenhos: list[dict] = []
    transacoes: list[dict] = []
    objetos: list[dict] = []
    movimentacoes: list[dict] = []
    subtipos: list[dict] = []

    for year in years:
        for suprido in api.fetch(f"/supridos/{year}"):
            for a in suprido.get("atosConcessao") or []:
                codigo = s(a.get("codigoAtoConcessao"))
                atos.append(
                    {
                        "ano": integer(a.get("ano")) or year,
                        "codigo_ato_concessao": codigo,
                        "codigo_suprido": s(suprido.get("codigo")),
                        "nome_suprido": s(suprido.get("nome")),
                        "codigo_orgao": s(g(suprido, "orgao", "codigo")),
                        "sigla_orgao": s(g(suprido, "orgao", "sigla")),
                        "orgao": s(g(suprido, "orgao", "nome")),
                        "matricula_solicitante": s(
                            a.get("matriculaSolicitante")
                        ),
                        "numero_processo": s(a.get("numeroProcesso")),
                        "indicador_regime_especial": flag(
                            a.get("regimeEspecial")
                        ),
                        "data": date(a.get("data")),
                        "data_publicacao_basf": date(
                            a.get("dataPublicacaoBASF")
                        ),
                        "prazo_aplicacao": date(a.get("prazoAplicacao")),
                        "prazo_comprovacao": date(a.get("prazoComprovacao")),
                        "data_aplicacao": date(a.get("dataAplicacao")),
                        "data_comprovacao": date(a.get("dataComprovacao")),
                        "valor_total_elementos_despesa": num(
                            a.get("valorTotalElementosDespesa")
                        ),
                        "valor_total_empenhos": num(
                            a.get("valorTotalEmpenhos")
                        ),
                        "valor_total_transacoes": num(
                            a.get("valorTotalTransacoes")
                        ),
                        "valor_total_movimentacoes": num(
                            a.get("valorTotalMovimentacoes")
                        ),
                        "data_processamento": datetime_(
                            a.get("dataProcessamento")
                        ),
                    }
                )
                for e in a.get("empenhos") or []:
                    empenhos.append(
                        {
                            "ano": year,
                            "codigo_ato_concessao": codigo,
                            "numero": s(e.get("numero")),
                            "rubrica": s(e.get("rubrica")),
                            "descricao": s(e.get("descricao")),
                            "valor_concedido": num(e.get("valorConcedido")),
                            "valor_executado": num(e.get("valorExecutado")),
                            "data": date(e.get("data")),
                            "data_processamento": datetime_(
                                e.get("dataProcessamento")
                            ),
                        }
                    )
                for t in a.get("transacoes") or []:
                    transacao_id = s(t.get("id"))
                    transacoes.append(
                        {
                            "ano": year,
                            "id_transacao": transacao_id,
                            "codigo_ato_concessao": codigo,
                            "tipo": s(t.get("tipo")),
                            "numero": s(t.get("numero")),
                            # tipoInscricaoTemp is the same value in enum form
                            # (PESSOA_JURIDICA vs "Pessoa jurídica"); only the
                            # readable label is kept.
                            "tipo_inscricao": s(t.get("tipoInscricao")),
                            "inscricao": s(t.get("inscricao")),
                            "fornecedor": s(t.get("fornecedor")),
                            "rubricas": s(t.get("rubricas")),
                            "valor": num(t.get("valor")),
                            "data": date(t.get("data")),
                            "data_processamento": datetime_(
                                t.get("dataProcessamento")
                            ),
                        }
                    )
                    for o in t.get("objetos") or []:
                        objetos.append(
                            {
                                "ano": year,
                                "id_transacao": transacao_id,
                                "id_objeto": s(o.get("id")),
                                "descricao_objeto": s(
                                    o.get("descricaoObjeto")
                                ),
                                "tipo_despesa": s(
                                    o.get("descricaoTipoDespesa")
                                ),
                                "subtipo_despesa": s(
                                    o.get("descricaoSubtipoDespesa")
                                ),
                                "rubrica": s(o.get("rubrica")),
                                "quantidade": num(o.get("quantidade")),
                                "valor_unitario": num(o.get("valorUnitario")),
                                "valor_total": num(o.get("valorTotal")),
                            }
                        )
                for m in a.get("movimentacoes") or []:
                    movimentacao_id = s(m.get("id"))
                    movimentacoes.append(
                        {
                            "ano": year,
                            "id_movimentacao": movimentacao_id,
                            "codigo_ato_concessao": codigo,
                            "tipo": s(m.get("tipo")),
                            "numero": s(m.get("numero")),
                            "tipo_inscricao": s(m.get("tipoInscricao")),
                            "inscricao": s(m.get("inscricao")),
                            "fornecedor": s(m.get("fornecedor")),
                            "rubricas": s(m.get("rubricas")),
                            "valor": num(m.get("valor")),
                            "data": date(m.get("data")),
                            "data_processamento": datetime_(
                                m.get("dataProcessamento")
                            ),
                        }
                    )
                    for st in m.get("subTipos") or []:
                        subtipos.append(
                            {
                                "ano": year,
                                "id_movimentacao": movimentacao_id,
                                "id_subtipo": s(st.get("id")),
                                "tipo_despesa": s(
                                    st.get("descricaoTipoDespesa")
                                ),
                                "subtipo_despesa": s(
                                    st.get("descricaoSubtipoDespesa")
                                ),
                                "rubrica": s(st.get("rubrica")),
                                "valor": num(st.get("valor")),
                            }
                        )

    # The source repeats some movimentações verbatim inside the same ato de
    # concessão — 3 of 836 in 2018, byte-identical including the ato — which
    # would otherwise double-count their value. Their subtipos inherit the
    # duplication, so both are deduplicated on their declared key.
    movimentacoes = dedupe(movimentacoes, ["ano", "id_movimentacao"])
    subtipos = dedupe(subtipos, ["ano", "id_movimentacao", "id_subtipo"])
    transacoes = dedupe(transacoes, ["ano", "id_transacao"])
    objetos = dedupe(objetos, ["ano", "id_transacao", "id_objeto"])

    return {
        "suprido_ato_concessao": dedupe(atos, ["ano", "codigo_ato_concessao"]),
        "suprido_empenho": dedupe(
            empenhos, ["ano", "codigo_ato_concessao", "numero"]
        ),
        "suprido_transacao": transacoes,
        "suprido_transacao_objeto": objetos,
        "suprido_movimentacao": movimentacoes,
        "suprido_movimentacao_subtipo": subtipos,
    }


# ====================================== F. Gestão e quadros de pessoal ======

# The six establishment reports consolidated into `quadro_pessoal`. Each entry
# maps one endpoint to (a) the dimension columns it fills and (b) its measures,
# split by period. The source's `variacao*` fields are deliberately absent: they
# are the percentage change between the ANTERIOR and ATUAL rows emitted here,
# and reproducing them would mix percentages into integer headcount columns.
QUADROS = (
    (
        "/gestao/quadro-cargos-efetivos",
        "cargo_efetivo",
        {
            "categoria": "categoria",
            "nivel": "nivel",
            "especialidade": "especialidade",
        },
        {
            "ATUAL": {
                "quantidade_cargos": "totalCargosHoje",
                "quantidade_ocupados": "totalOcupadosHoje",
                "quantidade_vagos": "totalVagosHoje",
            },
            "ANTERIOR": {
                "quantidade_ocupados": "totalOcupadosAnt",
                "quantidade_vagos": "totalVagosAnt",
            },
        },
    ),
    (
        "/gestao/quadro-funcoes-comissionadas",
        "funcao_comissionada",
        {"referencia": "referencia"},
        {
            "ATUAL": {
                "quantidade_total": "totalHoje",
                "quantidade_ocupados": "ocupadasEBloqueadasHoje",
                "quantidade_vagos": "livreHoje",
            },
            "ANTERIOR": {"quantidade_total": "totalAnoAnterior"},
        },
    ),
    (
        "/servidores/quadro-servidores-estaveis-e-nao-estaveis",
        "servidor_estavel",
        {"cargo": "cargo"},
        {
            "ATUAL": {
                "quantidade_estaveis": "qtdEstaveisHoje",
                "quantidade_nao_estaveis": "qtdNaoEstaveisHoje",
            },
            "ANTERIOR": {
                "quantidade_estaveis": "qtdEstaveisAnt",
                "quantidade_nao_estaveis": "qtdNaoEstaveisAnt",
            },
        },
    ),
    (
        "/servidores/quantitativos/pessoal",
        "pessoal",
        {
            "classe": "classe",
            "grupo": "grupo",
            "plano_carreira": "plano_carreira",
            "nivel_escolaridade": "nivel_escolaridade",
            "padrao_nivel_referencia": "padrao_nivel_referencia",
        },
        {
            "ATUAL": {
                "quantidade_estaveis": "estaveis",
                "quantidade_nao_estaveis": "nao_estaveis",
                "quantidade_subtotal": "subtotal",
                "quantidade_vagos": "vagos",
                "quantidade_total_ativo": "total_ativo",
                "quantidade_aposentados": "aposentados",
                "quantidade_instituidores_pensao": "instituidores_pensao",
                "quantidade_beneficiarios_pensao": "beneficiarios_pensao",
                "quantidade_total_inativo": "total_inativo",
            }
        },
    ),
    (
        "/servidores/quantitativos/cargos-funcoes",
        "cargo_funcao",
        {"nivel": "nivel", "tabela_vencimento": "tabVenc"},
        {
            "ATUAL": {
                "quantidade_subtotal": "subtotal",
                "quantidade_vagos": "vago",
                "quantidade_total": "total",
                "quantidade_com_opcao": "com_opcao",
                "quantidade_sem_opcao": "sem_opcao",
                "quantidade_sem_vinculo": "sem_vinculo",
            }
        },
    ),
    (
        "/senadores/quantitativos/senadores",
        "senador",
        {"cargo": "cargo"},
        {
            "ATUAL": {
                "quantidade_ocupados": "ocupados_ativo",
                "quantidade_vagos": "vagos_ativo",
                "quantidade_total_ativo": "total_ativo",
                "quantidade_aposentados": "aposentados",
                "quantidade_instituidores_pensao": "instituidores_pensao",
                "quantidade_beneficiarios_pensao": "beneficiarios_pensao",
                "quantidade_total_inativo": "total_inativo",
            }
        },
    ),
)

QUADRO_DIMENSIONS = (
    "categoria",
    "classe",
    "grupo",
    "cargo",
    "nivel",
    "especialidade",
    "referencia",
    "tabela_vencimento",
    "plano_carreira",
    "nivel_escolaridade",
    "padrao_nivel_referencia",
)

QUADRO_MEASURES = (
    "quantidade_cargos",
    "quantidade_ocupados",
    "quantidade_vagos",
    "quantidade_subtotal",
    "quantidade_total",
    "quantidade_com_opcao",
    "quantidade_sem_opcao",
    "quantidade_sem_vinculo",
    "quantidade_estaveis",
    "quantidade_nao_estaveis",
    "quantidade_total_ativo",
    "quantidade_aposentados",
    "quantidade_instituidores_pensao",
    "quantidade_beneficiarios_pensao",
    "quantidade_total_inativo",
)


def build_quadro_pessoal(extracted_at: str) -> list[dict]:
    """Consolidate the six establishment reports into one table."""
    rows = []
    for path, quadro, dims, periods in QUADROS:
        for r in api.fetch(path):
            for periodo, measures in periods.items():
                row = {
                    "data_extracao": extracted_at,
                    "data_referencia": date(r.get("data")),
                    "quadro": quadro,
                    "periodo": periodo,
                }
                for col in QUADRO_DIMENSIONS:
                    row[col] = s(r.get(dims[col])) if col in dims else None
                for col in QUADRO_MEASURES:
                    row[col] = (
                        integer(r.get(measures[col]))
                        if col in measures
                        else None
                    )
                rows.append(row)
    return rows


def build_diretor_coordenador(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "sigla_setor": s(g(r, "setor", "sigla")),
            "setor": s(g(r, "setor", "nome")),
            "sigla_setor_superior": s(g(r, "setor", "siglaSetorSuperior")),
            "setor_superior": s(g(r, "setor", "nomeSetorSuperior")),
            "matricula_titular": s(g(r, "titular", "matricula")),
            "nome_titular": s(g(r, "titular", "nome")),
            "cargo_titular": s(g(r, "titular", "cargo")),
            "email_titular": s(g(r, "titular", "email")),
            "referencia_chefia": s(g(r, "titular", "referenciaChefia")),
            "matricula_substituto": s(g(r, "substituto", "matricula")),
            "nome_substituto": s(g(r, "substituto", "nome")),
            "data_inicio_substituicao": date(g(r, "substituto", "dataInicio")),
            "data_fim_substituicao": date(g(r, "substituto", "dataFim")),
            "telefone": s(g(r, "setor", "telefone")),
            "endereco": s(g(r, "setor", "endereco")),
        }
        for r in api.fetch("/gestao/diretores-e-coordenadores")
    ]


def build_previsao_aposentadoria(extracted_at: str) -> list[dict]:
    return [
        {
            "data_extracao": extracted_at,
            "cargo": s(r.get("cargo")),
            "categoria": s(r.get("categoria")),
            "ano_direito": integer(r.get("anoDireito")),
            "mes_direito": integer(r.get("mesDireito")),
            "quantidade": integer(r.get("quantidade")),
        }
        for r in api.fetch("/servidores/previsao-aposentadoria")
    ]


# =================================================== G. Dicionário ==========

# Coded values this transform introduces, or that the source stores as codes.
# Readable labels the source already supplies (vinculo, situacao, modalidade,
# natureza_despesa, …) are deliberately absent: they need no dictionary, and
# their columns carry covered_by_dictionary = no.
SIM_NAO = {"sim": "Sim", "nao": "Não"}

DICIONARIO_ESTATICO: dict[tuple[str, str], dict[str, str]] = {
    ("senador_auxilio_moradia", "indicador_auxilio_moradia"): SIM_NAO,
    ("senador_auxilio_moradia", "indicador_imovel_funcional"): SIM_NAO,
    ("contratacao", "indicador_mao_de_obra"): SIM_NAO,
    ("licitacao", "indicador_registro_preco"): SIM_NAO,
    ("suprido_ato_concessao", "indicador_regime_especial"): SIM_NAO,
    ("senador_aposentado_pensionista", "tipo_beneficio"): {
        "aposentadoria": "Aposentadoria de ex-senador",
        "pensao": "Pensão de beneficiário de ex-senador",
    },
    ("senador_aposentado_pensionista", "regime"): {
        "geral": "Regime geral de aposentadoria de ex-senadores",
        "ipc": "Instituto de Previdência dos Congressistas",
        "pssc": "Plano de Seguridade Social dos Congressistas",
    },
    ("servidor_aposentado", "tipo_quadro"): {
        "efetivo": "Quadro de servidores efetivos",
        "comissionado": "Quadro de servidores comissionados",
    },
    ("servidor_cedido", "tipo_cessao"): {
        "cedido para o senado": "Servidor de outro órgão cedido ao Senado Federal",
        "cedido pelo senado": "Servidor do Senado Federal cedido a outro órgão",
        "cedido da infraero": "Servidor da Infraero cedido ao Senado Federal",
        "exercicio provisorio": "Servidor em exercício provisório no Senado Federal",
    },
    ("quadro_pessoal", "quadro"): {
        "cargo_efetivo": "Quadro de cargos efetivos",
        "funcao_comissionada": "Quadro de funções comissionadas",
        "servidor_estavel": "Quadro de servidores estáveis e não estáveis",
        "pessoal": "Quantitativo de pessoal",
        "cargo_funcao": "Quantitativo de cargos e funções",
        "senador": "Quantitativo de senadores",
    },
    ("quadro_pessoal", "periodo"): {
        "ATUAL": "Quantitativo na data de extração",
        "ANTERIOR": "Quantitativo no período anterior publicado pela fonte",
    },
}

# tipo_contratacao is the same code set on the parent and on every child table.
TIPO_CONTRATACAO_VALORES = {
    "contratos": "Contrato",
    "notas_empenho": "Nota de empenho",
    "atas_registro_preco": "Ata de registro de preço",
}
TABELAS_COM_TIPO_CONTRATACAO = (
    "contratacao",
    "contratacao_orgao_gestor",
    "contratacao_item",
    "contratacao_garantia",
    "contratacao_pagamento",
    "contratacao_documento_fiscal",
    "contratacao_pagamento_empenho",
)

# Columns whose labels the source itself supplies alongside the code, so the
# dictionary is derived from the data rather than hand-written.
RUBRICA_TABELAS = (
    "suprido_empenho",
    "suprido_transacao_objeto",
    "suprido_movimentacao_subtipo",
)


def build_dicionario(
    rubricas_por_tabela: dict[str, dict[str, str]] | None = None,
) -> list[dict]:
    """Assemble the dicionário from the static maps plus derived rubricas.

    ``rubricas_por_tabela`` maps each supridos table to the ``{código: rótulo}``
    pairs seen in it. It is accumulated during the extraction rather than
    recomputed from the finished tables, so the labels come from the same run as
    the data and the caller never has to hold those tables in memory.
    """
    rows = []

    def add(table: str, column: str, key: str, value: str) -> None:
        rows.append(
            {
                "id_tabela": table,
                "nome_coluna": column,
                "chave": key,
                "cobertura_temporal": None,
                "valor": value,
            }
        )

    for (table, column), mapping in DICIONARIO_ESTATICO.items():
        for key, value in mapping.items():
            add(table, column, key, value)

    for table in TABELAS_COM_TIPO_CONTRATACAO:
        for key, value in TIPO_CONTRATACAO_VALORES.items():
            add(table, "tipo_contratacao", key, value)

    # suprido_empenho is the only endpoint pairing a rubrica with its descrição;
    # the two child tables carry the bare code, so they borrow those labels.
    seen = rubricas_por_tabela or {}
    labels: dict[str, str] = {}
    for code, label in (seen.get("suprido_empenho") or {}).items():
        if label:
            labels.setdefault(code, label)
    for table in RUBRICA_TABELAS:
        for code in sorted(seen.get(table) or {}):
            add(table, "rubrica", code, labels.get(code) or code)

    return dedupe(rows, ["id_tabela", "nome_coluna", "chave"])
