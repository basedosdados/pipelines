"""Funções puras de br_senado_dados_abertos, sem Prefect."""

from __future__ import annotations

import calendar
import json
import os
import re
import shutil
import time
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from xml.etree.ElementTree import ParseError

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos.constants import (
    constants,
    constants_bloco,
    constants_comissao,
    constants_discurso,
    constants_lideranca,
    constants_mesa,
    constants_partido,
    constants_processo,
    constants_relatoria,
    constants_senador,
    constants_senador_cargo,
    constants_senador_comissao,
    constants_senador_filiacao,
    constants_senador_mandato,
    constants_votacao,
    constants_votacao_comissao,
    constants_votacao_comissao_parlamentar,
    constants_votacao_orientacao_bancada,
    constants_votacao_parlamentar,
)


def get_json(
    path: str,
    params: dict | None = None,
    retries: int = 6,
    backoff: float = 1.5,
    timeout: int = 120,
) -> Any:
    """Lê um endpoint da API em JSON, tentando de novo em resposta vazia ou erro.

    Args:
        path: caminho do endpoint abaixo de `constants.API_URL`.
        params: parâmetros da query string.
        retries: número de tentativas.
        backoff: base, em segundos, da espera linear entre as tentativas.
        timeout: tempo limite de cada requisição, em segundos.

    Returns:
        O JSON lido (dict ou list), ou None quando todas as tentativas
        respondem 200 com corpo vazio.

    Raises:
        httpx.RequestError: se alguma tentativa deu erro de rede e nenhuma
            teve sucesso.
        json.JSONDecodeError: se alguma tentativa recebeu JSON inválido e
            nenhuma teve sucesso.
        RuntimeError: se a última resposta teve status diferente de 200.
    """
    url = f"{constants.API_URL.value}{path}"
    last_error: Exception | None = None
    last_status: int | None = None
    for attempt in range(retries):
        try:
            response = httpx.get(
                url,
                params=params,
                headers=constants.JSON_HEADERS.value,
                timeout=timeout,
                follow_redirects=True,
            )
            last_status = response.status_code
            if (
                response.status_code == 200
                and response.content
                and response.text.strip()
            ):
                return response.json()
        except (httpx.RequestError, json.JSONDecodeError) as error:
            last_error = error
        time.sleep(backoff * (attempt + 1))
    if last_error is not None:
        raise last_error
    if last_status is not None and last_status != 200:
        raise RuntimeError(
            f"GET {url} failed: HTTP {last_status} after {retries} attempts"
        )
    return None


def get_json_safe(
    path: str, params: dict | None = None, retries: int = 6
) -> Any:
    """Lê um endpoint como `get_json`, devolvendo None em vez de levantar erro.

    Args:
        path: caminho do endpoint abaixo de `constants.API_URL`.
        params: parâmetros da query string.
        retries: número de tentativas.

    Returns:
        O JSON lido, ou None se a leitura falhar ou vier vazia.
    """
    try:
        return get_json(path, params, retries=retries)
    except Exception:
        return None


def _as_list(node: Any) -> list:
    """Normaliza para lista um nó que pode faltar ou vir como objeto solto.

    Args:
        node: nó lido do envelope da API.

    Returns:
        Lista vazia para None, o próprio nó se já for lista, ou uma lista com
        o nó.
    """
    if node is None:
        return []
    if isinstance(node, list):
        return node
    return [node]


def get_nested_value(data: Any, keys: list[str], default: Any = None) -> Any:
    """Percorre dicionários aninhados pelas chaves dadas.

    Args:
        data: dicionário de partida.
        keys: chaves, da mais externa para a mais interna.
        default: valor devolvido quando alguma chave falta.

    Returns:
        O valor encontrado no fim do caminho, ou `default`.
    """
    current = data
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def get_xml_records(
    path: str,
    record_tag: str,
    retries: int = 6,
    backoff: float = 1.5,
    timeout: int = 120,
) -> list[dict]:
    """Lê um endpoint em XML e devolve cada elemento `record_tag` como dicionário.

    Args:
        path: caminho do endpoint abaixo de `constants.API_URL`.
        record_tag: tag dos elementos a coletar (ex.: ``"Parlamentar"``).
        retries: número de tentativas.
        backoff: base, em segundos, da espera linear entre as tentativas.
        timeout: tempo limite de cada requisição, em segundos.

    Returns:
        Um dicionário por elemento, aninhado como o envelope JSON o aninharia;
        lista vazia quando alguma tentativa respondeu 200 com corpo vazio.

    Raises:
        httpx.RequestError: se alguma tentativa deu erro de rede e nenhuma
            teve sucesso.
        RuntimeError: se nenhuma tentativa respondeu 200.
    """
    url = f"{constants.API_URL.value}{path}"
    last_error: Exception | None = None
    last_status: int | None = None
    saw_empty_200 = False
    for attempt in range(retries):
        try:
            response = httpx.get(
                url,
                headers=constants.XML_HEADERS.value,
                timeout=timeout,
                follow_redirects=True,
            )
            last_status = response.status_code
            if response.status_code == 200:
                body = response.text.strip()
                if body:
                    return _parse_xml_records(body, record_tag)
                saw_empty_200 = True
        except httpx.RequestError as error:
            last_error = error
        time.sleep(backoff * (attempt + 1))
    if saw_empty_200:
        return []
    if last_error is not None:
        raise last_error
    raise RuntimeError(
        f"GET {url} (xml) failed: HTTP {last_status} after {retries} attempts"
    )


def _parse_xml_records(text: str, record_tag: str) -> list[dict]:
    """Extrai os elementos `record_tag` de um corpo XML, com ou sem raiz.

    Args:
        text: corpo XML da resposta.
        record_tag: tag dos elementos a coletar.

    Returns:
        Um dicionário por elemento encontrado, em qualquer profundidade.

    Raises:
        RuntimeError: se o corpo não puder ser lido nem depois de embrulhado
            numa raiz.
    """
    body = text.strip()
    if body.startswith("<?xml"):
        body = body.split("?>", 1)[-1].lstrip()
    try:
        root = ET.fromstring(body)
        found = [root] if root.tag == record_tag else root.iter(record_tag)
        return [_xml_to_dict(element) for element in found]
    except ParseError:
        pass
    try:
        root = ET.fromstring(f"<_bdwrap>{body}</_bdwrap>")
    except ParseError as error:
        raise RuntimeError(
            f"could not parse XML for <{record_tag}> records: {error}"
        ) from error
    return [_xml_to_dict(element) for element in root.iter(record_tag)]


def _xml_to_dict(element: ET.Element) -> Any:
    """Converte um elemento XML no formato que o envelope JSON teria.

    Args:
        element: elemento a converter.

    Returns:
        Para um elemento com filhos, um dicionário em que tags repetidas viram
        lista; para uma folha, o texto sem espaços nas pontas, ou None se
        vazio.
    """
    children = list(element)
    if not children:
        text = (element.text or "").strip()
        return text or None
    result: dict[str, Any] = {}
    for child in children:
        value = _xml_to_dict(child)
        if child.tag in result:
            existing = result[child.tag]
            if isinstance(existing, list):
                existing.append(value)
            else:
                result[child.tag] = [existing, value]
        else:
            result[child.tag] = value
    return result


def current_legislatura() -> int:
    """Calcula o número da legislatura em curso, de quatro anos cada.

    Returns:
        O número da legislatura que cobre o ano corrente.
    """
    year = pd.Timestamp.today().year
    return constants.CURRENT_LEG_REFERENCE.value + (
        max(0, year - constants.CURRENT_LEG_REFERENCE_YEAR.value) // 4
    )


def cleaned_string(value: object) -> str | None:
    """Converte um valor em texto sem espaços nas pontas.

    Args:
        value: valor lido da API.

    Returns:
        O texto, ou None para nulo, NaN ou texto vazio.
    """
    if value is None:
        return None
    if isinstance(value, float) and pd.isna(value):
        return None
    text = str(value).strip()
    return text or None


def normalize_date(value: object) -> str | None:
    """Extrai a data no formato AAAA-MM-DD do início de um valor.

    Args:
        value: valor lido da API.

    Returns:
        A data, ou None se o valor não começar com uma.
    """
    text = cleaned_string(value)
    if not text:
        return None
    text = text.replace("T", " ")
    match = re.match(r"(\d{4}-\d{2}-\d{2})", text)
    return match.group(1) if match else None


def normalize_datetime(value: object) -> str | None:
    """Normaliza um valor para AAAA-MM-DD HH:MM:SS.

    Args:
        value: valor lido da API.

    Returns:
        A data e hora, só a data quando o valor não traz hora, ou None se o
        valor não começar com uma data.
    """
    text = cleaned_string(value)
    if not text:
        return None
    text = text.replace("T", " ")
    match = re.match(r"(\d{4}-\d{2}-\d{2})[ ]?(\d{2}:\d{2}:\d{2})?", text)
    if not match:
        return None
    return (
        f"{match.group(1)} {match.group(2)}"
        if match.group(2)
        else match.group(1)
    )


def _frame(rows: list[dict], columns: list[str]) -> pd.DataFrame:
    """Monta um DataFrame com as colunas dadas, na ordem dada.

    Args:
        rows: linhas, uma por dicionário.
        columns: colunas da tabela, na ordem da arquitetura.

    Returns:
        DataFrame de objetos, com None nas colunas ausentes e no lugar de NaN.
    """
    df = pd.DataFrame(rows, columns=columns)
    # pyrefly: ignore [bad-argument-type]
    return df.astype(object).where(df.notna(), None)


def _fetch_many(
    paths: list[str],
    workers: int = constants.FETCH_WORKERS.value,
    retries: int = 6,
) -> list:
    """Lê vários endpoints em paralelo com `get_json_safe`.

    Args:
        paths: caminhos dos endpoints abaixo de `constants.API_URL`.
        workers: número de requisições simultâneas.
        retries: número de tentativas de cada requisição.

    Returns:
        O JSON de cada endpoint, na ordem de `paths`, com None nos que
        falharam.
    """
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(
            executor.map(
                lambda path: get_json_safe(path, retries=retries), paths
            )
        )


def clean_partido() -> pd.DataFrame:
    """Extrai e limpa a tabela `partido`.

    Returns:
        DataFrame com as colunas de `constants_partido.COLUMNS`.
    """
    response = get_json("/composicao/lista/partidos")
    rows = [
        {
            "id_partido": cleaned_string(partido.get("Codigo")),
            "sigla_partido": cleaned_string(partido.get("Sigla")),
            "nome_partido": cleaned_string(partido.get("Nome")),
            "data_criacao": normalize_date(partido.get("DataCriacao")),
            "data_extincao": normalize_date(partido.get("DataExtincao")),
        }
        for partido in _as_list(
            get_nested_value(
                response, ["ListaPartidos", "Partidos", "Partido"]
            )
        )
    ]
    return _frame(rows, constants_partido.COLUMNS.value).drop_duplicates(
        subset=["id_partido"]
    )


def clean_bloco() -> pd.DataFrame:
    """Extrai e limpa a tabela `bloco`.

    Returns:
        DataFrame com as colunas de `constants_bloco.COLUMNS`.
    """
    response = get_json("/composicao/lista/blocos")
    rows = [
        {
            "id_bloco": cleaned_string(bloco.get("CodigoBloco")),
            "nome_bloco": cleaned_string(bloco.get("NomeBloco")),
            "nome_apelido": cleaned_string(bloco.get("NomeApelido")),
            "data_criacao": normalize_date(bloco.get("DataCriacao")),
            "data_extincao": normalize_date(bloco.get("DataExtincao")),
        }
        for bloco in _as_list(
            get_nested_value(
                response, ["ListaBlocoParlamentar", "Blocos", "Bloco"]
            )
        )
    ]
    return _frame(rows, constants_bloco.COLUMNS.value).drop_duplicates(
        subset=["id_bloco"]
    )


def clean_lideranca() -> pd.DataFrame:
    """Extrai e limpa a tabela `lideranca`.

    Returns:
        DataFrame com as colunas de `constants_lideranca.COLUMNS`.
    """
    liderancas = _as_list(get_json("/composicao/lideranca"))
    rows = [
        {
            "id_lideranca": cleaned_string(lideranca.get("codigo")),
            "casa": cleaned_string(lideranca.get("casa")),
            "id_senador": cleaned_string(lideranca.get("codigoParlamentar")),
            "nome_parlamentar": cleaned_string(
                lideranca.get("nomeParlamentar")
            ),
            "sigla_tipo_lideranca": cleaned_string(
                lideranca.get("siglaTipoLideranca")
            ),
            "descricao_tipo_lideranca": cleaned_string(
                lideranca.get("descricaoTipoLideranca")
            ),
            "sigla_tipo_unidade_lideranca": cleaned_string(
                lideranca.get("siglaTipoUnidadeLideranca")
            ),
            "descricao_tipo_unidade_lideranca": cleaned_string(
                lideranca.get("descricaoTipoUnidadeLideranca")
            ),
            "id_partido_filiacao": cleaned_string(
                lideranca.get("codigoPartidoFiliacao")
            ),
            "sigla_partido_filiacao": cleaned_string(
                lideranca.get("siglaPartidoFiliacao")
            ),
            "nome_partido_filiacao": cleaned_string(
                lideranca.get("nomePartidoFiliacao")
            ),
            "id_bloco": cleaned_string(lideranca.get("codigoBloco")),
            "sigla_bloco": cleaned_string(lideranca.get("siglaBloco")),
            "nome_bloco": cleaned_string(lideranca.get("nomeBloco")),
            "numero_ordem_vice_lider": cleaned_string(
                lideranca.get("numeroOrdemViceLider")
            ),
            "data_designacao": normalize_date(lideranca.get("dataDesignacao")),
        }
        for lideranca in liderancas
    ]
    return _frame(rows, constants_lideranca.COLUMNS.value).drop_duplicates(
        subset=["id_lideranca"]
    )


def clean_comissao() -> pd.DataFrame:
    """Extrai e limpa a tabela `comissao`.

    Returns:
        DataFrame com as colunas de `constants_comissao.COLUMNS`.
    """
    response = get_json("/comissao/lista/colegiados")
    rows = [
        {
            "id_comissao": cleaned_string(comissao.get("Codigo")),
            "sigla_comissao": cleaned_string(comissao.get("Sigla")),
            "nome_comissao": cleaned_string(comissao.get("Nome")),
            "sigla_casa": cleaned_string(comissao.get("SiglaCasa")),
            "id_tipo_colegiado": cleaned_string(
                comissao.get("CodigoTipoColegiado")
            ),
            "sigla_tipo_colegiado": cleaned_string(
                comissao.get("SiglaTipoColegiado")
            ),
            "descricao_tipo_colegiado": cleaned_string(
                comissao.get("DescricaoTipoColegiado")
            ),
            "publica": cleaned_string(comissao.get("Publica")),
            "data_inicio": normalize_date(comissao.get("DataInicio")),
        }
        for comissao in _as_list(
            get_nested_value(
                response, ["ListaColegiados", "Colegiados", "Colegiado"]
            )
        )
    ]
    return _frame(rows, constants_comissao.COLUMNS.value).drop_duplicates(
        subset=["id_comissao"]
    )


def _read_mesa_cargos(endpoint: str) -> list[dict]:
    """Lê os cargos da Mesa num endpoint de composição.

    Args:
        endpoint: caminho do endpoint da Mesa (do Senado ou do Congresso).

    Returns:
        Uma linha de `mesa` por cargo.
    """
    response = get_json(endpoint)
    root = "MesaSenado" if "SF" in endpoint else "MesaCongresso"
    rows = []
    for colegiado in _as_list(
        get_nested_value(response, [root, "Colegiados", "Colegiado"])
    ):
        for cargo in _as_list(
            get_nested_value(colegiado, ["Cargos", "Cargo"])
        ):
            nome = cleaned_string(cargo.get("NomeParlamentar"))
            if nome:
                nome = re.sub(r"^Senador[a]?\s+", "", nome)
            rows.append(
                {
                    "id_colegiado": cleaned_string(
                        colegiado.get("CodigoColegiado")
                    ),
                    "sigla_colegiado": cleaned_string(
                        colegiado.get("SiglaColegiado")
                    ),
                    "nome_colegiado": cleaned_string(
                        colegiado.get("NomeColegiado")
                    ),
                    "cargo": cleaned_string(
                        (_as_list(cargo.get("Cargo")) or [None])[0]
                    ),
                    "id_senador": cleaned_string(cargo.get("Http")),
                    "nome_parlamentar": nome,
                    "bancada": cleaned_string(cargo.get("Bancada")),
                    "ordem": cleaned_string(cargo.get("NumeroOrdemImpressao")),
                    "origem": cleaned_string(cargo.get("Origem")),
                }
            )
    return rows


def clean_mesa() -> pd.DataFrame:
    """Extrai e limpa a tabela `mesa`, com a Mesa do Senado.

    Returns:
        DataFrame com as colunas de `constants_mesa.COLUMNS`.
    """
    rows = _read_mesa_cargos("/composicao/mesaSF")
    return _frame(rows, constants_mesa.COLUMNS.value)


def _legislatura_parlamentares(legislatura: int) -> list[dict]:
    """Lê, em XML, os parlamentares de uma legislatura.

    Args:
        legislatura: número da legislatura.

    Returns:
        Um dicionário por parlamentar; lista vazia quando a legislatura não
        tem parlamentares na API.
    """
    return get_xml_records(
        f"/senador/lista/legislatura/{legislatura}", record_tag="Parlamentar"
    )


def _parlamentar_uf(parlamentar: dict) -> str | None:
    """Lê a UF do parlamentar no mandato ou, na falta, na identificação.

    Args:
        parlamentar: registro `Parlamentar` do roster por legislatura.

    Returns:
        A sigla da UF, ou None quando nenhum dos dois blocos traz uma.
    """
    for mandato in _as_list(
        get_nested_value(parlamentar, ["Mandatos", "Mandato"])
    ):
        if isinstance(mandato, dict) and (
            uf := cleaned_string(mandato.get("UfParlamentar"))
        ):
            return uf
    identificacao = parlamentar.get("IdentificacaoParlamentar", {}) or {}
    return cleaned_string(identificacao.get("UfParlamentar"))


def clean_senador() -> pd.DataFrame:
    """Extrai e limpa a tabela `senador`, com uma linha por senador.

    Returns:
        DataFrame com as colunas de `constants_senador.COLUMNS`.

    Raises:
        RuntimeError: se nenhuma legislatura devolver parlamentares.
    """
    primeira_legislatura = constants.LEG_START.value
    ultima_legislatura = current_legislatura()
    rows: list[dict] = []
    for legislatura in range(primeira_legislatura, ultima_legislatura + 1):
        for parlamentar in _legislatura_parlamentares(legislatura):
            identificacao = (
                parlamentar.get("IdentificacaoParlamentar", {}) or {}
            )
            forma_tratamento = cleaned_string(
                identificacao.get("FormaTratamento")
            )
            rows.append(
                {
                    "id_senador": cleaned_string(
                        identificacao.get("CodigoParlamentar")
                    ),
                    "nome": cleaned_string(
                        identificacao.get("NomeParlamentar")
                    ),
                    "nome_completo": cleaned_string(
                        identificacao.get("NomeCompletoParlamentar")
                    ),
                    "sexo": cleaned_string(
                        identificacao.get("SexoParlamentar")
                    ),
                    "forma_tratamento": forma_tratamento.strip()
                    if forma_tratamento
                    else None,
                    "sigla_partido": cleaned_string(
                        identificacao.get("SiglaPartidoParlamentar")
                    ),
                    "sigla_uf": _parlamentar_uf(parlamentar),
                    "email": cleaned_string(
                        identificacao.get("EmailParlamentar")
                    ),
                    "url_foto": cleaned_string(
                        identificacao.get("UrlFotoParlamentar")
                    ),
                    "url_pagina": cleaned_string(
                        identificacao.get("UrlPaginaParlamentar")
                    ),
                    "url_pagina_particular": cleaned_string(
                        identificacao.get("UrlPaginaParticular")
                    ),
                    "id_publico_legislatura_atual": cleaned_string(
                        identificacao.get("CodigoPublicoNaLegAtual")
                    ),
                    "_legislatura": legislatura,
                }
            )
    if not rows:
        raise RuntimeError(
            "senador: no parlamentares returned for legislaturas "
            f"{primeira_legislatura}..{ultima_legislatura} — the dados-abertos "
            "roster endpoint changed shape again; inspect "
            "/senador/lista/legislatura/{legislatura}"
        )
    df = pd.DataFrame(rows)
    df = df[df["id_senador"].notna()].copy()
    df["_score"] = df[constants_senador.COLUMNS.value].notna().sum(axis=1)
    df = (
        df.sort_values(
            ["id_senador", "_score", "_legislatura"],
            ascending=[True, False, False],
            kind="mergesort",
        )
        .drop_duplicates(subset=["id_senador"], keep="first")
        .reset_index(drop=True)
    )
    return _frame(df.to_dict("records"), constants_senador.COLUMNS.value)


def _votacao_year(year: int) -> tuple[list[dict], list[dict]]:
    """Lê as votações nominais do Plenário em um ano.

    Args:
        year: ano consultado.

    Returns:
        As linhas de `votacao` e as de `votacao_parlamentar`.
    """
    votacoes = _as_list(
        get_json(
            "/votacao",
            {"dataInicio": f"{year}-01-01", "dataFim": f"{year}-12-31"},
        )
    )
    voto_map = constants.VOTO_MAP.value
    votacao_rows, votacao_parlamentar_rows = [], []
    for votacao in votacoes:
        data_sessao = normalize_date(votacao.get("dataSessao"))
        ano = data_sessao[:4] if data_sessao else str(year)
        id_votacao = cleaned_string(votacao.get("codigoSessaoVotacao"))
        tally = {
            "voto_sim": 0,
            "voto_nao": 0,
            "voto_abstencao": 0,
            "voto_outro": 0,
        }
        for voto in _as_list(votacao.get("votos")):
            key = voto_map.get(
                cleaned_string(voto.get("siglaVotoParlamentar")) or "",
                "voto_outro",
            )
            tally[key] += 1
            votacao_parlamentar_rows.append(
                {
                    "ano": ano,
                    "id_votacao": id_votacao,
                    "data_sessao": data_sessao,
                    "id_senador": cleaned_string(
                        voto.get("codigoParlamentar")
                    ),
                    "sigla_partido": cleaned_string(
                        voto.get("siglaPartidoParlamentar")
                    ),
                    "sigla_uf": cleaned_string(voto.get("siglaUFParlamentar")),
                    "voto": cleaned_string(voto.get("siglaVotoParlamentar")),
                    "descricao_voto": cleaned_string(
                        voto.get("descricaoVotoParlamentar")
                    ),
                }
            )
        informe_legislativo = votacao.get("informeLegislativo") or {}
        votacao_rows.append(
            {
                "ano": ano,
                "id_votacao": id_votacao,
                "data_sessao": data_sessao,
                "casa": cleaned_string(votacao.get("casaSessao")),
                "id_processo": cleaned_string(votacao.get("idProcesso")),
                "codigo_materia": cleaned_string(votacao.get("codigoMateria")),
                "identificacao_materia": cleaned_string(
                    votacao.get("identificacao")
                ),
                "sigla_materia": cleaned_string(votacao.get("sigla")),
                "numero_materia": cleaned_string(votacao.get("numero")),
                "ano_materia": cleaned_string(votacao.get("ano")),
                "id_sessao": cleaned_string(votacao.get("codigoSessao")),
                "id_sessao_legislativa": cleaned_string(
                    votacao.get("codigoSessaoLegislativa")
                ),
                "numero_sessao": cleaned_string(votacao.get("numeroSessao")),
                "sequencial_sessao": cleaned_string(
                    votacao.get("sequencialSessao")
                ),
                "sequencial_votacao": cleaned_string(
                    votacao.get("sequencialVotacao")
                ),
                "sigla_tipo_sessao": cleaned_string(
                    votacao.get("siglaTipoSessao")
                ),
                "descricao_votacao": cleaned_string(
                    votacao.get("descricaoVotacao")
                ),
                "ementa": cleaned_string(votacao.get("ementa")),
                "resultado_votacao": cleaned_string(
                    votacao.get("resultadoVotacao")
                ),
                "votacao_secreta": cleaned_string(
                    votacao.get("votacaoSecreta")
                ),
                "data_apresentacao": normalize_date(
                    votacao.get("dataApresentacao")
                ),
                "sigla_colegiado": cleaned_string(
                    informe_legislativo.get("siglaColegiado")
                ),
                "nome_colegiado": cleaned_string(
                    informe_legislativo.get("nomeColegiado")
                ),
                "voto_sim": str(tally["voto_sim"]),
                "voto_nao": str(tally["voto_nao"]),
                "voto_abstencao": str(tally["voto_abstencao"]),
                "voto_outro": str(tally["voto_outro"]),
            }
        )
    return votacao_rows, votacao_parlamentar_rows


def clean_votacao(years: range) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extrai e limpa as tabelas `votacao` e `votacao_parlamentar`.

    Args:
        years: anos consultados.

    Returns:
        DataFrames de `votacao` e de `votacao_parlamentar`, com as colunas de
        `constants_votacao.COLUMNS` e `constants_votacao_parlamentar.COLUMNS`.
    """
    votacao_rows, votacao_parlamentar_rows = [], []
    for year in years:
        year_votacao_rows, year_votacao_parlamentar_rows = _votacao_year(year)
        votacao_rows += year_votacao_rows
        votacao_parlamentar_rows += year_votacao_parlamentar_rows
        print(
            f"    votacao {year}: {len(year_votacao_rows)} votes, {len(year_votacao_parlamentar_rows)} individual"
        )
    return (
        _frame(votacao_rows, constants_votacao.COLUMNS.value),
        _frame(
            votacao_parlamentar_rows,
            constants_votacao_parlamentar.COLUMNS.value,
        ),
    )


def clean_orientacao(years: range) -> pd.DataFrame:
    """Extrai e limpa a tabela `votacao_orientacao_bancada`.

    Args:
        years: anos consultados.

    Returns:
        DataFrame com as colunas de `constants_votacao_orientacao_bancada.COLUMNS`.
    """
    rows = []
    for year in years:
        response = get_json(
            f"/plenario/votacao/orientacaoBancada/{year}0101/{year}1231"
        )
        votacoes = (
            _as_list(get_nested_value(response, ["votacoes"]))
            if isinstance(response, dict)
            else _as_list(response)
        )
        row_count = 0
        for votacao in votacoes:
            data_votacao = normalize_date(votacao.get("dataInicioVotacao"))
            ano = data_votacao[:4] if data_votacao else str(year)
            for orientacao in _as_list(votacao.get("orientacoesLideranca")):
                rows.append(
                    {
                        "ano": ano,
                        "id_votacao_sve": cleaned_string(
                            votacao.get("codigoVotacaoSve")
                        ),
                        "sequencial_votacao": cleaned_string(
                            votacao.get("sequencialVotacao")
                        ),
                        "data_votacao": data_votacao,
                        "sigla_materia": cleaned_string(
                            votacao.get("siglaTipoMateria")
                        ),
                        "numero_materia": cleaned_string(
                            votacao.get("numeroMateria")
                        ),
                        "ano_materia": cleaned_string(
                            votacao.get("anoMateria")
                        ),
                        "bancada": cleaned_string(orientacao.get("partido")),
                        "orientacao": cleaned_string(orientacao.get("voto")),
                        "data_hora_orientacao": normalize_datetime(
                            orientacao.get("dataHora")
                        ),
                    }
                )
                row_count += 1
        print(f"    orientacao {year}: {row_count} rows")
    return _frame(rows, constants_votacao_orientacao_bancada.COLUMNS.value)


def _parse_identificacao(
    identificacao: str | None,
) -> tuple[str | None, str | None, str | None]:
    """Separa sigla, número e ano de uma identificação `<SIGLA> <NUMERO>/<ANO>`.

    Args:
        identificacao: identificação da matéria ou do processo.

    Returns:
        Sigla, número e ano, ou três None se a identificação não casar com o
        formato.
    """
    if not identificacao:
        return None, None, None
    match = re.match(constants.IDENTIFICACAO_PATTERN.value, identificacao)
    if match:
        return match.group(1), match.group(2), match.group(3)
    return None, None, None


def clean_processo(years: range) -> pd.DataFrame:
    """Extrai e limpa a tabela `processo`.

    Args:
        years: anos consultados.

    Returns:
        DataFrame com as colunas de `constants_processo.COLUMNS`.
    """
    rows = []
    for year in years:
        processos = _as_list(get_json("/processo", {"ano": year}))
        for processo in processos:
            identificacao = cleaned_string(processo.get("identificacao"))
            sigla, numero, ano_identificacao = _parse_identificacao(
                identificacao
            )
            rows.append(
                {
                    "ano": ano_identificacao or str(year),
                    "id_processo": cleaned_string(processo.get("id")),
                    "codigo_materia": cleaned_string(
                        processo.get("codigoMateria")
                    ),
                    "identificacao": identificacao,
                    "sigla": sigla,
                    "numero": numero,
                    "autoria": cleaned_string(processo.get("autoria")),
                    "ementa": cleaned_string(processo.get("ementa")),
                    "objetivo": cleaned_string(processo.get("objetivo")),
                    "tipo_documento": cleaned_string(
                        processo.get("tipoDocumento")
                    ),
                    "tipo_conteudo": cleaned_string(
                        processo.get("tipoConteudo")
                    ),
                    "situacao_atual": cleaned_string(
                        processo.get("situacaoAtual")
                    ),
                    "sigla_tipo_deliberacao": cleaned_string(
                        processo.get("siglaTipoDeliberacao")
                    ),
                    "ente_identificador": cleaned_string(
                        processo.get("enteIdentificador")
                    ),
                    "casa_identificadora": cleaned_string(
                        processo.get("casaIdentificadora")
                    ),
                    "norma_gerada": cleaned_string(
                        processo.get("normaGerada")
                    ),
                    "apelido": cleaned_string(processo.get("apelido")),
                    "tramitando": cleaned_string(processo.get("tramitando")),
                    "data_apresentacao": normalize_date(
                        processo.get("dataApresentacao")
                    ),
                    "data_deliberacao": normalize_date(
                        processo.get("dataDeliberacao")
                    ),
                    "data_situacao_atual": normalize_date(
                        processo.get("dataSituacaoAtual")
                    ),
                    "data_ultima_atualizacao": normalize_datetime(
                        processo.get("dataUltimaAtualizacao")
                    ),
                    "ultima_informacao_atualizada": cleaned_string(
                        processo.get("ultimaInformacaoAtualizada")
                    ),
                    "url_documento": cleaned_string(
                        processo.get("urlDocumento")
                    ),
                }
            )
        if processos:
            print(f"    processo {year}: {len(processos)} records")
    return _frame(rows, constants_processo.COLUMNS.value).drop_duplicates(
        subset=["id_processo"]
    )


def _get_parlamentar(response: Any, root: str) -> dict:
    """Devolve o nó `Parlamentar` de um envelope por senador.

    Args:
        response: JSON lido do endpoint por senador.
        root: nome do envelope (ex.: ``"MandatoParlamentar"``).

    Returns:
        O nó `Parlamentar`, ou um dicionário vazio.
    """
    return get_nested_value(response, [root, "Parlamentar"]) or {}


def clean_senador_mandato(codes: list[str]) -> pd.DataFrame:
    """Extrai e limpa a tabela `senador_mandato`.

    Args:
        codes: códigos dos senadores (`id_senador`).

    Returns:
        DataFrame com as colunas de `constants_senador_mandato.COLUMNS`.
    """
    rows = []
    responses = _fetch_many([f"/senador/{code}/mandatos" for code in codes])
    for code, response in zip(codes, responses, strict=True):
        parlamentar = _get_parlamentar(response, "MandatoParlamentar")
        id_senador = cleaned_string(
            parlamentar.get("Codigo")
        ) or cleaned_string(code)
        for mandato in _as_list(
            get_nested_value(parlamentar, ["Mandatos", "Mandato"])
        ):
            primeira_legislatura = (
                mandato.get("PrimeiraLegislaturaDoMandato") or {}
            )
            segunda_legislatura = (
                mandato.get("SegundaLegislaturaDoMandato") or {}
            )
            rows.append(
                {
                    "id_senador": id_senador,
                    "id_mandato": cleaned_string(mandato.get("CodigoMandato")),
                    "sigla_uf": cleaned_string(mandato.get("UfParlamentar")),
                    "participacao": cleaned_string(
                        mandato.get("DescricaoParticipacao")
                    ),
                    "numero_legislatura_1": cleaned_string(
                        primeira_legislatura.get("NumeroLegislatura")
                    ),
                    "data_inicio_legislatura_1": normalize_date(
                        primeira_legislatura.get("DataInicio")
                    ),
                    "data_fim_legislatura_1": normalize_date(
                        primeira_legislatura.get("DataFim")
                    ),
                    "numero_legislatura_2": cleaned_string(
                        segunda_legislatura.get("NumeroLegislatura")
                    ),
                    "data_inicio_legislatura_2": normalize_date(
                        segunda_legislatura.get("DataInicio")
                    ),
                    "data_fim_legislatura_2": normalize_date(
                        segunda_legislatura.get("DataFim")
                    ),
                }
            )
    return _frame(
        rows, constants_senador_mandato.COLUMNS.value
    ).drop_duplicates(subset=["id_senador", "id_mandato"])


def clean_senador_filiacao(codes: list[str]) -> pd.DataFrame:
    """Extrai e limpa a tabela `senador_filiacao`.

    Args:
        codes: códigos dos senadores (`id_senador`).

    Returns:
        DataFrame com as colunas de `constants_senador_filiacao.COLUMNS`.
    """
    rows = []
    responses = _fetch_many([f"/senador/{code}/filiacoes" for code in codes])
    for code, response in zip(codes, responses, strict=True):
        parlamentar = _get_parlamentar(response, "FiliacaoParlamentar")
        id_senador = cleaned_string(
            parlamentar.get("Codigo")
        ) or cleaned_string(code)
        for filiacao in _as_list(
            get_nested_value(parlamentar, ["Filiacoes", "Filiacao"])
        ):
            partido = filiacao.get("Partido") or {}
            rows.append(
                {
                    "id_senador": id_senador,
                    "id_partido": cleaned_string(partido.get("CodigoPartido")),
                    "sigla_partido": cleaned_string(
                        partido.get("SiglaPartido")
                    ),
                    "data_filiacao": normalize_date(
                        filiacao.get("DataFiliacao")
                    ),
                    "data_desfiliacao": normalize_date(
                        filiacao.get("DataDesfiliacao")
                    ),
                }
            )
    return _frame(
        rows, constants_senador_filiacao.COLUMNS.value
    ).drop_duplicates(subset=["id_senador", "id_partido", "data_filiacao"])


def clean_senador_comissao(codes: list[str]) -> pd.DataFrame:
    """Extrai e limpa a tabela `senador_comissao`.

    Args:
        codes: códigos dos senadores (`id_senador`).

    Returns:
        DataFrame com as colunas de `constants_senador_comissao.COLUMNS`.
    """
    rows = []
    responses = _fetch_many([f"/senador/{code}/comissoes" for code in codes])
    for code, response in zip(codes, responses, strict=True):
        parlamentar = _get_parlamentar(response, "MembroComissaoParlamentar")
        id_senador = cleaned_string(
            parlamentar.get("Codigo")
        ) or cleaned_string(code)
        for participacao in _as_list(
            get_nested_value(parlamentar, ["MembroComissoes", "Comissao"])
        ):
            identificacao_comissao = (
                participacao.get("IdentificacaoComissao") or {}
            )
            rows.append(
                {
                    "id_senador": id_senador,
                    "id_comissao": cleaned_string(
                        identificacao_comissao.get("CodigoComissao")
                    ),
                    "sigla_comissao": cleaned_string(
                        identificacao_comissao.get("SiglaComissao")
                    ),
                    "sigla_casa": cleaned_string(
                        identificacao_comissao.get("SiglaCasaComissao")
                    ),
                    "participacao": cleaned_string(
                        participacao.get("DescricaoParticipacao")
                    ),
                    "data_inicio": normalize_date(
                        participacao.get("DataInicio")
                    ),
                    "data_fim": normalize_date(participacao.get("DataFim")),
                }
            )
    return _frame(
        rows, constants_senador_comissao.COLUMNS.value
    ).drop_duplicates(subset=["id_senador", "id_comissao", "data_inicio"])


def clean_senador_cargo(codes: list[str]) -> pd.DataFrame:
    """Extrai e limpa a tabela `senador_cargo`.

    Args:
        codes: códigos dos senadores (`id_senador`).

    Returns:
        DataFrame com as colunas de `constants_senador_cargo.COLUMNS`.
    """
    rows = []
    responses = _fetch_many([f"/senador/{code}/cargos" for code in codes])
    for code, response in zip(codes, responses, strict=True):
        parlamentar = _get_parlamentar(response, "CargoParlamentar")
        id_senador = cleaned_string(
            parlamentar.get("Codigo")
        ) or cleaned_string(code)
        for cargo in _as_list(
            get_nested_value(parlamentar, ["Cargos", "Cargo"])
        ):
            identificacao_comissao = cargo.get("IdentificacaoComissao") or {}
            rows.append(
                {
                    "id_senador": id_senador,
                    "id_comissao": cleaned_string(
                        identificacao_comissao.get("CodigoComissao")
                    ),
                    "sigla_comissao": cleaned_string(
                        identificacao_comissao.get("SiglaComissao")
                    ),
                    "id_cargo": cleaned_string(cargo.get("CodigoCargo")),
                    "descricao_cargo": cleaned_string(
                        cargo.get("DescricaoCargo")
                    ),
                    "data_inicio": normalize_date(cargo.get("DataInicio")),
                    "data_fim": normalize_date(cargo.get("DataFim")),
                }
            )
    return _frame(rows, constants_senador_cargo.COLUMNS.value).drop_duplicates(
        subset=["id_senador", "id_comissao", "id_cargo", "data_inicio"]
    )


def clean_relatoria(years: range) -> pd.DataFrame:
    """Extrai e limpa a tabela `relatoria`.

    Args:
        years: anos de apresentação dos processos consultados.

    Returns:
        DataFrame com as colunas de `constants_relatoria.COLUMNS`.
    """
    rows = []
    for year in years:
        relatorias = _as_list(get_json("/processo/relatoria", {"ano": year}))
        for relatoria in relatorias:
            data_designacao = normalize_date(relatoria.get("dataDesignacao"))
            rows.append(
                {
                    "ano": str(year),
                    "id_relatoria": cleaned_string(relatoria.get("id")),
                    "id_processo": cleaned_string(relatoria.get("idProcesso")),
                    "codigo_materia": cleaned_string(
                        relatoria.get("codigoMateria")
                    ),
                    "identificacao_processo": cleaned_string(
                        relatoria.get("identificacaoProcesso")
                    ),
                    "id_senador": cleaned_string(
                        relatoria.get("codigoParlamentar")
                    ),
                    "sigla_casa_relator": cleaned_string(
                        relatoria.get("casaRelator")
                    )
                    or cleaned_string(relatoria.get("siglaCasa")),
                    "id_colegiado": cleaned_string(
                        relatoria.get("codigoColegiado")
                    ),
                    "sigla_colegiado": cleaned_string(
                        relatoria.get("siglaColegiado")
                    ),
                    "nome_colegiado": cleaned_string(
                        relatoria.get("nomeColegiado")
                    ),
                    "id_tipo_colegiado": cleaned_string(
                        relatoria.get("codigoTipoColegiado")
                    ),
                    "tipo_relator": cleaned_string(
                        relatoria.get("descricaoTipoRelator")
                    ),
                    "id_tipo_relator": cleaned_string(
                        relatoria.get("idTipoRelator")
                    ),
                    "numero_autuacao": cleaned_string(
                        relatoria.get("numeroAutuacao")
                    ),
                    "tipo_encerramento": cleaned_string(
                        relatoria.get("descricaoTipoEncerramento")
                    ),
                    "data_designacao": data_designacao,
                    "data_destituicao": normalize_date(
                        relatoria.get("dataDestituicao")
                    ),
                    "tramitando": cleaned_string(relatoria.get("tramitando")),
                }
            )
        if relatorias:
            print(f"    relatoria {year}: {len(relatorias)} rows")
    return _frame(rows, constants_relatoria.COLUMNS.value).drop_duplicates(
        subset=["id_relatoria"]
    )


def clean_votacao_comissao(
    siglas: list[str],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extrai e limpa as tabelas `votacao_comissao` e `votacao_comissao_parlamentar`.

    Args:
        siglas: siglas das comissões consultadas.

    Returns:
        DataFrames de `votacao_comissao` e de `votacao_comissao_parlamentar`,
        com as colunas de `constants_votacao_comissao.COLUMNS` e
        `constants_votacao_comissao_parlamentar.COLUMNS`.
    """
    votacao_comissao_rows, votacao_comissao_parlamentar_rows = [], []
    responses = _fetch_many(
        [f"/votacaoComissao/comissao/{sigla}" for sigla in siglas]
    )
    for sigla, response in zip(siglas, responses, strict=True):
        votacoes = _as_list(
            get_nested_value(
                response, ["VotacoesComissao", "Votacoes", "Votacao"]
            )
        )
        for votacao in votacoes:
            data_reuniao = normalize_date(votacao.get("DataHoraInicioReuniao"))
            if not data_reuniao:
                continue
            ano = data_reuniao[:4]
            id_votacao = cleaned_string(votacao.get("CodigoVotacao"))
            identificacao = cleaned_string(votacao.get("IdentificacaoMateria"))
            sigla_materia, numero_materia, ano_materia = _parse_identificacao(
                identificacao
            )
            votacao_comissao_rows.append(
                {
                    "ano": ano,
                    "id_votacao": id_votacao,
                    "id_comissao": cleaned_string(
                        votacao.get("CodigoColegiado")
                    ),
                    "sigla_colegiado": cleaned_string(
                        votacao.get("SiglaColegiado")
                    ),
                    "nome_colegiado": cleaned_string(
                        votacao.get("NomeColegiado")
                    ),
                    "sigla_casa_colegiado": cleaned_string(
                        votacao.get("SiglaCasaColegiado")
                    ),
                    "id_reuniao": cleaned_string(votacao.get("CodigoReuniao")),
                    "numero_reuniao": cleaned_string(
                        votacao.get("NumeroReuniaoColegiado")
                    ),
                    "tipo_reuniao": cleaned_string(votacao.get("TipoReuniao")),
                    "data_reuniao": data_reuniao,
                    "identificacao_materia": identificacao,
                    "sigla_materia": sigla_materia,
                    "numero_materia": numero_materia,
                    "ano_materia": ano_materia,
                    "descricao": cleaned_string(
                        votacao.get("DescricaoVotacao")
                    ),
                    "id_senador_presidente": cleaned_string(
                        votacao.get("CodigoParlamentarPresidente")
                    ),
                    "voto_sim": cleaned_string(votacao.get("TotalVotosSim")),
                    "voto_nao": cleaned_string(votacao.get("TotalVotosNao")),
                    "voto_abstencao": cleaned_string(
                        votacao.get("TotalVotosAbstencao")
                    ),
                }
            )
            for voto in _as_list(get_nested_value(votacao, ["Votos", "Voto"])):
                votacao_comissao_parlamentar_rows.append(
                    {
                        "ano": ano,
                        "id_votacao": id_votacao,
                        "data_reuniao": data_reuniao,
                        "id_senador": cleaned_string(
                            voto.get("CodigoParlamentar")
                        ),
                        "sigla_partido": cleaned_string(
                            voto.get("SiglaPartidoParlamentar")
                        ),
                        "sigla_casa": cleaned_string(
                            voto.get("SiglaCasaParlamentar")
                        ),
                        "voto": cleaned_string(voto.get("QualidadeVoto")),
                        "voto_presidente": cleaned_string(
                            voto.get("VotoPresidente")
                        ),
                    }
                )
        if votacoes:
            print(f"    votacao_comissao {sigla}: {len(votacoes)} votes")
    votacao_comissao_df = _frame(
        votacao_comissao_rows, constants_votacao_comissao.COLUMNS.value
    ).drop_duplicates(subset=["id_votacao"])
    votacao_comissao_parlamentar_df = _frame(
        votacao_comissao_parlamentar_rows,
        constants_votacao_comissao_parlamentar.COLUMNS.value,
    ).drop_duplicates(subset=["id_votacao", "id_senador"])
    return votacao_comissao_df, votacao_comissao_parlamentar_df


def _discurso_rows(sessao: dict) -> list[dict]:
    """Monta as linhas de `discurso` de uma sessão.

    Args:
        sessao: nó `Sessao` do endpoint de discursos.

    Returns:
        Uma linha por pronunciamento da sessão.
    """
    data_sessao = normalize_date(sessao.get("DataSessao"))
    ano = data_sessao[:4] if data_sessao else None
    rows = []
    for pronunciamento in _as_list(
        get_nested_value(sessao, ["Pronunciamentos", "Pronunciamento"])
    ):
        tipo_uso_palavra = pronunciamento.get("TipoUsoPalavra") or {}
        rows.append(
            {
                "ano": ano,
                "id_pronunciamento": cleaned_string(
                    pronunciamento.get("CodigoPronunciamento")
                )
                or cleaned_string(pronunciamento.get("id")),
                "id_sessao": cleaned_string(sessao.get("CodigoSessao")),
                "data_sessao": data_sessao,
                "sigla_casa": cleaned_string(sessao.get("SiglaCasa")),
                "sigla_tipo_sessao": cleaned_string(sessao.get("TipoSessao")),
                "numero_sessao": cleaned_string(sessao.get("NumeroSessao")),
                "id_senador": cleaned_string(
                    pronunciamento.get("CodigoParlamentar")
                ),
                "tipo_autor": cleaned_string(pronunciamento.get("TipoAutor")),
                "sigla_partido": cleaned_string(pronunciamento.get("Partido")),
                "sigla_uf": cleaned_string(pronunciamento.get("UF")),
                "sigla_tipo_uso_palavra": cleaned_string(
                    tipo_uso_palavra.get("Sigla")
                ),
                "descricao_tipo_uso_palavra": cleaned_string(
                    tipo_uso_palavra.get("Descricao")
                ),
                "resumo": cleaned_string(pronunciamento.get("Resumo")),
                "indexacao": cleaned_string(pronunciamento.get("Indexacao")),
                "url_texto": cleaned_string(
                    pronunciamento.get("TextoIntegralTxt")
                ),
            }
        )
    return rows


def clean_discurso(years: range) -> pd.DataFrame:
    """Extrai e limpa a tabela `discurso`, consultando um mês por vez.

    Args:
        years: anos consultados.

    Returns:
        DataFrame com as colunas de `constants_discurso.COLUMNS`.
    """
    paths = []
    for year in years:
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]
            paths.append(
                f"/plenario/lista/discursos/{year}{month:02d}01/{year}{month:02d}{last_day:02d}"
            )
    responses = _fetch_many(paths, retries=3)
    rows = []
    for response in responses:
        for sessao in _as_list(
            get_nested_value(
                response, ["DiscursosSessao", "Sessoes", "Sessao"]
            )
        ):
            rows.extend(_discurso_rows(sessao))
    discurso_df = _frame(
        rows, constants_discurso.COLUMNS.value
    ).drop_duplicates(subset=["id_pronunciamento"])
    print(f"    discurso: {len(discurso_df)} pronouncements")
    return discurso_df


def _write_string_parquet(df: pd.DataFrame, path: str) -> None:
    """Grava `df` num parquet snappy com todas as colunas como texto.

    Args:
        df: tabela a gravar.
        path: caminho do arquivo.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([(column, pa.string()) for column in df.columns])
    table = pa.Table.from_pandas(
        df.astype(object), schema=schema, preserve_index=False
    )
    pq.write_table(table, path, compression="snappy")


def write_table(out_root: str, name: str, df: pd.DataFrame) -> str:
    """Grava uma tabela em `out_root/<name>/`, particionada por `ano` nas séries.

    Args:
        out_root: diretório de saída.
        name: nome da tabela.
        df: tabela a gravar.

    Returns:
        O diretório da tabela.
    """
    out_dir = os.path.join(out_root, name)
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    if name in constants.PARTITIONED.value:
        df = df[df["ano"].notna()]
        columns = [column for column in df.columns if column != "ano"]
        for ano, partition in df.groupby("ano"):
            _write_string_parquet(
                partition[columns],
                os.path.join(out_dir, f"ano={ano}", "data.parquet"),
            )
    else:
        _write_string_parquet(df, os.path.join(out_dir, "data.parquet"))
    return out_dir


def recent_window(prior_years: int = 1) -> range:
    """Devolve os anos da carga de rotina das séries.

    Args:
        prior_years: quantos anos antes do corrente entram na janela.

    Returns:
        Do ano corrente menos `prior_years` até o ano corrente.
    """
    current_year = pd.Timestamp.today().year
    return range(current_year - prior_years, current_year + 1)


def get_senador_codes(senador_df: pd.DataFrame) -> list[str]:
    """Lê os códigos dos senadores de uma tabela `senador`.

    Args:
        senador_df: tabela `senador`.

    Returns:
        Os `id_senador` não vazios.
    """
    return [code for code in senador_df["id_senador"].tolist() if code]


def get_comissao_siglas(comissao_df: pd.DataFrame) -> list[str]:
    """Lê as siglas das comissões de uma tabela `comissao`.

    Args:
        comissao_df: tabela `comissao`.

    Returns:
        As `sigla_comissao` distintas e não vazias.
    """
    return [
        sigla
        for sigla in comissao_df["sigla_comissao"].dropna().unique().tolist()
        if sigla
    ]


def clean_all(
    out_root: str, years: range | None = None, sample: bool = False
) -> dict:
    """Extrai, limpa e grava todas as tabelas em `out_root/<tabela>/`.

    Args:
        out_root: diretório de saída.
        years: anos das séries; None consulta o histórico inteiro. Os cadastros,
            as tabelas por senador e as votações em comissão ignoram a janela.
        sample: limita a 15 senadores e 6 comissões, para um teste rápido.

    Returns:
        O diretório de cada tabela, por nome, e em `"max_data_sessao"` a maior
        `data_sessao` de `votacao` (AAAA-MM-DD).
    """
    current_year = pd.Timestamp.today().year
    if years is None:
        vote_years = range(
            constants_votacao.START_YEAR.value, current_year + 1
        )
        processo_years = range(
            constants_processo.START_YEAR.value, current_year + 1
        )
        relatoria_years = range(
            constants_relatoria.START_YEAR.value, current_year + 1
        )
        discurso_years = range(
            constants_discurso.START_YEAR.value, current_year + 1
        )
    else:
        vote_years = processo_years = relatoria_years = discurso_years = years

    result: dict = {}
    result["partido"] = write_table(out_root, "partido", clean_partido())
    result["bloco"] = write_table(out_root, "bloco", clean_bloco())
    result["lideranca"] = write_table(out_root, "lideranca", clean_lideranca())
    comissao_df = clean_comissao()
    result["comissao"] = write_table(out_root, "comissao", comissao_df)
    result["mesa"] = write_table(out_root, "mesa", clean_mesa())
    senador_df = clean_senador()
    result["senador"] = write_table(out_root, "senador", senador_df)

    codes = get_senador_codes(senador_df)
    siglas = get_comissao_siglas(comissao_df)
    if sample:
        codes, siglas = codes[:15], siglas[:6]
    result["senador_mandato"] = write_table(
        out_root, "senador_mandato", clean_senador_mandato(codes)
    )
    result["senador_filiacao"] = write_table(
        out_root, "senador_filiacao", clean_senador_filiacao(codes)
    )
    result["senador_comissao"] = write_table(
        out_root, "senador_comissao", clean_senador_comissao(codes)
    )
    result["senador_cargo"] = write_table(
        out_root, "senador_cargo", clean_senador_cargo(codes)
    )

    votacao_df, votacao_parlamentar_df = clean_votacao(vote_years)
    result["votacao"] = write_table(out_root, "votacao", votacao_df)
    result["votacao_parlamentar"] = write_table(
        out_root, "votacao_parlamentar", votacao_parlamentar_df
    )
    result["votacao_orientacao_bancada"] = write_table(
        out_root, "votacao_orientacao_bancada", clean_orientacao(vote_years)
    )
    result["processo"] = write_table(
        out_root, "processo", clean_processo(processo_years)
    )
    result["relatoria"] = write_table(
        out_root, "relatoria", clean_relatoria(relatoria_years)
    )
    votacao_comissao_df, votacao_comissao_parlamentar_df = (
        clean_votacao_comissao(siglas)
    )
    result["votacao_comissao"] = write_table(
        out_root, "votacao_comissao", votacao_comissao_df
    )
    result["votacao_comissao_parlamentar"] = write_table(
        out_root,
        "votacao_comissao_parlamentar",
        votacao_comissao_parlamentar_df,
    )
    result["discurso"] = write_table(
        out_root, "discurso", clean_discurso(discurso_years)
    )

    max_data_sessao = (
        votacao_df["data_sessao"].dropna().max() if len(votacao_df) else None
    )
    result["max_data_sessao"] = max_data_sessao
    return result
