"""Funções puras de br_senado_dados_abertos_administrativos, sem Prefect."""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import shutil
import time
import unicodedata
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_senado_dados_abertos_administrativos.constants import (
    TABLES_CONSTANTS,
    constants,
    constants_dicionario,
    constants_quadro_pessoal,
    constants_servidor_cedido,
)

logger = logging.getLogger(__name__)


def get_json(
    path: str,
    params: dict | None = None,
    retries: int = 5,
    backoff: float = 1.5,
    timeout: int = 180,
) -> Any:
    """Lê um endpoint da API administrativa em JSON, tentando de novo em erro.

    Args:
        path: caminho do endpoint abaixo de `constants.API_URL`.
        params: parâmetros da query string.
        retries: número de tentativas.
        backoff: base, em segundos, da espera linear entre as tentativas.
        timeout: tempo limite de cada requisição, em segundos.

    Returns:
        O JSON lido, ou uma lista vazia quando a API responde 404.

    Raises:
        httpx.RequestError: se alguma tentativa deu erro de rede e nenhuma
            teve sucesso.
        json.JSONDecodeError: se alguma tentativa recebeu JSON inválido e
            nenhuma teve sucesso.
        RuntimeError: se nenhuma tentativa respondeu 200 com corpo.
    """
    url = f"{constants.API_URL.value}{path}"
    last_error: Exception | None = None
    last_status: int | None = None
    for attempt in range(retries):
        try:
            response = httpx.get(
                url,
                params=params,
                headers=constants.HEADERS.value,
                timeout=timeout,
                follow_redirects=True,
            )
            last_status = response.status_code
            if response.status_code == 404:
                return []
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
    raise RuntimeError(
        f"GET {url} failed: HTTP {last_status} after {retries} attempts"
    )


def get_legislativo_json(
    path: str, retries: int = 5, backoff: float = 1.5, timeout: int = 120
) -> Any:
    """Lê um endpoint da API de dados legislativos em JSON.

    Args:
        path: caminho do endpoint abaixo de `constants.LEGIS_API_URL`.
        retries: número de tentativas.
        backoff: base, em segundos, da espera linear entre as tentativas.
        timeout: tempo limite de cada requisição, em segundos.

    Returns:
        O JSON lido.

    Raises:
        httpx.RequestError: se alguma tentativa deu erro de rede e nenhuma
            teve sucesso.
        json.JSONDecodeError: se alguma tentativa recebeu JSON inválido e
            nenhuma teve sucesso.
        RuntimeError: se nenhuma tentativa respondeu 200 com corpo.
    """
    url = f"{constants.LEGIS_API_URL.value}{path}"
    last_error: Exception | None = None
    for attempt in range(retries):
        try:
            response = httpx.get(
                url,
                headers=constants.HEADERS.value,
                timeout=timeout,
                follow_redirects=True,
            )
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
    raise RuntimeError(f"GET {url} failed after {retries} attempts")


def unwrap_records(payload: Any) -> list[dict]:
    """Normaliza para uma lista de registros os dois envelopes da API.

    Args:
        payload: JSON lido, uma lista solta ou um objeto com a chave `data`.

    Returns:
        Os registros; lista vazia quando não há nenhum.
    """
    if payload is None:
        return []
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            return [data]
        return []
    if isinstance(payload, list):
        return list(payload)
    return []


def fetch_records(path: str, params: dict | None = None) -> list[dict]:
    """Lê um endpoint de lista da API administrativa.

    Args:
        path: caminho do endpoint abaixo de `constants.API_URL`.
        params: parâmetros da query string.

    Returns:
        Os registros do endpoint.
    """
    return unwrap_records(get_json(path, params))


def fan_out(
    items: Iterable[Any],
    function: Callable[[Any], Any],
    workers: int = constants.FETCH_WORKERS.value,
    label: str = "",
    fail_ratio: float = constants.FETCH_FAIL_RATIO.value,
) -> list[tuple[Any, Any]]:
    """Aplica uma função a vários itens em paralelo.

    Args:
        items: itens de entrada.
        function: função aplicada a cada item.
        workers: número de chamadas simultâneas.
        label: nome da consulta, usado nas mensagens.
        fail_ratio: fração máxima de itens com erro antes de levantar exceção.

    Returns:
        Pares `(item, resultado)`, na ordem em que as chamadas terminaram; o
        resultado é None nos itens que falharam.

    Raises:
        RuntimeError: se a fração de itens com erro passar de `fail_ratio`.
    """
    results: list[tuple[Any, Any]] = []
    failures: list[tuple[Any, Exception]] = []
    with ThreadPoolExecutor(workers) as executor:
        futures = {executor.submit(function, item): item for item in items}
        for future in as_completed(futures):
            item = futures[future]
            try:
                results.append((item, future.result()))
            except Exception as error:
                failures.append((item, error))
                results.append((item, None))

    if failures:
        total = len(results)
        ratio = len(failures) / total if total else 0.0
        sample = ", ".join(repr(item) for item, _ in failures[:5])
        message = (
            f"{len(failures)}/{total} requests failed after retries "
            f"({ratio:.1%}); first: {sample}"
        )
        if ratio > fail_ratio:
            raise RuntimeError(
                f"{label or 'fan_out'}: {message} — refusing to return a "
                f"silently incomplete crawl"
            )
        logger.warning("%s: %s", label or "fan_out", message)
    return results


def fetch_contratacoes() -> list[dict]:
    """Lê todas as contratações, com `tipo_contratacao` e `status` anexados.

    Returns:
        Uma contratação por par `(tipo_contratacao, id)`.
    """
    by_key: dict[tuple[str, Any], dict] = {}
    for status in constants.CONTRATO_STATUSES.value:
        for record in fetch_records(
            "/contratacoes/contratos", {"statusContratoParam": status}
        ):
            contratacao = dict(record)
            contratacao["tipo_contratacao"] = "contratos"
            contratacao["status"] = status
            by_key.setdefault(("contratos", contratacao["id"]), contratacao)

    for tipo in ["notas_empenho", "atas_registro_preco"]:
        for record in fetch_records(f"/contratacoes/{tipo}"):
            contratacao = dict(record)
            contratacao["tipo_contratacao"] = tipo
            contratacao["status"] = None
            by_key.setdefault((tipo, contratacao["id"]), contratacao)

    return list(by_key.values())


def fetch_sub_resource(
    contratacoes: list[dict],
    sub_resource: str,
    tipos: Iterable[str] = constants.TIPOS_CONTRATACAO.value,
) -> list[dict]:
    """Lê `/contratacoes/{tipo}/{id}/{sub_resource}` de cada contratação.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        sub_resource: nome do sub-recurso (ex.: ``"itens"``).
        tipos: tipos de contratação consultados; os outros são pulados.

    Returns:
        Os registros do sub-recurso, cada um com `tipo_contratacao` e
        `id_contratacao` da contratação de origem.
    """
    targets = [
        contratacao
        for contratacao in contratacoes
        if contratacao["tipo_contratacao"] in tipos
    ]

    def fetch_one(contratacao: dict) -> list[dict]:
        """Lê o sub-recurso de uma contratação."""
        return unwrap_records(
            get_json(
                f"/contratacoes/{contratacao['tipo_contratacao']}"
                f"/{contratacao['id']}/{sub_resource}"
            )
        )

    rows: list[dict] = []
    for contratacao, result in fan_out(
        targets, fetch_one, label=f"contratacoes/{sub_resource}"
    ):
        for record in result or []:
            row = dict(record)
            row["tipo_contratacao"] = contratacao["tipo_contratacao"]
            row["id_contratacao"] = contratacao["id"]
            rows.append(row)
    return rows


def fetch_pagamento_empenhos(pagamentos: list[dict]) -> list[dict]:
    """Lê os empenhos de cada pagamento de contratação.

    Args:
        pagamentos: pagamentos com `tipo_contratacao`, `id_contratacao` e `id`.

    Returns:
        Os empenhos, cada um com `tipo_contratacao`, `id_contratacao` e
        `id_pagamento` do pagamento de origem.
    """

    def fetch_one(pagamento: dict) -> list[dict]:
        """Lê os empenhos de um pagamento."""
        return unwrap_records(
            get_json(
                f"/contratacoes/{pagamento['tipo_contratacao']}"
                f"/{pagamento['id_contratacao']}"
                f"/pagamentos/{pagamento['id']}/empenhos"
            )
        )

    rows: list[dict] = []
    for pagamento, result in fan_out(
        pagamentos, fetch_one, label="contratacoes/pagamentos/empenhos"
    ):
        for record in result or []:
            row = dict(record)
            row["tipo_contratacao"] = pagamento["tipo_contratacao"]
            row["id_contratacao"] = pagamento["id_contratacao"]
            row["id_pagamento"] = pagamento["id"]
            rows.append(row)
    return rows


def cleaned_string(value: Any) -> str | None:
    """Converte para texto sem espaços nas pontas, com os nulos da fonte em None.

    Args:
        value: valor lido da API.

    Returns:
        O texto, ou None para None e para os valores de `constants.NULL_VALUES`.
    """
    if value is None:
        return None
    text = str(value).strip()
    if text.upper() in constants.NULL_VALUES.value:
        return None
    return text


def normalize_date(value: Any) -> str | None:
    """Converte uma data ISO ou `dd/mm/aaaa` para `aaaa-mm-dd`.

    Args:
        value: valor lido da API.

    Returns:
        A data em `aaaa-mm-dd`, ou None quando o valor não é uma data.
    """
    text = cleaned_string(value)
    if text is None:
        return None
    if re.match(constants.DATE_ISO_PATTERN.value, text):
        return text[:10]
    match = re.match(constants.DATE_BR_PATTERN.value, text)
    if match:
        day, month, year = match.groups()
        return f"{year}-{month}-{day}"
    return None


def normalize_datetime(value: Any) -> str | None:
    """Converte uma data e hora ISO para `aaaa-mm-dd hh:mm:ss`.

    Args:
        value: valor lido da API.

    Returns:
        A data e hora, ou None quando o valor não começa por uma data ISO.
    """
    text = cleaned_string(value)
    if text is None or not re.match(constants.DATE_ISO_PATTERN.value, text):
        return None
    return text.replace("T", " ")[:19]


def parse_number(value: Any) -> float | None:
    """Converte um número JSON ou um texto em formato brasileiro para float.

    Args:
        value: valor lido da API (ex.: ``527``, ``"16.368,74"``).

    Returns:
        O número, ou None quando o valor não é numérico.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = cleaned_string(value)
    if text is None:
        return None
    text = text.replace("R$", "").strip()
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def parse_integer(value: Any) -> int | None:
    """Converte um valor numérico para inteiro, truncando a parte decimal.

    Args:
        value: valor lido da API.

    Returns:
        O inteiro, ou None quando o valor não é numérico.
    """
    number = parse_number(value)
    if number is None:
        return None
    return int(number)


def normalize_flag(value: Any) -> str | None:
    """Converte as codificações de booleano da fonte para `sim` ou `nao`.

    Args:
        value: valor lido da API (``True``, ``"S"``, ``"N"``…).

    Returns:
        ``"sim"``, ``"nao"``, o próprio texto quando não é um booleano
        conhecido, ou None.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "sim" if value else "nao"
    text = cleaned_string(value)
    if text is None:
        return None
    upper = text.upper()
    if upper in {"S", "SIM", "TRUE", "1"}:
        return "sim"
    if upper in {"N", "NAO", "NÃO", "FALSE", "0"}:
        return "nao"
    return text


def parse_hours(value: Any) -> float | None:
    """Converte uma duração no formato `02h30` para horas.

    Args:
        value: valor lido da API.

    Returns:
        As horas, com os minutos em fração decimal, ou None.
    """
    text = cleaned_string(value)
    if text is None:
        return None
    match = re.match(constants.HOURS_PATTERN.value, text)
    if match:
        hours, minutes = match.groups()
        return int(hours) + int(minutes) / 60
    return parse_number(text)


def parse_weekly_hours(value: Any) -> int | None:
    """Lê o número de horas de uma jornada no formato `40 Horas`.

    Args:
        value: valor lido da API.

    Returns:
        As horas semanais, ou None.
    """
    text = cleaned_string(value)
    if text is None:
        return None
    match = re.match(constants.WEEKLY_HOURS_PATTERN.value, text)
    if match:
        return int(match.group(1))
    return None


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


def split_month_year(value: Any) -> tuple[int | None, int | None]:
    """Separa um `MM/AAAA` em ano e mês.

    Args:
        value: valor lido da API.

    Returns:
        O par `(ano, mes)`, ou `(None, None)` quando o valor não segue o
        formato.
    """
    text = cleaned_string(value)
    if text is None:
        return None, None
    match = re.match(constants.MONTH_YEAR_PATTERN.value, text)
    if not match:
        return None, None
    month, year = match.groups()
    return int(year), int(month)


def drop_duplicate_rows(rows: list[dict], keys: Iterable[str]) -> list[dict]:
    """Remove as linhas que repetem a chave, mantendo a primeira.

    Args:
        rows: linhas da tabela.
        keys: colunas que formam a chave.

    Returns:
        As linhas sem chave repetida, na ordem original.
    """
    key_columns = list(keys)
    seen: set[tuple] = set()
    unique_rows: list[dict] = []
    for row in rows:
        key = tuple(row.get(column) for column in key_columns)
        if key in seen:
            continue
        seen.add(key)
        unique_rows.append(row)
    return unique_rows


def normalize_name(value: Any) -> str:
    """Normaliza um nome para o cruzamento de senadores.

    Args:
        value: nome lido da API.

    Returns:
        O nome em maiúsculas, sem acentos e com espaços simples, ou texto
        vazio quando o nome falta.
    """
    text = cleaned_string(value)
    if text is None:
        return ""
    folded = (
        unicodedata.normalize("NFKD", text)
        .encode("ascii", "ignore")
        .decode()
        .upper()
    )
    return " ".join(folded.split())


def list_months(
    years: Iterable[int], through: dt.date
) -> list[tuple[int, int]]:
    """Lista os pares (ano, mês) dos anos dados até uma data.

    Args:
        years: anos consultados.
        through: data limite; meses posteriores a ela ficam de fora.

    Returns:
        Os pares `(ano, mes)`, em ordem.
    """
    periods = []
    for year in years:
        last_month = 12 if year < through.year else through.month
        for month in range(1, last_month + 1):
            periods.append((year, month))
    return periods


def list_years(
    first_year: int, years: Iterable[int] | None, today: dt.date
) -> list[int]:
    """Lista os anos consultados numa série.

    Args:
        first_year: primeiro ano publicado pela fonte.
        years: anos pedidos; None para o histórico completo.
        today: data de referência para o último ano.

    Returns:
        Os anos pedidos, ou de `first_year` até o ano de `today`.
    """
    if years is not None:
        return list(years)
    return list(range(first_year, today.year + 1))


def get_senador_reference() -> tuple[dict[str, dict], dict[str, str]]:
    """Lê os senadores das legislaturas do conjunto na API legislativa.

    Returns:
        Um par: a identificação de cada senador, por `id_senador`, e o
        cruzamento do nome parlamentar normalizado para o `id_senador`.
    """
    identificacoes: dict[str, dict] = {}
    crosswalk: dict[str, str] = {}
    for legislatura in range(
        constants.LEGISLATURA_START.value, constants.LEGISLATURA_END.value + 1
    ):
        payload = get_legislativo_json(
            f"/senador/lista/legislatura/{legislatura}"
        )
        node = get_nested_value(
            payload,
            ["ListaParlamentarLegislatura", "Parlamentares", "Parlamentar"],
        )
        if isinstance(node, list):
            parlamentares = node
        elif node:
            parlamentares = [node]
        else:
            parlamentares = []
        for parlamentar in parlamentares:
            identificacao = parlamentar.get("IdentificacaoParlamentar") or {}
            id_senador = cleaned_string(identificacao.get("CodigoParlamentar"))
            if not id_senador:
                continue
            nome = cleaned_string(identificacao.get("NomeParlamentar"))
            identificacoes[id_senador] = {
                "id_senador": id_senador,
                "nome_parlamentar": nome,
                "nome_completo": cleaned_string(
                    identificacao.get("NomeCompletoParlamentar")
                ),
                "sexo": cleaned_string(identificacao.get("SexoParlamentar")),
            }
            name_key = normalize_name(nome)
            if name_key:
                crosswalk[name_key] = id_senador
    for admin_name, legis_name in constants.CROSSWALK_OVERRIDES.value.items():
        id_senador = crosswalk.get(normalize_name(legis_name))
        if id_senador:
            crosswalk[normalize_name(admin_name)] = id_senador
    return identificacoes, crosswalk


def clean_senador(
    identificacoes: dict[str, dict], crosswalk: dict[str, str]
) -> list[dict]:
    """Monta a tabela `senador`, com os senadores atuais e os históricos.

    Args:
        identificacoes: identificação de cada senador, por `id_senador`, lida
            por `get_senador_reference`.
        crosswalk: nome parlamentar normalizado para `id_senador`.

    Returns:
        Uma linha por senador, ordenada por `id_senador`.
    """
    rows: dict[str, dict] = {}
    for id_senador, identificacao in identificacoes.items():
        rows[id_senador] = {
            "id_senador": id_senador,
            "nome_parlamentar": identificacao["nome_parlamentar"],
            "nome_completo": identificacao["nome_completo"],
            "sexo": identificacao["sexo"],
            "sigla_uf": None,
            "sigla_partido": None,
            "titular_suplente": None,
            "mandato": None,
            "data_nascimento": None,
            "email": None,
            "indicador_em_exercicio": "nao",
        }
    for record in fetch_records("/senadores"):
        id_senador = crosswalk.get(
            normalize_name(record.get("nomeParlamentar"))
        )
        if not id_senador:
            continue
        row = rows.setdefault(
            id_senador,
            {
                "id_senador": id_senador,
                "nome_parlamentar": cleaned_string(
                    record.get("nomeParlamentar")
                ),
                "nome_completo": None,
                "sexo": None,
            },
        )
        row["sigla_uf"] = cleaned_string(record.get("uf"))
        row["sigla_partido"] = cleaned_string(record.get("partido"))
        row["titular_suplente"] = cleaned_string(record.get("titularSuplente"))
        row["mandato"] = cleaned_string(record.get("mandato"))
        row["data_nascimento"] = normalize_date(record.get("dataNascimento"))
        row["email"] = cleaned_string(record.get("email"))
        row["indicador_em_exercicio"] = "sim"
    return sorted(rows.values(), key=lambda row: int(row["id_senador"]))


def clean_despesa_ceaps(years: Iterable[int]) -> list[dict]:
    """Extrai e limpa a tabela `despesa_ceaps`.

    Args:
        years: anos consultados.

    Returns:
        Uma linha por despesa.
    """
    rows = []
    for year in years:
        for record in fetch_records(f"/senadores/despesas_ceaps/{year}"):
            rows.append(
                {
                    "ano": parse_integer(record.get("ano")) or year,
                    "mes": parse_integer(record.get("mes")),
                    "id_despesa": cleaned_string(record.get("id")),
                    "id_senador": cleaned_string(record.get("codSenador")),
                    "tipo_despesa": cleaned_string(record.get("tipoDespesa")),
                    "tipo_documento": cleaned_string(
                        record.get("tipoDocumento")
                    ),
                    "documento": cleaned_string(record.get("documento")),
                    "data": normalize_date(record.get("data")),
                    "cpf_cnpj_fornecedor": cleaned_string(
                        record.get("cpfCnpj")
                    ),
                    "nome_fornecedor": cleaned_string(
                        record.get("fornecedor")
                    ),
                    "detalhamento": cleaned_string(record.get("detalhamento")),
                    "valor_reembolsado": parse_number(
                        record.get("valorReembolsado")
                    ),
                }
            )
    return rows


def clean_senador_gabinete(
    extracted_at: str, crosswalk: dict[str, str]
) -> list[dict]:
    """Extrai e limpa a tabela `senador_gabinete`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.
        crosswalk: nome parlamentar normalizado para `id_senador`.

    Returns:
        Uma linha por gabinete.
    """
    rows = []
    for record in fetch_records("/senadores"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_senador": crosswalk.get(
                    normalize_name(record.get("nomeParlamentar"))
                ),
                "endereco": cleaned_string(record.get("endereco")),
                "telefones": cleaned_string(record.get("telefones")),
                "fax": cleaned_string(record.get("fax")),
                "chefe_gabinete": cleaned_string(record.get("chefeGabinete")),
            }
        )
    return rows


def clean_senador_escritorio_apoio(
    extracted_at: str, crosswalk: dict[str, str]
) -> list[dict]:
    """Extrai e limpa a tabela `senador_escritorio_apoio`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.
        crosswalk: nome parlamentar normalizado para `id_senador`.

    Returns:
        Uma linha por escritório de apoio.
    """
    rows = []
    for record in fetch_records("/senadores/escritorios-apoio"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_senador": crosswalk.get(
                    normalize_name(
                        get_nested_value(record, ["parlamentar", "nome"])
                    )
                ),
                "nome_escritorio": cleaned_string(
                    get_nested_value(record, ["setor", "nome"])
                ),
                "sigla_setor": cleaned_string(
                    get_nested_value(record, ["setor", "sigla"])
                ),
                "endereco": cleaned_string(
                    get_nested_value(record, ["setor", "endereco"])
                ),
                "telefone": cleaned_string(
                    get_nested_value(record, ["setor", "telefone"])
                ),
            }
        )
    return rows


def clean_senador_auxilio_moradia(
    extracted_at: str, crosswalk: dict[str, str]
) -> list[dict]:
    """Extrai e limpa a tabela `senador_auxilio_moradia`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.
        crosswalk: nome parlamentar normalizado para `id_senador`.

    Returns:
        Uma linha por senador.
    """
    rows = []
    for record in fetch_records(
        "/senadores/auxilio-moradia-imoveis-funcionais"
    ):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_senador": crosswalk.get(
                    normalize_name(record.get("nomeParlamentar"))
                ),
                "indicador_auxilio_moradia": normalize_flag(
                    record.get("auxilioMoradia")
                ),
                "indicador_imovel_funcional": normalize_flag(
                    record.get("imovelFuncional")
                ),
            }
        )
    return rows


def clean_senador_aposentado_pensionista(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `senador_aposentado_pensionista`.

    Junta os quatro endpoints de benefícios de ex-senadores, com o regime e o
    tipo de benefício de cada um.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por benefício.
    """
    rows = []
    for record in fetch_records("/senadores/aposentados"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": cleaned_string(record.get("nome")),
                "tipo_beneficio": "aposentadoria",
                "regime": "geral",
                "tipo": cleaned_string(record.get("tipo")),
                "data_inicio": normalize_date(record.get("dataInicial")),
                "data_fim": None,
                "valor_remuneracao": parse_number(record.get("remuneracao")),
            }
        )
    for path, regime in [
        ("/senadores/aposentados-ipc", "ipc"),
        ("/senadores/aposentados-pssc", "pssc"),
    ]:
        for record in fetch_records(path):
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "nome": cleaned_string(record.get("nome")),
                    "tipo_beneficio": "aposentadoria",
                    "regime": regime,
                    "tipo": cleaned_string(record.get("tipo")),
                    "data_inicio": normalize_date(
                        record.get("dataAposentadoria")
                    ),
                    "data_fim": None,
                    "valor_remuneracao": None,
                }
            )
    for record in fetch_records("/senadores/pensionistas-ipc"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": cleaned_string(record.get("nome")),
                "tipo_beneficio": "pensao",
                "regime": "ipc",
                "tipo": cleaned_string(record.get("tipo")),
                "data_inicio": normalize_date(record.get("dataInicio")),
                "data_fim": normalize_date(record.get("dataFim")),
                "valor_remuneracao": None,
            }
        )
    return rows


def clean_servidor(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `servidor`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por servidor.
    """
    rows = []
    for record in fetch_records("/servidores/servidores"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_servidor": cleaned_string(record.get("sequencial")),
                "nome": cleaned_string(record.get("nome")),
                "vinculo": cleaned_string(record.get("vinculo")),
                "situacao": cleaned_string(record.get("situacao")),
                "cargo": cleaned_string(
                    get_nested_value(record, ["cargo", "nome"])
                ),
                "especialidade": cleaned_string(record.get("especialidade")),
                "padrao": cleaned_string(record.get("padrao")),
                "codigo_categoria": cleaned_string(
                    get_nested_value(record, ["categoria", "codigo"])
                ),
                "categoria": cleaned_string(
                    get_nested_value(record, ["categoria", "nome"])
                ),
                "codigo_funcao": cleaned_string(
                    get_nested_value(record, ["funcao", "codigo"])
                ),
                "funcao": cleaned_string(
                    get_nested_value(record, ["funcao", "nome"])
                ),
                "sigla_lotacao": cleaned_string(
                    get_nested_value(record, ["lotacao", "sigla"])
                ),
                "lotacao": cleaned_string(
                    get_nested_value(record, ["lotacao", "nome"])
                ),
                "tipo_cessao": cleaned_string(
                    get_nested_value(record, ["cedido", "tipo_cessao"])
                ),
                "orgao_origem": cleaned_string(
                    get_nested_value(record, ["cedido", "orgao_origem"])
                ),
                "orgao_destino": cleaned_string(
                    get_nested_value(record, ["cedido", "orgao_destino"])
                ),
                "ano_admissao": parse_integer(record.get("ano_admissao")),
            }
        )
    return rows


def clean_servidor_ativo(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `servidor_ativo`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por servidor ativo.
    """
    rows = []
    for record in fetch_records("/servidores"):
        hierarquia = record.get("hierarquiaCompleta") or []
        unidade = hierarquia[-1] if hierarquia else {}
        label = cleaned_string(unidade.get("nome")) or ""
        sigla, _, nome = label.partition(" - ")
        caminho = [cleaned_string(nivel.get("nome")) for nivel in hierarquia]
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": cleaned_string(record.get("nome")),
                "tipo_vinculo": cleaned_string(record.get("tipoVinculo")),
                "cargo": cleaned_string(record.get("cargo")),
                "categoria": cleaned_string(record.get("categoria")),
                "funcao": cleaned_string(record.get("funcao")),
                "data_admissao": normalize_date(record.get("dataAdmissao")),
                "jornada_semanal_horas": parse_weekly_hours(
                    record.get("jornadaSemanal")
                ),
                "afastamento": cleaned_string(record.get("afastamento")),
                "isencao_ponto": cleaned_string(record.get("isencaoPonto")),
                "sigla_lotacao": cleaned_string(sigla) if nome else None,
                "lotacao": cleaned_string(nome)
                if nome
                else cleaned_string(label),
                "nivel_lotacao": parse_integer(unidade.get("nivel")),
                "hierarquia_lotacao": " / ".join(
                    nivel for nivel in caminho if nivel
                )
                or None,
            }
        )
    return rows


def clean_servidor_remuneracao(
    periods: Iterable[tuple[int, int]],
) -> list[dict]:
    """Extrai e limpa a tabela `servidor_remuneracao`.

    Args:
        periods: pares `(ano, mes)` consultados.

    Returns:
        Uma linha por servidor, mês e tipo de folha.
    """
    rows = []
    for year, month in periods:
        for record in fetch_records(
            f"/servidores/remuneracoes/{year}/{month}"
        ):
            rows.append(
                {
                    "ano": parse_integer(record.get("ano")) or year,
                    "mes": parse_integer(record.get("mes")) or month,
                    "id_remuneracao": cleaned_string(record.get("sequencial")),
                    "nome": cleaned_string(record.get("nome")),
                    "tipo_folha": cleaned_string(record.get("tipo_folha")),
                    "remuneracao_basica": parse_number(
                        record.get("remuneracao_basica")
                    ),
                    "vantagens_pessoais": parse_number(
                        record.get("vantagens_pessoais")
                    ),
                    "funcao_comissionada": parse_number(
                        record.get("funcao_comissionada")
                    ),
                    "gratificacao_natalina": parse_number(
                        record.get("gratificacao_natalina")
                    ),
                    "horas_extras": parse_number(record.get("horas_extras")),
                    "auxilios": parse_number(record.get("auxilios")),
                    "diarias": parse_number(record.get("diarias")),
                    "vantagens_indenizatorias": parse_number(
                        record.get("vantagens_indenizatorias")
                    ),
                    "outras_eventuais": parse_number(
                        record.get("outras_eventuais")
                    ),
                    "abono_permanencia": parse_number(
                        record.get("abono_permanencia")
                    ),
                    "faltas": parse_number(record.get("faltas")),
                    "previdencia": parse_number(record.get("previdencia")),
                    "imposto_renda": parse_number(record.get("imposto_renda")),
                    "reversao_teto_constitucional": parse_number(
                        record.get("reversao_teto_constitucional")
                    ),
                    "remuneracao_liquida": parse_number(
                        record.get("remuneracao_liquida")
                    ),
                }
            )
    return drop_duplicate_rows(
        rows, ["ano", "mes", "id_remuneracao", "tipo_folha"]
    )


def clean_servidor_hora_extra(
    periods: Iterable[tuple[int, int]],
) -> tuple[list[dict], list[dict]]:
    """Extrai e limpa as tabelas `servidor_hora_extra` e `servidor_hora_extra_dia`.

    Args:
        periods: pares `(ano, mes)` consultados.

    Returns:
        Um par: as horas extras por mês e o detalhamento por dia.
    """
    horas_extras: list[dict] = []
    dias: list[dict] = []
    for year, month in periods:
        for record in fetch_records(
            f"/servidores/horas-extras/{year}/{month}"
        ):
            id_hora_extra = cleaned_string(record.get("sequencial"))
            prestacao = cleaned_string(record.get("mes_ano_prestacao"))
            horas_extras.append(
                {
                    "ano": year,
                    "mes": month,
                    "id_hora_extra": id_hora_extra,
                    "nome": cleaned_string(record.get("nome")),
                    "mes_ano_prestacao": prestacao,
                    "mes_ano_pagamento": cleaned_string(
                        record.get("mes_ano_pagamento")
                    ),
                    "valor_total": parse_number(record.get("valorTotal")),
                }
            )
            for dia in record.get("horas_extras") or []:
                dias.append(
                    {
                        "ano": year,
                        "mes": month,
                        "id_hora_extra": id_hora_extra,
                        "mes_ano_prestacao": prestacao,
                        "data": normalize_date(dia.get("dia")),
                        "quantidade_horas": parse_hours(dia.get("quantidade")),
                        "sigla_setor_prestacao": cleaned_string(
                            get_nested_value(dia, ["setor_prestacao", "sigla"])
                        ),
                        "setor_prestacao": cleaned_string(
                            get_nested_value(dia, ["setor_prestacao", "nome"])
                        ),
                    }
                )
    horas_extras = drop_duplicate_rows(
        horas_extras, ["ano", "mes", "id_hora_extra", "mes_ano_prestacao"]
    )
    dias = drop_duplicate_rows(
        dias,
        [
            "ano",
            "id_hora_extra",
            "mes_ano_prestacao",
            "data",
            "sigla_setor_prestacao",
        ],
    )
    return horas_extras, dias


def clean_servidor_aposentado(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `servidor_aposentado`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por aposentado, dos quadros efetivo e comissionado.
    """
    rows = []
    for path, quadro in [
        ("/servidores/aposentados-efetivos", "efetivo"),
        ("/servidores/aposentados-comissionados", "comissionado"),
    ]:
        for record in fetch_records(path):
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "matricula": cleaned_string(record.get("matricula")),
                    "tipo_quadro": quadro,
                    "nome": cleaned_string(record.get("nome")),
                    "categoria": cleaned_string(record.get("categoria")),
                    "cargo": cleaned_string(record.get("cargo")),
                    "data_aposentadoria": normalize_date(
                        record.get("dataAposentadoria")
                    ),
                    "tipo_aposentadoria": cleaned_string(
                        record.get("tipoAposentadoria")
                    ),
                }
            )
    return rows


def clean_servidor_exonerado(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `servidor_exonerado`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por exoneração.
    """
    rows = []
    for record in fetch_records("/servidores/exonerados"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "matricula": cleaned_string(record.get("matricula")),
                "nome": cleaned_string(record.get("nome")),
                "forma_vacancia": cleaned_string(record.get("formaVacancia")),
                "data_exercicio": normalize_date(record.get("dataExercicio")),
                "data_vacancia": normalize_date(record.get("dataVacancia")),
            }
        )
    return rows


def clean_servidor_cedido(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `servidor_cedido`.

    Junta os endpoints de `constants_servidor_cedido.CESSOES` e preenche o
    órgão de origem e o de destino conforme a direção de cada um.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por cessão.
    """
    senado_federal = constants_servidor_cedido.SENADO_FEDERAL.value
    rows = []
    for (
        path,
        tipo_cessao,
        direction,
        counterpart_key,
    ) in constants_servidor_cedido.CESSOES.value:
        for record in fetch_records(path):
            if counterpart_key is None:
                counterpart = constants_servidor_cedido.INFRAERO.value
            else:
                counterpart = cleaned_string(record.get(counterpart_key))
            if direction == "in":
                orgao_origem = counterpart
                orgao_destino = senado_federal
            else:
                orgao_origem = senado_federal
                orgao_destino = counterpart
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "matricula": cleaned_string(record.get("matricula")),
                    "tipo_cessao": tipo_cessao,
                    "nome": cleaned_string(record.get("nome")),
                    "orgao_origem": orgao_origem,
                    "orgao_destino": orgao_destino,
                    "cargo": cleaned_string(record.get("cargo")),
                    "categoria": cleaned_string(record.get("categoria")),
                    "lotacao": cleaned_string(record.get("lotacao")),
                    "data_exercicio": normalize_date(
                        record.get("dataExercicio")
                    ),
                }
            )
    return rows


def clean_pensionista(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `pensionista`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por pensionista.
    """
    rows = []
    for record in fetch_records("/servidores/pensionistas"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_pensionista": cleaned_string(record.get("sequencial")),
                "nome": cleaned_string(record.get("nome")),
                "vinculo": cleaned_string(record.get("vinculo")),
                "fundamento": cleaned_string(record.get("fundamento")),
                "nome_instituidor": cleaned_string(
                    record.get("nome_instituidor")
                ),
                "codigo_categoria": cleaned_string(
                    get_nested_value(record, ["categoria", "codigo"])
                ),
                "categoria": cleaned_string(
                    get_nested_value(record, ["categoria", "nome"])
                ),
                "cargo": cleaned_string(
                    get_nested_value(record, ["cargo", "nome"])
                ),
                "codigo_funcao": cleaned_string(
                    get_nested_value(record, ["funcao", "codigo"])
                ),
                "funcao": cleaned_string(
                    get_nested_value(record, ["funcao", "nome"])
                ),
                "ano_exercicio": parse_integer(record.get("ano_exercicio")),
                "data_obito": normalize_date(record.get("data_obito")),
                "data_inicio_pensao": normalize_date(
                    record.get("data_inicio_pensao")
                ),
            }
        )
    return rows


def clean_contratacao(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Limpa a tabela `contratacao`.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por contratação.
    """
    rows = []
    for record in contratacoes:
        rows.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": cleaned_string(
                    record.get("tipo_contratacao")
                ),
                "id_contratacao": cleaned_string(record.get("id")),
                "numero": cleaned_string(record.get("numero")),
                "numero_formatado": cleaned_string(
                    record.get("numero_formatado")
                ),
                "status": cleaned_string(record.get("status")),
                "sigla_sub_especie": cleaned_string(
                    get_nested_value(record, ["sub_especie", "sigla"])
                ),
                "sub_especie": cleaned_string(
                    get_nested_value(record, ["sub_especie", "nome"])
                ),
                "objeto": cleaned_string(record.get("objeto")),
                "cpf_cnpj_empresa": cleaned_string(
                    get_nested_value(record, ["empresa", "cpf_cnpj"])
                ),
                "nome_empresa": cleaned_string(
                    get_nested_value(record, ["empresa", "nome"])
                ),
                "numero_licitacao": cleaned_string(
                    get_nested_value(record, ["licitacao", "numero"])
                ),
                "modalidade_licitacao": cleaned_string(
                    get_nested_value(record, ["licitacao", "modalidade"])
                ),
                "sigla_unidade_gestora": cleaned_string(
                    get_nested_value(record, ["unidade_gestora", "sigla"])
                ),
                "unidade_gestora": cleaned_string(
                    get_nested_value(record, ["unidade_gestora", "nome"])
                ),
                "fundamentacao_legal": cleaned_string(
                    record.get("fundamentacao_legal")
                ),
                "processo_principal": cleaned_string(
                    record.get("processo_principal")
                ),
                "indicador_mao_de_obra": normalize_flag(
                    record.get("ind_mao_de_obra")
                ),
                "data_assinatura": normalize_date(
                    record.get("data_assinatura")
                ),
                "data_publicacao": normalize_date(
                    record.get("data_publicacao")
                ),
                "data_inicio_vigencia": normalize_date(
                    record.get("data_inicio_vigencia")
                ),
                "data_fim_vigencia": normalize_date(
                    record.get("data_fim_vigencia")
                ),
                "data_ultima_atualizacao": normalize_date(
                    record.get("data_ultima_atualizacao")
                ),
            }
        )
    return rows


def clean_contratacao_orgao_gestor(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Limpa a tabela `contratacao_orgao_gestor`, a partir das contratações.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por contratação, órgão gestor e tipo de gestão.
    """
    rows = []
    for record in contratacoes:
        for orgao in record.get("orgaos_gestores") or []:
            rows.append(
                {
                    "data_extracao": extracted_at,
                    "tipo_contratacao": cleaned_string(
                        record.get("tipo_contratacao")
                    ),
                    "id_contratacao": cleaned_string(record.get("id")),
                    "sigla_orgao_gestor": cleaned_string(orgao.get("sigla")),
                    "orgao_gestor": cleaned_string(orgao.get("nome")),
                    "tipo_gestao": cleaned_string(orgao.get("tipo_gestao")),
                }
            )
    return drop_duplicate_rows(
        rows,
        [
            "data_extracao",
            "tipo_contratacao",
            "id_contratacao",
            "sigla_orgao_gestor",
            "tipo_gestao",
        ],
    )


def clean_contratacao_item(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Extrai e limpa a tabela `contratacao_item`.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por item de contratação.
    """
    rows = []
    for record in fetch_sub_resource(contratacoes, "itens"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": cleaned_string(record["tipo_contratacao"]),
                "id_contratacao": cleaned_string(record["id_contratacao"]),
                "id_item": cleaned_string(record.get("id")),
                "numero_item": cleaned_string(record.get("numero")),
                "descricao": cleaned_string(record.get("descricao")),
                "quantidade": parse_number(record.get("quantidade")),
                "data_atualizacao": normalize_date(
                    record.get("data_atualizacao")
                ),
            }
        )
    return rows


def clean_contratacao_garantia(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Extrai e limpa a tabela `contratacao_garantia`.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por garantia.
    """
    rows = []
    for record in fetch_sub_resource(contratacoes, "garantias"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": cleaned_string(record["tipo_contratacao"]),
                "id_contratacao": cleaned_string(record["id_contratacao"]),
                "id_garantia": cleaned_string(record.get("id")),
                "tipo": cleaned_string(record.get("tipo")),
                "valor": parse_number(record.get("valor")),
                "data_vencimento": normalize_date(
                    record.get("data_vencimento")
                ),
            }
        )
    return rows


def clean_contratacao_pagamento(
    contratacoes: list[dict], extracted_at: str
) -> tuple[list[dict], list[dict], list[dict]]:
    """Extrai e limpa os pagamentos, os documentos fiscais e os empenhos.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        As tabelas `contratacao_pagamento`, `contratacao_documento_fiscal` e
        `contratacao_pagamento_empenho`, nessa ordem.
    """
    pagamentos: list[dict] = []
    documentos: list[dict] = []
    empenho_parents: list[dict] = []

    for record in fetch_sub_resource(contratacoes, "pagamentos"):
        tipo_contratacao = record["tipo_contratacao"]
        id_contratacao = record["id_contratacao"]
        pagamentos.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": cleaned_string(tipo_contratacao),
                "id_contratacao": cleaned_string(id_contratacao),
                "id_pagamento": cleaned_string(record.get("id")),
                "descricao_despesa": cleaned_string(
                    record.get("descricao_despesa")
                ),
                "valor_cobrado": parse_number(record.get("valor_cobrado")),
                "multa": parse_number(record.get("multa")),
                "glosa": parse_number(record.get("glosa")),
                "observacao": cleaned_string(record.get("observacao")),
            }
        )
        empenho_parents.append(
            {
                "tipo_contratacao": tipo_contratacao,
                "id_contratacao": id_contratacao,
                "id": record.get("id"),
            }
        )
        for documento in record.get("documentos_fiscais") or []:
            documentos.append(
                {
                    "data_extracao": extracted_at,
                    "tipo_contratacao": cleaned_string(tipo_contratacao),
                    "id_contratacao": cleaned_string(id_contratacao),
                    "id_documento_fiscal": cleaned_string(documento.get("id")),
                    "numero": cleaned_string(documento.get("numero")),
                    "data_emissao": normalize_date(
                        documento.get("data_emissao")
                    ),
                    "data_vencimento": normalize_date(
                        documento.get("data_vencimento")
                    ),
                }
            )

    documentos = drop_duplicate_rows(
        documentos,
        ["tipo_contratacao", "id_contratacao", "id_documento_fiscal"],
    )

    empenhos = []
    for record in fetch_pagamento_empenhos(empenho_parents):
        empenhos.append(
            {
                "data_extracao": extracted_at,
                "tipo_contratacao": cleaned_string(record["tipo_contratacao"]),
                "id_contratacao": cleaned_string(record["id_contratacao"]),
                "id_pagamento": cleaned_string(record["id_pagamento"]),
                "id_empenho": cleaned_string(record.get("id")),
                "natureza_despesa": cleaned_string(
                    record.get("natureza_despesa")
                ),
                "valor_empenhado": parse_number(record.get("valor_empenhado")),
                "valor_liquidado": parse_number(record.get("valor_liquidado")),
                "saldo": parse_number(record.get("saldo")),
            }
        )
    empenhos = drop_duplicate_rows(
        empenhos,
        ["tipo_contratacao", "id_contratacao", "id_pagamento", "id_empenho"],
    )
    return pagamentos, documentos, empenhos


def clean_contrato_aditivo(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Extrai e limpa a tabela `contrato_aditivo`.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por aditivo de contrato.
    """
    rows = []
    for record in fetch_sub_resource(
        contratacoes, "aditivos", tipos=["contratos"]
    ):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_contratacao": cleaned_string(record["id_contratacao"]),
                "id_aditivo": cleaned_string(record.get("id")),
                "numero": cleaned_string(record.get("numero")),
                "valor": parse_number(record.get("valor")),
                "data_assinatura": normalize_date(
                    record.get("data_assinatura")
                ),
                "data_publicacao": normalize_date(
                    record.get("data_publicacao")
                ),
                "data_atualizacao": normalize_date(
                    record.get("data_atualizacao")
                ),
            }
        )
    return drop_duplicate_rows(rows, ["id_contratacao", "id_aditivo"])


def clean_ata_acionamento(
    contratacoes: list[dict], extracted_at: str
) -> list[dict]:
    """Extrai e limpa a tabela `ata_acionamento`.

    Args:
        contratacoes: contratações lidas por `fetch_contratacoes`.
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por acionamento de ata de registro de preço.
    """
    rows = []
    for record in fetch_sub_resource(
        contratacoes, "acionamentos", tipos=["atas_registro_preco"]
    ):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_contratacao": cleaned_string(record["id_contratacao"]),
                "id_acionamento": cleaned_string(record.get("id")),
                "numero": cleaned_string(record.get("numero")),
                "objeto": cleaned_string(record.get("objeto")),
            }
        )
    return rows


def clean_licitacao(extracted_at: str) -> tuple[list[dict], list[dict]]:
    """Extrai e limpa as tabelas `licitacao` e `licitacao_detalhamento`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Um par: as licitações e os detalhamentos de cada uma.
    """
    licitacoes: list[dict] = []
    detalhamentos: list[dict] = []
    for record in fetch_records("/contratacoes/licitacoes"):
        id_licitacao = cleaned_string(record.get("id"))
        licitacoes.append(
            {
                "data_extracao": extracted_at,
                "id_licitacao": id_licitacao,
                "numero": cleaned_string(record.get("numero")),
                "modalidade": cleaned_string(record.get("modalidade")),
                "situacao": cleaned_string(record.get("situacao")),
                "objeto": cleaned_string(record.get("objeto")),
                "data_abertura": normalize_date(record.get("abertura")),
                "indicador_registro_preco": normalize_flag(
                    record.get("registro_preco")
                ),
                "orgao_origem": cleaned_string(record.get("orgao_origem")),
                "edital": cleaned_string(record.get("edital")),
            }
        )
        for detalhamento in record.get("detalhamentos") or []:
            detalhamentos.append(
                {
                    "data_extracao": extracted_at,
                    "id_licitacao": id_licitacao,
                    "id_detalhamento": cleaned_string(detalhamento.get("id")),
                    "tipo": cleaned_string(detalhamento.get("tipo")),
                    "descricao": cleaned_string(detalhamento.get("descricao")),
                    "data_criacao": normalize_date(
                        detalhamento.get("criacao")
                    ),
                    "link": cleaned_string(detalhamento.get("link")),
                }
            )
    return licitacoes, detalhamentos


def clean_empresa(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `empresa`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por empresa.
    """
    rows = []
    for record in fetch_records("/contratacoes/empresas"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "id_empresa": cleaned_string(record.get("id")),
                "cpf_cnpj": cleaned_string(record.get("cpf_cnpj")),
                "nome": cleaned_string(record.get("nome")),
            }
        )
    return drop_duplicate_rows(rows, ["id_empresa"])


def clean_terceirizado(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `terceirizado`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por trabalhador terceirizado.
    """
    rows = []
    for record in fetch_records("/contratacoes/terceirizados"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "cpf": cleaned_string(record.get("cpf")),
                "nome": cleaned_string(record.get("nome")),
                "id_contrato": cleaned_string(record.get("codContrato")),
                "numero_contrato": cleaned_string(
                    record.get("numeroContrato")
                ),
                "id_item_contrato": cleaned_string(record.get("codItem")),
                "situacao": cleaned_string(record.get("situacao")),
                "empresa": cleaned_string(record.get("empresa")),
                "sigla_lotacao": cleaned_string(
                    get_nested_value(record, ["lotacao", "sigla"])
                ),
                "lotacao": cleaned_string(
                    get_nested_value(record, ["lotacao", "nome"])
                ),
            }
        )
    return rows


def clean_menor_aprendiz(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `menor_aprendiz`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por menor aprendiz.
    """
    rows = []
    for record in fetch_records("/contratacoes/menores_aprendizes"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "cpf": cleaned_string(record.get("cpf")),
                "nome": cleaned_string(record.get("nome")),
                "situacao": cleaned_string(record.get("situacao")),
                "fornecedor": cleaned_string(record.get("fornecedor")),
                "sigla_orgao": cleaned_string(
                    get_nested_value(record, ["orgao", "sigla"])
                ),
                "orgao": cleaned_string(
                    get_nested_value(record, ["orgao", "nome"])
                ),
            }
        )
    return rows


def clean_estagiario(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `estagiario`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por estagiário.
    """
    rows = []
    for record in fetch_records("/colaboradores/estagiarios"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "nome": cleaned_string(record.get("nome")),
                "curso": cleaned_string(record.get("curso")),
                "sigla_orgao": cleaned_string(record.get("siglaOrgao")),
                "orgao": cleaned_string(record.get("nomeOrgao")),
            }
        )
    return rows


def clean_supridos(years: Iterable[int]) -> dict[str, list[dict]]:
    """Extrai e limpa as seis tabelas de supridos, uma requisição por ano.

    Args:
        years: anos consultados.

    Returns:
        As linhas de cada tabela, por nome de tabela: `suprido_ato_concessao`,
        `suprido_empenho`, `suprido_transacao`, `suprido_transacao_objeto`,
        `suprido_movimentacao` e `suprido_movimentacao_subtipo`.
    """
    atos: list[dict] = []
    empenhos: list[dict] = []
    transacoes: list[dict] = []
    objetos: list[dict] = []
    movimentacoes: list[dict] = []
    subtipos: list[dict] = []

    for year in years:
        for suprido in fetch_records(f"/supridos/{year}"):
            for ato in suprido.get("atosConcessao") or []:
                codigo_ato = cleaned_string(ato.get("codigoAtoConcessao"))
                atos.append(
                    {
                        "ano": parse_integer(ato.get("ano")) or year,
                        "codigo_ato_concessao": codigo_ato,
                        "codigo_suprido": cleaned_string(
                            suprido.get("codigo")
                        ),
                        "nome_suprido": cleaned_string(suprido.get("nome")),
                        "codigo_orgao": cleaned_string(
                            get_nested_value(suprido, ["orgao", "codigo"])
                        ),
                        "sigla_orgao": cleaned_string(
                            get_nested_value(suprido, ["orgao", "sigla"])
                        ),
                        "orgao": cleaned_string(
                            get_nested_value(suprido, ["orgao", "nome"])
                        ),
                        "matricula_solicitante": cleaned_string(
                            ato.get("matriculaSolicitante")
                        ),
                        "numero_processo": cleaned_string(
                            ato.get("numeroProcesso")
                        ),
                        "indicador_regime_especial": normalize_flag(
                            ato.get("regimeEspecial")
                        ),
                        "data": normalize_date(ato.get("data")),
                        "data_publicacao_basf": normalize_date(
                            ato.get("dataPublicacaoBASF")
                        ),
                        "prazo_aplicacao": normalize_date(
                            ato.get("prazoAplicacao")
                        ),
                        "prazo_comprovacao": normalize_date(
                            ato.get("prazoComprovacao")
                        ),
                        "data_aplicacao": normalize_date(
                            ato.get("dataAplicacao")
                        ),
                        "data_comprovacao": normalize_date(
                            ato.get("dataComprovacao")
                        ),
                        "valor_total_elementos_despesa": parse_number(
                            ato.get("valorTotalElementosDespesa")
                        ),
                        "valor_total_empenhos": parse_number(
                            ato.get("valorTotalEmpenhos")
                        ),
                        "valor_total_transacoes": parse_number(
                            ato.get("valorTotalTransacoes")
                        ),
                        "valor_total_movimentacoes": parse_number(
                            ato.get("valorTotalMovimentacoes")
                        ),
                        "data_processamento": normalize_datetime(
                            ato.get("dataProcessamento")
                        ),
                    }
                )
                for empenho in ato.get("empenhos") or []:
                    empenhos.append(
                        {
                            "ano": year,
                            "codigo_ato_concessao": codigo_ato,
                            "numero": cleaned_string(empenho.get("numero")),
                            "rubrica": cleaned_string(empenho.get("rubrica")),
                            "descricao": cleaned_string(
                                empenho.get("descricao")
                            ),
                            "valor_concedido": parse_number(
                                empenho.get("valorConcedido")
                            ),
                            "valor_executado": parse_number(
                                empenho.get("valorExecutado")
                            ),
                            "data": normalize_date(empenho.get("data")),
                            "data_processamento": normalize_datetime(
                                empenho.get("dataProcessamento")
                            ),
                        }
                    )
                for transacao in ato.get("transacoes") or []:
                    id_transacao = cleaned_string(transacao.get("id"))
                    transacoes.append(
                        {
                            "ano": year,
                            "id_transacao": id_transacao,
                            "codigo_ato_concessao": codigo_ato,
                            "tipo": cleaned_string(transacao.get("tipo")),
                            "numero": cleaned_string(transacao.get("numero")),
                            "tipo_inscricao": cleaned_string(
                                transacao.get("tipoInscricao")
                            ),
                            "inscricao": cleaned_string(
                                transacao.get("inscricao")
                            ),
                            "fornecedor": cleaned_string(
                                transacao.get("fornecedor")
                            ),
                            "rubricas": cleaned_string(
                                transacao.get("rubricas")
                            ),
                            "valor": parse_number(transacao.get("valor")),
                            "data": normalize_date(transacao.get("data")),
                            "data_processamento": normalize_datetime(
                                transacao.get("dataProcessamento")
                            ),
                        }
                    )
                    for objeto in transacao.get("objetos") or []:
                        objetos.append(
                            {
                                "ano": year,
                                "id_transacao": id_transacao,
                                "id_objeto": cleaned_string(objeto.get("id")),
                                "descricao_objeto": cleaned_string(
                                    objeto.get("descricaoObjeto")
                                ),
                                "tipo_despesa": cleaned_string(
                                    objeto.get("descricaoTipoDespesa")
                                ),
                                "subtipo_despesa": cleaned_string(
                                    objeto.get("descricaoSubtipoDespesa")
                                ),
                                "rubrica": cleaned_string(
                                    objeto.get("rubrica")
                                ),
                                "quantidade": parse_number(
                                    objeto.get("quantidade")
                                ),
                                "valor_unitario": parse_number(
                                    objeto.get("valorUnitario")
                                ),
                                "valor_total": parse_number(
                                    objeto.get("valorTotal")
                                ),
                            }
                        )
                for movimentacao in ato.get("movimentacoes") or []:
                    id_movimentacao = cleaned_string(movimentacao.get("id"))
                    movimentacoes.append(
                        {
                            "ano": year,
                            "id_movimentacao": id_movimentacao,
                            "codigo_ato_concessao": codigo_ato,
                            "tipo": cleaned_string(movimentacao.get("tipo")),
                            "numero": cleaned_string(
                                movimentacao.get("numero")
                            ),
                            "tipo_inscricao": cleaned_string(
                                movimentacao.get("tipoInscricao")
                            ),
                            "inscricao": cleaned_string(
                                movimentacao.get("inscricao")
                            ),
                            "fornecedor": cleaned_string(
                                movimentacao.get("fornecedor")
                            ),
                            "rubricas": cleaned_string(
                                movimentacao.get("rubricas")
                            ),
                            "valor": parse_number(movimentacao.get("valor")),
                            "data": normalize_date(movimentacao.get("data")),
                            "data_processamento": normalize_datetime(
                                movimentacao.get("dataProcessamento")
                            ),
                        }
                    )
                    for subtipo in movimentacao.get("subTipos") or []:
                        subtipos.append(
                            {
                                "ano": year,
                                "id_movimentacao": id_movimentacao,
                                "id_subtipo": cleaned_string(
                                    subtipo.get("id")
                                ),
                                "tipo_despesa": cleaned_string(
                                    subtipo.get("descricaoTipoDespesa")
                                ),
                                "subtipo_despesa": cleaned_string(
                                    subtipo.get("descricaoSubtipoDespesa")
                                ),
                                "rubrica": cleaned_string(
                                    subtipo.get("rubrica")
                                ),
                                "valor": parse_number(subtipo.get("valor")),
                            }
                        )

    return {
        "suprido_ato_concessao": drop_duplicate_rows(
            atos, ["ano", "codigo_ato_concessao"]
        ),
        "suprido_empenho": drop_duplicate_rows(
            empenhos, ["ano", "codigo_ato_concessao", "numero"]
        ),
        "suprido_transacao": drop_duplicate_rows(
            transacoes, ["ano", "id_transacao"]
        ),
        "suprido_transacao_objeto": drop_duplicate_rows(
            objetos, ["ano", "id_transacao", "id_objeto"]
        ),
        "suprido_movimentacao": drop_duplicate_rows(
            movimentacoes, ["ano", "id_movimentacao"]
        ),
        "suprido_movimentacao_subtipo": drop_duplicate_rows(
            subtipos, ["ano", "id_movimentacao", "id_subtipo"]
        ),
    }


def clean_quadro_pessoal(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `quadro_pessoal`, juntando seis relatórios.

    Cada relatório de `constants_quadro_pessoal.QUADROS` vira uma linha por
    registro e período (`ATUAL`, `ANTERIOR`), com as dimensões e as medidas
    que ele tem; as outras ficam nulas.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por relatório, registro e período.
    """
    rows = []
    for (
        path,
        quadro,
        dimensions,
        measures_by_periodo,
    ) in constants_quadro_pessoal.QUADROS.value:
        for record in fetch_records(path):
            for periodo, measures in measures_by_periodo.items():
                row: dict[str, Any] = {
                    "data_extracao": extracted_at,
                    "data_referencia": normalize_date(record.get("data")),
                    "quadro": quadro,
                    "periodo": periodo,
                }
                for column in constants_quadro_pessoal.DIMENSIONS.value:
                    if column in dimensions:
                        row[column] = cleaned_string(
                            record.get(dimensions[column])
                        )
                    else:
                        row[column] = None
                for column in constants_quadro_pessoal.MEASURES.value:
                    if column in measures:
                        row[column] = parse_integer(
                            record.get(measures[column])
                        )
                    else:
                        row[column] = None
                rows.append(row)
    return rows


def clean_diretor_coordenador(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `diretor_coordenador`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por setor.
    """
    rows = []
    for record in fetch_records("/gestao/diretores-e-coordenadores"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "sigla_setor": cleaned_string(
                    get_nested_value(record, ["setor", "sigla"])
                ),
                "setor": cleaned_string(
                    get_nested_value(record, ["setor", "nome"])
                ),
                "sigla_setor_superior": cleaned_string(
                    get_nested_value(record, ["setor", "siglaSetorSuperior"])
                ),
                "setor_superior": cleaned_string(
                    get_nested_value(record, ["setor", "nomeSetorSuperior"])
                ),
                "matricula_titular": cleaned_string(
                    get_nested_value(record, ["titular", "matricula"])
                ),
                "nome_titular": cleaned_string(
                    get_nested_value(record, ["titular", "nome"])
                ),
                "cargo_titular": cleaned_string(
                    get_nested_value(record, ["titular", "cargo"])
                ),
                "email_titular": cleaned_string(
                    get_nested_value(record, ["titular", "email"])
                ),
                "referencia_chefia": cleaned_string(
                    get_nested_value(record, ["titular", "referenciaChefia"])
                ),
                "matricula_substituto": cleaned_string(
                    get_nested_value(record, ["substituto", "matricula"])
                ),
                "nome_substituto": cleaned_string(
                    get_nested_value(record, ["substituto", "nome"])
                ),
                "data_inicio_substituicao": normalize_date(
                    get_nested_value(record, ["substituto", "dataInicio"])
                ),
                "data_fim_substituicao": normalize_date(
                    get_nested_value(record, ["substituto", "dataFim"])
                ),
                "telefone": cleaned_string(
                    get_nested_value(record, ["setor", "telefone"])
                ),
                "endereco": cleaned_string(
                    get_nested_value(record, ["setor", "endereco"])
                ),
            }
        )
    return rows


def clean_previsao_aposentadoria(extracted_at: str) -> list[dict]:
    """Extrai e limpa a tabela `previsao_aposentadoria`.

    Args:
        extracted_at: data da extração, em `aaaa-mm-dd`.

    Returns:
        Uma linha por cargo, categoria e mês de direito.
    """
    rows = []
    for record in fetch_records("/servidores/previsao-aposentadoria"):
        rows.append(
            {
                "data_extracao": extracted_at,
                "cargo": cleaned_string(record.get("cargo")),
                "categoria": cleaned_string(record.get("categoria")),
                "ano_direito": parse_integer(record.get("anoDireito")),
                "mes_direito": parse_integer(record.get("mesDireito")),
                "quantidade": parse_integer(record.get("quantidade")),
            }
        )
    return rows


def collect_rubricas(rows: list[dict]) -> dict[str, str]:
    """Junta os pares rubrica e descrição de uma tabela de supridos.

    Args:
        rows: linhas da tabela, com `rubrica` e, quando há, `descricao`.

    Returns:
        A descrição de cada rubrica; texto vazio quando a linha não tem.
    """
    rubricas: dict[str, str] = {}
    for row in rows:
        rubrica = row.get("rubrica")
        if rubrica:
            rubricas.setdefault(rubrica, row.get("descricao") or "")
    return rubricas


def _dicionario_row(table_id: str, column: str, key: str, value: str) -> dict:
    """Monta uma linha do dicionário.

    Args:
        table_id: tabela da coluna.
        column: coluna codificada.
        key: código.
        value: rótulo do código.

    Returns:
        A linha, com as colunas de `constants_dicionario.COLUMNS`.
    """
    return {
        "id_tabela": table_id,
        "nome_coluna": column,
        "chave": key,
        "cobertura_temporal": None,
        "valor": value,
    }


def clean_dicionario(
    rubricas_por_tabela: dict[str, dict[str, str]] | None = None,
) -> list[dict]:
    """Monta a tabela `dicionario`.

    Junta os valores fixos de `constants_dicionario` com as rubricas vistas
    nas tabelas de supridos.

    Args:
        rubricas_por_tabela: para cada tabela de supridos, a descrição de
            cada rubrica, como devolvida por `collect_rubricas`.

    Returns:
        Uma linha por tabela, coluna e código.
    """
    rows = []
    for (
        table_id,
        column,
    ), labels in constants_dicionario.STATIC_VALUES.value.items():
        for key, value in labels.items():
            rows.append(_dicionario_row(table_id, column, key, value))

    for table_id in constants_dicionario.TIPO_CONTRATACAO_TABLES.value:
        for (
            key,
            value,
        ) in constants_dicionario.TIPO_CONTRATACAO_VALUES.value.items():
            rows.append(
                _dicionario_row(table_id, "tipo_contratacao", key, value)
            )

    seen = rubricas_por_tabela or {}
    descricoes: dict[str, str] = {}
    source_table = constants_dicionario.RUBRICA_SOURCE_TABLE.value
    for rubrica, descricao in (seen.get(source_table) or {}).items():
        if descricao:
            descricoes.setdefault(rubrica, descricao)
    for table_id in constants_dicionario.RUBRICA_TABLES.value:
        for rubrica in sorted(seen.get(table_id) or {}):
            rows.append(
                _dicionario_row(
                    table_id,
                    "rubrica",
                    rubrica,
                    descricoes.get(rubrica) or rubrica,
                )
            )

    return drop_duplicate_rows(rows, ["id_tabela", "nome_coluna", "chave"])


def partition_column(table_id: str) -> str | None:
    """Diz a coluna de partição de uma tabela.

    Args:
        table_id: nome da tabela.

    Returns:
        ``"ano"``, ``"data_extracao"`` ou None, para as tabelas sem partição.
    """
    if table_id in constants.PARTITIONED.value:
        return "ano"
    if table_id in constants.SNAPSHOTS.value:
        return "data_extracao"
    return None


def to_staging_string(value: Any) -> str | None:
    """Converte um valor limpo para o texto gravado na staging.

    Args:
        value: valor de uma coluna.

    Returns:
        None para None, ``"true"``/``"false"`` para booleanos e `str` para o
        resto.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _write_string_parquet(
    rows: list[dict], columns: list[str], path: str
) -> None:
    """Grava as linhas num parquet snappy com todas as colunas como texto.

    Args:
        rows: linhas a gravar.
        columns: colunas, na ordem do arquivo.
        path: caminho do arquivo.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)
    schema = pa.schema([(column, pa.string()) for column in columns])
    arrays = {}
    for column in columns:
        arrays[column] = pa.array(
            [to_staging_string(row.get(column)) for row in rows],
            type=pa.string(),
        )
    pq.write_table(pa.table(arrays, schema=schema), path, compression="snappy")


def write_table(
    out_root: str, table_id: str, rows: list[dict], reset: bool = True
) -> str:
    """Grava uma tabela em `out_root/<table_id>/`, particionada quando tem partição.

    Uma tabela particionada sem linhas não grava arquivo nenhum.

    Args:
        out_root: diretório de saída.
        table_id: nome da tabela.
        rows: linhas da tabela.
        reset: True apaga o que já existe da tabela antes de gravar; False
            acrescenta partições às já gravadas.

    Returns:
        O diretório da tabela.
    """
    partition = partition_column(table_id)
    target = os.path.join(out_root, table_id)
    if reset and os.path.isdir(target):
        shutil.rmtree(target)
    os.makedirs(target, exist_ok=True)
    columns = [
        column
        for column in TABLES_CONSTANTS[table_id].COLUMNS.value
        if column != partition
    ]

    if partition is None:
        _write_string_parquet(
            rows, columns, os.path.join(target, "data.parquet")
        )
        return target

    partitions: dict[Any, list[dict]] = {}
    for row in rows:
        partitions.setdefault(row.get(partition), []).append(row)
    for key, partition_rows in partitions.items():
        if key is None:
            continue
        _write_string_parquet(
            partition_rows,
            columns,
            os.path.join(target, f"{partition}={key}", "data.parquet"),
        )
    return target


def clean_all(
    output: str,
    years: Iterable[int] | None = None,
    extracted_at: str | None = None,
    today: dt.date | None = None,
    sub_resources: bool = True,
) -> dict[str, int]:
    """Extrai e limpa todas as tabelas e grava cada uma em parquet.

    Cada tabela é gravada e liberada antes da próxima, e as séries de
    `despesa_ceaps`, `servidor_remuneracao`, `servidor_hora_extra` e dos
    supridos são gravadas um ano por vez.

    Args:
        output: diretório de saída.
        years: anos consultados nas séries; None para o histórico completo
            desde o primeiro ano de cada fonte.
        extracted_at: data da extração, em `aaaa-mm-dd`; padrão: `today`.
        today: data de referência; padrão: hoje.
        sub_resources: True inclui as tabelas filhas das contratações.

    Returns:
        O número de linhas gravadas em cada tabela.
    """
    today = today or dt.date.today()
    extracted_at = extracted_at or today.isoformat()
    counts: dict[str, int] = {}
    rubricas: dict[str, dict[str, str]] = {}

    def emit(table_id: str, rows: list[dict], reset: bool = True) -> None:
        """Grava as linhas de uma tabela e atualiza a contagem dela."""
        write_table(output, table_id, rows, reset=reset)
        if reset:
            counts[table_id] = len(rows)
        else:
            counts[table_id] = counts.get(table_id, 0) + len(rows)

    identificacoes, crosswalk = get_senador_reference()
    emit("senador", clean_senador(identificacoes, crosswalk))
    emit("senador_gabinete", clean_senador_gabinete(extracted_at, crosswalk))
    emit(
        "senador_escritorio_apoio",
        clean_senador_escritorio_apoio(extracted_at, crosswalk),
    )
    emit(
        "senador_auxilio_moradia",
        clean_senador_auxilio_moradia(extracted_at, crosswalk),
    )
    emit(
        "senador_aposentado_pensionista",
        clean_senador_aposentado_pensionista(extracted_at),
    )

    emit("servidor", clean_servidor(extracted_at))
    emit("servidor_ativo", clean_servidor_ativo(extracted_at))
    emit("servidor_aposentado", clean_servidor_aposentado(extracted_at))
    emit("servidor_exonerado", clean_servidor_exonerado(extracted_at))
    emit("servidor_cedido", clean_servidor_cedido(extracted_at))
    emit("pensionista", clean_pensionista(extracted_at))

    ceaps_years = list_years(constants.FIRST_YEAR_CEAPS.value, years, today)
    for index, year in enumerate(ceaps_years):
        emit("despesa_ceaps", clean_despesa_ceaps([year]), reset=index == 0)

    remuneracao_years = list_years(
        constants.FIRST_YEAR_REMUNERACAO.value, years, today
    )
    for index, year in enumerate(remuneracao_years):
        emit(
            "servidor_remuneracao",
            clean_servidor_remuneracao(list_months([year], today)),
            reset=index == 0,
        )

    hora_extra_years = list_years(
        constants.FIRST_YEAR_HORA_EXTRA.value, years, today
    )
    for index, year in enumerate(hora_extra_years):
        horas_extras, dias = clean_servidor_hora_extra(
            list_months([year], today)
        )
        emit("servidor_hora_extra", horas_extras, reset=index == 0)
        emit("servidor_hora_extra_dia", dias, reset=index == 0)

    supridos_years = list_years(
        constants.FIRST_YEAR_SUPRIDOS.value, years, today
    )
    for index, year in enumerate(supridos_years):
        supridos = clean_supridos([year])
        for table_id, rows in supridos.items():
            emit(table_id, rows, reset=index == 0)
            if table_id in constants_dicionario.RUBRICA_TABLES.value:
                rubricas.setdefault(table_id, {}).update(
                    collect_rubricas(rows)
                )

    contratacoes = fetch_contratacoes()
    emit("contratacao", clean_contratacao(contratacoes, extracted_at))
    emit(
        "contratacao_orgao_gestor",
        clean_contratacao_orgao_gestor(contratacoes, extracted_at),
    )
    licitacoes, detalhamentos = clean_licitacao(extracted_at)
    emit("licitacao", licitacoes)
    emit("licitacao_detalhamento", detalhamentos)
    emit("empresa", clean_empresa(extracted_at))

    if sub_resources:
        emit(
            "contratacao_item",
            clean_contratacao_item(contratacoes, extracted_at),
        )
        emit(
            "contratacao_garantia",
            clean_contratacao_garantia(contratacoes, extracted_at),
        )
        pagamentos, documentos, empenhos = clean_contratacao_pagamento(
            contratacoes, extracted_at
        )
        emit("contratacao_pagamento", pagamentos)
        emit("contratacao_documento_fiscal", documentos)
        emit("contratacao_pagamento_empenho", empenhos)
        emit(
            "contrato_aditivo",
            clean_contrato_aditivo(contratacoes, extracted_at),
        )
        emit(
            "ata_acionamento",
            clean_ata_acionamento(contratacoes, extracted_at),
        )
    del contratacoes

    emit("terceirizado", clean_terceirizado(extracted_at))
    emit("menor_aprendiz", clean_menor_aprendiz(extracted_at))
    emit("estagiario", clean_estagiario(extracted_at))

    emit("quadro_pessoal", clean_quadro_pessoal(extracted_at))
    emit("diretor_coordenador", clean_diretor_coordenador(extracted_at))
    emit("previsao_aposentadoria", clean_previsao_aposentadoria(extracted_at))

    emit("dicionario", clean_dicionario(rubricas))
    return counts
