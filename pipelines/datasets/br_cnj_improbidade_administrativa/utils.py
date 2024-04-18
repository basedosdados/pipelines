# -*- coding: utf-8 -*-
import re
import time
import typing

import httpx
from lxml import html
from unidecode import unidecode

from pipelines.datasets.br_cnj_improbidade_administrativa.constants import constants
from pipelines.utils.utils import log


def build_home_url_page(index: int) -> str:
    return constants.HOME_PAGE_TEMPLATE.value.format(index=index * 15)


def build_condenacao_url(id: str) -> str:
    return constants.CONDENACAO_URL_TEMPLATE.value.format(id=id)


def build_process_url(id: str) -> str:
    return constants.PROCESS_URL_TEMPLATE.value.format(id=id)


def build_people_info_url(sentence_id: str, id: str) -> str:
    return constants.PEOPLE_INFO_URL_TEMPLATE.value.format(sentence_id=sentence_id, people_id=id)


async def get_async(client: httpx.AsyncClient, url: str) -> httpx.Response:
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 Safari/537.36",
    }
    try:
        return await client.get(url, headers=headers)
    except (httpx.ConnectError, httpx.ReadError) as e:
        log(f"Error {e}, {url}")
        time.sleep(10.0)
        return await client.get(url, headers=headers)


class PeopleLine(typing.TypedDict):
    nome: str
    num_processo: str
    processo_id: str
    condenacao_id: str


def get_number_from_href_attr(href: str) -> str:
    return href.split("=")[-1].replace("\\'", "")


def parse_line_from_main_page(node) -> PeopleLine:
    # Condenacao
    (lhs_node,) = node.xpath("td[1]//a")
    # Processo
    (rhs_node,) = node.xpath("td[2]//a")

    condenacao_id = get_number_from_href_attr(lhs_node.get("href"))
    processo_id = get_number_from_href_attr(rhs_node.get("href"))
    nome: str = lhs_node.text
    num_processo: str = rhs_node.text

    return {
        "nome": nome,
        "num_processo": num_processo.strip(),
        "condenacao_id": condenacao_id,
        "processo_id": processo_id,
    }


def parse_peoples(response: httpx.Response) -> list[PeopleLine]:
    if response.status_code == 200:
        tree = html.fromstring(response.content)
        nodes: list = tree.xpath("table[1]")

        if len(nodes) == 0:
            return []

        if len(nodes) > 1:
            log(f"parse_peoples: more than one table element found: {response}")

        childrens = nodes[0].getchildren()

        return [
            parse_line_from_main_page(i) for i in childrens if i.get("class") != "\\'fundoTr\\'"
        ]
    else:
        return []


class Condenacao(typing.TypedDict):
    nome: str
    num_processo: str
    processo_id: str
    condenacao_id: str


def get_sibling_element(element) -> list:
    result = []

    while True:
        next = element if len(result) == 0 else result[-1].getnext()
        if next is None:
            break
        else:
            result.append(next)

    return result


def sanitize_string(string: str) -> str:
    return re.sub(r"\s+", " ", string.strip())


def parse_info_about_sentence(tr_element) -> tuple[str, str] | None:
    children_lhs_span = tr_element.find(".//td[1]//span")
    children_lhs = tr_element.find(".//td[1]") if children_lhs_span is None else children_lhs_span

    if children_lhs is None:
        return None

    text: str | None = children_lhs.text

    if text is not None:
        text_strip = text.strip()  # type: ignore
        if text_strip == "Tipo Julgamento:":
            value = tr_element.xpath('.//td[2]//input[@checked="checked"]')[0].get("value").strip()
            return (text_strip, value)
        elif text_strip == "Inelegibilidade":
            value = tr_element.find(".//td[2]//font//b").text.strip()
            return (text_strip, value)
        elif text_strip in [
            "Pagamento de multa?",
            "Ressarcimento integral do dano?",
            "Suspensão dos Direitos Políticos?",
            "Perda de Emprego/Cargo/Função Pública?",
            "Perda de bens ou valores acrescidos ilicitamente ao patrimônio?",
        ]:
            value = tr_element.find(".//td[2]").text_content()
            return (text_strip, sanitize_string(value))
        elif text_strip.startswith("Proibição"):
            value = tr_element.find(".//td[2]").text_content()
            return (" ".join(text_strip.split()), sanitize_string(value))
        else:
            return None
    else:
        span_lhs_id = children_lhs.get("id")
        if span_lhs_id == "labelDataJulgColeg":
            value = tr_element.find(".//td[2]//span").text.strip()
            return ("Data do trânsito em julgado", value)
        elif span_lhs_id == "labelPenaPrivativa":
            value = tr_element.find(".//td[2]").text_content()
            return (
                "Pena privativa de liberdade",
                sanitize_string(value),
            )
        else:
            return None


def parse_related_issues(tree) -> list[tuple[str, str]]:
    script = tree.find(".//body//div[2]//div[5]//script[16]")

    if script is None:
        return []

    matches = re.findall(
        r"addAssunto\('([^']+)',\s*'([^']+)',\s*(true|false)\);",
        " ".join(script.text.split()),
    )

    return [(match[0], match[1]) for match in matches]


def parse_table_relevant_data(tree) -> dict[str, list[str]]:
    node_table = tree.xpath('//*[@id="hierarquia"]')[0]

    childrens: list = node_table.getchildren()

    hash_map = {}

    for children in childrens:
        (lhs, rhs) = children.findall(".//div")
        label_lhs = lhs.find(".//label").text.strip()
        label_rhs = rhs.find(".//label").text.strip()

        # NOTE: campos na esquerda pode não ter um nome válido embora o da direita tenha algum valor
        # Exemplo: https://www.cnj.jus.br/improbidade_adm/visualizar_condenacao.php?seq_condenacao=5966
        # Em DADOS PROCESSUAIS RELEVANTES uma linha tem ":"
        if label_lhs == ":":
            continue

        if label_lhs in hash_map:
            hash_map[label_lhs] = [*hash_map[label_lhs], label_rhs]
        else:
            hash_map[label_lhs] = [label_rhs]

    return hash_map


def normalize_string(input_string):
    # Remove diacritics using unidecode
    normalized_string = unidecode(input_string)

    # Replace non-alphanumeric characters with underscores
    normalized_string = "".join(c if c.isalnum() or c.isspace() else "_" for c in normalized_string)

    # Replace spaces with underscores
    normalized_string = normalized_string.replace(" ", "_")

    # Convert to lowercase
    normalized_string = normalized_string.lower()

    return normalized_string


def make_unique_col_name(item: tuple[str, list[str]]) -> dict[str, str]:
    label, values = item

    if len(values) > 1:
        return {f"{label}_{index + 1}": v for index, v in enumerate(values)}
    else:
        return {label: values[0]}


def get_people_data(tree) -> dict[str, str | None]:
    node = tree.xpath(
        "body//div[2]//div[5]//table[2]//tr//td//table[2]//tr[2]//td//table//tr[2]//td[1]//span//a"
    )
    if node is None:
        return {"pessoa_id": None, "tipo_pessoa": None}

    # Example: "recuperarDadosRequerido('73236','F')"
    onclick_attr: str = node[0].get("onclick").strip()

    _, tail = onclick_attr.split("(")
    number, letter = tail.split(",")

    return {
        "pessoa_id": number.replace("'", ""),
        "tipo_pessoa": letter.replace("'", "").replace(")", ""),
    }


class SentenceResponse(typing.TypedDict):
    condenacao_id: str
    response: httpx.Response


def parse_sentence(sentence_response: SentenceResponse) -> dict:
    if sentence_response["response"].status_code != 200:
        return {"condenacao_id": sentence_response["condenacao_id"]}

    tree = html.fromstring(sentence_response["response"].content)

    script_elements: list = tree.xpath(".//script")

    # NOTE: Algumas paginas sao privadas e retorna com status_code == 400
    # Elas tem uma tag script com a funcao alert
    if len(script_elements) == 1:
        log(f"Sentence {sentence_response} is private. Script elements: {script_elements}")
        js_code = script_elements[0].text
        if (
            js_code
            == "alert('Usuário não tem permissão para visualizar os dados dessa Condenação.');history.back();"
        ):
            return {"condenacao_id": sentence_response["condenacao_id"]}

    # DD/MM/YYYY HH:MM:SS
    data_cadastro: str | None = None

    node_data_cadastro: list = tree.xpath(
        "body//div[2]//div[5]//table[2]//tr//td//table[1]//tr[1]//td[2]"
    )

    if len(node_data_cadastro) > 0:
        data_cadastro = node_data_cadastro[0].text.strip()

    process_info = parse_table_relevant_data(tree)

    data_people = get_people_data(tree)

    situacao: str | None = None

    node_situacao = tree.xpath(
        "body//div[2]//div[5]//table[2]//tr//td//table[2]//tr[2]//td//table//tr[2]//td[2]//span"
    )

    if len(node_situacao) > 0:
        situacao = node_situacao[0].text.strip()

    tr_info_condenacao = tree.xpath("body//div[2]//div[5]//table[2]//tr//td//table[2]//tr[5]")[0]

    lines_info_condenacao = [
        i
        for i in get_sibling_element(tr_info_condenacao)
        if not isinstance(i, html.HtmlComment) and i.get("style") is None
    ]

    info_about_sentence = list(
        filter(
            lambda x: x is not None,
            [parse_info_about_sentence(el) for el in lines_info_condenacao],
        )
    )

    related_issues = list(
        map(
            lambda index_issue: {f"assunto_{index_issue[0] + 1}": index_issue[1][1]},
            enumerate(parse_related_issues(tree)),
        )
    )

    unnest_related_issues = {key: value for item in related_issues for key, value in item.items()}

    process_info_unique_names = [make_unique_col_name(name) for name in process_info.items()]

    unnest_prc = {key: value for item in process_info_unique_names for key, value in item.items()}

    return {
        "condenacao_id": sentence_response["condenacao_id"],
        "data_cadastro": data_cadastro,
        "situacao": situacao,
        **unnest_prc,
        **unnest_related_issues,
        **data_people,
        **dict(info_about_sentence),  # type: ignore
    }


class PeopleInfoResponse(typing.TypedDict):
    condenacao_id: str
    response: httpx.Response


def parse_people_data(people_response: PeopleInfoResponse) -> dict[str, str | None]:
    if people_response["response"].status_code != 200:
        return {"condenacao_id": people_response["condenacao_id"]}

    content = people_response["response"].text

    infos = content.split("'")[1].split(",")

    def get_or_none(index: int) -> str | None:
        return infos[index] if index < len(infos) else None

    return {
        "condenacao_id": people_response["condenacao_id"],
        "sexo": get_or_none(3),
        "publico": get_or_none(4),
        "esfera_pessoa": get_or_none(5),
        "orgao": get_or_none(6),
        "cargo": get_or_none(7),
        "uf": get_or_none(8),
        "cod": get_or_none(9),
    }


class ProcessInfoResponse(typing.TypedDict):
    process_id: str
    response: httpx.Response


def parse_process(process_response: ProcessInfoResponse) -> dict:
    process_id = process_response["process_id"]

    if process_response["response"].status_code != 200:
        return {"processo_id": process_id}

    content = process_response["response"].content

    tree = html.fromstring(content)

    node = tree.xpath(".//body//div[2]//div[5]//div[4]//table[2]//tr//td//table//tr[5]/td[2]")

    if len(node) == 0:
        return {"process_id": process_id}

    text = node[0].text.strip()

    return {"processo_id": process_id, "data_propositura": text}
