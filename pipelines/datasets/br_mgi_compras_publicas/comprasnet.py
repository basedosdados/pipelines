"""ComprasNet legado scrape: winning offers and item event timelines.

The Compras.gov.br open-data API exposes the legado pregao header
(``3_consultarPregoes``) and its items (``4_consultarItensPregoes``), but two
things live only in the ComprasNet HTML and in no API:

* the **brand, manufacturer, model and offered-object description** of each
  winning item (``FornecedorResultado.asp``);
* the **per-item event timeline** — who adjudicated and homologated what, and
  when — together with the group/lot structure and the outcomes of items that
  were never homologated (``termohom.asp``), which endpoint 4 drops because it
  keys on ``dt_hom``.

The bid-by-bid session record (``AtaEletronico.asp``) is deliberately excluded:
it sits behind a CAPTCHA whose own text says it exists to stop automated
consultation. All-participant bids are available legitimately in
``br_cgu_licitacao_contrato.licitacao_participante``.

Coverage is 2001-06 to 2024-01. The source stopped receiving pregoes at the
Lei 14.133 cutover, so this is a closed archive, not a recurring feed.
"""

from __future__ import annotations

import html as _html
import logging
import re
import time

import requests

logger = logging.getLogger(__name__)

BASE = "https://comprasnet.gov.br/livre/Pregao/"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

# The ata list only accepts ``dd/mm/yyyy`` and answers a session-date window.
DATE_FMT = "%d/%m/%Y"

# Modalidade 5 is Pregao Eletronico; 3 is Concorrencia Eletronica. The list page
# returns the code as the third component of each row anchor.
ROW_KEY_RE = re.compile(r'href="#(\d+)-(\d+)-(\d+)"')
#: ``prgcod`` appears in whichever detail links the pregao actually has.
#: Keying on ``termoHomologacao`` alone drops every revoked and abandoned
#: pregao — 17% of a September 2019 sample — which is precisely the tail this
#: dataset exists to cover, since the API drops it too.
PRGCOD_RE = re.compile(
    r"(?:termoHomologacao|resultadoFornecedor|termoAdjudicacaoJulgamento"
    r"|Declaracoes|exibeQuadro|AtaCadReserva)\((\d{3,9})\s*[,)]"
)
CNPJ_RE = r"\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}"
CPF_RE = r"\d{3}\.\d{3}\.\d{3}-\d{2}"
FORNECEDOR_RE = re.compile(
    r'<td[^>]*colspan="7"[^>]*>\s*<b>\s*('
    + CNPJ_RE
    + "|"
    + CPF_RE
    + r")\s*</b>\s*-\s*(.*?)</td>",
    re.I | re.S,
)
TAG_RE = re.compile(r"<[^>]+>")
WS_RE = re.compile(r"\s+")


def build_session() -> requests.Session:
    """A session carrying the browser UA the ASP pages require."""
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    # ata4.asp reads filter state the filter page seeds into the ASP session.
    session.get(BASE + "ata0.asp", timeout=90)
    return session


def _text(fragment: str) -> str:
    """Strip tags and collapse whitespace, decoding entities."""
    without_script = re.sub(r"(?is)<script.*?</script>", " ", fragment)
    return WS_RE.sub(
        " ", _html.unescape(TAG_RE.sub(" ", without_script))
    ).strip()


def _number(raw: str | None) -> str:
    """``R$ 1.234,5600`` -> ``1234.5600``; empty string when absent.

    The pt-BR thousands separator is the dot, so it must go before the comma
    becomes the decimal point. Applied to quantities as well as money: a
    quantity of 21,500 units prints as ``21.500``, which would otherwise reach
    BigQuery as 21.5.
    """
    if not raw:
        return ""
    cleaned = raw.replace("R$", "").strip()
    if not cleaned or cleaned in {"-", "--"}:
        return ""
    return cleaned.replace(".", "").replace(",", ".")


def _iso_datetime(raw: str) -> str:
    """``25/09/2019 15:15:56`` -> ``2019-09-25 15:15:56``.

    Staging is all-STRING and the dbt model applies a bare
    ``safe_cast(... as datetime)``, which returns NULL for a dd/mm/yyyy string.
    Normalising here keeps the generated SQL uniform across every table instead
    of teaching the generator a per-column parse.
    """
    date_part, _, time_part = raw.partition(" ")
    day, month, year = date_part.split("/")
    return f"{year}-{month}-{day} {time_part}"


def compose_id_compra(numprp: str, uasg: str, modalidade: str) -> str:
    """Rebuild the SIASG ``id_compra`` the API keys on.

    ``id_compra`` is UASG(6) + modalidade(2) + numero(5) + ano(4) = 17 digits.
    The list page gives ``numprp`` as numero concatenated with the four-digit
    year, so ``102019`` is pregao 10 of 2019. Verified against
    ``3.1_consultarPregoes_Id`` on a 30-key sample: 30/30 resolved.
    """
    ano = numprp[-4:]
    numero = numprp[:-4] or "0"
    return f"{uasg.zfill(6)}{modalidade.zfill(2)}{numero.zfill(5)}{ano}"


def list_atas(
    session: requests.Session,
    date_start: str,
    date_end: str,
    tipo_pregao: str = "E",
    timeout: int = 900,
) -> list[tuple[str, str, str]]:
    """Every pregao whose session falls in ``[date_start, date_end]``.

    Dates are ``dd/mm/yyyy``. Returns ``(numprp, uasg, modalidade)`` triples.
    A month window answers in seconds; a full year degrades badly (194s for
    2007), so callers should page by month.
    """
    payload = {
        "rdTpPregao": tipo_pregao,
        "lstSrp": "T",
        "lstICMS": "T",
        "uf": "",
        "co_uasg": "",
        "txtlstUasg": "",
        "numprp": "",
        "dt_ini_sessao": date_start,
        "dt_fim_sessao": date_end,
    }
    response = session.post(BASE + "ata4.asp", data=payload, timeout=timeout)
    response.raise_for_status()
    response.encoding = "latin-1"
    return sorted(set(ROW_KEY_RE.findall(response.text)))


def fetch_prgcod(
    session: requests.Session,
    numprp: str,
    uasg: str,
    modalidade: str,
    timeout: int = 240,
    retries: int = 3,
) -> str | None:
    """Resolve the internal pregao surrogate key needed by the detail pages.

    ``prgcod`` is dense over roughly 100..1_182_019 but no page that takes it
    echoes the UASG code back, so it cannot be enumerated into a joinable key —
    this crosswalk hop is unavoidable.

    The id is read from any of the detail links the page carries. A revoked or
    abandoned pregao has no termo de homologacao, but still links
    ``resultadoFornecedor`` and ``Declaracoes`` with the same id.
    """
    params = {
        "co_no_uasg": uasg,
        "numprp": numprp,
        "codigoModalidade": modalidade,
        "f_lstSrp": "T",
        "f_Uf": "",
        "f_numPrp": "0",
        "f_codUasg": "",
        "f_codMod": modalidade,
        "f_tpPregao": "E",
        "f_lstICMS": "T",
    }
    for attempt in range(retries):
        try:
            response = session.get(
                BASE + "ata2.asp", params=params, timeout=timeout
            )
        except requests.RequestException:
            # ComprasNet drops connections sporadically over a long run. One
            # dropped connection must never end a multi-day harvest.
            if attempt == retries - 1:
                logger.warning(
                    "crosswalk %s/%s: connection failed", uasg, numprp
                )
                return None
            time.sleep(2 * (attempt + 1))
            continue
        if response.status_code != 200:
            # Was silent, which made a bad response indistinguishable from a
            # pregao that genuinely resolves to nothing.
            logger.warning(
                "crosswalk %s/%s: HTTP %s",
                uasg,
                numprp,
                response.status_code,
            )
            return None
        response.encoding = "latin-1"
        match = PRGCOD_RE.search(response.text)
        return match.group(1) if match else None
    return None


def fetch_page(
    session: requests.Session,
    page: str,
    prgcod: str,
    timeout: int = 300,
    retries: int = 3,
) -> str | None:
    """Fetch ``FornecedorResultado`` or ``termohom`` for one pregao.

    A 400 means the ``prgcod`` is a gap in the sequence (about 6% of the range)
    and returns ``None``. ``termohom.asp`` also answers 500 for a minority of
    pregoes; that is retried and then recorded as a miss, never raised, so one
    bad record cannot abort a multi-day harvest.
    """
    if page == "fornecedor_resultado":
        url = BASE + "FornecedorResultado.asp"
        params = {"prgcod": prgcod, "strTipoPregao": "E"}
    elif page == "termo_homologacao":
        url = BASE + "termohom.asp"
        params = {"prgcod": prgcod}
    else:
        raise ValueError(f"unknown page {page!r}")
    last_status = None
    for attempt in range(retries):
        try:
            response = session.get(url, params=params, timeout=timeout)
        except requests.RequestException:
            if attempt == retries - 1:
                raise
            time.sleep(2 * (attempt + 1))
            continue
        last_status = response.status_code
        # 400 marks a gap in the prgcod sequence; that is data, not an error.
        if response.status_code == 400:
            return None
        if response.status_code == 200:
            response.encoding = "latin-1"
            return response.text
        # termohom.asp answers 500 for a minority of pregoes, sometimes
        # persistently. Retry, then record the miss rather than aborting.
        time.sleep(2 * (attempt + 1))
    logger.warning(
        "%s prgcod=%s unavailable after %d tries (last %s)",
        page,
        prgcod,
        retries,
        last_status,
    )
    return None


# --------------------------------------------------------------------------- #
# Parsing
# --------------------------------------------------------------------------- #

TR_RE = re.compile(r"(?is)<tr\b.*?</tr>")
TD_RE = re.compile(r"(?is)<td\b[^>]*>(.*?)</td>")
ITEM_MARKER_RE = re.compile(
    r"(?is)<td[^>]*>\s*Item:\s*(\d+)\s*(?:-\s*GRUPO\s*(\d+)\s*)?</td>"
)
LABEL_RES = {
    "descricao_item": re.compile(r"(?is)<b>\s*Descri\S*o:\s*</b>(.*?)</td>"),
    "intervalo_minimo_lances": re.compile(
        r"(?is)<b>\s*Intervalo M\S*nimo entre Lances:\s*</b>(.*?)</td>"
    ),
}
EVENTOS_BLOCK_RE = re.compile(r"(?is)Eventos do Item(.*?)</table>")
DATETIME_RE = re.compile(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$")


def _cells(row: str) -> list[str]:
    return [_text(cell) for cell in TD_RE.findall(row)]


def parse_fornecedor_resultado(
    page: str, id_compra: str
) -> list[dict[str, str]]:
    """Winning offers, one row per supplier x item.

    Carries ``marca``, ``fabricante``, ``modelo_versao`` and the free-text
    ``descricao_detalhada_ofertada`` — what the supplier actually offered, which
    no API exposes.
    """
    rows: list[dict[str, str]] = []
    headers = list(FORNECEDOR_RE.finditer(page))
    if not headers:
        return rows
    ano = id_compra[-4:]
    for index, header in enumerate(headers):
        documento = header.group(1)
        nome = _text(header.group(2))
        end = (
            headers[index + 1].start()
            if index + 1 < len(headers)
            else len(page)
        )
        block = page[header.end() : end]
        current: dict[str, str] | None = None
        for row in TR_RE.findall(block):
            if "Marca:" in row or "Fabricante:" in row:
                if current is None:
                    continue
                for key, label in (
                    ("marca", r"Marca:"),
                    ("fabricante", r"Fabricante:"),
                    ("modelo_versao", r"Modelo / Vers\S*o:"),
                    (
                        "descricao_detalhada_ofertada",
                        r"Descri\S*o Detalhada do Objeto Ofertado:",
                    ),
                ):
                    match = re.search(
                        r"(?is)"
                        + label
                        + r"(?:&nbsp;|\s)*</span>\s*<span[^>]*>(.*?)</span>",
                        row,
                    )
                    current[key] = _text(match.group(1)) if match else ""
                continue
            cells = _cells(row)
            if len(cells) < 6 or not cells[0].isdigit():
                continue
            current = {
                "ano": ano,
                "id_compra": id_compra,
                "cnpj_cpf_fornecedor": documento,
                "nome_fornecedor": nome,
                "numero_item": cells[0],
                "descricao_item": cells[1],
                "unidade_fornecimento": cells[2],
                "quantidade": _number(cells[3]),
                "valor_unitario": _number(cells[4]),
                "valor_global": _number(cells[5]),
                "marca": "",
                "fabricante": "",
                "modelo_versao": "",
                "descricao_detalhada_ofertada": "",
            }
            rows.append(current)
    return rows


def parse_termo_homologacao(page: str, id_compra: str) -> list[dict[str, str]]:
    """Per-item event timeline, one row per event.

    Includes items that were cancelled, deserted or declared fracassado —
    ``4_consultarItensPregoes`` keys on ``dt_hom`` and drops those, which is a
    ~17% shortfall biased exactly toward failed procurement.

    ``numero_grupo`` comes from the item marker itself, which reads
    ``Item: 1 - GRUPO 1`` when the item was disputed as part of a lot. The group
    summary at the top of the page is not used: it lists member items only, and
    on pages where the lot numbering differs from the item numbering it cannot
    be matched back reliably.
    """
    rows: list[dict[str, str]] = []
    markers = list(ITEM_MARKER_RE.finditer(page))
    if not markers:
        return rows
    ano = id_compra[-4:]
    for index, marker in enumerate(markers):
        numero_item = marker.group(1)
        numero_grupo = marker.group(2) or ""
        end = (
            markers[index + 1].start()
            if index + 1 < len(markers)
            else len(page)
        )
        block = page[marker.end() : end]
        eventos = EVENTOS_BLOCK_RE.search(block)
        if not eventos:
            continue
        ordem = 0
        for row in TR_RE.findall(eventos.group(1)):
            cells = _cells(row)
            if len(cells) < 4 or not DATETIME_RE.match(cells[1]):
                continue
            ordem += 1
            rows.append(
                {
                    "ano": ano,
                    "id_compra": id_compra,
                    "numero_item": numero_item,
                    "numero_grupo": numero_grupo,
                    "ordem_evento": str(ordem),
                    "nome_evento": cells[0],
                    "data_hora_evento": _iso_datetime(cells[1]),
                    "nome_responsavel": "" if cells[2] == "-" else cells[2],
                    "observacoes": cells[3],
                }
            )
    return rows
