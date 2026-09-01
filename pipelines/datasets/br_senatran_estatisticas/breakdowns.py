"""Recortes mensais da frota publicados nas páginas anuais do gov.br/SENATRAN.

Cada recorte tem o mesmo formato — ``UF | Município | <dimensão…> | quantidade`` —
e difere apenas nas colunas de dimensão. Este módulo trata todos eles de forma
genérica, a partir da configuração em :data:`LAYOUTS`.

Funções puras: nenhum import do Prefect. As tasks ficam em ``tasks.py``.
"""

from __future__ import annotations

import datetime
import re
import unicodedata
import urllib.parse
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd
import polars as pl
import requests
from bs4 import BeautifulSoup

from pipelines.datasets.br_senatran_estatisticas.constants import (
    constants as senatran_constants,
)

MONTHS: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}

# O gov.br publica "Março" com grafias inconsistentes. Depois do asciify,
# "Março" -> "marco", mas os arquivos trazem também "Maro" (o "ç" some antes
# do upload) e as abreviações de três letras.
MONTH_ALIASES: dict[str, int] = {
    **MONTHS,
    "maro": 3,
    **{name[:3]: number for name, number in MONTHS.items()},
}


@dataclass(frozen=True)
class Layout:
    """Um recorte da frota publicado mensalmente."""

    table_id: str
    #: tokens que identificam o recorte no nome do arquivo, já normalizados.
    #: Vários porque o gov.br renomeia os recortes entre anos — p.ex. o mesmo
    #: recorte aparece como `ano_de_fabricacao_e_modelo` (2017),
    #: `ano_fab_mod` e `ano_fab_modelo` (2021).
    tokens: tuple[str, ...]
    #: nomes finais das colunas de dimensão, na ordem em que aparecem
    dimensions: tuple[str, ...]
    #: tokens que, se presentes, desqualificam o arquivo
    excludes: tuple[str, ...] = field(default=())


LAYOUTS: dict[str, Layout] = {
    "municipio_combustivel": Layout(
        table_id="municipio_combustivel",
        tokens=("combustivel",),
        dimensions=("combustivel",),
    ),
    "municipio_cor": Layout(
        table_id="municipio_cor",
        tokens=("cor",),
        dimensions=("cor",),
    ),
    "municipio_potencia": Layout(
        table_id="municipio_potencia",
        tokens=("potencia",),
        dimensions=("potencia",),
    ),
    "municipio_restricao": Layout(
        table_id="municipio_restricao",
        tokens=("restricao",),
        dimensions=("restricao",),
    ),
    "municipio_cep": Layout(
        table_id="municipio_cep",
        tokens=("cep",),
        dimensions=("cep",),
    ),
    "municipio_ano_fabricacao_modelo": Layout(
        table_id="municipio_ano_fabricacao_modelo",
        tokens=(
            "ano_fab",
            "ano_fab_mod",
            "ano_fab_modelo",
            "ano_de_fabricacao",
        ),
        dimensions=("ano_modelo", "ano_fabricacao"),
    ),
    "municipio_tipo_especie_eixos": Layout(
        table_id="municipio_tipo_especie_eixos",
        tokens=("especie", "tipoespecieeixo", "tipo_especie_eixos"),
        dimensions=("tipo_veiculo", "especie", "eixos"),
    ),
}


def normalize(text: str) -> str:
    """Normaliza um nome de arquivo ou cabeçalho para comparação.

    Decodifica escapes de URL, remove acentos, passa para minúsculas e colapsa
    tudo que não for alfanumérico em ``_``. É o que permite casar as várias
    gerações de nomenclatura do gov.br sem depender de um template.
    """
    text = urllib.parse.unquote(text)
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", text.lower()).strip("_")


def month_from_filename(filename: str) -> int | None:
    """Extrai o mês de um nome de arquivo, tolerando as grafias do gov.br."""
    normalized = normalize(filename)
    # nomes longos primeiro: "marco" antes de "mar", senão "mar" casa cedo demais
    for name in sorted(MONTH_ALIASES, key=len, reverse=True):
        if re.search(rf"(?:^|_){name}(?:_|$|\d)", normalized):
            return MONTH_ALIASES[name]
    return None


def extract_breakdown_links(
    year: int,
    layout: Layout,
    session: requests.Session | None = None,
) -> dict[int, str]:
    """Devolve ``{mês: url}`` para um recorte na página anual do gov.br.

    Casa pelo **nome do arquivo** normalizado, não pelo texto do link: os
    rótulos visíveis variam muito mais que os nomes, e é por isso que o
    extrator antigo (``extract_links_post_2012``) perde 2013-2016.
    """
    session = session or requests.Session()
    url = f"{senatran_constants.BASE_URL_POST_2012.value}/frota-de-veiculos-{year}"
    response = session.get(
        url, headers=senatran_constants.HEADERS.value, timeout=180
    )
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    candidates: dict[int, list[str]] = {}
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if not re.search(r"\.(xlsx|xls|csv|zip|rar)$", href, re.I):
            continue
        filename = href.rsplit("/", 1)[-1]
        normalized = normalize(filename)
        # Casamento por token delimitado, não por substring: "cor" e "cep" são
        # curtos e casariam dentro de "recorte", "concept" etc.
        if not any(
            re.search(rf"(?:^|_){re.escape(t)}(?:_|$)", normalized)
            for t in layout.tokens
        ):
            continue
        if any(bad in normalized for bad in layout.excludes):
            continue
        month = month_from_filename(filename)
        if month is None:
            continue
        absolute = (
            href
            if href.startswith("http")
            else urllib.parse.urljoin("https://www.gov.br", href)
        )
        candidates.setdefault(month, []).append(absolute)

    # O gov.br deixa duplicatas na página ("copy_of_", "copy2_of_"). Preferimos
    # o nome sem prefixo de cópia; empatando, o mais curto.
    resolved: dict[int, str] = {}
    for month, urls in candidates.items():
        resolved[month] = sorted(
            urls,
            key=lambda u: (
                "copy" in normalize(u.rsplit("/", 1)[-1]),
                len(u.rsplit("/", 1)[-1]),
            ),
        )[0]
    return resolved


class UnsupportedArchiveError(RuntimeError):
    """Arquivo compactado que não conseguimos abrir neste ambiente."""


def _spreadsheet_from_archive(path: Path) -> Path:
    """Extrai a planilha de dentro de um .zip/.rar e devolve seu caminho.

    Os recortes de 2013, 2015 e 2016 vêm compactados; de 2017 em diante são
    .xlsx direto. O ``rarfile`` depende de um binário externo (unrar/bsdtar) que
    existe no worker mas nem sempre localmente — por isso a falha é sinalizada
    com :class:`UnsupportedArchiveError`, para o backfill pular o mês em vez de
    abortar os outros 150.
    """
    from zipfile import ZipFile

    extension = path.suffix.lower().lstrip(".")
    if extension == "zip":
        opener = ZipFile
    elif extension == "rar":
        from rarfile import RarFile

        opener = RarFile
    else:
        raise ValueError(f"Extensão não suportada: {extension}")

    destino = path.parent / f"{path.stem}_extraido"
    destino.mkdir(parents=True, exist_ok=True)
    try:
        with opener(path) as arquivo:  # type: ignore[operator]
            arquivo.extractall(path=destino)
    except Exception as erro:
        raise UnsupportedArchiveError(
            f"Não foi possível extrair {path.name}: {erro}"
        ) from erro

    planilhas = [
        f
        for f in sorted(destino.rglob("*"))
        if f.suffix.lower() in {".xlsx", ".xls"}
    ]
    if not planilhas:
        raise UnsupportedArchiveError(
            f"Nenhuma planilha dentro de {path.name}"
        )
    return planilhas[0]


def read_breakdown(path: str | Path, layout: Layout) -> pl.DataFrame:
    """Lê um arquivo de recorte e devolve colunas já renomeadas.

    Aceita .xlsx/.xls direto ou compactado em .zip/.rar. O nome da planilha
    varia (``Layout C``, ``Layout D `` com espaço à direita), então usamos toda
    aba que não seja o glossário.

    **Todas as abas, não só a primeira.** Quando o recorte passa de 999.999
    linhas a fonte continua numa segunda aba — ``Layout E`` mais
    ``Continuação_Layout E`` no recorte de potência. Ler só a primeira perde a
    cauda do arquivo, que é ordenado por UF: em 2026-07 isso deixava de fora
    Sergipe, Tocantins e São Paulo a partir de Lençóis Paulista, 554 municípios
    ao todo, sem erro nenhum.
    """
    path = Path(path)
    if path.suffix.lower() in {".zip", ".rar"}:
        path = _spreadsheet_from_archive(path)
    excel = pd.ExcelFile(path)
    sheets = [s for s in excel.sheet_names if normalize(str(s)) != "glossario"]
    if not sheets:
        raise ValueError(f"Nenhuma aba de dados em {path}")
    # dtype=str em tudo: o CEP vem com zeros à esquerda que o pandas destrói ao
    # inferir int (069900 -> 69900), e o staging é all-STRING por convenção de
    # qualquer forma — o safe_cast do modelo dbt decide o tipo final.
    expected = 2 + len(layout.dimensions) + 1
    colunas = ["nome_uf", "nome_denatran", *layout.dimensions, "quantidade"]

    partes = []
    for aba in sheets:
        parte = pd.read_excel(path, sheet_name=aba, dtype=str)
        if parte.shape[1] < expected:
            raise ValueError(
                f"{path} [{aba}]: esperava >= {expected} colunas para o "
                f"recorte {layout.table_id}, encontrou {parte.shape[1]}"
            )
        # Recortar e renomear por posição *antes* de juntar: o cabeçalho muda
        # de grafia entre meses (`Município` vs `MUNICIPIO`), e `pd.concat`
        # alinha por nome — juntar as abas cruas produziria colunas extras
        # cheias de NaN em vez de empilhar as linhas.
        parte = parte.iloc[:, :expected]
        parte.columns = colunas
        partes.append(parte)

    frame = (
        partes[0] if len(partes) == 1 else pd.concat(partes, ignore_index=True)
    )
    return pl.from_pandas(frame.astype(str))


def build_municipio_lookup(
    nomes: pl.DataFrame, ibge: pl.DataFrame
) -> pl.DataFrame:
    """Constrói o cruzamento ``(sigla_uf, nome_denatran) -> id_municipio``.

    Feito **uma vez por arquivo sobre os nomes distintos**, e não linha a linha:
    nos recortes cada município aparece muitas vezes (uma linha por combustível,
    por cor etc.), então casar por linha seria lento e faria
    ``treat_uf`` acusar duplicidade em todo município.
    """
    from pipelines.datasets.br_senatran_estatisticas.utils import (
        fix_suggested_nome_ibge,
        get_city_name_ibge,
    )

    pieces = []
    for uf in sorted(nomes["sigla_uf"].unique().to_list()):
        nomes_uf = nomes.filter(pl.col("sigla_uf") == uf).unique(
            subset=["nome_denatran"]
        )
        ibge_uf = ibge.filter(pl.col("sigla_uf") == uf).with_columns(
            pl.col("nome")
            .apply(
                lambda n: normalize(n).replace("_", " "),
                return_dtype=pl.Utf8,
            )
            .alias("nome")
        )
        if ibge_uf.is_empty() or nomes_uf.is_empty():
            continue
        # ibge_uf ligado como default: sem isso o lambda captura a variável
        # do laço por referência (ruff B023)
        sugerido = nomes_uf["nome_denatran"].apply(
            lambda x, _ibge=ibge_uf: get_city_name_ibge(x, _ibge),
            return_dtype=pl.Utf8,
        )
        nomes_uf = nomes_uf.with_columns(sugerido.alias("suggested_nome_ibge"))
        nomes_uf = nomes_uf.with_columns(
            nomes_uf.apply(fix_suggested_nome_ibge)["map"].alias(
                "suggested_nome_ibge"
            )
        )
        pieces.append(
            nomes_uf.join(
                ibge_uf.select(["nome", "sigla_uf", "id_municipio"]),
                left_on=["suggested_nome_ibge", "sigla_uf"],
                right_on=["nome", "sigla_uf"],
                how="left",
            ).select(["sigla_uf", "nome_denatran", "id_municipio"])
        )
    if not pieces:
        raise ValueError("Nenhum município pôde ser cruzado com o IBGE")
    return pl.concat(pieces)


def clean_breakdown(
    frame: pl.DataFrame,
    layout: Layout,
    year: int,
    month: int,
    ibge: pl.DataFrame,
) -> tuple[pl.DataFrame, int]:
    """Transforma um recorte bruto no formato final da tabela.

    Devolve ``(dados, descartadas)`` — o segundo item conta as linhas sem UF
    reconhecida ou com "município não informado", que a fonte traz em todo mês.
    """
    from pipelines.datasets.br_senatran_estatisticas.utils import asciify

    reverse_ufs = {
        normalize(nome): sigla
        for sigla, nome in senatran_constants.DICT_UFS.value.items()
    }

    frame = frame.with_columns(
        pl.col("nome_uf")
        .apply(lambda x: reverse_ufs.get(normalize(x)), return_dtype=pl.Utf8)
        .alias("sigla_uf"),
        pl.col("nome_denatran")
        .apply(asciify, return_dtype=pl.Utf8)
        .str.to_lowercase()
        .alias("nome_denatran"),
    )

    descartadas = frame.filter(
        pl.col("sigla_uf").is_null()
        | (pl.col("nome_denatran") == "municipio nao informado")
    )
    frame = frame.filter(
        pl.col("sigla_uf").is_not_null()
        & (pl.col("nome_denatran") != "municipio nao informado")
    )

    lookup = build_municipio_lookup(
        frame.select(["sigla_uf", "nome_denatran"]), ibge
    )
    frame = frame.join(lookup, on=["sigla_uf", "nome_denatran"], how="left")

    frame = frame.with_columns(
        pl.lit(year, dtype=pl.Int64).alias("ano"),
        pl.lit(month, dtype=pl.Int64).alias("mes"),
        # id_municipio é identificador, não quantidade: STRING por convenção
        pl.col("id_municipio").cast(pl.Utf8).alias("id_municipio"),
        pl.col("quantidade")
        .str.replace_all(r"[^0-9-]", "")
        .cast(pl.Int64, strict=False)
        .alias("quantidade"),
    )

    final = frame.select(
        [
            "ano",
            "mes",
            "sigla_uf",
            "id_municipio",
            *layout.dimensions,
            "quantidade",
        ]
    )
    final = final.with_columns(
        [pl.col(dim).str.strip().alias(dim) for dim in layout.dimensions]
    )

    # Somar depois de aparar. A fonte emite variantes so de espaco do mesmo
    # rotulo -- `'0'` e `'0    '` no recorte de CEP, `'GASOLINA'` e
    # `'GASOLINA '` no de combustivel -- que viram a mesma chave assim que o
    # `strip` roda, e ai `(ano, mes, id_municipio, <dimensoes>)` deixa de ser
    # unica e o teste do dbt reprova a tabela inteira. Medido: 38 chaves
    # repetidas em municipio_cep 2026-07 e 7 em municipio_combustivel, sempre
    # a linha cheia mais uma de quantidade 1. Somar preserva o total.
    chave = ["ano", "mes", "sigla_uf", "id_municipio", *layout.dimensions]
    final = final.group_by(chave, maintain_order=True).agg(
        # `sum` devolve 0 quando o grupo e todo nulo; um quantidade ausente
        # tem que continuar ausente, nao virar zero.
        pl.when(pl.col("quantidade").is_null().all())
        .then(None)
        .otherwise(pl.col("quantidade").sum())
        .alias("quantidade")
    )

    return final, len(descartadas)


def reference_date(year: int, month: int) -> datetime.date:
    return datetime.date(year, month, 1)


def available_months(
    layout: Layout,
    start_after: datetime.date,
    today: datetime.date | None = None,
    session: requests.Session | None = None,
) -> list[tuple[int, int, str]]:
    """Meses publicados de um recorte, posteriores a ``start_after``.

    Devolve ``[(ano, mes, url), …]`` em ordem cronológica. Busca **uma vez por
    página anual**, e não uma vez por mês: o extrator antigo refaz o download da
    página a cada mês do intervalo, o que torna um backfill completo
    (~280 meses) dezenas de minutos mais lento sem necessidade.
    """
    session = session or requests.Session()
    today = today or datetime.date.today()
    encontrados: list[tuple[int, int, str]] = []
    for year in range(max(start_after.year, 2013), today.year + 1):
        try:
            links = extract_breakdown_links(year, layout, session=session)
        except Exception:
            # uma página anual indisponível não deve abortar o backfill inteiro
            continue
        for month, url in sorted(links.items()):
            reference = datetime.date(year, month, 1)
            if start_after < reference <= today:
                encontrados.append((year, month, url))
    return sorted(encontrados)


def download(url: str, dest_dir: str | Path, session=None) -> Path:
    """Baixa um arquivo de recorte, retomando se a conexão cair.

    O host do gov.br derruba conexões longas, então tentamos algumas vezes e
    conferimos o tamanho antes de aceitar o arquivo.
    """
    session = session or requests.Session()
    dest_dir = Path(dest_dir)
    dest_dir.mkdir(parents=True, exist_ok=True)
    destino = dest_dir / urllib.parse.unquote(
        url.rsplit("/", 1)[-1].split("?")[0]
    )
    ultimo_erro: Exception | None = None
    for _ in range(4):
        try:
            resposta = session.get(
                url, headers=senatran_constants.HEADERS.value, timeout=600
            )
            resposta.raise_for_status()
            destino.write_bytes(resposta.content)
            if destino.stat().st_size > 0:
                return destino
        except Exception as erro:
            ultimo_erro = erro
    raise RuntimeError(f"Falha ao baixar {url}: {ultimo_erro}")
