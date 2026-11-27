"""Funções puras de br_bndes_fundos_garantidores, sem Prefect."""

import html
import io
import json
import re
import shutil
from datetime import date, timedelta
from pathlib import Path

import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.datasets.br_bndes_fundos_garantidores.constants import (
    constants,
)
from pipelines.utils.data_tests.rows import check_rows_written
from pipelines.utils.utils import brasil_proxy_url, log

MESES = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def ler_itens_central(pagina: str) -> dict[str, dict]:
    """devolve os itens da central de downloads indexados pelo `nome`."""
    texto = html.unescape(pagina)
    itens = {}
    for bloco in re.findall(r'\{[^{}]*"titulo"[^{}]*\}', texto):
        try:
            item = json.loads(bloco)
        except json.JSONDecodeError:
            continue
        if item.get("nome"):
            itens[item["nome"]] = item
    return itens


def _fim_do_mes(ano: int, mes: int) -> date:
    """último dia do mês."""
    proximo = date(ano + mes // 12, mes % 12 + 1, 1)
    return proximo - timedelta(days=1)


def posicao_do_titulo(titulo: str) -> date | None:
    """data de "posição em DD.MM.AAAA" no título, ou None."""
    m = re.search(r"posi\w+ em (\d{2})\.(\d{2})\.(\d{4})", titulo)
    if not m:
        return None
    dia, mes, ano = map(int, m.groups())
    return date(ano, mes, dia)


def posicao_do_arquivo(nome_arquivo: str) -> date | None:
    """fim do mês indicado no nome do arquivo (AAAA-MM, AAAA-MM-DD ou MesAA), ou None."""
    m = re.search(r"(\d{4})-(\d{2})(?:-(\d{2}))?", nome_arquivo)
    if m:
        ano, mes = int(m.group(1)), int(m.group(2))
        return _fim_do_mes(ano, mes)
    m = re.search(r"\b([A-Za-z]{3})(\d{2})\b", nome_arquivo)
    if m and m.group(1).lower() in MESES:
        return _fim_do_mes(2000 + int(m.group(2)), MESES[m.group(1).lower()])
    return None


def buscar_item_central(nome: str) -> dict:
    """lê a central e devolve título, posição e url do csv do item `nome`."""
    resposta = httpx.get(
        constants.CENTRAL_DOWNLOADS_URL.value,
        headers=constants.HEADERS.value,
        timeout=constants.TIMEOUT.value,
        follow_redirects=True,
        proxy=brasil_proxy_url(),
    )
    resposta.raise_for_status()
    itens = ler_itens_central(resposta.text)
    if nome not in itens:
        raise ValueError(
            f"item {nome!r} não encontrado na central de downloads"
        )
    item = itens[nome]

    pelo_titulo = posicao_do_titulo(item.get("titulo", ""))
    pelo_arquivo = posicao_do_arquivo(item.get("nomeArquivo2", ""))
    candidatas = [d for d in (pelo_titulo, pelo_arquivo) if d]
    if not candidatas:
        raise ValueError(
            f"posição ilegível para {nome!r}: titulo={item.get('titulo')!r}, "
            f"arquivo={item.get('nomeArquivo2')!r}"
        )
    if (
        pelo_titulo
        and pelo_arquivo
        and (
            (pelo_titulo.year, pelo_titulo.month)
            != (pelo_arquivo.year, pelo_arquivo.month)
        )
    ):
        # o título já ficou um mês atrás do arquivo (ver README)
        log(
            f"posição diverge em {nome}: título {pelo_titulo}, "
            f"arquivo {pelo_arquivo}; usando a mais recente"
        )
    posicao = max(candidatas)

    url_csv = (
        constants.BNDES_URL.value + item["urlArquivo2"].split("&CACHEID")[0]
    )
    log(f"{nome}: posição {posicao}, csv {url_csv}")
    return {"titulo": item["titulo"], "posicao": posicao, "url_csv": url_csv}


def baixar_csv(url: str, destino: Path) -> Path:
    """baixa o csv da central para `destino`; só substitui o arquivo ao fim do download."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    parcial = destino.with_name(destino.name + ".part")
    with httpx.stream(
        "GET",
        url,
        headers=constants.HEADERS.value,
        timeout=constants.TIMEOUT.value,
        follow_redirects=True,
        proxy=brasil_proxy_url(),
    ) as resposta:
        resposta.raise_for_status()
        with open(parcial, "wb") as f:
            for bloco in resposta.iter_bytes():
                f.write(bloco)
    parcial.replace(destino)
    log(f"baixado {url} -> {destino} ({destino.stat().st_size} bytes)")
    return destino


def layout_do_fundo(table_id: str) -> dict:
    """formato do csv de `table_id`: o padrão com as diferenças do fundo."""
    return {
        **constants.LAYOUT_PADRAO.value,
        **constants.LAYOUT.value.get(table_id, {}),
    }


def ler_csv(csv_path: Path, encoding: str) -> pd.DataFrame:
    """lê o csv bruto como texto, sem converter vazios."""
    if encoding == "latin-1+cp850":
        # peac 2020: cabeçalho em latin-1 e dados em cp850
        cabecalho, _, corpo = csv_path.read_bytes().partition(b"\n")
        arquivo = io.StringIO(
            cabecalho.decode("latin-1") + "\n" + corpo.decode("cp850")
        )
        return pd.read_csv(arquivo, sep=";", dtype=str, keep_default_na=False)
    return pd.read_csv(
        csv_path, sep=";", encoding=encoding, dtype=str, keep_default_na=False
    )


def transformar(df: pd.DataFrame, layout: dict) -> pd.DataFrame:
    """leva ao cabeçalho padrão, padroniza valores e deriva `ano`; não remove linhas."""
    df = df.rename(columns=layout["colunas"])
    esperadas = set(constants.COLUNAS_FONTE.value) - set(
        layout["colunas_ausentes"]
    )
    faltando = esperadas - set(df.columns)
    if faltando:
        raise ValueError(f"colunas ausentes na fonte: {sorted(faltando)}")

    df = df.rename(columns=constants.RENAME.value)
    # quebra de linha, nbsp, espaço duplo e &amp; (ver README)
    for coluna in df.columns:
        df[coluna] = (
            df[coluna]
            .str.replace(r"\s+", " ", regex=True)
            .str.replace("&AMP;", "&", case=False, regex=False)
            .str.strip()
            .replace("", pd.NA)
        )
    df["porte_cliente"] = df["porte_cliente"].replace(constants.PORTE.value)

    # sem separador de milhar; decimal com vírgula, exceto cs-rs (ponto)
    numero = rf"\d+(?:{re.escape(layout['decimal'])}\d+)?"
    for coluna in constants.COLUNAS_VALOR.value:
        if coluna not in df.columns:
            continue
        fora = df[coluna].notna() & ~df[coluna].str.fullmatch(numero, na=True)
        if fora.any():
            raise ValueError(
                f"{coluna}: {fora.sum()} valores fora do formato, "
                f"ex.: {df.loc[fora, coluna].head(3).tolist()}"
            )
        df[coluna] = df[coluna].str.replace(
            layout["decimal"], ".", regex=False
        )

    datas = pd.to_datetime(
        df["data_solicitacao_outorga"],
        format=layout["formato_data"],
        errors="coerce",
    )
    if datas.isna().any():
        raise ValueError(
            f"data_solicitacao_outorga: {datas.isna().sum()} datas inválidas"
        )
    df["data_solicitacao_outorga"] = datas.dt.strftime("%Y-%m-%d")
    df["ano"] = datas.dt.year.astype(str)
    return df


def limpar(csv_path: Path, pasta_saida: Path, table_id: str) -> Path:
    """lê o csv bruto e grava parquet all-string em pasta_saida/ano=<ano>/."""
    shutil.rmtree(pasta_saida, ignore_errors=True)
    layout = layout_do_fundo(table_id)
    df = ler_csv(csv_path, layout["encoding"])
    linhas_fonte = len(df)
    df = transformar(df, layout)

    # sem as colunas que o fundo não publica
    colunas = [c for c in constants.COLUNAS_STAGING.value if c in df.columns]
    schema = pa.schema([(c, pa.string()) for c in colunas])
    for ano, grupo in df.groupby("ano"):
        tabela = pa.Table.from_pandas(
            grupo[colunas], schema=schema, preserve_index=False
        )
        pasta = pasta_saida / f"ano={ano}"
        pasta.mkdir(parents=True, exist_ok=True)
        pq.write_table(tabela, pasta / "data.parquet", compression="snappy")

    check_rows_written(rows_read=linhas_fonte, output_dir=pasta_saida)
    log(f"limpeza concluída: {df['ano'].nunique()} partições -> {pasta_saida}")
    return pasta_saida
