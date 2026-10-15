"""
Detecção de edições e download de br_inep_censo_educacao_superior.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente.

O acesso ao `download.inep.gov.br` reaproveita o `br_inep_enem`, que já trata a
cadeia de certificados incompleta do servidor, as conexões derrubadas e a
retomada de downloads interrompidos.
"""

import datetime
import re
from collections.abc import Iterable, Iterator
from email.utils import parsedate_to_datetime
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

from pipelines.datasets.br_inep_censo_educacao_superior.constants import (
    constants,
)
from pipelines.datasets.br_inep_enem.utils import (
    HEADERS,
    build_session,
    download_zip,
    extract_member,
)


def get_source_max_year() -> int:
    """Lê na página do Inep o ano mais recente publicado.

    O valor é a edição do Censo, não a data da consulta.

    Returns:
        O ano mais recente.

    Raises:
        ValueError: Se a página não listar nenhum zip de microdados.
    """
    response = build_session().get(
        constants.SOURCE_LINK.value,
        headers=HEADERS,
        verify=False,
        timeout=120,
    )
    response.raise_for_status()

    anos = re.findall(
        r"microdados_censo_da_educacao_superior_(\d{4})\.zip", response.text
    )
    if not anos:
        raise ValueError(
            "nenhum microdados_censo_da_educacao_superior_<ano>.zip em "
            f"{constants.SOURCE_LINK.value}"
        )
    return max(int(ano) for ano in anos)


def last_modified(ano: int) -> datetime.datetime:
    """Lê a data em que o Inep publicou o zip do ano pela última vez.

    Só o cabeçalho da resposta é lido; o corpo não é baixado. A requisição é um
    GET com `stream=True`, e não um HEAD, porque a repetição de `build_session`
    cobre apenas GET e o servidor do Inep derruba conexões com frequência.

    A data vem do cabeçalho `Last-Modified` do servidor. O texto "Atualizado em"
    da página não serve para isso: ele não acompanha as republicações.

    Args:
        ano: Edição do Censo.

    Returns:
        A data de modificação do zip, com fuso horário (UTC).

    Raises:
        requests.HTTPError: Se o zip do ano não existir.
        ValueError: Se o servidor não informar `Last-Modified`.
    """
    url = constants.DOWNLOAD_LINK.value.format(ano=ano)
    with build_session().get(
        url, headers=HEADERS, verify=False, stream=True, timeout=60
    ) as response:
        response.raise_for_status()
        cabecalho = response.headers.get("Last-Modified")

    if not cabecalho:
        raise ValueError(f"{url}: resposta sem Last-Modified")
    return parsedate_to_datetime(cabecalho)


def anos_publicados() -> list[int]:
    """Lista as edições que a pipeline acompanha, do primeiro ano da BD ao último publicado.

    Returns:
        Os anos, em ordem crescente.
    """
    return list(range(constants.FIRST_YEAR.value, get_source_max_year() + 1))


def anos_alterados(
    publicacoes: dict[int, datetime.datetime],
    ultima_atualizacao: datetime.date | None,
) -> list[int]:
    """Escolhe as edições publicadas pelo Inep depois da última atualização da tabela.

    Cobre os dois casos de uma vez: uma edição nova e a republicação de uma
    edição antiga têm `Last-Modified` posterior à última carga.

    Args:
        publicacoes: `Last-Modified` de cada ano, como devolvido por `last_modified`.
        ultima_atualizacao: `Table.Update.latest` da tabela na BD. `None` quando
            a tabela nunca foi atualizada: aí todos os anos entram.

    Returns:
        Os anos a recarregar, em ordem crescente.
    """
    return sorted(
        ano
        for ano, publicado_em in publicacoes.items()
        if ultima_atualizacao is None
        or publicado_em.date() > ultima_atualizacao
    )


def download_censo(ano: int, input_dir: Path) -> Path:
    """Baixa o zip do Censo da Educação Superior do ano.

    Se o zip já estiver em `input_dir`, o download é retomado de onde parou; se
    estiver completo, nada é baixado.

    Args:
        ano: Edição do Censo.
        input_dir: Diretório onde gravar o zip. Precisa existir.

    Returns:
        O caminho do zip.
    """
    url = constants.DOWNLOAD_LINK.value.format(ano=ano)
    zip_path = input_dir / url.split("/")[-1]
    download_zip(url, zip_path)
    return zip_path


def ler_arquitetura(table_id: str) -> pd.DataFrame:
    """Lê a arquitetura versionada da tabela.

    A arquitetura é o de-para entre o CSV do Inep (`original_name`) e a tabela da
    BD (`name`), na ordem das colunas da tabela.

    Args:
        table_id: `curso` ou `ies`.

    Returns:
        As colunas `name`, `bigquery_type` e `original_name`, uma linha por coluna.
    """
    caminho = constants.ARCHITECTURE_DIR.value / f"{table_id}.csv"
    return pd.read_csv(caminho, dtype=str)[
        ["name", "bigquery_type", "original_name"]
    ]


def extrair_csv(zip_path: Path, table_id: str) -> Path:
    """Extrai do zip o CSV da tabela, ao lado do zip.

    O membro é procurado por trecho do nome, porque o nome completo muda entre
    anos. A extração passa pelo `extract_member` do `br_inep_enem`, que também
    abre zips em deflate64.

    Args:
        zip_path: Zip do ano, baixado por `download_censo`.
        table_id: `curso` ou `ies`.

    Returns:
        O caminho do CSV extraído.

    Raises:
        ValueError: Se o zip não tiver exatamente um CSV da tabela.
    """
    trecho = constants.MEMBERS.value[table_id]
    with ZipFile(zip_path) as arquivo:
        membros = [
            nome
            for nome in arquivo.namelist()
            if trecho in nome.upper() and nome.upper().endswith(".CSV")
        ]
        if len(membros) != 1:
            raise ValueError(
                f"{zip_path.name}: esperava um CSV com {trecho!r}, achei {membros}"
            )
        destino = zip_path.parent / Path(membros[0]).name
        extract_member(arquivo, zip_path, membros[0], destino)
    return destino


def nome_atual(coluna: str, table_id: str) -> str:
    """Traduz um nome antigo de coluna do Inep para o nome atual.

    Args:
        coluna: Nome no cabeçalho do CSV.
        table_id: `curso` ou `ies`.

    Returns:
        O nome atual; o próprio nome, se não for antigo.
    """
    for antigo, atual in constants.OLD_PREFIXES.value[table_id].items():
        if coluna.startswith(antigo):
            return atual + coluna.removeprefix(antigo)
    return coluna


def verificar_colunas(
    csv_path: Path, arquitetura: pd.DataFrame, table_id: str
) -> None:
    """Compara o cabeçalho do CSV do Inep com a arquitetura e avisa no log.

    Não interrompe a execução. Avisa as colunas que o Inep publicou e a
    arquitetura não lista (serão descartadas) e as que a arquitetura lista e o
    Inep não publicou (ficarão nulas). É por esses avisos que uma mudança do Inep
    aparece na primeira execução depois dela.

    Args:
        csv_path: CSV extraído por `extrair_csv`.
        arquitetura: Saída de `ler_arquitetura`.
        table_id: `curso` ou `ies`.
    """
    with open(csv_path, encoding=constants.ENCODING.value) as arquivo:
        cabecalho = [
            nome_atual(coluna, table_id)
            for coluna in arquivo.readline()
            .rstrip("\r\n")
            .split(constants.SEPARATOR.value)
        ]

    originais = set(arquitetura.original_name)
    ignoradas = sorted(set(cabecalho) - originais)
    ausentes = sorted(originais - set(cabecalho))
    if ignoradas:
        print(
            f"AVISO {csv_path.name}: {len(ignoradas)} colunas da fonte fora da "
            f"arquitetura, descartadas: {ignoradas}"
        )
    if ausentes:
        print(
            f"AVISO {csv_path.name}: {len(ausentes)} colunas da arquitetura "
            f"ausentes na fonte, ficarão nulas: {ausentes}"
        )


def ler_microdados(
    csv_path: Path, arquitetura: pd.DataFrame, table_id: str
) -> Iterator[pd.DataFrame]:
    """Lê do CSV do Inep só as colunas da arquitetura, em blocos, como texto.

    Ler como texto é o que preserva os valores: lidos como número, um CEP perde o
    zero à esquerda e ganha `.0`, e o código de área `04` vira `4` (foi o que
    aconteceu nas cargas manuais de 2023 e 2024). Campos vazios ficam como texto
    vazio.

    Args:
        csv_path: CSV extraído por `extrair_csv`.
        arquitetura: Saída de `ler_arquitetura`.
        table_id: `curso` ou `ies`.

    Yields:
        Blocos de até `CHUNK_SIZE` linhas, com os nomes atuais do Inep.
    """
    originais = set(arquitetura.original_name)
    blocos = pd.read_csv(
        csv_path,
        sep=constants.SEPARATOR.value,
        encoding=constants.ENCODING.value,
        usecols=lambda coluna: nome_atual(coluna, table_id) in originais,
        dtype=str,
        keep_default_na=False,
        chunksize=constants.CHUNK_SIZE.value,
    )
    for bloco in blocos:
        yield bloco.rename(columns=lambda coluna: nome_atual(coluna, table_id))


def aplicar_arquitetura(
    df: pd.DataFrame, arquitetura: pd.DataFrame, ano: int
) -> pd.DataFrame:
    """Renomeia, ordena e completa as colunas conforme a arquitetura.

    A saída tem exatamente as colunas da arquitetura, na ordem dela, exceto `ano`,
    que vai no caminho da partição (`ano=<ano>/`).

    Vazio tem dois significados, tratados de forma diferente:

    - **Campo vazio numa coluna de quantidade que existe na fonte vira `0`.** Até
      2019 o Inep deixa vazio o que não se aplica (vagas e inscritos nas linhas de
      EaD por município); de 2020 em diante, grava `0`. As cargas anteriores já
      gravavam `0` em todos os anos, e é isso que se mantém, para a série não
      mudar de formato em 2020.
    - **Coluna ausente na fonte fica vazia (nula).** O dado não existe naquele
      ano, como as colunas criadas pelo Inep em 2023 e 2024.

    Args:
        df: Saída de `ler_microdados`.
        arquitetura: Saída de `ler_arquitetura`.
        ano: Edição do Censo; conferida contra a coluna de ano do CSV.

    Returns:
        O DataFrame com os nomes da BD.

    Raises:
        ValueError: Se alguma linha do CSV for de outro ano.
    """
    de_para = dict(
        zip(arquitetura.original_name, arquitetura.name, strict=True)
    )
    quantidades = [
        original
        for original, tipo in zip(
            arquitetura.original_name, arquitetura.bigquery_type, strict=True
        )
        if tipo == "int64" and original in df.columns
    ]
    df = df.copy()
    df[quantidades] = df[quantidades].replace("", "0")

    saida = (
        df.reindex(columns=list(de_para)).rename(columns=de_para).fillna("")
    )

    anos = set(saida["ano"].unique())
    if anos != {str(ano)}:
        raise ValueError(f"CSV de {ano} traz linhas dos anos {sorted(anos)}")
    return saida.drop(columns="ano")


def ajustar_valores(
    df: pd.DataFrame, arquitetura: pd.DataFrame
) -> pd.DataFrame:
    """Converte para o formato do staging os valores que o Inep codifica diferente.

    - Booleanos (`bigquery_type` `boolean`): `1`/`0` viram `True`/`False`, que o
      `safe_cast(... as boolean)` do dbt entende.
    - `id_municipio`: código IBGE com 7 dígitos.
    - `id_curso_cine`: o Inep grava o código entre aspas (`"0115L07"`), de
      propósito, para planilhas não o converterem em número. As aspas saem.

    Args:
        df: Saída de `aplicar_arquitetura`.
        arquitetura: Saída de `ler_arquitetura`.

    Returns:
        O DataFrame ajustado.
    """
    df = df.copy()

    booleanos = arquitetura.loc[arquitetura.bigquery_type == "boolean", "name"]
    for coluna in booleanos:
        df[coluna] = df[coluna].replace({"1": "True", "0": "False"})

    preenchido = df["id_municipio"] != ""
    df.loc[preenchido, "id_municipio"] = df.loc[
        preenchido, "id_municipio"
    ].str.zfill(7)

    if "id_curso_cine" in df.columns:
        df["id_curso_cine"] = df["id_curso_cine"].str.strip('"')

    return df


def gravar_staging(
    blocos: Iterable[pd.DataFrame], ano: int, output_dir: Path
) -> Path:
    """Grava o CSV de staging do ano na partição Hive, bloco a bloco.

    O caminho `ano=<ano>/data.csv` é o mesmo dos arquivos já carregados: o upload
    em `append` substitui o arquivo do ano sem tocar nos outros.

    Args:
        blocos: Blocos já tratados por `ajustar_valores`.
        ano: Edição do Censo.
        output_dir: Raiz da tabela (o diretório que vai para o upload).

    Returns:
        O caminho do CSV gravado.
    """
    destino = output_dir / f"ano={ano}" / "data.csv"
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.unlink(missing_ok=True)
    for i, bloco in enumerate(blocos):
        bloco.to_csv(
            destino, index=False, encoding="utf-8", mode="a", header=i == 0
        )
    return destino


def tratar_tabela(
    table_id: str, ano: int, zip_path: Path, output_dir: Path
) -> Path:
    """Transforma o CSV do Inep de um ano no CSV de staging da tabela.

    Args:
        table_id: `curso` ou `ies`.
        ano: Edição do Censo.
        zip_path: Zip do ano, baixado por `download_censo`.
        output_dir: Raiz da tabela; o CSV vai em `ano=<ano>/data.csv`.

    Returns:
        O caminho do CSV de staging.
    """
    arquitetura = ler_arquitetura(table_id)
    csv_path = extrair_csv(zip_path, table_id)
    try:
        verificar_colunas(csv_path, arquitetura, table_id)
        blocos = (
            ajustar_valores(
                aplicar_arquitetura(bloco, arquitetura, ano), arquitetura
            )
            for bloco in ler_microdados(csv_path, arquitetura, table_id)
        )
        return gravar_staging(blocos, ano, output_dir)
    finally:
        csv_path.unlink()
