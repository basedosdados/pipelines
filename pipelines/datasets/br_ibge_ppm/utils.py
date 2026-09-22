"""
Download, limpeza e particionamento de br_ibge_ppm.

O módulo não importa Prefect. As funções são chamadas pelas tasks de `tasks.py`
e também podem ser executadas diretamente, o que permite conferir a contagem de
linhas antes do upload.
"""

import json
import re
import shutil
from functools import reduce
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipelines.datasets.br_ibge_ppm.constants import constants

KEY_COLUMNS = ["ano", "sigla_uf", "id_municipio"]

# "São Paulo (SP)" é o formato atual do nome da localidade; "São Paulo - SP" é o
# que a API devolvia antes, e o que o código anterior esperava.
UF_PATTERN = re.compile(r"[(-]\s*([A-Z]{2})\)?$")


def build_session() -> requests.Session:
    """Sessão que repete quando a API do IBGE falha.

    Um pedido cobre os 5.570 municípios de uma vez, e o servidor responde 5xx
    com alguma frequência nesse volume.

    Returns:
        A sessão, com repetição e espera crescente entre tentativas.
    """
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        backoff_factor=3,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def build_paths(table_id: str) -> tuple[Path, Path]:
    """Cria os diretórios de trabalho da tabela.

    O `output/` é apagado a cada chamada, de modo que sobra de uma execução
    anterior não entre no upload seguinte.

    Args:
        table_id: Slug da tabela.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(constants.PATH.value) / table_id
    input_dir, output_dir = base / "input", base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def get_source_max_date(table_id: str) -> str:
    """Lê nos metadados do SIDRA até que ano a fonte publicou a tabela.

    O valor é o ano de referência da pesquisa, não a data da consulta, e é o que
    o flow compara com o intervalo que a tabela já cobre. Quando a tabela junta
    mais de um agregado, vale o menor dos anos: só é possível montar a linha
    quando os dois lados existem.

    Args:
        table_id: Slug da tabela.

    Returns:
        O ano mais recente publicado, no formato `%Y`.
    """
    session = build_session()
    agregados = {
        series["agregado"]
        for series in constants.TABLES.value[table_id]["series"]
    }

    anos = []
    for agregado in sorted(agregados):
        response = session.get(
            constants.METADATA_LINK.value.format(agregado=agregado),
            timeout=120,
        )
        response.raise_for_status()
        anos.append(int(response.json()["periodicidade"]["fim"]))

    return str(min(anos))


def resolve_years(
    table_id: str, backfill_years: list[str] | None, source_max_date: str
) -> list[str]:
    """Decide quais anos a execução vai carregar.

    Sem backfill é só o ano mais recente da fonte. Com backfill, são os anos
    pedidos, em ordem.

    Args:
        table_id: Slug da tabela.
        backfill_years: Anos a recarregar, no formato `%Y`, ou None.
        source_max_date: Ano mais recente publicado, no formato `%Y`.

    Returns:
        Os anos a carregar, em ordem crescente.

    Raises:
        ValueError: Se algum ano pedido estiver fora do que a fonte publica para
            a tabela.
    """
    if not backfill_years:
        return [source_max_date]

    first_year = constants.TABLES.value[table_id]["first_year"]
    anos = sorted(set(backfill_years))
    fora = [
        ano
        for ano in anos
        if not (first_year <= int(ano) <= int(source_max_date))
    ]
    if fora:
        raise ValueError(
            f"{table_id}: a fonte publica de {first_year} a {source_max_date}, "
            f"e estes anos estão fora: {fora}"
        )
    return anos


def series_requests(table_id: str) -> list[tuple[dict[str, Any], str | None]]:
    """Enumera as requisições que compõem a tabela.

    Uma requisição por par variável/categoria, que é o recorte que a API
    responde sem estourar: os 5.570 municípios de uma categoria de uma vez.

    Args:
        table_id: Slug da tabela.

    Yields:
        Cada especificação de série junto da categoria pedida, `None` quando o
        agregado não tem classificação.
    """
    pedidos = []
    for series in constants.TABLES.value[table_id]["series"]:
        for categoria in series.get("categorias", [None]):
            pedidos.append((series, categoria))
    return pedidos


def series_path(
    input_dir: Path, series: dict[str, Any], categoria: str | None
) -> Path:
    """Nome do arquivo bruto de uma requisição.

    O nome carrega agregado, variável e categoria porque a limpeza reabre cada
    arquivo pela especificação que o pediu, e não pelo que estiver na pasta.

    Args:
        input_dir: Diretório de entrada da tabela.
        series: Especificação da série.
        categoria: Categoria pedida, ou `None`.

    Returns:
        O caminho do JSON da requisição.
    """
    nome = f"{series['agregado']}_{series['variavel']}"
    if categoria:
        nome = f"{nome}_{categoria}"
    return input_dir / f"{nome}.json"


def download_table(table_id: str, ano: str) -> Path:
    """Baixa da API do IBGE as séries que compõem a tabela no ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar, no formato `%Y`.

    Returns:
        O diretório de entrada com os JSONs baixados.
    """
    input_dir, _ = build_paths(table_id)
    session = build_session()

    for antigo in input_dir.glob("*.json"):
        antigo.unlink()

    for series, categoria in series_requests(table_id):
        url = constants.SERIES_LINK.value.format(
            agregado=series["agregado"],
            ano=ano,
            variavel=series["variavel"],
        )
        if categoria:
            url += constants.CLASSIFICATION_PARAM.value.format(
                classificacao=series["classificacao"], categoria=categoria
            )

        response = session.get(url, timeout=30 * 60)
        response.raise_for_status()
        series_path(input_dir, series, categoria).write_text(
            json.dumps(response.json())
        )

    return input_dir


def parse_uf(nome: str) -> str:
    """Extrai a sigla da unidade da federação do nome do município.

    Args:
        nome: Nome da localidade como a API o devolve.

    Returns:
        A sigla da unidade da federação.

    Raises:
        ValueError: Se o nome não terminar com a sigla.
    """
    match = UF_PATTERN.search(nome.strip())
    if not match:
        raise ValueError(f"não achei a sigla da UF em {nome!r}")
    return match.group(1)


def build_row(
    serie: dict[str, Any],
    series: dict[str, Any],
    ano: str,
    label: str | None,
    unidade: str | None,
) -> dict[str, str | None]:
    """Monta a linha de um município a partir da série da API.

    Args:
        serie: Série de um município, como a API a devolve.
        series: Especificação da série.
        ano: Ano pedido, no formato `%Y`.
        label: Nome da categoria, quando o agregado tem classificação.
        unidade: Unidade da variável, quando a série carrega a coluna.

    Returns:
        A linha, com as colunas de chave e a coluna de valor da série.

    Raises:
        ValueError: Se a série trouxer ano diferente do pedido.
    """
    if set(serie["serie"]) != {ano}:
        raise ValueError(
            f"esperava só o ano {ano}, achei {sorted(serie['serie'])}"
        )

    row: dict[str, str | None] = {
        "ano": ano,
        "sigla_uf": parse_uf(serie["localidade"]["nome"]),
        "id_municipio": serie["localidade"]["id"],
        series["column"]: serie["serie"][ano],
    }
    if label is not None:
        row[series["label_column"]] = label
    if series.get("unit_column"):
        row[series["unit_column"]] = unidade
    return row


def parse_series(
    payload: list[dict[str, Any]],
    series: dict[str, Any],
    label_column: str | None,
    ano: str,
) -> pd.DataFrame:
    """Transforma a resposta da API nas linhas de uma série.

    Args:
        payload: Resposta da API, já desserializada.
        series: Especificação da série.
        label_column: Coluna que recebe o nome da categoria, ou `None` quando o
            agregado não tem classificação.
        ano: Ano pedido, no formato `%Y`.

    Returns:
        Uma linha por município, com as colunas de chave, a coluna de valor da
        série e, quando a série carrega a unidade, a coluna `unidade`.
    """
    series = {**series, "label_column": label_column}
    linhas = []

    for variavel in payload:
        unidade = variavel["unidade"]
        if unidade in constants.MONETARY_UNITS.value:
            unidade = None

        for resultado in variavel["resultados"]:
            label = next(
                (
                    nome
                    for classificacao in resultado["classificacoes"]
                    for nome in classificacao["categoria"].values()
                ),
                None,
            )
            linhas.extend(
                build_row(
                    serie,
                    series,
                    ano,
                    label if label_column else None,
                    unidade,
                )
                for serie in resultado["series"]
            )

    return pd.DataFrame(linhas, dtype=str)


def read_series(
    table_id: str, ano: str, input_dir: Path
) -> list[pd.DataFrame]:
    """Lê os JSONs baixados e devolve uma tabela por série.

    Args:
        table_id: Slug da tabela.
        ano: Ano pedido, no formato `%Y`.
        input_dir: Diretório de entrada da tabela.

    Returns:
        Uma tabela por série, cada uma com as colunas de chave e a sua coluna de
        valor.

    Raises:
        FileNotFoundError: Se algum JSON esperado não estiver em `input/`.
    """
    label_column = constants.TABLES.value[table_id]["label_column"]
    partes: dict[str, list[pd.DataFrame]] = {}

    for series, categoria in series_requests(table_id):
        path = series_path(input_dir, series, categoria)
        if not path.exists():
            raise FileNotFoundError(
                f"{path} não existe; rode o download antes"
            )
        parte = parse_series(
            json.loads(path.read_text()), series, label_column, ano
        )
        if not parte.empty:
            partes.setdefault(series["column"], []).append(parte)

    return [pd.concat(grupo, ignore_index=True) for grupo in partes.values()]


def clean_table(table_id: str, ano: str) -> Path:
    """Junta as séries do ano numa tabela e grava o particionado.

    Cada série vem de uma variável diferente do SIDRA e cobre o mesmo recorte de
    município e produto, então a junção é por chave e **externa**: município que
    aparece numa variável e falta na outra tem que sobreviver, com nulo do lado
    que falta.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar, no formato `%Y`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.

    Raises:
        ValueError: Se a fonte não devolver nenhuma linha para o ano.
    """
    input_dir, output_dir = build_paths(table_id)
    label_column = constants.TABLES.value[table_id]["label_column"]
    columns = constants.TABLES.value[table_id]["columns"]

    partes = read_series(table_id, ano, input_dir)
    if not partes:
        raise ValueError(f"{table_id}: a fonte não devolveu dado para {ano}")

    chaves = KEY_COLUMNS + ([label_column] if label_column else [])
    dataframe = reduce(
        lambda esquerda, direita: esquerda.merge(
            direita, on=chaves, how="outer"
        ),
        partes,
    )

    valores = [column for column in columns if column not in chaves]
    for column in valores:
        if column not in dataframe.columns:
            dataframe[column] = None
    # Dicionário, e não lista: `replace(lista, None)` faz o pandas preencher
    # para baixo em vez de anular.
    dataframe[valores] = dataframe[valores].replace(
        dict.fromkeys(constants.NULL_VALUES.value)
    )

    dataframe = dataframe[columns].sort_values(chaves)
    print(f"{table_id} {ano}: {len(dataframe)} linhas")

    write_partitions(
        dataframe,
        constants.TABLES.value[table_id]["partition_columns"],
        output_dir,
    )
    return output_dir


def write_partitions(
    dataframe: pd.DataFrame,
    partition_columns: list[str],
    output_dir: Path,
) -> None:
    """Grava em partições Hive com todas as colunas como texto.

    A staging é toda STRING por convenção da casa, e o `.sql` faz `safe_cast` de
    cada coluna.

    Args:
        dataframe: Dados a gravar.
        partition_columns: Colunas que compõem o caminho da partição.
        output_dir: Raiz do particionado.
    """
    for keys, group in dataframe.groupby(partition_columns, dropna=False):
        keys = keys if isinstance(keys, tuple) else (keys,)
        partition = output_dir.joinpath(
            *(
                f"{col}={key}"
                for col, key in zip(partition_columns, keys, strict=True)
            )
        )
        partition.mkdir(parents=True, exist_ok=True)
        # `astype(str)` escreveria NULL como a string "nan", que o `safe_cast`
        # não desfaz.
        group.drop(columns=partition_columns).map(
            lambda value: None if pd.isna(value) else str(value)
        ).to_parquet(partition / "data.parquet", compression="snappy")
