"""
Funções de download e tratamento do br_inmet_bdmep.
"""

import re
import zipfile
from datetime import date, datetime
from pathlib import Path

import httpx
import polars as pl
from unidecode import unidecode

from pipelines.datasets.br_inmet_bdmep.constants import (
    ConstantsEstacao,
    ConstantsMicrodados,
)
from pipelines.utils.utils import log


def download_year_zip(year: int, input_dir: Path) -> int:
    """
    Baixa e descompacta o ZIP de dados históricos do INMET de um ano. Em
    janeiro, se o ZIP do ano ainda não existir na fonte, baixa o do ano
    anterior.

    Args:
        year (int): ano do ZIP a baixar.
        input_dir (Path): diretório onde o ZIP é salvo e descompactado.

    Returns:
        int: ano do ZIP baixado.

    Raises:
        FileNotFoundError: se a fonte não tiver o ZIP do ano e, em janeiro,
            nem o do ano anterior.
    """
    input_dir.mkdir(parents=True, exist_ok=True)

    candidate_years = [year]
    if date.today().month == 1:
        candidate_years.append(year - 1)

    for candidate_year in candidate_years:
        url = ConstantsMicrodados.DOWNLOAD_URL.value.format(
            year=candidate_year
        )
        zip_path = input_dir / f"{candidate_year}.zip"

        with httpx.stream(
            method="GET",
            url=url,
            headers=ConstantsMicrodados.HEADERS.value,
            timeout=60,
            follow_redirects=True,
        ) as response:
            if response.status_code == 404:
                log(f"{url} não encontrado")
                continue
            response.raise_for_status()

            log("Iniciando Download...")
            with open(zip_path, "wb") as file:
                for chunk in response.iter_bytes(chunk_size=1024 * 1024):
                    file.write(chunk)

        with zipfile.ZipFile(zip_path) as archive:
            archive.extractall(input_dir)

        log("Dados baixados.")
        return candidate_year

    raise FileNotFoundError(
        f"A fonte não tem o ZIP de nenhum destes anos: {candidate_years}"
    )


def list_station_files(input_dir: Path) -> list[Path]:
    """
    Lista os CSVs de estação descompactados do ZIP, na raiz do diretório ou
    em subpastas.

    Args:
        input_dir (Path): diretório onde o ZIP foi descompactado.

    Returns:
        list[Path]: caminhos dos CSVs, em ordem alfabética.

    Raises:
        FileNotFoundError: se o diretório não tiver nenhum CSV.
    """
    station_files = sorted(input_dir.rglob("*.CSV"))
    if not station_files:
        raise FileNotFoundError(f"Nenhum CSV de estação em {input_dir}")
    return station_files


def get_date_from_path(path: Path) -> date:
    """
    Extrai a data final do nome do CSV de uma estação
    (`..._01-01-2026_A_30-09-2026.CSV`).

    Args:
        path (Path): caminho do CSV.

    Returns:
        date: data final do período coberto pelo arquivo.

    Raises:
        ValueError: se o nome do arquivo não terminar em `DD-MM-AAAA.CSV`.
    """
    match = re.search(r"(\d{2}-\d{2}-\d{4})\.CSV$", path.name)
    if match is None:
        raise ValueError(f"Data não encontrada no nome de {path.name}")
    return datetime.strptime(match.group(1), "%d-%m-%Y").date()


def get_station_code(path: Path) -> str:
    """
    Extrai o código da estação do nome do CSV
    (`INMET_CO_DF_A001_BRASILIA_..._A_30-09-2026.CSV`).

    Args:
        path (Path): caminho do CSV.

    Returns:
        str: código da estação.
    """
    return path.name.split("_")[3]


def list_latest_station_files(input_dir: Path) -> list[Path]:
    """
    Lista, para cada estação, só o CSV de data final mais recente. O ZIP
    traz algumas estações em mais de um arquivo, e o mais novo revisa os
    valores dos anteriores.

    Args:
        input_dir (Path): diretório onde o ZIP foi descompactado.

    Returns:
        list[Path]: um CSV por estação, em ordem alfabética.
    """
    latest_files: dict[str, Path] = {}
    for path in list_station_files(input_dir):
        code = get_station_code(path)
        current = latest_files.get(code)
        if current is None or get_date_from_path(path) > get_date_from_path(
            current
        ):
            latest_files[code] = path
    return sorted(latest_files.values())


def get_max_date(input_dir: Path) -> date:
    """
    Devolve a data final mais recente entre os CSVs de estação.

    Args:
        input_dir (Path): diretório onde o ZIP foi descompactado.

    Returns:
        date: maior data final entre os nomes dos arquivos.
    """
    return max(
        get_date_from_path(path) for path in list_station_files(input_dir)
    )


def rename_columns(columns: list[str]) -> list[str]:
    """
    Converte os nomes de coluna do INMET nos nomes da tabela, aplicando em
    ordem os padrões de `RENAME_PATTERNS` aos nomes em minúsculas e sem
    acento.

    Args:
        columns (list[str]): nomes de coluna do CSV da estação.

    Returns:
        list[str]: nomes renomeados, na mesma ordem.
    """
    names = [unidecode(column.lower()) for column in columns]
    for pattern, new_name in ConstantsMicrodados.RENAME_PATTERNS.value:
        regex = re.compile(pattern)
        names = [new_name if regex.search(name) else name for name in names]
    return names


def parse_measure(column: str) -> pl.Expr:
    """
    Converte uma coluna de medida em número, trocando a vírgula decimal por
    ponto e o valor `-9999` por nulo.

    Args:
        column (str): nome da coluna, já renomeada.

    Returns:
        pl.Expr: expressão com o valor em `Float64`, com o mesmo nome.
    """
    value = pl.col(column).str.replace(",", ".", literal=True).cast(pl.Float64)
    return pl.when(value != -9999).then(value).alias(column)


def read_station_header(path: Path) -> dict[str, str]:
    """
    Lê as 8 linhas de cabeçalho do CSV de uma estação (`REGIAO:;NE`,
    `UF:;AL`, ...), na ordem de `HEADER_FIELDS`.

    Args:
        path (Path): caminho do CSV.

    Returns:
        dict[str, str]: valor de cada campo de `HEADER_FIELDS`, como está
            no arquivo.
    """
    with open(path, encoding=ConstantsMicrodados.ENCODING.value) as file:
        return {
            field: next(file).split(";")[1].strip()
            for field in ConstantsMicrodados.HEADER_FIELDS.value
        }


def read_station_file(path: Path) -> pl.DataFrame:
    """
    Lê o CSV de uma estação no formato da tabela `microdados`. O código da
    estação vem da quarta linha do cabeçalho (`CODIGO (WMO)`).

    Args:
        path (Path): caminho do CSV.

    Returns:
        pl.DataFrame: dados da estação, com as colunas de `COLUMNS_ORDER`.
    """
    id_estacao = read_station_header(path)["id_estacao"]

    station = pl.read_csv(
        path,
        separator=";",
        skip_rows=8,
        encoding=ConstantsMicrodados.ENCODING.value,
        infer_schema_length=0,
    )
    station.columns = rename_columns(station.columns)

    expressions = [
        pl.col("data").str.replace_all("/", "-").str.to_date("%Y-%m-%d"),
        (pl.col("hora").str.slice(0, 2) + ":00:00").alias("hora"),
        pl.lit(id_estacao).alias("id_estacao"),
    ]
    expressions.extend(
        parse_measure(column)
        for column in ConstantsMicrodados.COLUMNS_ORDER.value[3:]
    )

    return station.select(expressions)


def build_year_csv(input_dir: Path, output_dir: Path, year: int) -> Path:
    """
    Grava os CSVs de estação de um ano num único CSV em `ano=<year>/`,
    lendo e escrevendo uma estação por vez. De cada estação entra só o
    arquivo mais recente (`list_latest_station_files`).

    Args:
        input_dir (Path): diretório onde o ZIP foi descompactado.
        output_dir (Path): raiz das partições.
        year (int): ano do ZIP.

    Returns:
        Path: caminho do CSV gravado.
    """
    partition_dir = output_dir / f"ano={year}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"microdados_{year}.csv"

    with open(file_path, "wb") as file:
        for index, station_file in enumerate(
            list_latest_station_files(input_dir)
        ):
            read_station_file(station_file).write_csv(
                file, include_header=index == 0
            )

    return file_path


def parse_foundation_date(value: str) -> str | None:
    """
    Converte a data de fundação do cabeçalho para `AAAA-MM-DD`. Com o ano
    em dois dígitos (`27/04/08`), a data que cairia no futuro volta um
    século.

    Args:
        value (str): data de fundação como está no cabeçalho.

    Returns:
        str | None: data em `AAAA-MM-DD`, ou nulo se o campo vier vazio.

    Raises:
        ValueError: se a data não estiver em nenhum formato de
            `FOUNDATION_DATE_FORMATS`.
    """
    if not value:
        return None

    for date_format in ConstantsEstacao.FOUNDATION_DATE_FORMATS.value:
        try:
            foundation_date = datetime.strptime(value, date_format).date()
        except ValueError:
            continue
        if foundation_date > date.today():
            foundation_date = foundation_date.replace(
                year=foundation_date.year - 100
            )
        return foundation_date.isoformat()

    raise ValueError(f"Data de fundação em formato desconhecido: {value!r}")


def read_station_info(path: Path) -> dict[str, str | None]:
    """
    Lê o cadastro de uma estação no cabeçalho do CSV, no formato da staging
    da tabela `estacao`.

    Args:
        path (Path): caminho do CSV.

    Returns:
        dict[str, str | None]: valores das colunas de
            `ConstantsEstacao.COLUMNS_ORDER`, como texto.
    """
    header = read_station_header(path)
    return {
        "id_estacao": header["id_estacao"],
        "estacao": header["estacao"],
        "sigla_uf": header["sigla_uf"],
        "latitude": header["latitude"].replace(",", "."),
        "longitude": header["longitude"].replace(",", "."),
        "altitude": header["altitude"].replace(",", "."),
        "data_fundacao": parse_foundation_date(header["data_fundacao"]),
    }


def build_station_csv(input_dir: Path, output_dir: Path, year: int) -> Path:
    """
    Grava o cadastro das estações de um ano em `ano=<year>/`, lido do
    cabeçalho de cada CSV. De cada estação entra só o arquivo mais recente
    (`list_latest_station_files`).

    Args:
        input_dir (Path): diretório onde o ZIP foi descompactado.
        output_dir (Path): raiz das partições.
        year (int): ano do ZIP.

    Returns:
        Path: caminho do CSV gravado.
    """
    partition_dir = output_dir / f"ano={year}"
    partition_dir.mkdir(parents=True, exist_ok=True)
    file_path = partition_dir / f"estacao_{year}.csv"

    stations = [
        read_station_info(station_file)
        for station_file in list_latest_station_files(input_dir)
    ]
    pl.from_dicts(
        stations,
        schema={
            column: pl.String
            for column in ConstantsEstacao.COLUMNS_ORDER.value
        },
    ).write_csv(file_path)

    return file_path
