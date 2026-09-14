"""
Download, limpeza e particionamento de br_ms_sinasc.

O módulo não importa Prefect. As funções são chamadas pelas tasks de
`tasks.py` e também podem ser executadas diretamente.
"""

import os
import shutil
import tempfile
import urllib.request
from pathlib import Path
from urllib.error import URLError

import basedosdados as bd
import pandas as pd
from datasus_dbc import decompress as dbc2dbf
from dbfread import DBF

from pipelines.datasets.br_ms_sinasc.constants import constants


def build_paths(table_id: str, ano: int) -> tuple[Path, Path]:
    """Cria os diretórios de trabalho do ano e devolve os dois caminhos.

    O `output/` é apagado a cada chamada; o `input/` é preservado. O ano compõe
    o caminho, de modo que execuções de anos diferentes não compartilham
    diretório.

    Args:
        table_id: Slug da tabela.
        ano: Ano da carga.

    Returns:
        Os caminhos de `input/` e de `output/`, nessa ordem.
    """
    base = Path(constants.PATH.value) / table_id / str(ano)
    input_dir, output_dir = base / "input", base / "output"
    shutil.rmtree(output_dir, ignore_errors=True)
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return input_dir, output_dir


def list_ftp_years(directory_url: str) -> set[int]:
    """Lê a listagem de um diretório do FTP e devolve os anos com arquivo.

    Args:
        directory_url: URL do diretório, terminada em barra.

    Returns:
        Os anos extraídos dos nomes no padrão `DN<UF><ANO>.dbc`.
    """
    with urllib.request.urlopen(directory_url, timeout=120) as response:
        listing = response.read().decode("latin1")

    years = set()

    for line in listing.splitlines():
        name = line.split()[-1] if line.strip() else ""
        if not name.upper().startswith("DN") or not name.lower().endswith(
            ".dbc"
        ):
            continue
        stem = name[:-4]
        if len(stem) >= 8 and stem[-4:].isdigit():
            years.add(int(stem[-4:]))
    return years


def get_source_max_year() -> str:
    """Devolve o ano mais recente publicado na fonte.

    O valor é a competência do dado, não a data da consulta.

    Returns:
        O ano mais recente, no formato `%Y`.

    Raises:
        RuntimeError: Se nenhum arquivo for encontrado no diretório.
    """
    years = list_ftp_years(constants.FTP_DIR.value)
    if not years:
        raise RuntimeError(
            "nenhum arquivo DN*.dbc encontrado no FTP do DATASUS — a fonte "
            "mudou de layout ou está fora do ar"
        )
    return str(max(years))


def download_year(ano: int, input_dir: Path) -> Path:
    """Baixa os arquivos `.dbc` das 27 UFs do ano.

    UF ausente na fonte é registrada no log e ignorada; a carga prossegue com as
    demais. Falha de rede propaga, para a task repetir em vez de gravar um ano
    incompleto.

    Args:
        ano: Ano a baixar.
        input_dir: Diretório de destino.

    Returns:
        O diretório de destino.

    Raises:
        URLError: Se a fonte responder algo que não seja 550.
        RuntimeError: Se nenhuma das 27 UFs for baixada.
    """
    missing = []
    for sigla_uf in constants.UFS.value:
        url = constants.FTP.value.format(sigla_uf=sigla_uf, ano=ano)
        destination = input_dir / f"DN{sigla_uf}{ano}.dbc"
        try:
            with (
                urllib.request.urlopen(url, timeout=300) as response,
                open(destination, "wb") as file,
            ):
                shutil.copyfileobj(response, file)
        except URLError as error:
            destination.unlink(missing_ok=True)
            # 550 é arquivo inexistente no FTP. Qualquer outra falha é de rede:
            # repetir a task inteira é melhor que gravar um ano incompleto.
            if "550" not in str(error):
                raise
            missing.append(sigla_uf)
            print(f"  {sigla_uf}: ausente na fonte — {error}")

    if len(missing) == len(constants.UFS.value):
        raise RuntimeError(
            f"nenhuma UF baixada para {ano} — não há o que carregar"
        )
    if missing:
        print(f"UFs ausentes em {ano}: {', '.join(missing)}")

    return input_dir


def read_dbc(filepath: Path, encoding: str = "iso-8859-1") -> pd.DataFrame:
    """Descompacta um arquivo `.dbc` e lê o `.dbf` resultante.

    Args:
        filepath: Caminho do arquivo `.dbc`.
        encoding: Codificação do `.dbf`.

    Returns:
        O conteúdo do arquivo.
    """
    file_descriptor, tmp_path = tempfile.mkstemp(
        suffix=".dbf", dir=tempfile.gettempdir()
    )
    os.close(file_descriptor)
    try:
        dbc2dbf(str(filepath), tmp_path)
        table = DBF(tmp_path, encoding=encoding, load=False)
        return pd.DataFrame(iter(table))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def load_municipios() -> dict[str, str]:
    """Lê o de-para de código de município do diretório da Base dos Dados.

    Returns:
        De-para do código de 6 dígitos para o de 7.
    """
    municipios = bd.read_sql(
        "SELECT id_municipio, id_municipio_6 "
        "FROM `basedosdados-dev.br_bd_diretorios_brasil.municipio`",
        billing_project_id="basedosdados-dev",
        from_file=True,
    ).astype(str)

    six_digit = municipios["id_municipio_6"]
    seven_digit = municipios["id_municipio"]

    return dict(zip(six_digit, seven_digit, strict=True))


def convert_municipio_6_to_7(
    dataframe: pd.DataFrame, column: str, municipios: dict[str, str]
) -> pd.DataFrame:
    """Converte para 7 dígitos os valores de município gravados com 6.

    A conversão é por valor: a fonte trocou de formato ao longo da série e um
    mesmo arquivo pode trazer as duas larguras. Valor de 6 dígitos sem
    correspondência no diretório vira nulo; valor de outra largura passa
    intacto, e a validação fica com o modelo.

    Args:
        dataframe: Dados a converter.
        column: Coluna de município.
        municipios: De-para devolvido por `load_municipios`.

    Returns:
        Os dados com a coluna convertida, ou inalterados se ela não existir.
    """
    if column not in dataframe.columns:
        return dataframe

    values = dataframe[column]
    is_six = values.notna() & values.str.len().eq(6)
    dataframe.loc[is_six, column] = values[is_six].map(municipios)
    return dataframe


def parse_date(value: object) -> str | None:
    """Converte uma data no formato `DDMMAAAA`.

    Ano acima de `MAX_YEAR` é erro de digitação, e não data — `25069202` é o ano
    9202. Vira nulo, senão o valor cai fora do diretório de tempo e derruba o
    teste de relacionamento da coluna. O limite é só superior: os anos de três
    dígitos em `data_nascimento_mae` (978 por 1978) e a sentinela 1899 de
    `data_recebimento` são o que a fonte publica, e ficam como estão.

    Args:
        value: Valor bruto do arquivo.

    Returns:
        A data no formato `AAAA-MM-DD`, ou None se o valor não for uma data.
    """
    if not value:
        return None
    text = str(value).strip()
    if len(text) < 8 or not text.isdigit() or text == "00000000":
        return None
    if int(text[4:8]) > constants.MAX_YEAR.value:
        return None
    return f"{text[4:8]}-{text[2:4]}-{text[0:2]}"


def parse_hora(value: object) -> str | None:
    """Converte um horário no formato `HHMM`.

    Args:
        value: Valor bruto do arquivo.

    Returns:
        O horário no formato `HH:MM:00`, ou None se o valor não for um horário
        válido.
    """
    if not value:
        return None

    text = str(value).strip()

    if not text.isdigit() or len(text) > 4:
        return None

    text = text.zfill(4)

    if int(text[0:2]) > 23 or int(text[2:4]) > 59:
        return None

    return f"{text[0:2]}:{text[2:4]}:00"


def ensure_schema_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Completa as colunas ausentes e aplica a ordem da arquitetura.

    Args:
        dataframe: Dados de um ano, que pode não trazer todas as colunas.

    Returns:
        Os dados com todas as colunas de `COLUMNS`, na ordem do modelo.
    """
    for column in constants.COLUMNS.value:
        if column not in dataframe.columns:
            dataframe[column] = None

    return dataframe[constants.COLUMNS.value]


def process_file(
    filepath: Path, ano: int, sigla_uf: str, municipios: dict[str, str]
) -> pd.DataFrame:
    """Lê o arquivo de uma UF e devolve os dados no schema da arquitetura.

    Args:
        filepath: Caminho do arquivo `.dbc`.
        ano: Ano do arquivo.
        sigla_uf: Sigla da unidade da federação.
        municipios: De-para devolvido por `load_municipios`.

    Returns:
        Os dados renomeados e convertidos. Coluna fora de `RENAME` é descartada
        por `ensure_schema_columns`, que devolve só as colunas de `COLUMNS`.
    """
    dataframe = read_dbc(filepath)

    dataframe.columns = dataframe.columns.str.upper()

    dataframe = dataframe.astype(str).replace(
        {"None": None, "nan": None, "": None, "NA": None}
    )

    dataframe = dataframe.rename(columns=constants.RENAME.value)
    # `DTRECORIG` e `DTRECORIGA` apontam para o mesmo destino: se algum ano
    # trouxer os dois, o rename duplicaria a coluna.

    dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

    dataframe["ano"] = ano
    dataframe["sigla_uf"] = sigla_uf

    for column in constants.MUNICIPIO_COLUMNS.value:
        dataframe = convert_municipio_6_to_7(dataframe, column, municipios)

    for column in constants.DATE_COLUMNS.value:
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].apply(parse_date)

    if "hora_nascimento" in dataframe.columns:
        dataframe["hora_nascimento"] = dataframe["hora_nascimento"].apply(
            parse_hora
        )

    return ensure_schema_columns(dataframe)


def clean_year(
    table_id: str, ano: int, input_dir: Path, output_dir: Path
) -> Path:
    """Limpa os arquivos do ano e grava o particionado em CSV.

    Args:
        table_id: Slug da tabela, que define as colunas de partição.
        ano: Ano processado.
        input_dir: Diretório com os arquivos `.dbc`.
        output_dir: Raiz do particionado.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.

    Raises:
        RuntimeError: Se nenhum arquivo do ano for encontrado em `input_dir`.
    """
    table = constants.TABLES.value[table_id]
    partition_columns = table["partition_columns"]
    file_prefix = table["file_prefix"]
    file_name = table["file_name"]
    municipios = load_municipios()
    total = 0

    for filepath in sorted(input_dir.glob(f"{file_prefix}*{ano}.dbc")):
        sigla_uf = filepath.stem[len(file_prefix) :][:2]
        dataframe = process_file(filepath, ano, sigla_uf, municipios)

        partition = output_dir / f"ano={ano}" / f"sigla_uf={sigla_uf}"
        partition.mkdir(parents=True, exist_ok=True)
        dataframe.drop(columns=partition_columns).to_csv(
            partition / file_name, index=False, na_rep=""
        )
        total += len(dataframe)

    if total == 0:
        raise RuntimeError(
            f"nenhum arquivo processado para {ano} — `input/` está vazio"
        )

    print(f"{ano}: {total:,} linhas")
    return output_dir


def download_table(table_id: str, ano: int) -> Path:
    """Prepara os diretórios e baixa o ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar.

    Returns:
        O diretório de entrada com os arquivos baixados.
    """
    input_dir, _ = build_paths(table_id, ano)
    return download_year(ano=ano, input_dir=input_dir)


def clean_table(table_id: str, ano: int) -> Path:
    """Limpa o ano já baixado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    input_dir, output_dir = build_paths(table_id, ano)

    return clean_year(
        table_id=table_id,
        ano=ano,
        input_dir=input_dir,
        output_dir=output_dir,
    )
