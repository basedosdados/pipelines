"""
Download, limpeza e particionamento de br_ms_sim.

O módulo não importa Prefect. As funções são chamadas pelas tasks de
`tasks.py` e também podem ser executadas diretamente.
"""

import os
import shutil
import tempfile
import urllib.request
from pathlib import Path

import basedosdados as bd
import pandas as pd
from datasus_dbc import decompress as dbc2dbf
from dbfread import DBF

from pipelines.datasets.br_ms_sim.constants import constants

FINAL, PRELIM = "definitivo", "preliminar"


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
        Os anos extraídos dos nomes no padrão `DO<UF><ANO>.dbc`.
    """
    with urllib.request.urlopen(directory_url, timeout=120) as response:
        listing = response.read().decode("latin1")

    years = set()
    for line in listing.splitlines():
        name = line.split()[-1] if line.strip() else ""
        if not name.upper().startswith("DO") or not name.lower().endswith(
            ".dbc"
        ):
            continue
        stem = name[:-4]
        if len(stem) >= 8 and stem[-4:].isdigit():
            years.add(int(stem[-4:]))
    return years


def get_source_max_year() -> str:
    """Devolve o ano mais recente publicado na fonte.

    Considera os dois diretórios, já que o ano corrente costuma existir apenas
    no preliminar. O valor é a competência do dado, não a data da consulta.

    Returns:
        O ano mais recente, no formato `%Y`.

    Raises:
        RuntimeError: Se nenhum arquivo for encontrado nos dois diretórios.
    """
    years = list_ftp_years(constants.FTP_FINAL_DIR.value) | list_ftp_years(
        constants.FTP_PRELIM_DIR.value
    )
    if not years:
        raise RuntimeError(
            "nenhum arquivo DO*.dbc encontrado no FTP do DATASUS — a fonte "
            "mudou de layout ou está fora do ar"
        )
    return str(max(years))


def resolve_year_source(ano: int) -> str:
    """Diz de qual diretório o ano deve ser baixado.

    O definitivo tem precedência sobre o preliminar. Um ano fechado pelo DATASUS
    passa a existir nos dois diretórios, e reprocessá-lo substitui o dado
    preliminar pelo definitivo.

    Args:
        ano: Ano a resolver.

    Returns:
        `"definitivo"` ou `"preliminar"`.

    Raises:
        FileNotFoundError: Se o ano não existir em nenhum dos dois diretórios.
    """
    if ano in list_ftp_years(constants.FTP_FINAL_DIR.value):
        return FINAL
    if ano in list_ftp_years(constants.FTP_PRELIM_DIR.value):
        return PRELIM
    raise FileNotFoundError(
        f"ano {ano} não está no FTP do DATASUS, nem definitivo nem preliminar"
    )


def download_year(ano: int, source: str, input_dir: Path) -> Path:
    """Baixa os arquivos `.dbc` das 27 UFs do ano.

    UF ausente na fonte é registrada no log e ignorada; a carga prossegue com as
    demais.

    Args:
        ano: Ano a baixar.
        source: `"definitivo"` ou `"preliminar"`.
        input_dir: Diretório de destino.

    Returns:
        O diretório de destino.

    Raises:
        RuntimeError: Se nenhuma das 27 UFs for baixada.
    """
    template = (
        constants.FTP_FINAL.value
        if source == FINAL
        else constants.FTP_PRELIM.value
    )

    missing = []
    for sigla_uf in constants.UFS.value:
        url = template.format(sigla_uf=sigla_uf, ano=ano)
        destination = input_dir / f"DO{sigla_uf}{ano}.dbc"
        try:
            urllib.request.urlretrieve(url, filename=str(destination))
        except Exception as error:
            destination.unlink(missing_ok=True)
            missing.append(sigla_uf)
            print(f"  {sigla_uf}: ausente na fonte — {error}")

    if len(missing) == len(constants.UFS.value):
        raise RuntimeError(
            f"nenhuma UF baixada para {ano} ({source}) — não há o que carregar"
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
        # `load=True` guardaria os registros também na própria DBF, dobrando o
        # pico de memória nas UFs grandes.
        table = DBF(tmp_path, encoding=encoding, load=False)
        return pd.DataFrame(iter(table))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


def load_municipios() -> pd.DataFrame:
    """Lê o de-para de código de município do diretório da Base dos Dados.

    Returns:
        As colunas `id_municipio` e `id_municipio_6`, ambas como texto.
    """
    return bd.read_sql(
        "SELECT id_municipio, id_municipio_6 "
        "FROM `basedosdados-dev.br_bd_diretorios_brasil.municipio`",
        billing_project_id="basedosdados-dev",
        from_file=True,
    ).astype(str)


def convert_municipio_6_to_7(
    dataframe: pd.DataFrame,
    column_6: str,
    column_7: str,
    municipios: pd.DataFrame,
) -> pd.DataFrame:
    """Substitui o código de município de 6 dígitos pelo de 7 do IBGE.

    Args:
        dataframe: Dados a converter.
        column_6: Coluna de origem, com o código de 6 dígitos.
        column_7: Nome da coluna resultante.
        municipios: De-para devolvido por `load_municipios`.

    Returns:
        Os dados com a coluna convertida, ou inalterados se `column_6` não
        existir.
    """
    if column_6 not in dataframe.columns:
        return dataframe
    dataframe = dataframe.merge(
        municipios[["id_municipio_6", "id_municipio"]],
        how="left",
        left_on=column_6,
        right_on="id_municipio_6",
    )
    dataframe = dataframe.drop(columns=[column_6, "id_municipio_6"])
    return dataframe.rename(columns={"id_municipio": column_7})


def convert_municipio_resid_ocor(
    dataframe: pd.DataFrame, ano: int, municipios: pd.DataFrame
) -> pd.DataFrame:
    """Converte os municípios de residência e de ocorrência.

    Até 2005 a fonte grava esses campos com 7 dígitos, e as colunas são apenas
    renomeadas.

    Args:
        dataframe: Dados a converter.
        ano: Ano do arquivo.
        municipios: De-para devolvido por `load_municipios`.

    Returns:
        Os dados com as duas colunas convertidas.
    """
    if ano <= 2005:
        renames = {}
        if "id_municipio_6_resid" in dataframe.columns:
            renames["id_municipio_6_resid"] = "id_municipio_residencia"
        if "id_municipio_6_ocor" in dataframe.columns:
            renames["id_municipio_6_ocor"] = "id_municipio_ocorrencia"
        return dataframe.rename(columns=renames)

    dataframe = convert_municipio_6_to_7(
        dataframe,
        "id_municipio_6_resid",
        "id_municipio_residencia",
        municipios,
    )
    return convert_municipio_6_to_7(
        dataframe, "id_municipio_6_ocor", "id_municipio_ocorrencia", municipios
    )


def parse_date(value: object) -> str | None:
    """Converte uma data no formato `DDMMAAAA`.

    Args:
        value: Valor bruto do arquivo.

    Returns:
        A data no formato `AAAA-MM-DD`, ou None se o valor não for uma data.
    """
    if not value:
        return None
    text = str(value).strip()
    if len(text) < 8 or text == "00000000":
        return None
    return f"{text[4:8]}-{text[2:4]}-{text[0:2]}"


def parse_hora(value: object) -> str | None:
    """Converte um horário no formato `HHMM`.

    Args:
        value: Valor bruto do arquivo.

    Returns:
        O horário no formato `HH:MM:00`, ou None se o valor for mais curto que
        quatro dígitos.
    """
    if not value:
        return None
    text = str(value).strip()
    if len(text) < 4:
        return None
    text = text.zfill(4)
    return f"{text[0:2]}:{text[2:4]}:00"


def parse_idade(value: object) -> float | None:
    """Converte a idade codificada do SIM em anos.

    O primeiro dígito é a unidade — 1 minuto, 2 hora, 3 mês, 4 ano, 5 ano acima
    de 100 — e os demais são a quantidade.

    Args:
        value: Valor bruto do arquivo.

    Returns:
        A idade em anos, com duas casas decimais, ou None se a unidade for
        desconhecida ou a quantidade não for numérica.
    """
    if not value:
        return None
    text = str(value).strip()
    if len(text) < 2:
        return None
    unit = text[0]
    try:
        amount = int(text[1:])
    except ValueError:
        return None
    if unit == "1":
        idade = 0.0
    elif unit == "2":
        idade = amount / 365
    elif unit == "3":
        idade = amount / 12
    elif unit == "4":
        idade = float(amount)
    elif unit == "5":
        idade = float(100 + amount)
    else:
        return None
    return round(idade, 2)


def recode_columns(dataframe: pd.DataFrame) -> pd.DataFrame:
    """Nulifica os códigos de ausência e traduz os demais para rótulos.

    Args:
        dataframe: Dados a recodificar.

    Returns:
        Uma cópia dos dados com as colunas de `NULLIFY` e `RECODE` tratadas.
    """
    dataframe = dataframe.copy()

    for column, invalid in constants.NULLIFY.value.items():
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].replace(invalid, None)

    for column, mapping in constants.RECODE.value.items():
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].replace(mapping)

    # Código fora do dicionário da própria fonte atravessaria o replace intacto
    # e viraria rótulo inválido na tabela.
    for column in constants.RECODE_STRICT.value:
        if column not in dataframe.columns:
            continue
        valid = set(constants.RECODE.value[column].values())
        unknown = dataframe[column].notna() & ~dataframe[column].isin(valid)
        dataframe.loc[unknown, column] = None

    if "peso" in dataframe.columns:
        dataframe["peso"] = dataframe["peso"].replace(["0"], None)

    return dataframe


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
    filepath: Path,
    ano: int,
    sigla_uf: str,
    municipios: pd.DataFrame,
    is_prelim: bool,
) -> pd.DataFrame:
    """Lê o arquivo de uma UF e devolve os dados no schema da arquitetura.

    Args:
        filepath: Caminho do arquivo `.dbc`.
        ano: Ano do arquivo.
        sigla_uf: Sigla da unidade da federação.
        municipios: De-para devolvido por `load_municipios`.
        is_prelim: Se o arquivo vem do diretório preliminar, o que define o
            valor de `dado_preliminar`.

    Returns:
        Os dados renomeados, convertidos e recodificados.
    """
    dataframe = read_dbc(filepath)
    dataframe.columns = dataframe.columns.str.upper()
    dataframe = dataframe.astype(str).replace(
        {"None": None, "nan": None, "": None}
    )
    dataframe = dataframe.replace("NA", None)

    dataframe = dataframe.rename(columns=constants.RENAME.value)
    dataframe = dataframe.drop(columns=["ORIGEM", "UFINFORM"], errors="ignore")

    dataframe["ano"] = ano
    dataframe["sigla_uf"] = sigla_uf
    dataframe["dado_preliminar"] = "1" if is_prelim else "0"

    dataframe = convert_municipio_resid_ocor(dataframe, ano, municipios)
    dataframe = convert_municipio_6_to_7(
        dataframe,
        "id_municipio_6_svo_iml",
        "id_municipio_svo_iml",
        municipios,
    )
    dataframe = convert_municipio_6_to_7(
        dataframe,
        "id_municipio_6_naturalidade",
        "id_municipio_naturalidade",
        municipios,
    )

    for column in constants.DATE_COLUMNS.value:
        if column in dataframe.columns:
            dataframe[column] = dataframe[column].apply(parse_date)

    if "hora_obito" in dataframe.columns:
        dataframe["hora_obito"] = dataframe["hora_obito"].apply(parse_hora)

    if "idade_raw" in dataframe.columns:
        dataframe["idade"] = dataframe["idade_raw"].apply(parse_idade)
        dataframe = dataframe.drop(columns=["idade_raw"])

    dataframe = recode_columns(dataframe)
    return ensure_schema_columns(dataframe)


def clean_year(
    table_id: str,
    ano: int,
    source: str,
    input_dir: Path,
    output_dir: Path,
) -> Path:
    """Limpa os arquivos do ano e grava o particionado em CSV.

    Args:
        table_id: Slug da tabela, que define as colunas de partição.
        ano: Ano processado.
        source: `"definitivo"` ou `"preliminar"`.
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
    municipios = load_municipios()
    is_prelim = source == PRELIM
    total = 0

    for filepath in sorted(input_dir.glob(f"{file_prefix}*{ano}.dbc")):
        sigla_uf = filepath.stem[len(file_prefix) :][:2]
        dataframe = process_file(
            filepath, ano, sigla_uf, municipios, is_prelim
        )

        partition = output_dir / f"ano={ano}" / f"sigla_uf={sigla_uf}"
        partition.mkdir(parents=True, exist_ok=True)
        dataframe.drop(columns=partition_columns).to_csv(
            partition / f"{table_id}.csv", index=False
        )
        total += len(dataframe)

    if total == 0:
        raise RuntimeError(
            f"nenhum arquivo processado para {ano} — `input/` está vazio"
        )

    print(f"{ano} ({source}): {total:,} linhas")
    return output_dir


def download_table(table_id: str, ano: int, source: str) -> Path:
    """Prepara os diretórios e baixa o ano.

    Args:
        table_id: Slug da tabela.
        ano: Ano a baixar.
        source: `"definitivo"` ou `"preliminar"`.

    Returns:
        O diretório de entrada com os arquivos baixados.
    """
    input_dir, _ = build_paths(table_id, ano)
    return download_year(ano=ano, source=source, input_dir=input_dir)


def clean_table(table_id: str, ano: int, source: str) -> Path:
    """Limpa o ano já baixado.

    Args:
        table_id: Slug da tabela.
        ano: Ano a limpar.
        source: `"definitivo"` ou `"preliminar"`.

    Returns:
        O diretório particionado, no formato esperado por `upload_to_gcs`.
    """
    input_dir, output_dir = build_paths(table_id, ano)
    return clean_year(
        table_id=table_id,
        ano=ano,
        source=source,
        input_dir=input_dir,
        output_dir=output_dir,
    )
