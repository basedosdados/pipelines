"""
Utils for br_sp_saopaulo_geosampa_iptu.
"""

import codecs
import csv
import datetime
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from pipelines.datasets.br_sp_saopaulo_geosampa_iptu.constants import (
    COLUNAS_DECIMAIS,
    COLUNAS_INTEIRAS,
    COLUNAS_STAGING,
    HEADERS,
    MAX_ANOS_BUSCA,
    N_CAMPOS_FONTE,
    RENAME,
    URL_TEMPLATE,
)


def ano_existe(ano: int) -> bool:
    """indica se o zip do ano está publicado."""
    url = URL_TEMPLATE.format(ano=ano)
    with requests.get(url, headers=HEADERS, stream=True, timeout=60) as r:
        r.raise_for_status()
        # pedaços podem ter 1 byte
        inicio = b""
        for pedaco in r.iter_content(chunk_size=1024):
            inicio += pedaco
            if len(inicio) >= 2:
                break
    # ano inexistente também responde 200, com html
    return inicio.startswith(b"PK")


def ultimo_ano_publicado(hoje: datetime.date | None = None) -> int:
    """devolve o ano mais recente publicado."""
    hoje = hoje or datetime.date.today()
    primeiro = hoje.year + 1
    for ano in range(primeiro, primeiro - MAX_ANOS_BUSCA, -1):
        if ano_existe(ano):
            return ano
    raise ValueError(
        f"Nenhum IPTU publicado entre {primeiro - MAX_ANOS_BUSCA + 1} e "
        f"{primeiro}; a URL do GeoSampa pode ter mudado."
    )


def baixar_ano(ano: int, destino: Path) -> Path:
    """baixa o zip do ano, extrai o csv e devolve o caminho dele."""
    destino.mkdir(parents=True, exist_ok=True)
    zip_path = destino / f"IPTU_{ano}.zip"
    url = URL_TEMPLATE.format(ano=ano)
    with requests.get(url, headers=HEADERS, stream=True, timeout=600) as r:
        r.raise_for_status()
        with open(zip_path, "wb") as f:
            for pedaco in r.iter_content(chunk_size=1024 * 1024):
                f.write(pedaco)

    try:
        if not zipfile.is_zipfile(zip_path):
            raise ValueError(
                f"IPTU {ano} não é um ZIP; o ano pode não existir."
            )
        with zipfile.ZipFile(zip_path) as z:
            csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
            if len(csvs) != 1:
                raise ValueError(
                    f"esperado 1 CSV no ZIP de {ano}, achados: {csvs}"
                )
            csv_path = Path(z.extract(csvs[0], destino))
    finally:
        zip_path.unlink()
    return csv_path


def formatar_data(serie: pd.Series) -> pd.Series:
    """converte `dd/mm/aa` em `aaaa-mm-dd`; vazio continua vazio."""
    partes = serie.str.extract(r"^(\d{2})/(\d{2})/(\d{2})$")
    data = "20" + partes[2] + "-" + partes[1] + "-" + partes[0]
    return data.fillna("")


def limpar_iptu(df: pd.DataFrame) -> pd.DataFrame:
    """padroniza um csv do iptu (tudo string) para o formato do staging."""
    df = df.rename(columns=RENAME)
    df = df.apply(lambda col: col.str.strip())
    # remove aspas que envolvem o valor inteiro
    df = df.apply(
        lambda col: col.str.replace(
            r'^"(.*)"$', r"\1", regex=True
        ).str.replace('""', '"', regex=False)
    )
    # valor só com aspa vira vazio
    df = df.replace('"', "")

    for col in COLUNAS_DECIMAIS:
        df[col] = (
            df[col]
            .str.replace(",", ".", regex=False)
            .str.replace(r"^0+(?=\d)", "", regex=True)
        )

    for col in COLUNAS_INTEIRAS:
        df[col] = (
            df[col]
            .str.replace(r"[.,]0+$", "", regex=True)
            .str.replace(r"^0+(?=\d)", "", regex=True)
        )

    for col in ["cep", "numero_contribuinte"]:
        df[col] = df[col].str.replace("-", "", regex=False)

    df["data_cadastramento"] = formatar_data(df["data_cadastramento"])
    return df[["ano", *COLUNAS_STAGING]]


def detectar_encoding(csv_path: Path) -> str:
    """devolve `utf-8` ou `latin-1`."""
    decoder = codecs.getincrementaldecoder("utf-8")()
    try:
        with open(csv_path, "rb") as f:
            while bloco := f.read(16 * 1024 * 1024):
                decoder.decode(bloco)
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        return "latin-1"
    return "utf-8"


def _campos_ok(linha: bytes) -> bool | None:
    """true se a linha está ok; none se só tem campos vazios a mais."""
    campos = linha.rstrip(b"\r\n").split(b";")
    if len(campos) == N_CAMPOS_FONTE:
        return True
    if len(campos) > N_CAMPOS_FONTE and not any(campos[N_CAMPOS_FONTE:]):
        return None
    return False


def normalizar_linhas(csv_path: Path) -> int:
    """valida os campos de cada linha e devolve o total de linhas."""
    n, corrigir = 0, False
    with open(csv_path, "rb") as f:
        for i, linha in enumerate(f, 1):
            ok = _campos_ok(linha)
            if ok is False:
                raise ValueError(
                    f"{csv_path.name}, linha {i}: {linha.count(b';') + 1} "
                    f"campos, esperado {N_CAMPOS_FONTE}"
                )
            corrigir = corrigir or ok is None
            n += 1

    if corrigir:
        tmp = csv_path.with_suffix(".tmp")
        with open(csv_path, "rb") as f, open(tmp, "wb") as g:
            for linha in f:
                if _campos_ok(linha) is None:
                    campos = linha.rstrip(b"\r\n").split(b";")
                    linha = b";".join(campos[:N_CAMPOS_FONTE]) + b"\n"
                g.write(linha)
        tmp.replace(csv_path)
    return n - 1


def validar_pedaco(df: pd.DataFrame) -> None:
    """falha se algum contribuinte foge do formato."""
    invalidos = ~df["numero_contribuinte"].str.fullmatch(r"\d{11}")
    if invalidos.any():
        exemplos = df.loc[invalidos, "numero_contribuinte"].head(3).tolist()
        raise ValueError(f"numero_contribuinte fora do formato: {exemplos}")


def tratar_ano(csv_path: Path, output_dir: Path, ano: int) -> Path:
    """limpa o csv do ano em pedaços e grava em `ano=<ano>/data.parquet`."""
    destino = output_dir / f"ano={ano}"
    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / "data.parquet"
    saida.unlink(missing_ok=True)

    linhas_fonte = normalizar_linhas(csv_path)
    linhas_saida = 0
    schema = pa.schema([pa.field(c, pa.string()) for c in COLUNAS_STAGING])
    leitor = pd.read_csv(
        csv_path,
        sep=";",
        dtype=str,
        keep_default_na=False,
        # aspas não delimitam campos nesta fonte
        quoting=csv.QUOTE_NONE,
        encoding=detectar_encoding(csv_path),
        chunksize=200_000,
    )
    with pq.ParquetWriter(saida, schema, compression="snappy") as escritor:
        for pedaco in leitor:
            limpo = limpar_iptu(pedaco).drop(columns="ano")
            validar_pedaco(limpo)
            linhas_saida += len(limpo)
            limpo = limpo.replace("", None)  # vazio vira nulo
            escritor.write_table(
                pa.Table.from_pandas(
                    limpo, schema=schema, preserve_index=False
                )
            )

    # nenhuma linha perdida ou juntada
    if linhas_saida != linhas_fonte:
        saida.unlink()
        raise ValueError(
            f"IPTU {ano}: {linhas_fonte} linhas na fonte, {linhas_saida} na saída"
        )
    return output_dir
