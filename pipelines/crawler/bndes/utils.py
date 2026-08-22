"""
Funcoes puras do crawler br_bndes_operacoes_contratadas.

Tres responsabilidades:
  - get_source_last_modified: le o sinal de atualizacao (CKAN last_modified) -> poll
  - download_csv: baixa o CSV consolidado (stream p/ disco + resume via Range)
  - clean: le o CSV bruto e grava Parquet particionado por ano (schema explicito)

get_source_last_modified/download_csv/clean sao genericas as duas tabelas de
constants.TABLES_CONFIGS ("operacoes_indiretas_automaticas" e
"operacoes_nao_automaticas"); a URL/schema de cada uma e resolvida pelo
chamador (url ou table_id), nao tem default fixo aqui.

Passo a passo de implementacao de cada funcao: ver task_davi/ROADMAP.md, secao 2.
"""

import shutil
from datetime import datetime
from pathlib import Path

import basedosdados as bd
import httpx
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pipelines.crawler.bndes.constants import (
    constants,
    constants_administracao_publica,
    constants_exportacao_bens,
)
from pipelines.utils.utils import log

CHUNKSIZE = 200_000


def get_source_last_modified(url: str) -> datetime:
    """
    Le o `last_modified` do recurso no CKAN (sinal de atualizacao do poll).

    Faz GET em `url` (RESOURCE_SHOW_URL da tabela) e parseia
    result["last_modified"] com constants.LAST_MODIFIED_FORMAT (comum as
    duas tabelas do dataset).

    Args:
        url (str): RESOURCE_SHOW_URL do recurso CKAN da tabela.

    Returns:
        datetime: Data/hora da ultima publicacao do CSV no portal.
    """
    response = httpx.get(url)
    response.raise_for_status()
    last_modified_date_iso = response.json()["result"]["last_modified"]
    last_modified_date = datetime.strptime(
        last_modified_date_iso, constants.LAST_MODIFIED_FORMAT.value
    )
    log(f"Fonte last_modified: {last_modified_date}")

    return last_modified_date


def parse_decimal_ptbr(s: pd.Series) -> pd.Series:
    """
    Normaliza o separador decimal pt-BR do CSV (virgula -> ponto).

    Vale p/ valor_operacao, valor_desembolsado e taxa_juros: no CSV do dados
    abertos nao ha separador de milhar, entao so troca ',' por '.'. Devolve
    STRING (nao numero): o staging do BD e todo string e quem tipa e o
    safe_cast do dbt; aqui so deixamos o decimal no formato que o safe_cast
    aceita (ex.: "8750,50" -> "8750.50").

    Args:
        s (pd.Series): Serie de strings (ex.: "8750,50").

    Returns:
        pd.Series: Serie de strings com '.' decimal (ex.: "8750.50").
    """

    return s.str.replace(",", ".", regex=False)


def download_csv(
    dest: Path,
    url: str,
    chunk_size: int = 1024 * 1024,
) -> Path:
    """
    Baixa o CSV consolidado streamando p/ disco, com resume via Range.

    Memoria constante (nao carrega o 1,2 GB na RAM). Se `dest` ja existe
    parcialmente, retoma de onde parou pedindo `Range: bytes=<n>-`. Resposta
    206 (Partial Content) -> append a partir da posicao atual; resposta 200 ->
    o servidor ignorou o Range, rebaixar do zero (modo "wb"). Ao validar o
    tamanho final, lembrar que em 206 o Content-Length do response e o do trecho
    pedido, nao o do arquivo inteiro.

    Args:
        dest (Path): Caminho de destino do arquivo .csv.
        url (str): URL de download (DOWNLOAD_URL da tabela).
        chunk_size (int): Tamanho do bloco de escrita (bytes).

    Returns:
        Path: O proprio `dest`, ja com o arquivo completo.
    """

    Path.mkdir(dest.parent, parents=True, exist_ok=True)
    bytes_downloaded = dest.stat().st_size if dest.is_file() else 0

    headers = (
        {"Range": f"bytes={bytes_downloaded}-"} if bytes_downloaded else {}
    )

    with httpx.stream(
        method="GET", url=url, headers=headers, timeout=20
    ) as response:
        if response.status_code == 200:
            mode = "wb"
        elif response.status_code == 206:
            mode = "ab"
        else:
            response.raise_for_status()

        origem = (
            f"resume de {bytes_downloaded} bytes"
            if mode == "ab"  # pyrefly: ignore [unbound-name]
            else "download novo"
        )
        log(f"Baixando CSV do BNDES para {dest} ({origem})")

        with open(dest, mode=mode) as fd:
            for chunk in response.iter_bytes(chunk_size=chunk_size):
                fd.write(chunk)

        if response.status_code == 200:
            file_length = int(response.headers["Content-Length"])

        if response.status_code == 206:
            file_length = int(
                # pyrefly: ignore [unnecessary-type-conversion]
                str(response.headers["Content-Range"]).split("/")[-1]
            )

        # pyrefly: ignore [unbound-name]
        if file_length != dest.stat().st_size:
            raise httpx.HTTPError(
                "Download não pode ser finalizado mesmo com várias tentativas."
            )

        log(f"Download concluído: {file_length} bytes em {dest}")

        return dest


def _extract_cnae_hierarchy(
    dataframe: pd.DataFrame,
    cnae_column: str,
    levels: list[str] | None = None,
    verify_diretorios: bool = True,
    df_diretorios: pd.DataFrame | None = None,
) -> pd.DataFrame:
    df_extracted = dataframe.copy()

    if levels is None:
        levels = ["secao", "divisao", "grupo", "classe", "subclasse"]
    cnae_series = df_extracted[cnae_column].astype(str).str.strip().str.upper()

    # 1. Extração dos códigos da hierarquia via Regex
    if "secao" in levels:
        df_extracted["secao_cnae"] = cnae_series.str.extract(
            r"(^[A-Z]){1}", expand=False
        )

    if "divisao" in levels:
        df_extracted["divisao_cnae"] = cnae_series.str.extract(
            r"^[A-Z]{1}(\d{2})", expand=False
        )

    if "grupo" in levels:
        df_extracted["grupo_cnae"] = cnae_series.str.extract(
            r"^[A-Z]{1}(\d{3})", expand=False
        )

    if "classe" in levels:
        df_extracted["classe_cnae"] = cnae_series.str.extract(
            r"^[A-Z]{1}(\d{5})", expand=False
        )

    if "subclasse" in levels:
        df_extracted["subclasse_cnae"] = cnae_series.str.extract(
            r"^[A-Z]{1}(\d{7})", expand=False
        )

    # 2. Limpeza de Níveis Inexistentes
    # Se o código do BNDES for de nível alto (Divisão/Grupo), anula os níveis inferiores falsos
    if "subclasse_cnae" in df_extracted.columns:
        df_extracted.loc[
            df_extracted["subclasse_cnae"].str.endswith("00000"),
            "subclasse_cnae",
        ] = None

    if "classe_cnae" in df_extracted.columns:
        df_extracted.loc[
            df_extracted["classe_cnae"].str.endswith("0000"), "classe_cnae"
        ] = None

    if "grupo_cnae" in df_extracted.columns:
        df_extracted.loc[
            df_extracted["grupo_cnae"].str.endswith("000"), "grupo_cnae"
        ] = None
    if "divisao_cnae" in df_extracted.columns:
        df_extracted.loc[
            df_extracted["grupo_cnae"].str.endswith("00"), "divisao_cnae"
        ] = None

    # Remove a coluna bruta original
    df_extracted = df_extracted.drop(columns=[cnae_column])

    # 3. Validação opcional cruzada com o diretório da Base dos Dados
    if verify_diretorios and df_diretorios is not None:
        for col in levels:
            col_name = f"{col}_cnae"
            if (
                col_name in df_extracted.columns
                and col in df_diretorios.columns
            ):
                valid_codes = set(
                    df_diretorios[col].dropna().astype(str).unique()
                )

                # Anula valores que não existem na dimensão oficial
                mask_invalid = df_extracted[col_name].notna() & ~df_extracted[
                    col_name
                ].isin(valid_codes)
                df_extracted.loc[mask_invalid, col_name] = None

    return df_extracted


def get_cnae_diretorios(levels: list[str] | None = None) -> pd.DataFrame:

    if levels is None:
        levels = ["secao", "divisao", "grupo", "classe", "subclasse"]
    df_diretorios = bd.read_sql(
        """
                SELECT DISTINCT {} 
                FROM `basedosdados.br_bd_diretorios_brasil.cnae_2`
                """.format(",".join(levels)),
        from_file=True,
    )
    return df_diretorios


def _transform_chunk(
    df: pd.DataFrame, table_id: str, df_diretorios: pd.DataFrame | None = None
) -> pd.DataFrame:
    """
    Aplica as transformacoes de limpeza a um chunk do CSV (tudo string na entrada).

    Portado de task_davi/clean.py, adaptado ao CSV do dados abertos.

    Args:
        df (pd.DataFrame): Chunk cru lido com dtype=str.
        table_id (str): Chave em constants.TABLES_CONFIGS (ex.:
            "operacoes_indiretas_automaticas") usada p/ resolver
            DROP_COLUMNS/RENAME/ORDER_COLUMNS da tabela.
        df_diretorios (pd.DataFrame | None): DataFrame com os diretórios CNAE.

    Returns:
        pd.DataFrame: Chunk limpo, com as colunas de ORDER_COLUMNS da tabela
            (inclui `ano` derivado de data_contratacao).
    """
    table_configs = constants.TABLES_CONFIGS.value[table_id]
    df_dropped_cols = df.drop(columns=table_configs["DROP_COLUMNS"])

    df_renamed_cols = df_dropped_cols.rename(columns=table_configs["RENAME"])

    df_striped = df_renamed_cols.apply(lambda col: col.str.strip())

    df_striped["fonte_recurso"] = df_striped["fonte_recurso"].replace(
        "-", pd.NA
    )

    df_striped[["valor_operacao", "valor_desembolsado", "taxa_juros"]] = (
        df_striped[
            ["valor_operacao", "valor_desembolsado", "taxa_juros"]
        ].apply(parse_decimal_ptbr)
    )

    date = pd.to_datetime(
        df_striped["data_contratacao"], format="%Y-%m-%d", errors="coerce"
    )

    df_striped["ano"] = date.dt.year.astype("Int64")

    n_sem_ano = int(df_striped["ano"].isna().sum())
    if n_sem_ano:
        log(
            f"AVISO: {n_sem_ano} linha(s) com data_contratacao inválida "
            "(ano nulo) — descartada(s) do particionamento."
        )
        df_striped = df_striped[df_striped["ano"].notna()]

    df_striped["id_municipio"] = df_striped["id_municipio"].str.replace(
        r"\.0$", "", regex=True
    )

    # valido = 7 digitos E nao e sentinela de "municipio desconhecido"
    # ("9999999", 122 linhas; o "0" ja cai no fullmatch por ter 1 digito).
    valido = df_striped["id_municipio"].str.fullmatch(r"\d{7}") & (
        df_striped["id_municipio"] != "9999999"
    )

    df_striped["id_municipio"] = df_striped["id_municipio"].where(
        valido, pd.NA
    )

    if table_id == "operacoes_nao_automaticas" and df_diretorios is not None:
        df_striped = _extract_cnae_hierarchy(
            df_striped,
            "codigo_cnae_2",
            df_diretorios=df_diretorios,
        )
    return df_striped[table_configs["ORDER_COLUMNS"]]


def clean(csv_path: Path, output_dir: Path, table_id: str) -> Path:
    """
    Le o CSV bruto em chunks e grava Parquet particionado por ano.

    Le com read_csv(sep=";", encoding="cp1252", dtype=str, chunksize=CHUNKSIZE),
    limpa cada chunk com _transform_chunk e vai anexando cada ano num
    pq.ParquetWriter proprio (um por particao, mantido aberto entre chunks) ->
    memoria constante. O SCHEMA da tabela (via table_id) garante que anos
    espalhados por varios chunks gravem tipos consistentes.

    Args:
        csv_path (Path): CSV bruto baixado.
        output_dir (Path): Raiz de saida; grava output_dir/ano=<ano>/data.parquet.
        table_id (str): Chave em constants.TABLES_CONFIGS que identifica a
            tabela (ORDER_COLUMNS/SCHEMA), repassada a _transform_chunk.

    Returns:
        Path: `output_dir` (raiz das particoes gravadas).
    """
    configs = constants.TABLES_CONFIGS.value[table_id]
    file_cols = [c for c in configs["ORDER_COLUMNS"] if c != "ano"]

    shutil.rmtree(output_dir, ignore_errors=True)

    writers = {}
    total_rows = 0

    if table_id == "operacoes_nao_automaticas":
        df_diretorios = get_cnae_diretorios()
    else:
        df_diretorios = None

    for i, chunk in enumerate(
        pd.read_csv(
            csv_path,
            sep=";",
            encoding="cp1252",
            dtype=str,
            chunksize=CHUNKSIZE,
        ),
        start=1,
    ):
        df = _transform_chunk(chunk, table_id, df_diretorios=df_diretorios)
        for year, group in df.groupby("ano"):
            # pyrefly: ignore [bad-argument-type]
            if int(year) not in writers:
                Path.mkdir(
                    # pyrefly: ignore [bad-argument-type]
                    output_dir / f"ano={int(year)}/",
                    parents=True,
                    exist_ok=True,
                )

                # pyrefly: ignore [bad-argument-type]
                writers[int(year)] = pq.ParquetWriter(
                    # pyrefly: ignore [bad-argument-type]
                    output_dir / f"ano={int(year)}/data.parquet",
                    configs["SCHEMA"],
                    compression="snappy",
                )

            table = pa.Table.from_pandas(
                group[file_cols],
                schema=configs["SCHEMA"],
                preserve_index=False,
            )

            # pyrefly: ignore [bad-argument-type]
            writers[int(year)].write_table(table)

        total_rows += len(df)
        log(f"Chunk {i}: {len(df)} linhas ({total_rows} acumuladas)")

    for write in writers.values():
        write.close()

    log(
        f"Limpeza concluída: {total_rows} linhas em {len(writers)} "
        f"partições (anos) -> {output_dir}"
    )
    return output_dir


def _transform_administracao_publica(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa o CSV de operacoes_administracao_publica (tudo string na entrada).

    Filtra so as operacoes no nivel CONTRATADA, renomeia as colunas para os nomes
    BD, normaliza a UF, deriva o ano da contratacao e devolve as colunas de
    constants_administracao_publica.ORDER_COLUMNS.

    Args:
        df (pd.DataFrame): CSV cru lido com dtype=str.

    Returns:
        pd.DataFrame: colunas de ORDER_COLUMNS (inclui `ano`), so linhas CONTRATADA.
    """
    df = df.rename(columns=constants_administracao_publica.RENAME.value)
    df = df[
        df["nivel_atual"]
        == constants_administracao_publica.NIVEL_ATUAL_KEEP.value
    ]

    df = df.apply(lambda c: c.str.strip())

    df["sigla_uf"] = df["sigla_uf"].apply(lambda x: pd.NA if x == "-" else x)

    df["ano"] = pd.to_datetime(
        df["data_nivel_atual"], format="%Y-%m-%d", errors="coerce"
    ).dt.year.astype("Int64")

    n_sem_ano = int(df["ano"].isna().sum())
    if n_sem_ano:
        log(
            f"AVISO: {n_sem_ano} linha(s) com data_nivel_atual inválida "
            "(ano nulo) — descartada(s) do particionamento."
        )
        df = df[df["ano"].notna()]

    df["nome_municipio"] = df["nome_municipio"].apply(
        lambda x: pd.NA if x in ("-", "DIVERSOS", "SEM MUNICIPIO") else x
    )

    return df[constants_administracao_publica.ORDER_COLUMNS.value]


def clean_administracao_publica(csv_path: Path, output_dir: Path) -> Path:
    """
    Le o CSV de operacoes_administracao_publica e grava Parquet particionado por ano.

    Arquivo pequeno (~4,9 mil linhas) -> le inteiro (sem chunk). Grava
    output_dir/ano=<ano>/data.parquet com constants_administracao_publica.SCHEMA
    (staging all-string).

    Args:
        csv_path (Path): CSV bruto baixado.
        output_dir (Path): raiz de saida; grava output_dir/ano=<ano>/data.parquet.

    Returns:
        Path: `output_dir`.
    """
    shutil.rmtree(output_dir, ignore_errors=True)
    df = pd.read_csv(csv_path, sep=";", encoding="cp1252", dtype=str)

    df = _transform_administracao_publica(df)

    columns = [
        c
        for c in constants_administracao_publica.ORDER_COLUMNS.value
        if c != "ano"
    ]

    for year, group in df.groupby("ano"):
        table = pa.Table.from_pandas(
            group[columns],
            schema=constants_administracao_publica.SCHEMA.value,
            preserve_index=False,
        )

        # pyrefly: ignore [bad-argument-type]
        table_path = output_dir / f"ano={int(year)}"

        table_path.mkdir(parents=True, exist_ok=True)

        pq.write_table(
            table, table_path / "data.parquet", compression="snappy"
        )

    log(
        f"Limpeza concluída: {len(df)} linhas em {df['ano'].nunique()} "
        f"partições (anos) -> {output_dir}"
    )
    return output_dir


def _split_setor_subsetor(setor_subsetor: pd.Series) -> pd.DataFrame:
    """
    Separa o campo composto `SETOR/SUBSETOR` no ULTIMO "/".

    O corte e no ultimo separador porque o proprio setor pode conter barra
    ("COMERCIO/SERVICOS/<subsetor>"). Valor sem barra e setor puro, com
    subsetor nulo.

    Args:
        setor_subsetor (pd.Series): coluna original concatenada.

    Returns:
        pd.DataFrame: colunas `setor_bndes` e `subsetor_bndes`.
    """
    partes = setor_subsetor.str.rsplit("/", n=1, expand=True)

    if partes.shape[1] == 1:
        partes[1] = pd.NA

    return partes.rename(columns={0: "setor_bndes", 1: "subsetor_bndes"})


def _normalize_garantia(garantia: pd.Series) -> pd.Series:
    """
    Unifica as grafias de `tipo_garantia`.

    Padroniza o separador de tipos combinados para " / ", protegendo antes os
    rotulos de ROTULOS_COMPOSTOS, que tem "/" no proprio nome.

    Args:
        garantia (pd.Series): coluna original.

    Returns:
        pd.Series: coluna com as grafias unificadas.
    """
    protegida = garantia.str.replace(
        "Seguro de Crédito", "Seguro de crédito", regex=False
    )

    for rotulo in constants_exportacao_bens.ROTULOS_COMPOSTOS.value:
        esquerda, direita = rotulo.split("/")
        protegida = protegida.str.replace(
            rf"{esquerda}\s*/\s*{direita}",
            rotulo.replace("/", "\x00"),
            regex=True,
        )

    padronizada = protegida.str.replace(r"\s*/\s*", " / ", regex=True)

    return padronizada.str.replace("\x00", "/", regex=False)


def _transform_exportacao_bens(df: pd.DataFrame) -> pd.DataFrame:
    """
    Limpa o CSV de operacoes_exportacao_bens (tudo string na entrada).

    Renomeia para os nomes BD, quebra setor_subsetor em setor_bndes/
    subsetor_bndes, normaliza tipo_garantia e sigla_moeda, anula a sentinela de
    pais e deriva o ano da contratacao.

    Args:
        df (pd.DataFrame): CSV cru lido com dtype=str.

    Returns:
        pd.DataFrame: colunas de ORDER_COLUMNS (inclui `ano`).
    """
    df = df.rename(columns=constants_exportacao_bens.RENAME.value)

    df = df.apply(lambda c: c.str.strip())

    df[["setor_bndes", "subsetor_bndes"]] = _split_setor_subsetor(
        df["setor_subsetor"]
    )

    df["tipo_garantia"] = _normalize_garantia(df["tipo_garantia"])

    df["sigla_moeda"] = df["sigla_moeda"].replace(
        constants_exportacao_bens.MOEDA.value
    )

    df["nome_pais_destino"] = df["nome_pais_destino"].replace(
        constants_exportacao_bens.PAIS_SENTINELA.value, pd.NA
    )

    df["ano"] = pd.to_datetime(
        df["data_contratacao"], format="%Y-%m-%d", errors="coerce"
    ).dt.year.astype("Int64")

    n_sem_ano = int(df["ano"].isna().sum())
    if n_sem_ano:
        log(
            f"AVISO: {n_sem_ano} linha(s) com data_contratacao inválida "
            "(ano nulo) — descartada(s) do particionamento."
        )
        df = df[df["ano"].notna()]

    return df[constants_exportacao_bens.ORDER_COLUMNS.value]


def clean_exportacao_bens(csv_path: Path, output_dir: Path) -> Path:
    """
    Le o CSV de operacoes_exportacao_bens e grava Parquet particionado por ano.

    Arquivo pequeno -> le inteiro (sem chunk). Grava
    output_dir/ano=<ano>/data.parquet com constants_exportacao_bens.SCHEMA
    (staging all-string).

    Args:
        csv_path (Path): CSV bruto baixado.
        output_dir (Path): raiz de saida; grava output_dir/ano=<ano>/data.parquet.

    Returns:
        Path: `output_dir`.
    """
    shutil.rmtree(output_dir, ignore_errors=True)
    df = pd.read_csv(csv_path, sep=";", encoding="cp1252", dtype=str)

    df = _transform_exportacao_bens(df)

    columns = [
        c for c in constants_exportacao_bens.ORDER_COLUMNS.value if c != "ano"
    ]

    for year, group in df.groupby("ano"):
        table = pa.Table.from_pandas(
            group[columns],
            schema=constants_exportacao_bens.SCHEMA.value,
            preserve_index=False,
        )

        # pyrefly: ignore [bad-argument-type]
        table_path = output_dir / f"ano={int(year)}"

        table_path.mkdir(parents=True, exist_ok=True)

        pq.write_table(
            table, table_path / "data.parquet", compression="snappy"
        )

    log(
        f"Limpeza concluída: {len(df)} linhas em {df['ano'].nunique()} "
        f"partições (anos) -> {output_dir}"
    )
    return output_dir
