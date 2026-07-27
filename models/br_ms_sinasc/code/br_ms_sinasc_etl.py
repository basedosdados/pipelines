"""
br_ms_sinasc — Microdados do SINASC
====================================
Processamento dos microdados do Sistema de Informações sobre Nascidos Vivos
(SINASC/DATASUS).

**Versão local (Windows / Linux / macOS)**

Estrutura de pastas esperada em ``BASE_DIR``:

    DN/
    ├── input/          ← arquivos .dbc baixados do FTP
    ├── output/         ← CSVs particionados por ano/UF
    └── extra/
        └── id_mun.csv  ← tabela de municípios (copie manualmente)

As pastas ``input`` e ``output`` são criadas automaticamente ao executar.

Primeira execução — instale as dependências:

    pip install datasus-dbc dbfread pandas
"""

from __future__ import annotations

import logging
import os
import tempfile
import urllib.request
from pathlib import Path

import pandas as pd
from datasus_dbc import decompress as dbc2dbf  # Windows + Linux + macOS
from dbfread import DBF

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

# Diretório raiz do projeto — ajuste se necessário
BASE_DIR = Path(r"D:\Devops\Git\areadetrabalho\DN")
INPUT_DIR = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"
EXTRA_DIR = BASE_DIR / "extra"

URL_TEMPLATE = (
    "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/"
    "DN{uf}{ano}.dbc"
)

UFS: list[str] = [
    "AC",
    "AL",
    "AM",
    "AP",
    "BA",
    "CE",
    "DF",
    "ES",
    "GO",
    "MA",
    "MG",
    "MS",
    "MT",
    "PA",
    "PB",
    "PE",
    "PI",
    "PR",
    "RJ",
    "RN",
    "RO",
    "RR",
    "RS",
    "SC",
    "SE",
    "SP",
    "TO",
]

DATE_COLUMNS: list[str] = [
    "data_nascimento",
    "data_cadastro",
    "data_recebimento",
    "data_recebimento_original",
    "data_nascimento_mae",
    "data_ultima_menstruacao",
    "data_declaracao",
]

ORDEM_COLUNAS: list[str] = [
    "sequencial_nascimento",
    "id_municipio_nascimento",
    "local_nascimento",
    "codigo_estabelecimento",
    "data_nascimento",
    "hora_nascimento",
    "sexo",
    "peso",
    "raca_cor",
    "apgar1",
    "apgar5",
    "id_anomalia",
    "codigo_anomalia",
    "semana_gestacao",
    "semana_gestacao_estimada",
    "gestacao_agr",
    "tipo_gravidez",
    "tipo_parto",
    "inicio_pre_natal",
    "pre_natal",
    "pre_natal_agr",
    "classificacao_pre_natal",
    "quantidade_filhos_vivos",
    "quantidade_filhos_mortos",
    "id_pais_mae",
    "id_uf_mae",
    "id_municipio_mae",
    "id_pais_residencia",
    "id_municipio_residencia",
    "data_nascimento_mae",
    "idade_mae",
    "escolaridade_mae",
    "serie_escolar_mae",
    "escolaridade_2010_mae",
    "escolaridade_2010_agr_mae",
    "estado_civil_mae",
    "ocupacao_mae",
    "raca_cor_mae",
    "gestacoes_ant",
    "quantidade_parto_normal",
    "quantidade_parto_cesareo",
    "data_ultima_menstruacao",
    "tipo_apresentacao",
    "inducao_parto",
    "cesarea_antes_parto",
    "tipo_robson",
    "idade_pai",
    "cartorio",
    "registro_cartorio",
    "data_registro_cartorio",
    "origem",
    "numero_lote",
    "versao_sistema",
    "data_cadastro",
    "data_recebimento",
    "data_recebimento_original",
    "diferenca_data",
    "data_declaracao",
    "funcao_responsavel",
    "documento_responsavel",
    "formacao_profissional_responsavel",
    "status_dn",
    "status_dn_nova",
    "paridade",
]

RENAME_96_19: dict[str, str] = {
    "contador": "sequencial_nascimento",
    "CONTADOR": "sequencial_nascimento",
    "CODMUNNASC": "id_municipio_nascimento",
    "LOCNASC": "local_nascimento",
    "CODESTAB": "codigo_estabelecimento",
    "DTNASC": "data_nascimento",
    "HORANASC": "hora_nascimento",
    "SEXO": "sexo",
    "PESO": "peso",
    "RACACOR": "raca_cor",
    "APGAR1": "apgar1",
    "APGAR5": "apgar5",
    "IDANOMAL": "id_anomalia",
    "CODANOMAL": "codigo_anomalia",
    "SEMAGESTAC": "semana_gestacao",
    "TPMETESTIM": "semana_gestacao_estimada",
    "GESTACAO": "gestacao_agr",
    "GRAVIDEZ": "tipo_gravidez",
    "PARTO": "tipo_parto",
    "MESPRENAT": "inicio_pre_natal",
    "CONSPRENAT": "pre_natal",
    "CONSULTAS": "pre_natal_agr",
    "KOTELCHUCK": "classificacao_pre_natal",
    "QTDFILVIVO": "quantidade_filhos_vivos",
    "QTDFILMORT": "quantidade_filhos_mortos",
    "NATURALMAE": "id_pais_mae",
    "CODUFNATU": "id_uf_mae",
    "CODMUNNATU": "id_municipio_mae",
    "CODPAISRES": "id_pais_residencia",
    "CODMUNRES": "id_municipio_residencia",
    "DTNASCMAE": "data_nascimento_mae",
    "IDADEMAE": "idade_mae",
    "ESCMAE": "escolaridade_mae",
    "SERIESCMAE": "serie_escolar_mae",
    "ESCMAE2010": "escolaridade_2010_mae",
    "ESCMAEAGR1": "escolaridade_2010_agr_mae",
    "ESTCIVMAE": "estado_civil_mae",
    "CODOCUPMAE": "ocupacao_mae",
    "RACACORMAE": "raca_cor_mae",
    "QTDGESTANT": "gestacoes_ant",
    "QTDPARTNOR": "quantidade_parto_normal",
    "QTDPARTCES": "quantidade_parto_cesareo",
    "DTULTMENST": "data_ultima_menstruacao",
    "TPAPRESENT": "tipo_apresentacao",
    "NUMREGCART": "registro_cartorio",
    "CODCART": "cartorio",
    "DTREGCART": "data_registro_cartorio",
    "STTRABPART": "inducao_parto",
    "STCESPARTO": "cesarea_antes_parto",
    "TPROBSON": "tipo_robson",
    "IDADEPAI": "idade_pai",
    "ORIGEM": "origem",
    "NUMEROLOTE": "numero_lote",
    "VERSAOSIST": "versao_sistema",
    "DTCADASTRO": "data_cadastro",
    "DTRECEBIM": "data_recebimento",
    "DTRECORIGA": "data_recebimento_original",
    "DTRECORIG": "data_recebimento_original",
    "DIFDATA": "diferenca_data",
    "DTDECLARAC": "data_declaracao",
    "TPFUNCRESP": "funcao_responsavel",
    "TPDOCRESP": "documento_responsavel",
    "TPNASCASSI": "formacao_profissional_responsavel",
    "STDNEPIDEM": "status_dn",
    "STDNNOVA": "status_dn_nova",
    "PARIDADE": "paridade",
}

# Colunas adicionadas a partir de 2014 (ausentes em anos anteriores)
COLUNAS_ADICIONAIS_14_19: list[str] = [
    "cartorio",
    "registro_cartorio",
    "data_registro_cartorio",
]

# ---------------------------------------------------------------------------
# I/O — leitura de .dbc
# ---------------------------------------------------------------------------


def read_dbc(
    filepath: str | Path, encoding: str = "iso-8859-1"
) -> pd.DataFrame:
    """
    Lê um arquivo .dbc e retorna um DataFrame.

    Converte .dbc → .dbf temporário em %TEMP% (via datasus-dbc) e carrega
    com dbfread. O arquivo temporário é removido ao final.

    Parameters
    ----------
    filepath:
        Caminho para o arquivo .dbc.
    encoding:
        Codificação dos campos de texto (padrão: iso-8859-1).

    Returns
    -------
    pd.DataFrame
    """
    filepath = Path(filepath)
    tmp_fd, tmp_path = tempfile.mkstemp(
        suffix=".dbf", dir=tempfile.gettempdir()
    )
    os.close(tmp_fd)
    try:
        dbc2dbf(str(filepath), tmp_path)
        table = DBF(tmp_path, encoding=encoding, load=True)
        return pd.DataFrame(iter(table))
    finally:
        Path(tmp_path).unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# Setup de diretórios
# ---------------------------------------------------------------------------


def _criar_diretorios_base() -> None:
    """Cria as pastas base do projeto caso não existam."""
    for _dir in (INPUT_DIR, OUTPUT_DIR, EXTRA_DIR):
        _dir.mkdir(parents=True, exist_ok=True)


def criar_diretorios_uf(ano: int, ufs: list[str] = UFS) -> None:
    """Cria os diretórios de saída para cada UF do ano informado."""
    for uf in ufs:
        caminho = OUTPUT_DIR / f"ano={ano}" / f"sigla_uf={uf}"
        caminho.mkdir(parents=True, exist_ok=True)
    logger.info("Diretórios criados para ano=%d.", ano)


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------


def carregar_id_municipios() -> pd.DataFrame:
    """Carrega o arquivo de mapeamento de municípios."""
    caminho = EXTRA_DIR / "id_mun.csv"
    return pd.read_csv(caminho, dtype="string")


def _reporthook(block_num: int, block_size: int, total_size: int) -> None:
    """Callback de progresso para ``urllib.request.urlretrieve``."""
    baixado = block_num * block_size
    if total_size > 0:
        pct = min(baixado / total_size * 100, 100)
        print(
            f"\r  {pct:5.1f}%  ({baixado // 1024} / {total_size // 1024} KB)",
            end="",
            flush=True,
        )
    else:
        print(f"\r  {baixado // 1024} KB baixados...", end="", flush=True)


def baixar_arquivo_dbc(uf: str, ano: int) -> Path:
    """
    Realiza o download do arquivo .dbc do SINASC via FTP (stdlib).

    Parameters
    ----------
    uf:
        Sigla da unidade federativa (ex.: 'SP').
    ano:
        Ano de referência (ex.: 2023).

    Returns
    -------
    Path
        Caminho local do arquivo baixado.
    """
    url = URL_TEMPLATE.format(uf=uf, ano=ano)
    destino = INPUT_DIR / f"DN{uf}{ano}.dbc"
    if destino.exists():
        logger.info("Arquivo já existe, download ignorado: %s", destino.name)
    else:
        logger.info("Baixando %s", url)
        urllib.request.urlretrieve(
            url, filename=str(destino), reporthook=_reporthook
        )
        print()  # quebra de linha após barra de progresso
        logger.info(
            "Download concluído: %s (%.1f KB)",
            destino.name,
            destino.stat().st_size / 1024,
        )
    return destino


def baixar_todos_arquivos(ano: int, ufs: list[str] = UFS) -> None:
    """Itera sobre todas as UFs e realiza o download dos arquivos .dbc."""
    for uf in ufs:
        baixar_arquivo_dbc(uf, ano)


# ---------------------------------------------------------------------------
# Comparação de cabeçalhos
# ---------------------------------------------------------------------------


def obter_cabecalho_ano(uf_referencia: str, ano: int) -> set[str] | None:
    """
    Lê o cabeçalho do arquivo .dbc de uma UF para o ano informado e aplica
    o dicionário de renomeação, retornando o conjunto de colunas padronizadas.

    Returns
    -------
    set[str] | None
        ``None`` se o arquivo não existir.
    """
    caminho = INPUT_DIR / f"DN{uf_referencia}{ano}.dbc"
    if not caminho.exists():
        logger.warning(
            "Arquivo não encontrado para UF=%s, ano=%d: %s",
            uf_referencia,
            ano,
            caminho,
        )
        return None

    df_amostra = read_dbc(str(caminho), encoding="iso-8859-1")
    colunas_originais = set(df_amostra.columns)
    return {RENAME_96_19.get(c, c) for c in colunas_originais}


def sugerir_dicionario(
    colunas_novas: set[str],
    colunas_antigas: set[str],
) -> dict[str, str]:
    """
    Gera um dicionário de renomeação sugerido para colunas do ano novo
    ainda ausentes em ``RENAME_96_19``.
    """
    ja_mapeadas = set(RENAME_96_19.keys())
    sem_mapeamento = colunas_novas - ja_mapeadas
    return {col: f"NOVO_NOME_PARA_{col}" for col in sorted(sem_mapeamento)}


def comparar_cabecalhos(ano: int, uf_referencia: str = "SP") -> bool:
    """
    Compara o cabeçalho do *ano* informado com o do *ano anterior*.

    Returns
    -------
    bool
        ``True``  — cabeçalhos idênticos (independente de ordem); pode processar.
        ``False`` — cabeçalhos divergentes; exibe diff e sugere novo dicionário.
    """
    ano_anterior = ano - 1
    logger.info(
        "Comparando cabeçalhos: ano=%d vs ano_anterior=%d (UF=%s)",
        ano,
        ano_anterior,
        uf_referencia,
    )

    cols_novo = obter_cabecalho_ano(uf_referencia, ano)
    cols_anterior = obter_cabecalho_ano(uf_referencia, ano_anterior)

    if cols_novo is None:
        logger.error(
            "Não foi possível ler o cabeçalho do ano=%d. "
            "Verifique se o arquivo foi baixado.",
            ano,
        )
        return False

    if cols_anterior is None:
        logger.warning(
            "Arquivo do ano anterior (%d) não encontrado. "
            "Pulando comparação e prosseguindo com ano=%d.",
            ano_anterior,
            ano,
        )
        return True

    apenas_no_novo = cols_novo - cols_anterior
    apenas_no_anterior = cols_anterior - cols_novo

    if not apenas_no_novo and not apenas_no_anterior:
        logger.info(
            "✅ Cabeçalhos idênticos entre %d e %d. Processamento autorizado.",
            ano_anterior,
            ano,
        )
        return True

    logger.warning(
        "❌ Cabeçalhos DIFERENTES entre %d e %d.", ano_anterior, ano
    )

    if apenas_no_novo:
        logger.warning(
            "Colunas presentes em %d mas AUSENTES em %d:\n  %s",
            ano,
            ano_anterior,
            sorted(apenas_no_novo),
        )
    if apenas_no_anterior:
        logger.warning(
            "Colunas presentes em %d mas AUSENTES em %d:\n  %s",
            ano_anterior,
            ano,
            sorted(apenas_no_anterior),
        )

    caminho_novo = INPUT_DIR / f"DN{uf_referencia}{ano}.dbc"
    df_raw = read_dbc(str(caminho_novo), encoding="iso-8859-1")
    sugestao = sugerir_dicionario(set(df_raw.columns), cols_anterior)

    if sugestao:
        logger.warning(
            "💡 Sugestão de dicionário para colunas sem mapeamento:\n%s",
            "\n".join(f"  '{k}': '{v}'," for k, v in sugestao.items()),
        )

    return False


# ---------------------------------------------------------------------------
# Transformações
# ---------------------------------------------------------------------------


def converter_datas(df: pd.DataFrame) -> pd.DataFrame:
    """Converte as colunas de data para o tipo ``date``."""
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col], format="%d%m%Y", errors="coerce"
            ).dt.date
    return df


def converter_hora(df: pd.DataFrame) -> pd.DataFrame:
    """Converte a coluna ``hora_nascimento`` para o tipo ``time``."""
    if "hora_nascimento" in df.columns:
        df["hora_nascimento"] = pd.to_datetime(
            df["hora_nascimento"], format="%H%M", errors="coerce"
        ).dt.time
    return df


def padronizar_municipio(
    df: pd.DataFrame,
    coluna: str,
    id_mun: pd.DataFrame,
) -> pd.DataFrame:
    """
    Se a coluna de município tiver 6 dígitos, faz o merge com ``id_mun``
    para obter o código de 7 dígitos (padrão IBGE).
    """
    if coluna not in df.columns:
        return df

    primeiro_valor = df.sort_values(by=[coluna]).iloc[0][coluna]
    if pd.isna(primeiro_valor) or len(str(primeiro_valor)) != 6:
        return df

    df = df.merge(
        id_mun,
        how="left",
        left_on=coluna,
        right_on="id_municipio_6",
        validate="many_to_one",
    )
    df = df.drop(columns=[coluna, "id_municipio_6", "sigla_uf", "regiao"])
    return df.rename(columns={"id_municipio": coluna})


def enriquecer_municipio_mae(
    df: pd.DataFrame, id_mun: pd.DataFrame
) -> pd.DataFrame:
    """Garante que ``id_municipio_mae`` esteja no formato de 7 dígitos."""
    df = df.merge(
        id_mun[["id_municipio", "id_municipio_6"]],
        how="left",
        left_on="id_municipio_mae",
        right_on="id_municipio_6",
        validate="many_to_one",
    )
    df = df.drop(columns=["id_municipio_mae", "id_municipio_6"])
    return df.rename(columns={"id_municipio": "id_municipio_mae"})


def processar_uf(uf: str, ano: int, id_mun: pd.DataFrame) -> pd.DataFrame:
    """
    Pipeline de transformação para uma UF/ano.

    Etapas
    ------
    1. Leitura do .dbc
    2. Renomeação
    3. Conversão de datas/hora
    4. Padronização de municípios
    5. Adição de colunas legado ausentes
    6. Reordenação das colunas
    """
    caminho = INPUT_DIR / f"DN{uf}{ano}.dbc"
    logger.info("  Processando UF=%s, ano=%d ...", uf, ano)

    df = read_dbc(str(caminho), encoding="iso-8859-1")
    df = df.rename(columns=RENAME_96_19)
    df = converter_datas(df)
    df = converter_hora(df)

    for col_mun in (
        "id_municipio_nascimento",
        "id_municipio_mae",
        "id_municipio_residencia",
    ):
        try:
            df = padronizar_municipio(df, col_mun, id_mun)
        except Exception as exc:
            logger.warning("Erro ao padronizar coluna '%s': %s", col_mun, exc)

    try:
        df = enriquecer_municipio_mae(df, id_mun)
    except Exception as exc:
        logger.warning("Erro ao enriquecer id_municipio_mae: %s", exc)

    df = pd.concat([df, pd.DataFrame(columns=COLUNAS_ADICIONAIS_14_19)])
    colunas_disponiveis = [c for c in ORDEM_COLUNAS if c in df.columns]
    return df[colunas_disponiveis]


def exportar_csv(df: pd.DataFrame, uf: str, ano: int) -> Path:
    """Exporta o DataFrame para CSV particionado por ano/UF."""
    destino = OUTPUT_DIR / f"ano={ano}" / f"sigla_uf={uf}" / "data.csv"
    df.to_csv(
        destino,
        index=False,
        encoding="utf-8",
        na_rep="",
        date_format="%Y-%m-%d",
    )
    logger.info("  Exportado: %s", destino)
    return destino


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------


def processar_ano(
    ano: int,
    ufs: list[str] = UFS,
    uf_referencia: str = "SP",
    forcar: bool = False,
) -> None:
    """
    Pipeline completo do SINASC para um ano inteiro.

    Etapas
    ------
    1. Cria diretórios de saída.
    2. Baixa os arquivos .dbc de todas as UFs (FTP DATASUS).
    3. Compara cabeçalhos com o ano anterior:
       - Iguais      → processa.
       - Diferentes  → reporta diff + sugere dicionário;
                       interrompe (salvo ``forcar=True``).
    4. Transforma e exporta cada UF.

    Parameters
    ----------
    ano:
        Ano a processar (ex.: 2023).
    ufs:
        Lista de UFs; padrão = todas as 27.
    uf_referencia:
        UF amostrada na comparação de cabeçalhos.
    forcar:
        Se ``True``, processa mesmo com cabeçalhos divergentes.
    """
    logger.info("=" * 60)
    logger.info("Iniciando processamento SINASC - ano=%d", ano)
    logger.info("=" * 60)

    criar_diretorios_uf(ano, ufs)
    baixar_todos_arquivos(ano, ufs)

    cabecalhos_ok = comparar_cabecalhos(ano, uf_referencia=uf_referencia)

    if not cabecalhos_ok and not forcar:
        logger.error(
            "Processamento interrompido: divergência de cabeçalhos. "
            "Atualize RENAME_96_19 ou use forcar=True."
        )
        return

    if not cabecalhos_ok:
        logger.warning("forcar=True — prosseguindo apesar da divergência.")

    id_mun = carregar_id_municipios()

    for uf in ufs:
        try:
            df = processar_uf(uf, ano, id_mun)
            exportar_csv(df, uf, ano)
        except Exception:
            logger.exception("Erro ao processar UF=%s", uf)

    logger.info("Processamento concluído para ano=%d.", ano)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _criar_diretorios_base()
    logger.info("BASE_DIR : %s", BASE_DIR)
    logger.info("INPUT_DIR : %s  (existe: %s)", INPUT_DIR, INPUT_DIR.exists())
    logger.info(
        "OUTPUT_DIR: %s  (existe: %s)", OUTPUT_DIR, OUTPUT_DIR.exists()
    )
    logger.info("EXTRA_DIR : %s  (existe: %s)", EXTRA_DIR, EXTRA_DIR.exists())

    # Altere o ano conforme necessário.
    # Use forcar=True somente se quiser avançar mesmo com cabeçalhos divergentes.
    # Dica: para um teste rápido com apenas duas UFs:
    #   processar_ano(ano=2023, ufs=["SP", "RJ"])
    processar_ano(ano=2024)
