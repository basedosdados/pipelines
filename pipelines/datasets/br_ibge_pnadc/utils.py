"""
Funções puras para gerar o dicionário da PNAD Contínua (br_ibge_pnadc).

Portado de `models/br_ibge_pnadc/code/dicionario_pnadc.py`, que não era
reprodutível (caminhos absolutos para máquina local). Nada aqui importa Prefect:
os wrappers `@task` ficam em `tasks.py`.

O dicionário é montado a partir de dois arquivos da documentação do IBGE:

1. `dicionario_PNADC_microdados_trimestral.xls` — dentro de um zip; traz as
   variáveis da pesquisa, separadas em "Partes" (uma delas é educação).
2. `Estrutura_Ocupacao_COD.xls` — a estrutura hierárquica de ocupação (COD),
   usada para reconstruir a variável V4010.
"""

import zipfile
from pathlib import Path

import httpx
import pandas as pd

# ===== Constantes =====

URL_DOCUMENTACAO = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Trimestral/Microdados/Documentacao"
)

ARQUIVO_ZIP_DICIONARIO = "Dicionario_e_input_20221031.zip"
ARQUIVO_DICIONARIO_NO_ZIP = "dicionario_PNADC_microdados_trimestral.xls"
ARQUIVO_OCUPACAO = "Estrutura_Ocupacao_COD.xls"

#: Colunas do bloco de escolaridade. O Excel do IBGE as lista apenas sob a Parte
#: de educação, mas a tabela `microdados` também as contém — a mesma
#: decodificação serve as duas.
COLUNAS_V3_EM_MICRODADOS = [
    "V3001",
    "V3002",
    "V3002A",
    "V3003",
    "V3003A",
    "V3004",
    "V3005",
    "V3005A",
    "V3006",
    "V3006A",
    "V3007",
    "V3008",
    "V3009",
    "V3009A",
    "V3010",
    "V3011",
    "V3011A",
    "V3012",
    "V3013",
    "V3013A",
    "V3013B",
    "V3014",
]

#: Colunas codificadas cuja `chave` vem com zero à esquerda no dicionário
#: (`01`-`09`) mas com dígito único no dado (`1`-`9`).
#: V4010 NÃO entra aqui: é código hierárquico onde o zero é semântico
#: (`0110` != `110`) — ver issue #1699.
COLUNAS_STRIP_ZERO = [
    "V2005",
    "V4072",
    "V4074A",
    "V3003",
    "V3003A",
    "V3006",
    "V3009",
    "V3009A",
    "V3013",
]

#: Ordem final das colunas do dicionário (contrato com o staging/dbt).
COLUNAS_DICIONARIO = [
    "id_tabela",
    "nome_coluna",
    "chave",
    "cobertura_temporal",
    "valor",
]


def cell(i: int, row: pd.Series) -> str:
    """Retorna a célula `i` da linha como texto limpo, tratando NaN como "".

    Args:
        i: Índice posicional da coluna.
        row: Linha do DataFrame (via `iterrows`).

    Returns:
        Valor da célula como string sem espaços nas bordas, ou "" se for NaN.
    """
    return str(row[i]).strip() if pd.notna(row[i]) else ""


def download_documentacao(dest: Path) -> tuple[Path, Path]:
    """Baixa e extrai os arquivos de documentação do IBGE.

    Args:
        dest: Diretório onde salvar/extrair os arquivos. Criado se não existir.

    Returns:
        Caminhos para (dicionário PNADC, estrutura de ocupação), nessa ordem.
    """
    dest.mkdir(parents=True, exist_ok=True)

    downloaded_zip = httpx.get(
        url=f"{URL_DOCUMENTACAO}/{ARQUIVO_ZIP_DICIONARIO}"
    )

    downloaded_zip.raise_for_status()

    downloaded_docs = httpx.get(url=f"{URL_DOCUMENTACAO}/{ARQUIVO_OCUPACAO}")

    downloaded_docs.raise_for_status()

    dest_zip = dest / ARQUIVO_ZIP_DICIONARIO

    dest_docs = dest / ARQUIVO_OCUPACAO

    dest_zip.write_bytes(downloaded_zip.content)
    dest_docs.write_bytes(downloaded_docs.content)

    with zipfile.ZipFile(dest_zip) as zf:
        zip_path = Path(zf.extract(ARQUIVO_DICIONARIO_NO_ZIP, dest))

    return (zip_path, dest_docs)


def parse_dicionario_ibge(path: Path) -> pd.DataFrame:
    """Lê o Excel do dicionário do IBGE e o traduz para registros.

    Traduz a planilha fielmente, sem aplicar correções de domínio — estas ficam
    em `duplicar_bloco_v3` e `normalizar_chaves`.

    O layout da planilha é posicional (sem header): a coluna 0 traz o marcador
    de "Parte", a 2 o código da variável, a 5 a chave e a 6 o valor.

    Args:
        path: Caminho para `dicionario_PNADC_microdados_trimestral.xls`.

    Returns:
        Colunas de `COLUNAS_DICIONARIO`. `cobertura_temporal` é sempre "(1)".
    """
    df = pd.read_excel(path, sheet_name=0, header=None)

    registers: list[dict] = []

    # Dois estados carregados linha a linha (ver docstring):
    id_tabel = "microdados"  # muda quando aparece uma linha "Parte …"
    col_name = ""  # muda quando col2 (código da variável) não está vazia

    for _, row in df.iterrows():
        col0, col2, col5, col6 = (
            cell(0, row),
            cell(2, row),
            cell(5, row),
            cell(6, row),
        )

        if col0.startswith("Parte"):
            id_tabel = (
                "educacao"
                if "Características de educação" in col0
                else "microdados"
            )
            continue

        if col2 != "":
            col_name = col2

        if not (col5 and col6):
            continue

        register = {
            "id_tabela": id_tabel,
            "nome_coluna": col_name,
            "chave": col5,
            "cobertura_temporal": "(1)",
            "valor": col6,
        }

        registers.append(register)

    registers_without_v4010 = [
        register
        for register in registers
        if register["nome_coluna"] != "V4010"
    ]

    return pd.DataFrame(registers_without_v4010, columns=COLUNAS_DICIONARIO)


def parse_estrutura_ocupacao(path: Path) -> pd.DataFrame:
    """Lê a estrutura de ocupação (COD) e monta as linhas da variável V4010.

    A planilha é hierárquica: colunas GG, SG, SUB e GB, da menos para a mais
    específica, e a denominação ao lado. Cada linha preenche só o nível a que
    pertence — a chave é o nível mais específico presente.

    Atenção: o arquivo da fonte é `.xls` (formato antigo), não `.xlsx`. Zeros à
    esquerda são **semânticos** aqui e devem ser preservados.

    Args:
        path: Caminho para `Estrutura_Ocupacao_COD.xls`.

    Returns:
        Colunas de `COLUNAS_DICIONARIO`, com `nome_coluna` == "V4010".
    """
    # dtype=str preserva os zeros à esquerda (semânticos no V4010); header=None
    # porque as 3 primeiras linhas são título/cabeçalho, não dados.
    df = pd.read_excel(path, header=None, dtype=str)

    registers: list[dict] = []
    seen: set[str] = set()  # chaves já processadas (para dropar repetidas)

    for _, row in df.iterrows():
        gg, sg, sub, gb = (
            cell(0, row),
            cell(1, row),
            cell(2, row),
            cell(3, row),
        )
        value = cell(4, row)

        if gb != "":
            key = gb
        elif sg != "":
            key = sg
        elif sub != "":
            key = sub
        elif gg != "":
            key = gg
        else:
            continue

        if (
            value == ""
            or value in ("Denominação", "Grande Grupo")
            or key in seen
        ):
            continue

        seen.add(key)

        dict_to_row = {
            "id_tabela": "microdados",
            "nome_coluna": "V4010",
            "chave": key,
            "cobertura_temporal": "(1)",
            "valor": value,
        }

        registers.append(dict_to_row)

    return pd.DataFrame(registers, columns=COLUNAS_DICIONARIO)


def duplicar_bloco_v3(
    df: pd.DataFrame, colunas: list[str] = COLUNAS_V3_EM_MICRODADOS
) -> pd.DataFrame:
    """Replica as linhas do bloco de escolaridade sob `id_tabela='microdados'`.

    O Excel do IBGE lista essas colunas apenas na Parte de educação, mas a
    tabela `microdados` também as contém. Sem isso, o teste
    `custom_dictionary_coverage` de `microdados` acusa as chaves como órfãs.

    Args:
        df: Dicionário já parseado.
        colunas: Colunas a replicar.

    Returns:
        `df` acrescido das linhas replicadas.
    """
    df_copy = df[
        (df["id_tabela"] == "educacao") & (df["nome_coluna"].isin(colunas))
    ].copy()

    df_copy["id_tabela"] = "microdados"

    df = pd.concat([df, df_copy], ignore_index=True)

    return df


def normalizar_chaves(
    df: pd.DataFrame, colunas: list[str] = COLUNAS_STRIP_ZERO
) -> pd.DataFrame:
    """Remove o zero à esquerda da `chave` nas colunas codificadas.

    Args:
        df: Dicionário já parseado.
        colunas: Colunas a normalizar. V4010 não deve entrar — ver `COLUNAS_STRIP_ZERO`.

    Returns:
        `df` com a `chave` normalizada apenas nas colunas indicadas.
    """
    df = df.copy()

    marked_rows = df["nome_coluna"].isin(colunas)

    df.loc[marked_rows, "chave"] = (
        df.loc[marked_rows, "chave"].str.lstrip("0").replace("", "0")
    )

    return df


def build_dicionario(dest: Path) -> pd.DataFrame:
    """Monta o dicionário completo da PNADC, pronto para o staging.

    Orquestra download, parsing e as duas correções de domínio.

    Args:
        dest: Diretório de trabalho para os downloads.

    Returns:
        Dicionário final, ordenado e com todas as colunas como string.
    """
    zip_path, doc_path = download_documentacao(dest)
    dict_ibge = parse_dicionario_ibge(zip_path)
    df_microdados = duplicar_bloco_v3(dict_ibge)
    df = normalizar_chaves(df_microdados)
    df_v4010 = parse_estrutura_ocupacao(doc_path)

    df = pd.concat([df, df_v4010], ignore_index=True)

    df = df.sort_values(by=["id_tabela", "nome_coluna", "chave"])

    return df.astype(str)
