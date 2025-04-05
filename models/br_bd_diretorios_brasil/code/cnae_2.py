# -*- coding: utf-8 -*-
import pandas as pd

# linka nome das versoes com nome dos arquivos baixados de :https://concla.ibge.gov.br/classificacoes/download-concla.html
files = {
    "cnae_2_3": "CNAE_Subclasses_2_3_Estrutura_Detalhada.xlsx",
    "cnae_2_2": "subclasses-cnae-2-2-estrutura.xls",
    "cnae_2_1": "cnae21_estrutura_detalhada.xls",
    "cnae_2_0": "CNAE20_Subclasses_EstruturaDetalhada.xls",
}

# mapeia o range de subclasses que cada seção aceita para fazer uma verificação no resultado final
mapping_secoes_range = {
    "A": range(1, 4),
    "B": range(5, 10),
    "C": range(10, 40),
    "D": range(35, 36),
    "E": range(36, 40),
    "F": range(41, 44),
    "G": range(45, 48),
    "H": range(49, 54),
    "I": range(55, 57),
    "J": range(58, 64),
    "K": range(64, 67),
    "L": range(68, 69),
    "M": range(69, 76),
    "N": range(77, 83),
    "O": range(84, 85),
    "P": range(85, 86),
    "Q": range(86, 89),
    "R": range(90, 94),
    "S": range(94, 97),
    "T": range(97, 98),
    "U": range(99, 100),
}


# define funções
def filtrar_por_secao(df):
    """
    Filtra o DataFrame removendo linhas que possuem subclasses que não pertencem à seção correta.

    Args:
        df (pd.DataFrame): DataFrame com as colunas 'secao' e 'subclasse'.

    Returns:
        pd.DataFrame: DataFrame filtrado.
    """

    def validar_linha(row):
        secao = row["secao"]
        subclasse = int(row["subclasse"][:2])
        if secao in mapping_secoes_range:
            return subclasse in mapping_secoes_range[secao]
        return False

    return df[df.apply(validar_linha, axis=1)]


def carregar_arquivo_excel(caminho_arquivo):
    """
    Carrega um arquivo Excel.

    Args:
        caminho_arquivo (str): O caminho para o arquivo Excel.

    Returns:
        pd.ExcelFile: O arquivo Excel carregado.
    """
    return pd.ExcelFile(caminho_arquivo)


def extrair_folha(xls, nome_folha):
    """
    Extrai uma folha do arquivo Excel.

    Args:
        xls (pd.ExcelFile): O arquivo Excel.
        nome_folha (str): O nome da folha a ser extraída.

    Returns:
        pd.DataFrame: O DataFrame contendo os dados da folha.
    """
    return pd.read_excel(xls, sheet_name=nome_folha, dtype=str)


def extrair_secao(df_input):
    """Extrai a seção e sua descrição."""
    try:
        secao = df_input[df_input["Seção"].apply(lambda x: len(str(x)) == 1)]
        secao = secao[["Seção", "Unnamed: 6"]].rename(
            columns={"Seção": "secao", "Unnamed: 6": "descricao_secao"}
        )
        secao["descricao_secao"] = secao["descricao_secao"].str.title()
    except:
        secao = df_input[df_input["Seção"].apply(lambda x: len(str(x)) == 1)]
        secao = secao[["Seção", "Unnamed: 5"]].rename(
            columns={"Seção": "secao", "Unnamed: 5": "descricao_secao"}
        )
        secao["descricao_secao"] = secao["descricao_secao"].str.title()
    return secao


def extrair_divisao(df_input):
    """Extrai a divisão e sua descrição."""
    try:
        divisao = df_input[
            df_input["Divisão"].apply(lambda x: len(str(x)) == 2)
        ]
        divisao = divisao[["Divisão", "Unnamed: 6"]].rename(
            columns={"Divisão": "divisao", "Unnamed: 6": "descricao_divisao"}
        )
        divisao["divisao"] = divisao["divisao"].str.strip()
        divisao["descricao_divisao"] = divisao["descricao_divisao"].str.title()
    except:
        divisao = df_input[
            df_input["Divisão"].apply(lambda x: len(str(x)) == 2)
        ]
        divisao = divisao[["Divisão", "Unnamed: 5"]].rename(
            columns={"Divisão": "divisao", "Unnamed: 5": "descricao_divisao"}
        )
        divisao["divisao"] = divisao["divisao"].str.strip()
        divisao["descricao_divisao"] = divisao["descricao_divisao"].str.title()
    return divisao


def extrair_grupo(df_input):
    """Extrai o grupo e sua descrição."""
    try:
        grupo = df_input[df_input["Grupo"].apply(lambda x: len(str(x)) == 4)]
        grupo = grupo[["Grupo", "Unnamed: 6"]].rename(
            columns={"Grupo": "grupo", "Unnamed: 6": "descricao_grupo"}
        )
        grupo["grupo"] = grupo["grupo"].str.replace(".", "", regex=False)
    except:
        grupo = df_input[df_input["Grupo"].apply(lambda x: len(str(x)) == 4)]
        grupo = grupo[["Grupo", "Unnamed: 5"]].rename(
            columns={"Grupo": "grupo", "Unnamed: 5": "descricao_grupo"}
        )
        grupo["grupo"] = grupo["grupo"].str.replace(".", "", regex=False)
    return grupo


def extrair_classe(df_input):
    """Extrai a classe e sua descrição."""
    try:
        classe = df_input[
            df_input["Classe"].apply(lambda x: len(str(x)) == 7)
        ].drop_duplicates()
        classe = classe[["Classe", "Unnamed: 6"]].rename(
            columns={"Classe": "classe", "Unnamed: 6": "descricao_classe"}
        )
        classe["classe"] = classe["classe"].str.replace(".", "", regex=False)
        classe["classe"] = classe["classe"].str.replace("-", "", regex=False)
    except:
        classe = df_input[
            df_input["Classe"].apply(lambda x: len(str(x)) == 7)
        ].drop_duplicates()
        classe = classe[["Classe", "Unnamed: 5"]].rename(
            columns={"Classe": "classe", "Unnamed: 5": "descricao_classe"}
        )
        classe["classe"] = classe["classe"].str.replace(".", "", regex=False)
        classe["classe"] = classe["classe"].str.replace("-", "", regex=False)
    return classe


def extrair_subclasse(df_input):
    """Extrai a subclasse e sua descrição."""
    try:
        subclasse = df_input[["Subclasse", "Unnamed: 6"]].rename(
            columns={
                "Subclasse": "subclasse",
                "Unnamed: 6": "descricao_subclasse",
            }
        )
        subclasse = subclasse[
            subclasse["subclasse"].apply(lambda x: len(str(x)) == 9)
        ].drop_duplicates()
        subclasse["subclasse"] = subclasse["subclasse"].str.replace(
            "-", "", regex=False
        )
        subclasse["subclasse"] = subclasse["subclasse"].str.replace(
            "/", "", regex=False
        )
    except:
        subclasse = df_input[["Subclasse", "Unnamed: 5"]].rename(
            columns={
                "Subclasse": "subclasse",
                "Unnamed: 5": "descricao_subclasse",
            }
        )
        subclasse = subclasse[
            subclasse["subclasse"].apply(lambda x: len(str(x)) == 9)
        ].drop_duplicates()
        subclasse["subclasse"] = subclasse["subclasse"].str.replace(
            "-", "", regex=False
        )
        subclasse["subclasse"] = subclasse["subclasse"].str.replace(
            "/", "", regex=False
        )
    return subclasse


def limpar_e_preparar_df(df_input):
    """Limpa e prepara o DataFrame.A partir das subclasses são criados as divisões, grupos e classes"""

    df = df_input[
        (df_input["Seção"].apply(lambda x: len(str(x)) == 1))
        | (df_input["Seção"].isna())
    ]
    df = df[["Seção", "Subclasse"]].rename(
        columns={"Seção": "secao", "Subclasse": "subclasse"}
    )
    df = df.fillna(method="ffill")
    df = df[
        df["subclasse"].apply(lambda x: len(str(x)) == 9)
    ].drop_duplicates()
    df["subclasse"] = df["subclasse"].str.replace("-", "", regex=False)
    df["subclasse"] = (
        df["subclasse"].str.replace("/", "", regex=False).str.strip()
    )
    df["divisao"] = df["subclasse"].str[:2]
    df["grupo"] = df["subclasse"].str[:3]
    df["classe"] = df["subclasse"].str[:5]

    # Filtrar por seção
    df = filtrar_por_secao(df)

    return df


def mesclar_dataframes(df, secao, divisao, grupo, classe, subclasse):
    """Mescla os DataFrames de seções, divisões, grupos, classes e subclasses."""
    df = df.merge(secao, left_on="secao", right_on="secao", how="left")
    df = df.merge(divisao, left_on="divisao", right_on="divisao", how="left")
    df = df.merge(grupo, left_on="grupo", right_on="grupo", how="left")
    df = df.merge(classe, left_on="classe", right_on="classe", how="left")
    df = df.merge(
        subclasse, left_on="subclasse", right_on="subclasse", how="left"
    )
    return df


def adicionar_versao_cnae(df, versao):
    """Adiciona a coluna versao_cnae ao DataFrame."""
    df["versao_cnae"] = versao
    return df


def criar_dataframe_indicadores(df_concatenado, versoes):
    """Cria variáveis de indicadores para as diferentes versões de CNAE."""
    df_indicadores = (
        df_concatenado[["subclasse"]].drop_duplicates().reset_index(drop=True)
    )
    for versao in versoes:
        indicador_col = f"indicador_{versao}"
        df_indicadores[indicador_col] = df_indicadores["subclasse"].apply(
            lambda x: 1
            if x
            in df_concatenado[df_concatenado["versao_cnae"] == versao][
                "subclasse"
            ].values
            else 0
        )
    return df_indicadores


def salvar_para_csv(df, nome_arquivo):
    """Salva o DataFrame em um arquivo CSV."""
    df.to_csv(nome_arquivo, encoding="utf-8", sep=",", na_rep="", index=False)


def processar_cnae(files):
    """Processa todas as versões de CNAE e concatena os resultados em um único DataFrame."""
    df_concatenado = pd.DataFrame()

    for versao, caminho in files.items():
        print(f"---- fazendo arquivo {versao}")

        xls = carregar_arquivo_excel(caminho)
        df_input = extrair_folha(xls, nome_folha=0)
        secao = extrair_secao(df_input)
        print(secao.groupby("secao").filter(lambda x: len(x) > 1).count())
        divisao = extrair_divisao(df_input)
        grupo = extrair_grupo(df_input)
        classe = extrair_classe(df_input)
        subclasse = extrair_subclasse(df_input)
        print(f"----- antes de preparar {subclasse.shape}")
        df = limpar_e_preparar_df(df_input)
        print(df.shape)
        df = mesclar_dataframes(df, secao, divisao, grupo, classe, subclasse)
        print(df.shape)
        df = adicionar_versao_cnae(df, versao)
        print(df.shape)
        df_concatenado = pd.concat([df_concatenado, df], ignore_index=True)
        print(df.shape)
    versoes = files.keys()
    df_indicadores = criar_dataframe_indicadores(df_concatenado, versoes)
    df_final = df_concatenado.merge(df_indicadores, on="subclasse", how="left")

    return df_final


def main(files, nome_arquivo_saida):
    """Função principal que orquestra o processo de extração e transformação dos dados."""
    df_final = processar_cnae(files)
    df_final.drop("versao_cnae", axis=1, inplace=True)
    df_final.drop_duplicates(subset=["subclasse"], inplace=True)
    salvar_para_csv(df_final, nome_arquivo_saida)


# Gera arquivo único na estrutura final
if __name__ == "__main__":
    nome_arquivo_saida = "cnae_2.csv"
    main(files, nome_arquivo_saida)
