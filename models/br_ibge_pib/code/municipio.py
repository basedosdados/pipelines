"""
PROJETO: ETL de Dados do PIB dos Municípios (IBGE)
AUTOR: Erick de S.C. Araújo
DATA: 05/05/2026
DESCRIÇÃO:
    Este script realiza o download automatizado de bases de dados do IBGE,
    extrai os arquivos compactados, realiza a limpeza e normalização dos
    dados financeiros e geográficos, e exporta os resultados consolidados
    em formatos Excel (Inteiro) e Parquet (String/Varchar).
"""

import os
import zipfile

import pandas as pd
import requests

# Configurações de caminhos
base_dir = r"D:\Devops\Python\br_ibge_pib"
temp_dir = os.path.join(base_dir, "Temp")
os.makedirs(temp_dir, exist_ok=True)

urls = [
    "https://ftp.ibge.gov.br/Pib_Municipios/2022_2023/base/base_de_dados_2002_2009_xlsx.zip",
    "https://ftp.ibge.gov.br/Pib_Municipios/2022_2023/base/base_de_dados_2010_2023_xlsx.zip",
]

# 1. Download e Extração
for url in urls:
    caminho_zip = os.path.join(base_dir, os.path.basename(url))
    if not os.path.exists(caminho_zip):
        print(f"Baixando {url}...")
        r = requests.get(url)
        with open(caminho_zip, "wb") as f:
            f.write(r.content)

    with zipfile.ZipFile(caminho_zip, "r") as z:
        z.extractall(temp_dir)

# 2. Definição das Âncoras
ancoras = {
    "base_de_dados_2002_2009": {
        1100015: "id_municipio",
        2002: "ano",
        111290.995: "pib",
        7549.266: "impostos_liquidos",
        103741.729: "va",
        27013.223: "va_agropecuaria",
        9376.871: "va_industria",
        24651.113: "va_servicos",
        42700.523: "va_adespss",
    },
    "base_de_dados_2010_2023": {
        1100015: "id_municipio",
        2010: "ano",
        262076.878: "pib",
        20957.111: "impostos_liquidos",
        241119.767: "va",
        69260.391: "va_agropecuaria",
        16118.534: "va_industria",
        62496.185: "va_servicos",
        93244.656: "va_adespss",
    },
}

# Lista atualizada com impostos_liquidos
colunas_finais = [
    "id_municipio",
    "ano",
    "pib",
    "impostos_liquidos",
    "va",
    "va_agropecuaria",
    "va_industria",
    "va_servicos",
    "va_adespss",
]


def identificar_e_limpar(caminho_arquivo):
    print(f"Processando: {os.path.basename(caminho_arquivo)}")
    df_raw = pd.read_excel(caminho_arquivo, header=None)

    chave_ancora = (
        "base_de_dados_2002_2009"
        if "2002" in caminho_arquivo
        else "base_de_dados_2010_2023"
    )
    mapa_referencia = ancoras[chave_ancora]

    mapping_detectado = {}
    row_start = -1

    for col_idx in df_raw.columns:
        col_data = pd.to_numeric(df_raw[col_idx], errors="coerce")
        for val_ancora, var_nome in mapa_referencia.items():
            match = col_data[col_data == val_ancora]
            if not match.empty:
                mapping_detectado[col_idx] = var_nome
                row_start = match.index[0]

    if row_start == -1:
        raise ValueError(f"Âncoras não encontradas em {caminho_arquivo}")

    df = df_raw.iloc[row_start:].copy()
    df = df.rename(columns=mapping_detectado)

    # Filtrar apenas colunas do nosso interesse que foram encontradas
    df = df[[c for c in colunas_finais if c in df.columns]].copy()

    # Remove lixo (linhas de notas/rodapé)
    df = df.dropna(subset=["id_municipio", "ano"])

    # Arredondamento e conversão para Inteiro (para o Excel)
    for col in df.columns:
        df[col] = (
            pd.to_numeric(df[col], errors="coerce").round(0).astype("Int64")
        )

    return df


# 4. Execução Principal
arquivos = [
    os.path.join(temp_dir, f)
    for f in os.listdir(temp_dir)
    if f.endswith(".xlsx")
]
lista_dfs = []

for f in arquivos:
    try:
        lista_dfs.append(identificar_e_limpar(f))
    except Exception as e:
        print(f"Erro ao processar {f}: {e}")

if lista_dfs:
    df_final = pd.concat(lista_dfs, ignore_index=True)
    df_final = df_final[colunas_finais]  # Força a ordem exata das colunas

    # --- EXPORTAÇÃO ---

    # 1. EXCEL (Números Inteiros)
    saida_excel = os.path.join(base_dir, "pib_municipios.xlsx")
    df_final.to_excel(saida_excel, index=False)

    # 2. PARQUET (Tudo Varchar/String)
    saida_parquet = os.path.join(base_dir, "pib_municipios.parquet")
    df_parquet = df_final.astype(str)

    # Limpeza de strings (remove .0 de floats convertidos e trata nulos)
    for col in df_parquet.columns:
        df_parquet[col] = df_parquet[col].str.replace(r"\.0$", "", regex=True)
        df_parquet[col] = df_parquet[col].replace(
            ["<NA>", "nan", "None"], None
        )

    df_parquet.to_parquet(saida_parquet, index=False)

    print("\n" + "=" * 40)
    print("ETL FINALIZADO COM SUCESSO")
    print("=" * 40)
    print(f"Colunas geradas: {df_final.columns.tolist()}")
    print(df_final.head())
else:
    print("Falha: Nenhum dado foi processado.")
