# =====================================================
# curso
# =====================================================

"""
Script de tratamento dos microdados do Censo da Educação Superior 2024 - tabela: curso.
Autor: Thais Filipi
Data: novembro/2025

Objetivo:
    - Ler microdados brutos e metadados (arquitetura)
    - Padronizar nomes e estrutura de colunas
    - Gerar arquivo pronto para upload ao BigQuery via Base dos Dados
"""

# =====================================================
# Importações e setup inicial
# =====================================================

import os
import sys

import numpy as np
import pandas as pd
import requests
from IPython.display import clear_output

# =====================================================
# Configuração do ambiente (Colab + credenciais)
# =====================================================


def montar_drive():
    """Monta o Google Drive no Colab, se necessário."""
    from google.colab import drive

    drive.mount("/content/drive", force_remount=True)


def configurar_basedosdados():
    """Cria o arquivo config.toml com credenciais locais e parâmetros do BD."""
    destino = "/root/.basedosdados/"
    os.makedirs(os.path.join(destino, "credentials"), exist_ok=True)

    # Caminho onde estão suas credenciais
    path_credenciais = "/content/drive/MyDrive/Credenciais/"
    os.system(f"cp -r {path_credenciais} {destino}")

    # Arquivo base de configuração
    url_config = "https://raw.githubusercontent.com/basedosdados/sdk/refs/heads/master/python-package/basedosdados/core/config.toml"
    config_text = requests.get(url_config).text

    # Substituições
    config_text = (
        config_text.replace(
            'credentials_path = ""',
            'credentials_path = "/root/.basedosdados/credentials/staging.json"',
        )
        .replace('name = ""', 'name = "basedosdados-dev"')
        .replace('bucket_name = ""', 'bucket_name = "basedosdados-dev"')
        .replace(
            'url = ""',
            'url = "https://backend.basedosdados.org/api/v1/graphql"',
        )
    )

    with open(os.path.join(destino, "config.toml"), "w") as f:
        f.write(config_text)

    clear_output()
    print("Configuração do Base dos Dados concluída.")


# =====================================================
# Funções auxiliares
# =====================================================


def preparar_dados_para_bq(df_dados, df_apoio, log=True):
    """
    Filtra, renomeia e organiza as colunas de df_dados conforme df_apoio
    (vem da tabela de arquitetura, com as colunas 'name' e 'original_name').
    Adiciona colunas ausentes com NaN.

    Args:
        df_dados (pd.DataFrame): base bruta a ser tratada
        df_apoio (pd.DataFrame): esquema de referência (name, original_name)
        log (bool): se True, imprime quais colunas foram criadas

    Returns:
        pd.DataFrame: base renomeada e ordenada
    """
    df_filtrado = pd.DataFrame()
    colunas_criadas = []
    # Alterando os nomes das colunas e criando as colunas ausente em 2024
    for _, row in df_apoio.iterrows():
        nome_original = row["original_name"]
        nome_final = row["name"]

        if nome_original in df_dados.columns:
            df_filtrado[nome_final] = df_dados[nome_original]
        else:
            df_filtrado[nome_final] = np.nan
            colunas_criadas.append(nome_final)

    # Garantindo a ordem e selecionado as colunas igual ao df_apoio
    df_filtrado = df_filtrado[df_apoio["name"]]

    if log:
        if colunas_criadas:
            print(f"Colunas ausentes criadas com NaN: {colunas_criadas}")
        else:
            print("Nenhuma coluna ausente; todas presentes.")

    return df_filtrado


# =====================================================
# Pipeline principal
# =====================================================


def main():
    """Executa o pipeline completo de tratamento da tabela curso dos microdados 2024."""
    print(
        "Iniciando o tratamento da tabela curso do Censo da Educação Superior 2024..."
    )

    # Monta o drive e configura o ambiente
    montar_drive()
    configurar_basedosdados()

    # Instalações (mantidas aqui por compatibilidade com Colab)
    os.system("pip install -q basedosdados==2.0.0b16 ruamel.yaml")
    os.system("git clone -q https://github.com/Winzen/utils-colab.git")
    sys.path.insert(0, "/content/utils-colab/scripts")
    clear_output()

    # -------------------------------------------------
    # Leitura dos dados
    # -------------------------------------------------
    path_dados = "/content/drive/MyDrive/conjuntos/dados_brutos/br_inep_censo_educacao_superior/curso/"

    print("Lendo arquivos...")
    df_2024 = pd.read_csv(
        os.path.join(path_dados, "MICRODADOS_CADASTRO_CURSOS_2024.csv"),
        sep=";",
        encoding="windows-1252",
        low_memory=False,
    )

    df_apoio = pd.read_csv(
        os.path.join(path_dados, "arquitetura_apoio.csv"), sep=","
    )

    # -------------------------------------------------
    # Diagnóstico de colunas
    # -------------------------------------------------
    colunas_bd = df_apoio["original_name"]
    colunas_2024 = df_2024.columns

    valores_novos = set(colunas_2024) - set(colunas_bd)
    colunas_faltando = set(colunas_bd) - set(colunas_2024)

    print(f"Colunas não presentes no esquema: {valores_novos}")
    print(f"Colunas ausentes em 2024: {colunas_faltando}")

    # -------------------------------------------------
    # Tratamento e formatação
    # -------------------------------------------------
    df_tratado = preparar_dados_para_bq(df_2024, df_apoio)
    df_tratado["indicador_gratuito"] = df_tratado[
        "indicador_gratuito"
    ].replace({1: True, 0: False})
    df_tratado["id_municipio"] = (
        pd.to_numeric(df_tratado["id_municipio"], errors="coerce")
        .astype("Int64")
        .astype("string")
        .str.zfill(7)
    )
    df_tratado["tipo_grau_academico"] = (
        pd.to_numeric(df_tratado["tipo_grau_academico"], errors="coerce")
        .astype("Int64")
        .astype("string")
    )
    df_tratado = df_tratado.replace(
        [r"^\s*$", "nan", "NaN", "NAN", np.nan], None
    )
    df_tratado = df_tratado.drop(columns=["ano"])

    # -------------------------------------------------
    # Exportação final
    # -------------------------------------------------
    path_saida = "/content/drive/My Drive/conjuntos/br_inep_censo_educacao_superior/curso/ano=2024/"
    os.makedirs(path_saida, exist_ok=True)

    output_path = os.path.join(path_saida, "data.csv")
    df_tratado.to_csv(output_path, encoding="utf-8", index=False)

    print(f"Arquivo tratado salvo em: {output_path}")

    # -------------------------------------------------
    # Upload via Base dos Dados
    # -------------------------------------------------
    try:
        import basedosdados as bd

        tb = bd.Table(
            dataset_id="br_inep_censo_educacao_superior", table_id="curso"
        )
        tb.create(
            path=path_saida,
            if_table_exists="replace",
            if_storage_data_exists="replace",
            source_format="csv",
        )

        print("Upload concluído com sucesso!")

    except Exception as e:
        print(f"Erro durante upload no BD: {e}")


# =====================================================
# Execução
# =====================================================

if __name__ == "__main__":
    main()
