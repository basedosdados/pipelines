"""
Processamento PNAD Contínua - Educação
"""

# IMPORTS
import gc
import os
import re

import basedosdados as bd
import pandas as pd
import polars as pl
import requests

input = "/content/drive/MyDrive/pnadc"
output = os.path.join(input, "educacao")

LAYOUT_URL = (
    "https://ftp.ibge.gov.br/Trabalho_e_Rendimento/"
    "Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/"
    "Anual/Microdados/Trimestre/Trimestre_2/Documentacao/"
    "input_PNADC_trimestre2_20221221.txt"
)

# Variáveis desejadas
vars_pnad = [
    "Ano",
    "Trimestre",
    "UF",
    "Capital",
    "RM_RIDE",
    "UPA",
    "Estrato",
    "V1008",
    "V1014",
    "V1016",
    "V1022",
    "V1023",
    "V1027",
    "V1028",
    "V1029",
    "V1033",
    "posest",
    "posest_sxi",
    "V2001",
    "V2003",
    "V3001",
    "V3002",
    "V3002A",
    "V3003A",
    "V3004",
    "V3004A",
    "V3005A",
    "V3006",
    "V3006A",
    "V3006B",
    "V3006C",
    "V3007",
    "V3008",
    "V3009A",
    "V3010",
    "V3010A",
    "V3011A",
    "V3012",
    "V3013",
    "V3013A",
    "V3013B",
    "V3014",
    "V3017",
    "V3017A",
    "V3018",
    "V3019",
    "V3019A",
    "V3020",
    "V3020B",
    "V3020C",
    "V3021",
    "V3021A",
    "V3022",
    "V3022A",
    "V3022C",
    "V3022D",
    "V3022E",
    "V3023",
    "V3023A",
    "V3024",
    "V3025",
    "V3026",
    "V3026A",
    "V3028",
    "V3029",
    "V3029A",
    "V3030",
    "V3030A",
    "V3032",
    "V3033",
    "V3033A",
    "V3033B",
    "V3034",
    "V3034A",
    "V3034B",
    "V3034C",
] + [f"V1028{str(i).zfill(3)}" for i in range(1, 201)]

# DOWNLOAD E INTERPRETAÇÃO DO LAYOUT SAS

print("Baixando layout SAS")
resp = requests.get(LAYOUT_URL)
resp.encoding = "latin1"
layout = resp.text.splitlines()

pattern = re.compile(r"@(\d+)\s+([A-Za-z0-9_]+)\s+\$?(\d+)\.?")
layout_dict = {}

for line in layout:
    m = pattern.search(line)
    if m:
        pos = int(m.group(1))
        length = int(m.group(3))
        var = m.group(2)
        layout_dict[var] = (pos - 1, pos - 1 + length)

vars_encontradas = [v for v in vars_pnad if v in layout_dict]
vars_faltantes = [v for v in vars_pnad if v not in layout_dict]

print("Variáveis encontradas:", len(vars_encontradas))
print("Variáveis faltantes:", vars_faltantes)

colspecs = [layout_dict[v] for v in vars_encontradas]
names = vars_encontradas

# LEITURA DO ARQUIVO
# Arquivo baixado através do link: https://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Anual/Microdados/Trimestre/Trimestre_2/Dados/
arquivo_dados = os.path.join(input, "PNADC_2024_trimestre2.txt")

df = pd.read_fwf(
    arquivo_dados, colspecs=colspecs, names=names, dtype=str, encoding="latin1"
)

# AJUSTES DE NOMES E IDENTIFICADORES
df.columns = [
    col if col.startswith("V") else col.lower() for col in df.columns
]

df = df.rename(columns={"upa": "id_upa", "estrato": "id_estrato"})

df["id_domicilio"] = df["id_upa"] + df["V1008"] + df["V1014"]
df["id_pessoa"] = df["id_domicilio"] + df["V2003"]

# CRIANDO SIGLA_UF
uf_codigo_para_sigla = {
    "11": "RO",
    "12": "AC",
    "13": "AM",
    "14": "RR",
    "15": "PA",
    "16": "AP",
    "17": "TO",
    "21": "MA",
    "22": "PI",
    "23": "CE",
    "24": "RN",
    "25": "PB",
    "26": "PE",
    "27": "AL",
    "28": "SE",
    "29": "BA",
    "31": "MG",
    "32": "ES",
    "33": "RJ",
    "35": "SP",
    "41": "PR",
    "42": "SC",
    "43": "RS",
    "50": "MS",
    "51": "MT",
    "52": "GO",
    "53": "DF",
}
df["sigla_uf"] = df["uf"].map(uf_codigo_para_sigla)

# REORDENANDO COLUNAS

ordem_id = [
    "ano",
    "trimestre",
    "id_uf",
    "sigla_uf",
    "capital",
    "rm_ride",
    "id_upa",
    "id_estrato",
    "id_domicilio",
    "id_pessoa",
]

ordem_existentes = [c for c in ordem_id if c in df.columns]
resto = [c for c in df.columns if c not in ordem_existentes]

df = df[ordem_existentes + resto]

df["ano"] = pd.to_numeric(df["ano"], errors="coerce")
df["trimestre"] = pd.to_numeric(df["trimestre"], errors="coerce")

# SALVANDO ARQUIVO
csv_saida = os.path.join(input, "df_2024.csv")
df.to_csv(csv_saida, index=False, na_rep="")

# PARTICIONAMENTO POR ano E sigla_uf

os.makedirs(output, exist_ok=True)

df_temp = pl.read_csv(csv_saida, ignore_errors=True)

for cols, data in df_temp.group_by(["ano", "sigla_uf"]):
    ano, sigla_uf = cols
    part = data.drop(["ano", "sigla_uf"])

    dir_part = os.path.join(output, f"ano={ano}", f"sigla_uf={sigla_uf}")
    os.makedirs(dir_part, exist_ok=True)

    part.write_csv(os.path.join(dir_part, "educacao.csv"))

gc.collect()
print("Particionamento concluído.")

# TIPAGEM DE DADOS

fixed_casts = {
    "ano": "Int64",
    "trimestre": "Int64",
    "id_uf": "string",
    "sigla_uf": "string",
    "capital": "string",
    "rm_ride": "string",
    "id_upa": "string",
    "id_estrato": "string",
    "id_domicilio": "string",
    "id_pessoa": "string",
    "v1008": "string",
    "v1014": "string",
    "v1016": "Int64",
    "v1022": "string",
    "v1023": "string",
    "v1027": "float64",
    "v1028": "float64",
    "v1029": "Int64",
    "v1033": "Int64",
    "posest": "string",
    "posest_sxi": "string",
    "v2001": "Int64",
    "v2003": "Int64",
}

float_range_cols = [f"v1028{str(i).zfill(3)}" for i in range(1, 201)]


def safe_cast_df(df):
    for col, typ in fixed_casts.items():
        if col in df.columns:
            df[col] = df[col].astype(typ)

    for col in float_range_cols:
        if col in df.columns:
            df[col] = df[col].astype("float64")

    return df


def process_file(path):
    print(f"Corrigindo: {path}")

    if path.endswith(".csv"):
        df = pd.read_csv(path, dtype=str)
    elif path.endswith(".parquet"):
        df = pd.read_parquet(path)
    else:
        return

    df = safe_cast_df(df)

    if path.endswith(".csv"):
        df.to_csv(path, index=False)
    else:
        df.to_parquet(path, index=False)


for root, _dirs, files in os.walk(output):
    for f in files:
        if f.endswith(".csv") or f.endswith(".parquet"):
            try:
                process_file(os.path.join(root, f))
            except Exception as e:
                print(f"Erro no arquivo {f}: {e}")

print("\nProcessamento finalizado com sucesso.")

# SUBINDO DADOS PARTICIONADOS NA BD
tb = bd.Table(dataset_id="br_ibge_pnadc", table_id="educacao")

tb.create(
    path=output,  # Só precisa colocar o caminho da pasta raiz da partição
    if_table_exists="replace",
    if_storage_data_exists="replace",
    source_format="csv",
)
