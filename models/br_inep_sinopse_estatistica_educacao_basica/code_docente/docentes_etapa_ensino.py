# -*- coding: utf-8 -*-
import os

import basedosdados as bd
import numpy as np
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

INPUT = os.path.join(os.getcwd(), "input")
OUTPUT = os.path.join(os.getcwd(), "output")

# os.makedirs(INPUT, exist_ok=True)
# os.makedirs(OUTPUT, exist_ok=True)

RENAMES_ETAPA_ENSINO_SERIE = {
    "Educacao Basica": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola11": "Educação Infantil - Pré Escola",
        "Total12": "",
        "Anos Iniciais13": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais14": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico": "Ensino Médio - Propedêutico",
        "Ensino Médio Normal/Magistério": "Ensino Médio - Normal/Magistério",
        "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Fundamental22": "EJA - Ensino Fundamental",
        "Ensino Médio23": "EJA - Ensino Médio",
        "Classes Comuns25": "Educação Especial - Classes Comuns",
        "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
        #####
        # Para valores anteriores a 2016 e 2013
        ####
        # "Classes Comuns": "Educação Especial - Classes Comuns",
        # "Classes Exclusivas": "Educação Especial - Classes Exclusivas",
    },
    "Educacao Infantil": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": "Creche - Pública",
        "Federal": "Creche - Federal",
        "Estadual": "Creche - Estadual",
        "Municipal": "Creche - Municipal",
        "Privada": "Creche - Privada",
        "Pública.1": "Pré-Escola - Pública",
        "Federal.1": "Pré-Escola - Federal",
        "Estadual.1": "Pré-Escola - Estadual",
        "Municipal.1": "Pré-Escola - Municipal",
        "Privada.1": "Pré-Escola - Privada",
    },
    "Ensino Fundamental": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": "Anos Iniciais - Pública",
        "Federal": "Anos Iniciais - Federal",
        "Estadual": "Anos Iniciais - Estadual",
        "Municipal": "Anos Iniciais - Municipal",
        "Privada": "Anos Iniciais - Privada",
        "Pública.1": "Anos Finais - Pública",
        "Federal.1": "Anos Finais - Federal",
        "Estadual.1": "Anos Finais - Estadual",
        "Municipal.1": "Anos Finais - Municipal",
        "Privada.1": "Anos Finais - Privada",
        "Pública.2": "Turmas Multi - Pública",
        "Federal.2": "Turmas Multi - Federal",
        "Estadual.2": "Turmas Multi - Estadual",
        "Municipal.2": "Turmas Multi - Municipal",
        "Privada.2": "Turmas Multi - Privada",
    },
    "Educacao Profissional":  # {
    # "Unnamed: 1": "uf",
    # "Unnamed: 3": "id_municipio",
    # "Pública": "Curso Técnico Integrado (Ensino Médio Integrado) - Pública",
    # "Federal": "Curso Técnico Integrado (Ensino Médio Integrado) - Federal",
    # "Estadual": "Curso Técnico Integrado (Ensino Médio Integrado) - Estadual",
    # "Municipal": "Curso Técnico Integrado (Ensino Médio Integrado) - Municipal",
    # "Privada": "Curso Técnico Integrado (Ensino Médio Integrado) - Privada",
    # "Pública.1": "Ensino Médio Normal/Magistério - Pública",
    # "Federal.1": "Ensino Médio Normal/Magistério - Federal",
    # "Estadual.1": "Ensino Médio Normal/Magistério - Estadual",
    # "Municipal.1": "Ensino Médio Normal/Magistério - Municipal",
    # "Privada.1": "Ensino Médio Normal/Magistério - Privada",
    # "Pública.2": "Curso Técnico Concomitante - Pública",
    # "Federal.2": "Curso Técnico Concomitante - Federal",
    # "Estadual.2": "Curso Técnico Concomitante - Estadual",
    # "Municipal.2": "Curso Técnico Concomitante - Municipal",
    # "Privada.2": "Curso Técnico Concomitante - Privada",
    # "Pública.3": "Curso Técnico Subsequente - Pública",
    # "Federal.3": "Curso Técnico Subsequente - Federal",
    # "Estadual.3": "Curso Técnico Subsequente - Estadual",
    # "Municipal.3": "Curso Técnico Subsequente - Municipal",
    # "Privada.3": "Curso Técnico Subsequente - Privada",
    # "Pública.4": "Curso Técnico Misto (Concomitante e Subsequente) - Pública",
    # "Federal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Federal",
    # "Estadual.4": "Curso Técnico Misto (Concomitante e Subsequente) - Estadual",
    # "Municipal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Municipal",
    # "Privada.4": "Curso Técnico Misto (Concomitante e Subsequente) - Privada",
    # "Pública.5": "Curso Técnico Integrado a EJA - Pública",
    # "Federal.5": "Curso Técnico Integrado a EJA - Federal",
    # "Estadual.5": "Curso Técnico Integrado a EJA - Estadual",
    # "Municipal.5": "Curso Técnico Integrado a EJA - Municipal",
    # "Privada.5": "Curso Técnico Integrado a EJA - Privada",
    # "Pública.6": "EJA Ensino Fundamental Projovem Urbano - Pública",
    # "Federal.6": "EJA Ensino Fundamental Projovem Urbano - Federal",
    # "Estadual.6": "EJA Ensino Fundamental Projovem Urbano - Estadual",
    # "Municipal.6": "EJA Ensino Fundamental Projovem Urbano - Municipal",
    # "Privada.6": "EJA Ensino Fundamental Projovem Urbano - Privada",
    # "Pública.7": "Curso FIC Concomitante - Pública",
    # "Federal.7": "Curso FIC Concomitante - Federal",
    # "Estadual.7": "Curso FIC Concomitante - Estadual",
    # "Municipal.7": "Curso FIC Concomitante - Municipal",
    # "Privada.7": "Curso FIC Concomitante - Privada",
    # "Pública.8": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Pública",
    # "Federal.8": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Federal",
    # "Estadual.8": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Estadual",
    # "Municipal.8": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Municipal",
    # "Privada.8": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Privada",
    # "Pública.9": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Pública",
    # "Federal.9": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Federal",
    # "Estadual.9": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Estadual",
    # "Municipal.9": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Municipal",
    # "Privada.9": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Privada",
    # },
    #####
    # Valores antes depois de 2018
    ####
    {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": "Curso Técnico Integrado (Ensino Médio Integrado) - Pública",
        "Federal": "Curso Técnico Integrado (Ensino Médio Integrado) - Federal",
        "Estadual": "Curso Técnico Integrado (Ensino Médio Integrado) - Estadual",
        "Municipal": "Curso Técnico Integrado (Ensino Médio Integrado) - Municipal",
        "Privada": "Curso Técnico Integrado (Ensino Médio Integrado) - Privada",
        "Pública.1": "Ensino Médio Normal/Magistério - Pública",
        "Federal.1": "Ensino Médio Normal/Magistério - Federal",
        "Estadual.1": "Ensino Médio Normal/Magistério - Estadual",
        "Municipal.1": "Ensino Médio Normal/Magistério - Municipal",
        "Privada.1": "Ensino Médio Normal/Magistério - Privada",
        "Pública.2": "Curso Técnico Concomitante - Pública",
        "Federal.2": "Curso Técnico Concomitante - Federal",
        "Estadual.2": "Curso Técnico Concomitante - Estadual",
        "Municipal.2": "Curso Técnico Concomitante - Municipal",
        "Privada.2": "Curso Técnico Concomitante - Privada",
        "Pública.3": "Curso Técnico Subsequente - Pública",
        "Federal.3": "Curso Técnico Subsequente - Federal",
        "Estadual.3": "Curso Técnico Subsequente - Estadual",
        "Municipal.3": "Curso Técnico Subsequente - Municipal",
        "Privada.3": "Curso Técnico Subsequente - Privada",
        "Pública.4": "Curso Técnico Misto (Concomitante e Subsequente) - Pública",
        "Federal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Federal",
        "Estadual.4": "Curso Técnico Misto (Concomitante e Subsequente) - Estadual",
        "Municipal.4": "Curso Técnico Misto (Concomitante e Subsequente) - Municipal",
        "Privada.4": "Curso Técnico Misto (Concomitante e Subsequente) - Privada",
        "Pública.5": "Curso Técnico Integrado a EJA - Pública",
        "Federal.5": "Curso Técnico Integrado a EJA - Federal",
        "Estadual.5": "Curso Técnico Integrado a EJA - Estadual",
        "Municipal.5": "Curso Técnico Integrado a EJA - Municipal",
        "Privada.5": "Curso Técnico Integrado a EJA - Privada",
        "Pública.6": "Curso FIC Concomitante - Pública",
        "Federal.6": "Curso FIC Concomitante - Federal",
        "Estadual.6": "Curso FIC Concomitante - Estadual",
        "Municipal.6": "Curso FIC Concomitante - Municipal",
        "Privada.6": "Curso FIC Concomitante - Privada",
        "Pública.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Pública",
        "Federal.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Federal",
        "Estadual.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Estadual",
        "Municipal.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Municipal",
        "Privada.7": "Curso FIC Integrado na Modalidade EJA de Nível Fundamental - Privada",
        "Pública.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Pública",
        "Federal.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Federal",
        "Estadual.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Estadual",
        "Municipal.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Municipal",
        "Privada.8": "Curso FIC Integrado na Modalidade EJA de Nível Médio - Privada",
    },
    "EJA": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Pública": "Ensino Fundamental - Pública",
        "Federal": "Ensino Fundamental - Federal",
        "Estadual": "Ensino Fundamental - Estadual",
        "Municipal": "Ensino Fundamental - Municipal",
        "Privada": "Ensino Fundamental - Privada",
        "Pública.1": "Ensino Médio - Pública",
        "Federal.1": "Ensino Médio - Federal",
        "Estadual.1": "Ensino Médio - Estadual",
        "Municipal.1": "Ensino Médio - Municipal",
        "Privada.1": "Ensino Médio - Privada",
    },
    "Educacao Especial": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola11": "Educação Infantil - Pré Escola",
        "Anos Iniciais13": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais14": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico": "Ensino Médio - Propedêutico",
        "Ensino Médio Normal/Magistério": "Ensino Médio - Normal/Magistério",
        "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Fundamental22": "EJA - Ensino Fundamental",
        "Ensino Médio23": "EJA - Ensino Médio",
        "Classes Comuns25": "Educação Especial - Classes Comuns",
        "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
        #####
        # Para valores anteriores a 2016 e 2013
        ####
        # "Classes Comuns": "Educação Especial - Classes Comuns",
        # "Classes Exclusivas": "Educação Especial - Classes Exclusivas",
    },
    "Educacao Especial - Classes Comuns": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola11": "Educação Infantil - Pré Escola",
        "Anos Iniciais13": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais14": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico": "Ensino Médio - Propedêutico",
        "Ensino Médio  Normal/ Magistério": "Ensino Médio - Normal/Magistério",
        "Ensino Médio Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Fundamental22": "EJA - Ensino Fundamental",
        "Ensino Médio23": "EJA - Ensino Médio",
    },
    "Educacao Especial - Classes Exclusivas": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola11": "Educação Infantil - Pré Escola",
        "Anos Iniciais13": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais14": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico": "Ensino Médio - Propedêutico",
        "Ensino Médio  Normal/ Magistério": "Ensino Médio - Normal/Magistério",
        "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Fundamental22": "EJA - Ensino Fundamental",
        "Ensino Médio23": "EJA - Ensino Médio",
    },
    "Educacao Indigena": {
        "Unnamed: 1": "uf",
        "Unnamed: 3": "id_municipio",
        "Creche": "Educação Infantil - Creche",
        "Pré-Escola11": "Educação Infantil - Pré Escola",
        "Anos Iniciais13": "Ensino Fundamental - Anos Iniciais",
        "Anos Finais14": "Ensino Fundamental - Anos Finais",
        "Ensino Médio Propedêutico": "Ensino Médio - Propedêutico",
        "Ensino Médio Normal/Magistério": "Ensino Médio - Normal/Magistério",
        "Curso Técnico Integrado (Ensino Médio Integrado)": "Ensino Médio - Curso Técnico Integrado",
        "Associada ao Ensino Médio18": "Educação Profissional Técnica de Nível Médio - Associada ao Ensino Médio",
        "Curso Técnico Concomitante": "Educação Profissional Técnica de Nível Médio - Curso Técnico Concomitante",
        "Curso Técnico Subsequente": "Educação Profissional Técnica de Nível Médio - Curso Técnico Subsequente",
        "Curso Técnico Misto (Concomitante e Subsequente)": "Educação Profissional Técnica de Nível Médio - Curso Técnico Misto (Concomitante e Subsequente)",
        "Curso FIC Concomitante": "Educação Profissional - Curso FIC Concomitante",
        "Curso FIC Integrado na Modalidade EJA20": "Educação Profissional - Curso FIC Integrado na Modalidade EJA",
        "Ensino Fundamental22": "EJA - Ensino Fundamental",
        "Ensino Médio23": "EJA - Ensino Médio",
        "Classes Comuns25": "Educação Especial - Classes Comuns",
        "Classes Exclusivas26": "Educação Especial - Classes Exclusivas",
    },
}


etapa_ensino = {
    "educacao_basica": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Basica"],
        "chave": "Educação Básica 2.1",
        "valor": "Educacao Basica",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_infantil": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Infantil"],
        "chave": "Educação Infantil 2.6",
        # "chave": "Educação Infantil 2.5", # Em 2010, a chave é 2.5
        "valor": "Educacao Infantil",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "ensino_fundamental": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Ensino Fundamental"],
        "chave": "Ensino Fundamental 2.16",
        # "chave": "Ensino Fundamental 2.13", # Em 2010, a chave é 2.13
        "valor": "Ensino Fundamental",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_profissional": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Profissional"],
        "chave": "Educação Profissional 2.33",
        # "chave": "Educação Profissional 2.26", # Em 2010, a chave é 26
        "valor": "Ensino Profissional",
        "skiprows": 9,
        "table": "docente_etapa_ensino",
    },
    "EJA": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["EJA"],
        "chave": "EJA 2.38",
        # "chave": "EJA 2.30", # Em 2010, a chave é 2.30
        "valor": "EJA",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_especial": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Especial"],
        "chave": "Educação Especial 2.43",
        # "chave": "Educação Especial 2.34", # Em 2010, a chave é 2.34
        "valor": "Educacao Especial",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_especial_classes_comuns": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educacao Especial - Classes Comuns"
        ],
        "chave": "Classes Comuns 2.44",
        # "chave": "Classes Comuns 2.35", # Em 2010, a chave é 2.35
        "valor": "Educacao Especial - Classes Comuns",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_especial_classes_exclusivas": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE[
            "Educacao Especial - Classes Exclusivas"
        ],
        "chave": "Classes Exclusivas 2.50",
        # "chave": "Classes Exclusivas 2.49", # Em 2011, a chave é 2.49
        # "chave" : "Classes Exclusivas 2.39", # Em 2010, a chave é 2.39
        "valor": "Educacao Especial - Classes Exclusivas",
        "skiprows": 8,
        "table": "docente_etapa_ensino",
    },
    "educacao_indigena": {
        "dicionario": RENAMES_ETAPA_ENSINO_SERIE["Educacao Indigena"],
        "chave": "Educação Indígena 2.56",
        "valor": "Educacao Indigena",
        "skiprows": 9,
        "table": "docente_etapa_ensino",
    },
}


def read_sheet(
    table: str,
    ano: int,
    chave: str,
    valor: str,
    dicionario: dict,
    skiprows: int = 9,
) -> pd.DataFrame:
    print("Tratando dados de", valor)
    df = pd.read_excel(
        os.path.join(
            INPUT,
            f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
            f"Sinopse_Estatistica_da_Educacao_Basica_{ano}.xlsx",
        ),
        skiprows=skiprows,
        sheet_name=chave,
    )

    sheets_etapa_ensino_serie = {chave: valor}

    dfs_etapa_ensino_serie = {
        name: pd.read_excel(
            os.path.join(
                INPUT,
                f"Sinopse_Estatistica_da_Educacao_Basica_{ano}",
                f"Sinopse_Estatistica_da_Educacao_Basica_{ano}.xlsx",
            ),
            skiprows=skiprows,
            sheet_name=sheet_name,
        )
        for sheet_name, name in sheets_etapa_ensino_serie.items()
    }

    dataframes = {}
    for table_name, columns in dfs_etapa_ensino_serie.items():
        df = pd.DataFrame(columns)
        dataframes[table_name] = df

    print(df.columns)

    def drop_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
        cols_drop = [
            col
            for col in df.columns
            if col.startswith("Unnamed") or col.startswith("Total")
        ]

        return df.drop(columns=cols_drop)

    dfs_etapa_ensino_serie = {
        name: drop_unused_columns(
            df.rename(columns=dicionario, errors="raise")
        )
        for name, df in dfs_etapa_ensino_serie.items()
    }

    df_etapa_ensino = pd.concat(
        [
            df.pipe(
                lambda d: d.loc[
                    (d["id_municipio"].notna()) & (d["id_municipio"] != " "),
                ]
            )
            .pipe(
                lambda d: pd.melt(
                    d,
                    id_vars=["id_municipio", "uf"],
                    value_vars=d.columns.difference(
                        ["id_municipio", "uf"]
                    ).tolist(),  # Convert to list
                    var_name="etapa_ensino",
                    value_name="quantidade_docentes",
                )
            )
            .assign(tipo_classe=tipo_classe)
            for tipo_classe, df in dfs_etapa_ensino_serie.items()
        ]
    )

    df_etapa_ensino["etapa_ensino"] = (
        df_etapa_ensino["etapa_ensino"]
        .str.strip()
        .replace("", np.nan)
        .dropna()
    )

    bd_dir = bd.read_sql(
        "SELECT nome, sigla FROM `basedosdados.br_bd_diretorios_brasil.uf`",
        billing_project_id="basedosdados",
        reauth=False,
    )

    df_etapa_ensino["etapa_ensino"] = (
        df_etapa_ensino["etapa_ensino"].str.strip().replace("", np.nan)
    )

    df_etapa_ensino["quantidade_docentes"] = df_etapa_ensino[
        "quantidade_docentes"
    ].astype(int)

    df_etapa_ensino = df_etapa_ensino[
        pd.notna(df_etapa_ensino["etapa_ensino"])
    ]
    df_etapa_ensino["uf"] = (
        df_etapa_ensino["uf"]
        .apply(lambda uf: uf.strip())
        .replace({i["nome"]: i["sigla"] for i in bd_dir.to_dict("records")})  # type: ignore
    )
    df_etapa_ensino = df_etapa_ensino.rename(
        columns={"uf": "sigla_uf"}, errors="raise"
    )
    for sigla_uf, df in df_etapa_ensino.groupby("sigla_uf"):
        path = os.path.join(
            OUTPUT, f"{table}", f"ano={ano}", f"sigla_uf={sigla_uf}"
        )
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"), index=False, mode="w"
            )
        else:
            df.drop(columns=["sigla_uf"]).to_csv(
                os.path.join(path, "data.csv"),
                index=False,
                mode="a",
                header=False,
            )

    return df_etapa_ensino


lista = [
    "educacao_basica",
    "educacao_infantil",
    "ensino_fundamental",
    "educacao_profissional",
    "EJA",
    "educacao_especial",
    "educacao_especial_classes_comuns",
    "educacao_especial_classes_exclusivas",
    # "educacao_indigena",
]

for x in lista:
    read_sheet(
        table=etapa_ensino[x]["table"],
        ano=2021,
        chave=etapa_ensino[x]["chave"],
        valor=etapa_ensino[x]["valor"],
        dicionario=etapa_ensino[x]["dicionario"],
        skiprows=etapa_ensino[x]["skiprows"],
    )
