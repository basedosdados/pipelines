# -*- coding: utf-8 -*-
import difflib

import pandas as pd


def getStateLetters(data):
    state_lower = data["estado"].str.lower()
    states = {
        "acre": "AC",
        "alagoas": "AL",
        "amapá": "AP",
        "amazonas": "AM",
        "bahia": "BA",
        "ceará": "CE",
        "distrito federal": "DF",
        "espírito santo": "ES",
        "goiás": "GO",
        "maranhão": "MA",
        "mato grosso": "MT",
        "mato grosso do sul": "MS",
        "minas gerais": "MG",
        "pará": "PA",
        "paraíba": "PB",
        "paraná": "PR",
        "pernambuco": "PE",
        "piauí": "PI",
        "rio de janeiro": "RJ",
        "rio grande do norte": "RN",
        "rio grande do sul": "RS",
        "rondônia": "RO",
        "roraima": "RR",
        "santa catarina": "SC",
        "são paulo": "SP",
        "sergipe": "SE",
        "tocantins": "TO",
    }
    data["sigla_uf"] = state_lower.replace(states)
    data = data.drop("estado", axis=1)
    return data


def getCityUfField(data):
    data["cidade_uf"] = (
        data["municipio"].str.title() + " - " + data["sigla_uf"]
    )
    return data[["cidade_uf"]]


def getUniqueCities(data):
    data["cidade_uf"] = data["cidade_uf"].drop_duplicates()
    return data.dropna()


def getInpeCities():
    year_data = pd.read_csv("./input/year_fire_data.csv")[
        ["municipio", "estado"]
    ]
    month_data = pd.read_csv("./input/month_fire_data.csv")[
        ["municipio", "estado"]
    ]

    cities = pd.concat([month_data, year_data], axis=0)
    cities = getStateLetters(cities)
    cities = getCityUfField(cities)
    cities = getUniqueCities(cities)

    return cities


def getDirCities():
    dir_data = pd.read_csv(
        "./extra/diretorio_municipios.csv"
    )  # Tabela de Municípios do Diretório da Base dos Dados
    dir_data["cidade_uf"] = dir_data["nome"] + " - " + dir_data["sigla_uf"]
    return dir_data


def getSimilarCities(city, dir_data):
    results = difflib.get_close_matches(
        city, dir_data["cidade_uf"], cutoff=0.8
    )
    if len(results) > 0:
        return results[0]
    else:
        return None


def mapCityNames(inpe_data, dir_data):
    inpe_data["cidade_uf_dir"] = inpe_data["cidade_uf"].apply(
        lambda x: getSimilarCities(x, dir_data)
    )
    inpe_data["cidade_uf"] = inpe_data["cidade_uf"].str.upper()
    return inpe_data


if __name__ == "__main__":
    inpe_data = getInpeCities()
    dir_data = getDirCities()
    inpe_data = mapCityNames(inpe_data, dir_data)

    # Após a geração do arquivo, recomenda-se uma validação manual para checar
    # se os casos onde não houveram um mapeamento exato do nome do município
    # geraram resultados satisfatórios
    inpe_data.to_csv("./extra/auxiliary_files/map_test_2.csv")
