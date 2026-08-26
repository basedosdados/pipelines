"""Constantes do conjunto br_fnde_fundeb — Indicadores do SIOPE (FNDE).

Indicadores legais e educacionais calculados pelo FNDE a partir das declarações
bimestrais de estados, DF e municípios no SIOPE.

O contexto da fonte e as decisões de desenho estão em
`pipelines/datasets/br_fnde_fundeb/README.md`.
"""

from enum import Enum

_UNIT_PERCENT = "percentual"
_UNIT_CURRENCY = "reais"

# Unidade do VAL_INDI por par (esfera, COD_INDI). A fonte mistura percentual e
# reais na mesma coluna, e a chave é o par porque o mesmo COD_INDI designa
# indicadores diferentes em cada esfera: 27 códigos aparecem nas duas, e nos 27
# o indicador é outro.
#
# São 57 percentuais, 36 em reais e 2 sem unidade — os do grupo 5 (IDEB), que
# somam duas linhas na série inteira e não preenchem nenhuma das duas colunas de
# valor. O comentário ao lado de cada entrada é o COD_EXIB.
#
# O mapa é fechado: indicador ausente daqui levanta KeyError em vez de cair num
# default. A fonte cria indicadores novos ao longo do tempo (o 1.9 apareceu em
# 2026), e um default silencioso gravaria reais na coluna de percentual.
_INDICATOR_UNITS = {
    # Estadual
    ("Estadual", "5"): _UNIT_PERCENT,  # 1.5
    ("Estadual", "40"): _UNIT_PERCENT,  # 1.1
    ("Estadual", "42"): _UNIT_PERCENT,  # 1.2
    ("Estadual", "43"): _UNIT_PERCENT,  # 1.3
    ("Estadual", "44"): _UNIT_PERCENT,  # 1.4
    ("Estadual", "45"): _UNIT_PERCENT,  # 2.1
    ("Estadual", "46"): _UNIT_PERCENT,  # 2.2
    ("Estadual", "47"): _UNIT_PERCENT,  # 2.3
    ("Estadual", "48"): _UNIT_PERCENT,  # 2.4
    ("Estadual", "49"): _UNIT_PERCENT,  # 2.5
    ("Estadual", "50"): _UNIT_PERCENT,  # 2.6
    ("Estadual", "51"): _UNIT_PERCENT,  # 2.7
    ("Estadual", "52"): _UNIT_PERCENT,  # 2.8
    ("Estadual", "53"): _UNIT_PERCENT,  # 2.9
    ("Estadual", "54"): _UNIT_CURRENCY,  # 2.10
    ("Estadual", "55"): _UNIT_PERCENT,  # 2.11
    ("Estadual", "56"): _UNIT_PERCENT,  # 2.12
    ("Estadual", "59"): _UNIT_PERCENT,  # 3.1
    ("Estadual", "60"): _UNIT_PERCENT,  # 3.2
    ("Estadual", "63"): _UNIT_PERCENT,  # 3.5
    ("Estadual", "64"): _UNIT_PERCENT,  # 3.6
    ("Estadual", "65"): _UNIT_CURRENCY,  # 4.1
    ("Estadual", "66"): _UNIT_CURRENCY,  # 4.2
    ("Estadual", "67"): _UNIT_CURRENCY,  # 4.3
    ("Estadual", "68"): _UNIT_CURRENCY,  # 4.4
    ("Estadual", "69"): _UNIT_CURRENCY,  # 4.5
    ("Estadual", "70"): _UNIT_CURRENCY,  # 4.6
    ("Estadual", "71"): _UNIT_CURRENCY,  # 4.7
    ("Estadual", "72"): _UNIT_CURRENCY,  # 4.8
    ("Estadual", "73"): _UNIT_CURRENCY,  # 4.9
    ("Estadual", "74"): _UNIT_CURRENCY,  # 4.10
    ("Estadual", "75"): _UNIT_CURRENCY,  # 4.11
    ("Estadual", "76"): _UNIT_PERCENT,  # 4.12
    ("Estadual", "77"): _UNIT_PERCENT,  # 4.13
    ("Estadual", "80"): _UNIT_PERCENT,  # 6.1
    ("Estadual", "81"): _UNIT_PERCENT,  # 6.2
    ("Estadual", "82"): _UNIT_PERCENT,  # 6.3
    ("Estadual", "83"): _UNIT_CURRENCY,  # 7.1
    ("Estadual", "84"): _UNIT_CURRENCY,  # 7.2
    ("Estadual", "93"): _UNIT_PERCENT,  # 1.6
    ("Estadual", "94"): _UNIT_CURRENCY,  # 7.3
    ("Estadual", "95"): _UNIT_PERCENT,  # 1.7
    ("Estadual", "96"): _UNIT_CURRENCY,  # 8.1
    ("Estadual", "97"): _UNIT_CURRENCY,  # 8.2
    ("Estadual", "98"): _UNIT_PERCENT,  # 1.8
    ("Estadual", "99"): _UNIT_PERCENT,  # 1.9
    # Municipal
    ("Municipal", "24"): _UNIT_PERCENT,  # 1.1
    ("Municipal", "27"): _UNIT_PERCENT,  # 1.4
    ("Municipal", "28"): _UNIT_PERCENT,  # 2.1
    ("Municipal", "29"): _UNIT_PERCENT,  # 2.2
    ("Municipal", "30"): _UNIT_PERCENT,  # 2.3
    ("Municipal", "31"): _UNIT_PERCENT,  # 2.4
    ("Municipal", "32"): _UNIT_PERCENT,  # 2.5
    ("Municipal", "33"): _UNIT_PERCENT,  # 2.6
    ("Municipal", "34"): _UNIT_PERCENT,  # 2.7
    ("Municipal", "35"): _UNIT_PERCENT,  # 2.8
    ("Municipal", "36"): _UNIT_PERCENT,  # 2.9
    ("Municipal", "38"): _UNIT_PERCENT,  # 3.1
    ("Municipal", "39"): _UNIT_PERCENT,  # 3.2
    ("Municipal", "42"): _UNIT_PERCENT,  # 3.5
    ("Municipal", "43"): _UNIT_PERCENT,  # 3.6
    ("Municipal", "44"): _UNIT_CURRENCY,  # 4.1
    ("Municipal", "45"): _UNIT_CURRENCY,  # 4.2
    ("Municipal", "46"): _UNIT_CURRENCY,  # 4.3
    ("Municipal", "47"): _UNIT_CURRENCY,  # 2.10
    ("Municipal", "48"): _UNIT_PERCENT,  # 2.11
    ("Municipal", "49"): _UNIT_PERCENT,  # 2.12
    ("Municipal", "52"): _UNIT_CURRENCY,  # 4.4
    ("Municipal", "53"): _UNIT_CURRENCY,  # 4.5
    ("Municipal", "54"): _UNIT_CURRENCY,  # 4.6
    ("Municipal", "55"): _UNIT_CURRENCY,  # 4.7
    ("Municipal", "56"): _UNIT_CURRENCY,  # 4.8
    ("Municipal", "57"): _UNIT_CURRENCY,  # 4.9
    ("Municipal", "58"): _UNIT_CURRENCY,  # 4.10
    ("Municipal", "59"): _UNIT_CURRENCY,  # 4.11
    ("Municipal", "60"): _UNIT_PERCENT,  # 4.12
    ("Municipal", "61"): _UNIT_PERCENT,  # 4.13
    ("Municipal", "62"): _UNIT_PERCENT,  # 6.1
    ("Municipal", "63"): _UNIT_PERCENT,  # 6.2
    ("Municipal", "64"): _UNIT_PERCENT,  # 6.3
    ("Municipal", "65"): None,  # 5.1
    ("Municipal", "66"): None,  # 5.2
    ("Municipal", "67"): _UNIT_PERCENT,  # 1.2
    ("Municipal", "68"): _UNIT_PERCENT,  # 1.3
    ("Municipal", "69"): _UNIT_CURRENCY,  # 7.1
    ("Municipal", "70"): _UNIT_CURRENCY,  # 7.2
    ("Municipal", "84"): _UNIT_CURRENCY,  # 4.14
    ("Municipal", "85"): _UNIT_CURRENCY,  # 4.15
    ("Municipal", "89"): _UNIT_PERCENT,  # 1.5
    ("Municipal", "90"): _UNIT_PERCENT,  # 1.6
    ("Municipal", "91"): _UNIT_CURRENCY,  # 7.3
    ("Municipal", "92"): _UNIT_PERCENT,  # 1.7
    ("Municipal", "93"): _UNIT_CURRENCY,  # 8.1
    ("Municipal", "94"): _UNIT_CURRENCY,  # 8.2
    ("Municipal", "95"): _UNIT_PERCENT,  # 1.9
}


# Linhas da tabela `dicionario`: (id_tabela, chave, cobertura_temporal, valor).
# A coluna `nome_coluna` é sempre "id_indicador" e por isso não se repete aqui.
#
# São as 112 linhas observadas na série 2021-2024 (produto 53) mais 2026
# (produto 54). O nome do indicador é texto livre reescrito pela fonte: 9 dos 95
# pares esfera/indicador mudaram de nome, sempre em 2022, e em quatro deles o
# texto de 2021 volta em 2023 — daí as coberturas partidas `(1)2021` e
# `2023(1)`.
#
# Notação `inicio(1)fim`, em que a ponta em branco significa "até onde a tabela
# vai": `(1)` e `2023(1)` seguem válidas quando um ano novo entra na base, e só
# as coberturas fechadas envelhecem.
#
# A lista é mantida à mão, e é editada quando a fonte reescreve um nome ou cria
# um indicador — situação que o `warn_unknown_names` registra em WARNING durante
# a limpeza. Ver a seção "Os nomes mudam de ano para ano" do README do
# conjunto.
_DICTIONARY_ROWS = [
    (
        "indicador_estadual",
        "5",
        "(1)2021",
        "Percentual de aplicação em Despesas de Capital da complementação da União - VAAT - FUNDEB (Minimo de 15%)",
    ),
    (
        "indicador_estadual",
        "5",
        "2022(1)2022",
        "Percentual de aplicação em Despesas de Capital - VAAT - FUNDEB (Minimo de 15%) - Inciso XXIII, Portaria Conjunta MGI/MF/CGU Nº 33 de 30-08-2023",
    ),
    (
        "indicador_estadual",
        "5",
        "2023(1)",
        "Percentual de aplicação em Despesas de Capital da complementação da União - VAAT - FUNDEB (Minimo de 15%)",
    ),
    (
        "indicador_estadual",
        "40",
        "(1)2021",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo de 25% para estados, DF e municípios)",
    ),
    (
        "indicador_estadual",
        "40",
        "2022(1)2022",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo 25% para estados, DF e municípios) - inciso XXI, art. 29, Porta Conj MGI/MF/CGU Nº 33 de 30-08-23",
    ),
    (
        "indicador_estadual",
        "40",
        "2023(1)",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo de 25% para estados, DF e municípios)",
    ),
    (
        "indicador_estadual",
        "42",
        "(1)2021",
        "Percentual de aplicação do FUNDEF ou FUNDEB na remuneração dos profissionais da educação (mínimo de 70%)",
    ),
    (
        "indicador_estadual",
        "42",
        "2022(1)2022",
        "Percentual de aplicação de recursos do FUNDEB na remuneração dos profissionais da educação (mínimo 70% - inciso XI, art. 212-A, da CF/88) - Inciso XXII, art.29, Port Conj MGI/MF/CGU Nº  33 de 30.08.23",
    ),
    (
        "indicador_estadual",
        "42",
        "2023(1)",
        "Percentual de aplicação do FUNDEF ou FUNDEB na remuneração dos profissionais da educação (mínimo de 70%)",
    ),
    (
        "indicador_estadual",
        "43",
        "(1)",
        "Percentual de aplicação do FUNDEF ou FUNDEB em despesas com MDE, que não remuneração do magistério (máximo de 40%)",
    ),
    (
        "indicador_estadual",
        "43",
        "2024(1)2024",
        "Percentual de aplicação do FUNDEF ou FUNDEB em despesas com MDE, que não remuneração do magistério (máximo de 30%)",
    ),
    (
        "indicador_estadual",
        "44",
        "(1)",
        "Percentual das receitas do  FUNDEB não aplicadas no exercício (máximo de10%)",
    ),
    (
        "indicador_estadual",
        "45",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados na educação infantil",
    ),
    (
        "indicador_estadual",
        "46",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados no ensino fundamental",
    ),
    (
        "indicador_estadual",
        "47",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados no ensino médio",
    ),
    (
        "indicador_estadual",
        "48",
        "(1)",
        "Percentual das despesas com educação infantil em relação à despesa total com educação",
    ),
    (
        "indicador_estadual",
        "49",
        "(1)",
        "Percentual das despesas com ensino fundamental em relação à despesa total com educação",
    ),
    (
        "indicador_estadual",
        "50",
        "(1)",
        "Percentual das despesas com ensino médio em relação à despesa total com educação",
    ),
    (
        "indicador_estadual",
        "51",
        "(1)",
        "Percentual das despesas com educação superior em relação à despesa total com educação",
    ),
    (
        "indicador_estadual",
        "52",
        "(1)",
        "Percentual das despesas em educação em relação às despesas de todas as áreas",
    ),
    (
        "indicador_estadual",
        "53",
        "(1)",
        "Percentual das despesas com alimentação escolar em relação à despesa total com educação",
    ),
    (
        "indicador_estadual",
        "54",
        "(1)",
        "Investimento com material didático por aluno da educação básica",
    ),
    (
        "indicador_estadual",
        "55",
        "(1)",
        "Percentual de despesas correntes em educação em relação à despesa total em MDE",
    ),
    (
        "indicador_estadual",
        "56",
        "(1)",
        "Percentual de investimentos de capital em educação em relação à despesa total em MDE",
    ),
    (
        "indicador_estadual",
        "59",
        "(1)",
        "Percentual das despesas com aposentadorias e pensões da área educacional em relação às despesas totais com MDE",
    ),
    (
        "indicador_estadual",
        "60",
        "(1)",
        "Percentual das despesas com pessoal e encargos sociais da área educacional em relação à despesa total com MDE",
    ),
    (
        "indicador_estadual",
        "63",
        "(1)",
        "Percentual das despesas com recursos do FUNDEB com professores em relação à despesa total com MDE",
    ),
    (
        "indicador_estadual",
        "64",
        "(1)",
        "Percentual das despesas com profissionais não docentes em relação à despesa total com MDE",
    ),
    (
        "indicador_estadual",
        "65",
        "(1)",
        "Investimento educacional por aluno da educação infantil",
    ),
    (
        "indicador_estadual",
        "66",
        "(1)",
        "Investimento educacional por aluno do ensino fundamental",
    ),
    (
        "indicador_estadual",
        "67",
        "(1)",
        "Investimento educacional por aluno do ensino médio",
    ),
    (
        "indicador_estadual",
        "68",
        "(1)",
        "Investimento educacional por aluno da educação superior",
    ),
    (
        "indicador_estadual",
        "69",
        "(1)",
        "Investimento educacional por aluno da educação de jovens e adultos",
    ),
    (
        "indicador_estadual",
        "70",
        "(1)",
        "Investimento educacional por aluno da educação especial",
    ),
    (
        "indicador_estadual",
        "71",
        "(1)",
        "Investimento educacional por aluno da educação profissional",
    ),
    (
        "indicador_estadual",
        "72",
        "(1)",
        "Investimento educacional por aluno da educação básica",
    ),
    ("indicador_estadual", "73", "(1)", "Investimento educacional por aluno"),
    (
        "indicador_estadual",
        "74",
        "(1)",
        "Despesa com professores por aluno da educação básica",
    ),
    (
        "indicador_estadual",
        "75",
        "(1)",
        "Despesas com profissionais não docentes da área educacional por aluno da educação básica",
    ),
    (
        "indicador_estadual",
        "76",
        "(1)",
        "Percentual de investimento por aluno da educação superior em relação ao investimento por aluno da educação básica",
    ),
    (
        "indicador_estadual",
        "77",
        "(1)",
        "Percentual de investimento por aluno em relação ao PIB per capita",
    ),
    (
        "indicador_estadual",
        "80",
        "(1)",
        "Percentual das receitas de transferências realizadas pelo FNDE em relação à receita total",
    ),
    (
        "indicador_estadual",
        "81",
        "(1)",
        "Percentual das receitas de impostos em relação à receita total.",
    ),
    (
        "indicador_estadual",
        "82",
        "(1)",
        "Percentual das receitas de transferências constitucionais em relação à receita total.",
    ),
    (
        "indicador_estadual",
        "83",
        "(1)",
        "Superávit/Déficit do ente federado no exercício",
    ),
    (
        "indicador_estadual",
        "84",
        "(1)",
        "Saldo financeiro do FUNDEB no exercício atual",
    ),
    (
        "indicador_estadual",
        "93",
        "(1)2021",
        "Percentual de aplicação em Despesas na Educação Infantil - VAAT - FUNDEB (Proporção 50% do VAAT Total - §3º, art. 212-A, da CF/88, correspondente ao indicador 1.7)",
    ),
    (
        "indicador_estadual",
        "93",
        "2022(1)2022",
        "Percentual de aplicação de recursos da complementação VAAT na Educação Infantil - VAAT- FUNDEB (Proporção 50% - §3º, art. 212-A, da CF/88  )-Inciso XXIV, art.29, Port Conj MGI/MF/CGU Nº 33 de 30.08.23",
    ),
    (
        "indicador_estadual",
        "93",
        "2023(1)",
        "Percentual de aplicação em Despesas na Educação Infantil da complementação da União - VAAT - FUNDEB (Proporção 50% do VAAT Total))",
    ),
    (
        "indicador_estadual",
        "94",
        "(1)",
        "Recursos do FUNDEB do exercício não utilizado",
    ),
    (
        "indicador_estadual",
        "95",
        "(1)",
        "Indicador para Educação Infantil - IEI - Percentual mínimo da complementação VAAT a ser aplicado em educação Infantil",
    ),
    (
        "indicador_estadual",
        "96",
        "(1)",
        "Valor exigido de aplicação de impostos em MDE (Mínimo de 25%)",
    ),
    (
        "indicador_estadual",
        "97",
        "(1)",
        "Valor aplicado em MDE da receita de impostos",
    ),
    (
        "indicador_estadual",
        "98",
        "2022(1)",
        "Percentual de destinação de recursos de impostos e transferências ao Fundeb (mínimo de 20% para estados e DF - inciso II, art.  212-A, da CF/88). Inciso XXV, art. 29, da PC n° 33/2023",
    ),
    (
        "indicador_estadual",
        "99",
        "2026(1)",
        "Percentual de aplicação FUNDEB vinculadas à Fomento ETI (mínimo de 4% para estados, DF e municípios), conforme previsto art.212-A, inciso XV, da CF.",
    ),
    (
        "indicador_municipal",
        "24",
        "(1)2021",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo de 25% para estados, DF e municípios)",
    ),
    (
        "indicador_municipal",
        "24",
        "2022(1)2022",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo 25% para estados, DF e municípios) - inciso XXI, art. 29, Porta Conj MGI/MF/CGU Nº 33 de 30-08-23",
    ),
    (
        "indicador_municipal",
        "24",
        "2023(1)",
        "Percentual de aplicação das receitas de impostos e transferências vinculadas à educação em MDE (mínimo de 25% para estados, DF e municípios)",
    ),
    (
        "indicador_municipal",
        "27",
        "(1)",
        "Percentual das receitas do  FUNDEB não aplicadas no exercício (máximo de 10%)",
    ),
    (
        "indicador_municipal",
        "28",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados na educação infantil",
    ),
    (
        "indicador_municipal",
        "29",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados no ensino fundamental",
    ),
    (
        "indicador_municipal",
        "30",
        "(1)",
        "Percentual dos recursos do FUNDEB aplicados no ensino médio",
    ),
    (
        "indicador_municipal",
        "31",
        "(1)",
        "Percentual das despesas com educação infantil em relação à despesa total com educação",
    ),
    (
        "indicador_municipal",
        "32",
        "(1)",
        "Percentual das despesas com ensino fundamental em relação à despesa total com educação",
    ),
    (
        "indicador_municipal",
        "33",
        "(1)",
        "Percentual das despesas com ensino médio em relação à despesa total com educação",
    ),
    (
        "indicador_municipal",
        "34",
        "(1)",
        "Percentual das despesas com educação superior em relação à despesa total com educação",
    ),
    (
        "indicador_municipal",
        "35",
        "(1)",
        "Percentual das despesas em educação em relação às despesas de todas as áreas",
    ),
    (
        "indicador_municipal",
        "36",
        "(1)",
        "Percentual das despesas com alimentação escolar em relação à despesa total com educação",
    ),
    (
        "indicador_municipal",
        "38",
        "(1)",
        "Percentual das despesas com aposentadorias e pensões da área educacional em relação às despesas totais com MDE",
    ),
    (
        "indicador_municipal",
        "39",
        "(1)",
        "Percentual das despesas com pessoal e encargos sociais da área educacional em relação à despesa total com MDE",
    ),
    (
        "indicador_municipal",
        "42",
        "(1)",
        "Percentual das despesas com recursos do FUNDEB com professores em relação à despesa total com MDE",
    ),
    (
        "indicador_municipal",
        "43",
        "(1)",
        "Percentual das despesas com recursos do FUNDEB com profissionais não docentes em relação à despesa total com MDE",
    ),
    (
        "indicador_municipal",
        "44",
        "(1)",
        "Investimento educacional por aluno da educação infantil",
    ),
    (
        "indicador_municipal",
        "45",
        "(1)",
        "Investimento educacional por aluno do ensino fundamental",
    ),
    (
        "indicador_municipal",
        "46",
        "(1)",
        "Investimento educacional por aluno do ensino médio",
    ),
    (
        "indicador_municipal",
        "47",
        "(1)",
        "Investimento com material didático por aluno da educação básica",
    ),
    (
        "indicador_municipal",
        "48",
        "(1)",
        "Percentual de despesas correntes em educação em relação à despesa total em MDE",
    ),
    (
        "indicador_municipal",
        "49",
        "(1)",
        "Percentual de investimentos de capital em educação em relação à despesa total em MDE",
    ),
    (
        "indicador_municipal",
        "52",
        "(1)",
        "Investimento educacional por aluno da educação superior",
    ),
    (
        "indicador_municipal",
        "53",
        "(1)",
        "Investimento educacional por aluno da educação de jovens e adultos",
    ),
    (
        "indicador_municipal",
        "54",
        "(1)",
        "Investimento educacional por aluno da educação especial",
    ),
    (
        "indicador_municipal",
        "55",
        "(1)",
        "Investimento educacional por aluno da educação profissional",
    ),
    (
        "indicador_municipal",
        "56",
        "(1)",
        "Investimento educacional por aluno da educação básica",
    ),
    ("indicador_municipal", "57", "(1)", "Investimento educacional por aluno"),
    (
        "indicador_municipal",
        "58",
        "(1)",
        "Despesa com professores por aluno da educação básica",
    ),
    (
        "indicador_municipal",
        "59",
        "(1)",
        "Despesas com profissionais não docentes da área educacional por aluno da educação básica",
    ),
    (
        "indicador_municipal",
        "60",
        "(1)",
        "Percentual de investimento por aluno da educação superior em relação ao investimento por aluno da educação básica",
    ),
    (
        "indicador_municipal",
        "61",
        "(1)",
        "Percentual de investimento por aluno em relação ao PIB per capita",
    ),
    (
        "indicador_municipal",
        "62",
        "(1)",
        "Percentual das receitas de transferências realizadas pelo FNDE em relação à receita total",
    ),
    (
        "indicador_municipal",
        "63",
        "(1)",
        "Percentual das receitas de impostos em relação à receita total.",
    ),
    (
        "indicador_municipal",
        "64",
        "(1)",
        "Percentual das receitas de transferências constitucionais em relação à receita total.",
    ),
    (
        "indicador_municipal",
        "65",
        "2023(1)2023",
        "Índice de Desenvolvimento da Educação Básica - IDEB - Séries Iniciais",
    ),
    (
        "indicador_municipal",
        "66",
        "2023(1)2023",
        "Índice de Desenvolvimento da Educação Básica - IDEB - Séries Finais",
    ),
    (
        "indicador_municipal",
        "67",
        "(1)2021",
        "Percentual de aplicação do FUNDEB na remuneração dos profissionais da educação (mínimo de 70%)",
    ),
    (
        "indicador_municipal",
        "67",
        "2022(1)2022",
        "Percentual de aplicação de recursos do FUNDEB na remuneração dos profissionais da educação (mínimo 70% - inciso XI, art. 212-A, da CF/88) - Inciso XXII, art.29, Port Conj MGI/MF/CGU Nº  33 de 30.08.23",
    ),
    (
        "indicador_municipal",
        "67",
        "2023(1)",
        "Percentual de aplicação do FUNDEB na remuneração dos profissionais da educação (mínimo de 70%) - Inciso XXII, Portaria Conjunta MGI/MF/CGU Nº 33 de30-08-2023",
    ),
    (
        "indicador_municipal",
        "68",
        "(1)",
        "Percentual de aplicação do FUNDEB em despesas com MDE, que não remuneração dos profissionais da educação (máximo de 30%)",
    ),
    (
        "indicador_municipal",
        "69",
        "(1)",
        "Superávit/Déficit do ente federado no exercício",
    ),
    (
        "indicador_municipal",
        "70",
        "(1)",
        "Saldo financeiro do FUNDEB no exercício atual",
    ),
    (
        "indicador_municipal",
        "84",
        "(1)",
        "Investimento educacional por aluno da educação infantil - creche",
    ),
    (
        "indicador_municipal",
        "85",
        "(1)",
        "Investimento educacional por aluno da educação infantil - pre-escola",
    ),
    (
        "indicador_municipal",
        "89",
        "(1)2021",
        "Percentual de aplicação em Despesas de Capital da complementação da União - VAAT - FUNDEB (Minimo de 15%)",
    ),
    (
        "indicador_municipal",
        "89",
        "2022(1)2022",
        "Percentual de aplicação em Despesas de Capital - VAAT - FUNDEB (Minimo de 15%) - Inciso XXIII, Portaria Conjunta MGI/MF/CGU Nº 33 de 30-08-2023",
    ),
    (
        "indicador_municipal",
        "89",
        "2023(1)",
        "Percentual de aplicação em Despesas de Capital - VAAT - FUNDEB (Minimo de 15%) - Inciso XXIII, Portaria Conjunta MGI/MF/CGU Nº 33 de30-08-2023",
    ),
    (
        "indicador_municipal",
        "90",
        "(1)2021",
        "Percentual de aplicação em Despesas na Educação Infantil  - VAAT - FUNDEB (Proporção 50% do VAAT Total - §3º, art. 212-A, da CF/88, correspondente ao indicador 1.7)",
    ),
    (
        "indicador_municipal",
        "90",
        "2022(1)2022",
        "Percentual de aplicação de recursos da complementação VAAT na Educação Infantil - VAAT- FUNDEB (Proporção 50% - §3º, art. 212-A, da CF/88  )-Inciso XXIV, art.29, Port Conj MGI/MF/CGU Nº 33 de 30.08.23",
    ),
    (
        "indicador_municipal",
        "90",
        "2023(1)",
        "Percentual de aplicação em Despesas na Educação Infantil  - VAAT - FUNDEB (Proporção 50% do VAAT Total) - Correspondente ao indicador 1.7 - Inciso XXIV, Portaria Conjunta MGI/MF/CGU Nº 33 de30-08-2023",
    ),
    (
        "indicador_municipal",
        "91",
        "(1)",
        "Recursos do FUNDEB do exercício não utilizado",
    ),
    (
        "indicador_municipal",
        "92",
        "(1)",
        "Indicador para Educação Infantil - IEI - Percentual mínimo da complementação VAAT a ser aplicado em educação Infantil",
    ),
    (
        "indicador_municipal",
        "93",
        "(1)",
        "Valor exigido de aplicação de impostos em MDE (Mínimo de 25%)",
    ),
    (
        "indicador_municipal",
        "94",
        "(1)",
        "Valor aplicado em MDE da receita de impostos",
    ),
    (
        "indicador_municipal",
        "95",
        "2026(1)",
        "Percentual de aplicação FUNDEB vinculadas à Fomento ETI (mínimo de 4% para estados, DF e municípios), conforme previsto art.212-A, inciso XV, da CF.",
    ),
]


class constants(Enum):
    """Constantes do conjunto br_fnde_fundeb.

    Nome da classe em minúsculo segue a convenção do repo para enums de
    constantes de dataset.
    """

    DATASET_ID = "br_fnde_fundeb"

    TABLE_STATE = "indicador_estadual"
    TABLE_MUNICIPALITY = "indicador_municipal"
    TABLE_DICTIONARY = "dicionario"
    ALL_TABLES = ["indicador_estadual", "indicador_municipal", "dicionario"]

    # Formato das datas trocadas com o backend (poll e update da fonte).
    DATE_FORMAT = "%Y-%m-%d"

    # A Plataforma Antonieta de Barros é uma SPA: a página do produto não carrega
    # link de download. O arquivo sai deste endpoint da API.
    #
    # Restrições medidas: `HEAD` responde 405, a resposta não traz
    # `Last-Modified`, e `Range` é ignorado (download interrompido recomeça do
    # zero).
    API_BASE = "https://www.fnde.gov.br/plataforma-antonieta-de-barros-api"
    ARTIFACT_URL = "{api}/products/data-products/{product_id}/artifact"
    PRODUCT_URL = "{api}/products/data-products/{product_id}"

    # Os dois produtos que compõem a série: o 53 cobre 2021 a 2024 com os 6
    # bimestres fechados, o 54 cobre o exercício corrente. Nenhum dos dois
    # publica 2025 (ver "O hiato de 2025" no README do conjunto).
    PRODUCT_HISTORY = 53
    PRODUCT_CURRENT = 54

    # Nome do arquivo dentro de cada produto, usado para nomear o download.
    PRODUCT_FILENAMES = {
        53: "Indicadores_SIOPE_ate_2024.txt.gz",
        54: "Indicadores_SIOPE.txt.gz",
    }

    SEPARATOR = ";"
    ENCODING = "utf-8"

    # Cabeçalho declarado pela fonte, idêntico nos dois produtos.
    SOURCE_HEADER = [
        "TIPO",
        "NUM_ANO",
        "NUM_PERI",
        "COD_UF",
        "SIG_UF",
        "COD_MUNI",
        "NOM_MUNI",
        "COD_INDI",
        "COD_EXIB",
        "NOM_INDI",
        "COD_GRUP",
        "NOM_GRUP_INDI",
        "VAL_INDI",
        "DT_ATUALIZACAO",
    ]

    # Em linha `TIPO=Estadual` os campos COD_MUNI e NOM_MUNI são omitidos, não
    # enviados vazios: a linha tem 12 campos, não 14. Daí dois mapas de índice,
    # escolhidos pela contagem de campos da linha.
    FIELDS_STATE = 12
    FIELDS_MUNICIPALITY = 14

    INDEX_MUNICIPALITY = {
        "TIPO": 0,
        "NUM_ANO": 1,
        "NUM_PERI": 2,
        "COD_UF": 3,
        "SIG_UF": 4,
        "COD_MUNI": 5,
        "NOM_MUNI": 6,
        "COD_INDI": 7,
        "COD_EXIB": 8,
        "NOM_INDI": 9,
        "COD_GRUP": 10,
        "NOM_GRUP_INDI": 11,
        "VAL_INDI": 12,
        "DT_ATUALIZACAO": 13,
    }

    INDEX_STATE = {
        "TIPO": 0,
        "NUM_ANO": 1,
        "NUM_PERI": 2,
        "COD_UF": 3,
        "SIG_UF": 4,
        "COD_INDI": 5,
        "COD_EXIB": 6,
        "NOM_INDI": 7,
        "COD_GRUP": 8,
        "NOM_GRUP_INDI": 9,
        "VAL_INDI": 10,
        "DT_ATUALIZACAO": 11,
    }

    # Valores da coluna TIPO, que decide em qual tabela a linha cai.
    TIPO_STATE = "Estadual"
    TIPO_MUNICIPALITY = "Municipal"

    # Ordem das colunas de cada tabela, espelhando a arquitetura. É o que o
    # schema do parquet de staging carrega.
    COLUMNS_STATE = [
        "ano",
        "bimestre",
        "sigla_uf",
        "id_indicador",
        "codigo_indicador",
        "valor_percentual",
        "valor_real",
    ]

    COLUMNS_MUNICIPALITY = [
        "ano",
        "bimestre",
        "sigla_uf",
        "id_municipio",
        "id_indicador",
        "codigo_indicador",
        "valor_percentual",
        "valor_real",
    ]

    COLUMNS_DICTIONARY = [
        "id_tabela",
        "nome_coluna",
        "chave",
        "cobertura_temporal",
        "valor",
    ]

    # Valor fixo da coluna `nome_coluna` do dicionário: a coluna que cada chave
    # decodifica nas duas tabelas de fato.
    DICTIONARY_COLUMN = "id_indicador"

    DICTIONARY_ROWS = _DICTIONARY_ROWS

    # Coluna de particionamento das duas tabelas de fato.
    PARTITION_COLUMNS = ["ano"]

    UNIT_PERCENT = _UNIT_PERCENT
    UNIT_CURRENCY = _UNIT_CURRENCY
    INDICATOR_UNITS = _INDICATOR_UNITS
