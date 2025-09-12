from enum import Enum
from typing import ClassVar


class constants(Enum):
    URLS: ClassVar[dict[str, str]] = {
        "populacao_grupos_idade": "https://apisidra.ibge.gov.br/values/t/1209/n3/all/v/allxp/p/all/c58/all",
        "populacao_residente": "https://apisidra.ibge.gov.br/values/t/9514/n6/all/v/allxp/p/all/c2/all/c287/allxt/c286/allxt",
        "indice_envelhecimento": "https://apisidra.ibge.gov.br/values/t/9515/n3/all/n6/all/v/all/p/all/d/v8845%202,v10612%202",
        "populacao_residente_variacao_absoluta": "https://apisidra.ibge.gov.br/values/t/4709/n6/all/v/all/p/all/d/v10605%202",
        "area_territorial_densidade_demografica": "https://apisidra.ibge.gov.br/values/t/4714/n6/all/v/all/p/all/d/v614%202",
        "domicilios_recenseados_especie": "https://apisidra.ibge.gov.br/values/t/4711/n6/all/v/allxp/p/all/c3/allxt",
        "domicilios_moradores": "https://apisidra.ibge.gov.br/values/t/4712/n6/all/v/all/p/all/d/v5930%202",
        "populacao_residente_quilombola": "https://apisidra.ibge.gov.br/values/t/9578/n6/all/v/93,4709/p/all/c2661/allxt",
        "domicilios_moradores_quilombolas": "https://apisidra.ibge.gov.br/values/t/9727/n6/all/v/381,382,5930,7085,7097/p/all/c2661/allxt/d/v5930%202,v7085%202",
        "domicilios_pelo_menos_um_morador_quilombola": "https://apisidra.ibge.gov.br/values/t/9724/n6/all/v/7081,7082,7083,7084,7085/p/all/c2661/allxt/d/v7084%202,v7085%202",
        "populacao_residente_territorios_quilombolas": "https://apisidra.ibge.gov.br/values/t/9723/n145/all/v/6559,7079/p/all",
        "domicilios_moradores_territorios_quilombolas": "https://apisidra.ibge.gov.br/values/t/9725/n145/all/v/7086,7087,7091,7095,7096/p/all/d/v7095%202,v7096%202",
        "domicilios_pelo_menos_um_territorios_quilombolas": "https://apisidra.ibge.gov.br/values/t/9726/n145/all/v/7081,7082,7083,7084,7085/p/all/d/v7084%202,v7085%202",
        "populacao_residente_indigena_municipio": "https://apisidra.ibge.gov.br/values/t/9718/n6/all/v/93,350/p/all/c1714/allxt/c2661/allxt",
        "domicilios_moradores_indigenas_municipio": "https://apisidra.ibge.gov.br/values/t/9728/n6/all/v/381,382,5930,6554,8691/p/all/c2661/allxt/d/v5930%202,v6554%202",
        "domicilios_pelo_menos_um_morador_indigenas_municipio": "https://apisidra.ibge.gov.br/values/t/9720/n6/all/v/5938,6554,7088,7089,7090/p/all/c2661/allxt/d/v5938%202,v6554%202",
        "alfabetizacao_grupo_idade_sexo_raca": "https://apisidra.ibge.gov.br/values/t/9542/n6/all/v/allxp/p/all/c59/allxt/c2/allxt/c86/allxt/c287/2999,3000,9482,9483,9484,93086,93087",
        "favela_comunidade_urbana_populacao_domicilio": "https://apisidra.ibge.gov.br/values/t/9887/n170/all/v/allxp/p/all/c3/59993",
        "favela_comunidade_urbana_alfabetizacao": "https://apisidra.ibge.gov.br/values/t/9886/n170/all/v/1009907/p/all/c59/1023/c86/95251/c2/6794/c58/95253/d/v1009907%202",
        "favela_comunidade_urbana_canalizacao_agua": "https://apisidra.ibge.gov.br/values/t/9897/n170/all/v/1009914/p/all/c1817/allxt/c301/72053/c86/95251/c2/6794/c58/95253/d/v1009914%202",
        "favela_comunidade_urbana_banheiro": "https://apisidra.ibge.gov.br/values/t/9898/n170/all/v/1009914/p/all/c458/12032,12046,72118,72119/c11558/46292/c86/95251/c2/6794/c58/95253/d/v1009914%202",
        "favela_comunidade_urbana_destino_lixo": "https://apisidra.ibge.gov.br/values/t/9899/n170/all/v/1009914/p/all/c67/1091,2520,72122,72123,72124/c86/95251/c2/6794/c58/95253/d/v1009914%202",
        "favela_comunidade_urbana_indice_envelhecimento": "https://apisidra.ibge.gov.br/values/t/9885/n170/all/v/all/p/all/c86/95251/d/v9614%202",
        "favela_comunidade_urbana_domicilio_particular_coletivo": "https://apisidra.ibge.gov.br/values/t/9887/n170/all/v/9909/p/all/c3/2504,2505,10004,10005,60001",
        "favela_comunidade_urbana_populacao_cor_raca": "https://apisidra.ibge.gov.br/values/t/9884/n170/all/v/1009612/p/all/c86/allxt/c2/6794/c58/95253/d/v1009612%202",
        "populacao_grupo_idade_sexo_raca_favela_comunidade_urbana": "https://apisidra.ibge.gov.br/values/t/9884/n170/all/v/allxp/p/all/c86/allxt/c2/allxt/c58/allxt",
    }

    # NOTE: CNEFE não será baixado do SIDRA
    CNEFE_FTP_URL = "http://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/Censo_Demografico_2022/Arquivos_CNEFE/UF"

    CNEFE_FILE_NAMES: ClassVar[dict[str, str]] = {
        "RO": "11_RO.zip",
        "AC": "12_AC.zip",
        "AM": "13_AM.zip",
        "RR": "14_RR.zip",
        "PA": "15_PA.zip",
        "AP": "16_AP.zip",
        "TO": "17_TO.zip",
        "MA": "21_MA.zip",
        "PI": "22_PI.zip",
        "CE": "23_CE.zip",
        "RN": "24_RN.zip",
        "PB": "25_PB.zip",
        "PE": "26_PE.zip",
        "AL": "27_AL.zip",
        "SE": "28_SE.zip",
        "BA": "29_BA.zip",
        "MG": "31_MG.zip",
        "ES": "32_ES.zip",
        "RJ": "33_RJ.zip",
        "SP": "35_SP.zip",
        "PR": "41_PR.zip",
        "SC": "42_SC.zip",
        "RS": "43_RS.zip",
        "MS": "50_MS.zip",
        "MT": "51_MT.zip",
        "GO": "52_GO.zip",
        "DF": "53_DF.zip",
    }
