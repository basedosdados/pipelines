from enum import Enum


class constants(Enum):
    tabelas = {
        "registros_novos_cac": {  # ! 1
            "save_table": "models/br_sou_da_paz_armas_municoes/output/registros_novos_cac.csv",
            "real_file_id": "17763cN6OyBnMGvVOJM4fYMBhBHYK2O_l",
            "sheet_name": "1-CAC novos registros ",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1D7Kgw0XrSl4Hf__CXDvJkgNbVHaFtWMpMH3XyHNB_wg/edit#gid=0",
        },
        "registros_ativos_cac": {  # ! 2
            "save_table": "models/br_sou_da_paz_armas_municoes/output/registros_ativos_cac.csv",
            "real_file_id": "1-W4mCS6I8v7zi2keunATt-gL3yR8xa-1",
            "sheet_name": "2-CAC registros ativos ",
            "url_architecture": "https://docs.google.com/spreadsheets/d/14T_0JSteoNRBlKiFvdvO6VffiwkE0oAiDFsQWEB4Zqc/edit#gid=0",
        },
        "armas_acervo_cac": {  # ! 3
            "save_table": "models/br_sou_da_paz_armas_municoes/output/armas_acervo_cac.csv",
            "real_file_id": "1MqXGT_lVvYMmjNTVRugWfCGImOH4sVsa",
            "sheet_name": "3-CACS acervo armas",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1uWgQeqa4Nd8ikNiqx75FVgroZHn1jXtH0cIDmd_I2pA/edit#gid=0",
        },
        "pessoa_fisica_cac": {  # ! 4
            "save_table": "models/br_sou_da_paz_armas_municoes/output/pessoa_fisica_cac.csv",
            "real_file_id": "1d6sPi8mZfCTDJS-RGuiIu3FeNfm_820S",
            "sheet_name": "4-CACs pessoas",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1awbiZiSUeyeG3j0wIuAT0m8iFBJ_YnffAnHLnN5qnmM/edit#gid=0",
        },
        "armas_acervo_outras_categorias": {  # ! 5
            "save_table": "models/br_sou_da_paz_armas_municoes/output/armas_acervo_outras_categorias.csv",
            "real_file_id": "1sner7mYyq-SG3q4mRY8EAjXNsvSEgPuD",
            "sheet_name": "5-Outras categ EB acervo",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1f_tWIRaxjmUmHmfJdnGk7YmUiwmYXzbxQWuPDnHGiSA/edit#gid=0",
        },
        "armas_novas_outras_categorias": {  # ! 6
            "save_table": "models/br_sou_da_paz_armas_municoes/output/armas_novas_outras_categorias.csv",
            "real_file_id": "18JGQF2QW32jbSeuY9Xp871GSXhSt-gri",
            "sheet_name": "6-Outras categ EB novas",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1BXGxsmPOPi0T1nXjWR2xtzvdgBshSJoM90gqtyiZKUk/edit#gid=0",
        },
        "pessoa_fisica_outras_categorais": {  # ! 7
            "save_table": "models/br_sou_da_paz_armas_municoes/output/pessoa_fisica_outras_categorais.csv",
            "real_file_id": "1q38wIjoCZ0U_xuif-9eHP0HTT4kMd5dR",
            "sheet_name": "7-Outras categ EB (pessoa fisic",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1EWtT6sZAaYFkZ6ohFz8eGpkXqaRRUAJ2O9A3xh_Qme4/edit#gid=0",
        },
        "registros_ativos_policia_federal": {  # ! 8
            "save_table": "models/br_sou_da_paz_armas_municoes/output/registros_ativos_policia_federal.csv",
            "real_file_id": "1CpxDD0J_s_t3EkFnCRhKPZMFrd9fqJAL",
            "sheet_name": "8-Registros ativos PF",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1m-9OXsJz8w0vXeN9Gb1RD2jzgLtNOhXhdXITs8UBRAg/edit#gid=0",
        },
        "registros_novos_policia_federal": {  # ! 9
            "save_table": "models/br_sou_da_paz_armas_municoes/output/registros_novos_policia_federal.csv",
            "real_file_id": "1hTfEw5leAU8pdFBgxWs3kYtbKaVRSnpH",
            "sheet_name": "9-novos registros PF",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1DbLgOhOtGJssjMNdQfG4PSWtcl9dFlu6D9nMCweXT9w/edit#gid=0",
        },
        "municoes": {  # ! 10
            "save_table": "models/br_sou_da_paz_armas_municoes/output/municoes.csv",
            "real_file_id": "1o75cfWs3x3CoYlgJtyBTJMP0fPFyGUUV",
            "sheet_name": "10-munições vendidas",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1Is5q_8derxT-Q_1PAyTNCkQaLBfS--qWHBUaSaXBRpE/edit#gid=0",
        },
        "lojas_registro_ativo": {  # ! 11
            "save_table": "models/br_sou_da_paz_armas_municoes/output/lojas_registro_ativo.csv",
            "real_file_id": "1lJZC-DE7bICemzoKOk1NZS9fNkMmRBpy",
            "sheet_name": "11 - Lojas Registros Ativos EB",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1KHhlypUj50HqPTUkAXO7fiCR-dQMQKBmt3S6ifDGRGk/edit#gid=0",
        },
        "entidades_registro_ativo": {  # ! 12
            "save_table": "models/br_sou_da_paz_armas_municoes/output/entidades_registro_ativo.csv",
            "real_file_id": "1KU89ajzXntYYbwm7KBqkCDc43gw1ayeo",
            "sheet_name": "12- Entidades Registro Ativo EB",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1-0oWbypc83wgRyXz5qqo0Sn1oajKJHASgUkoqrMLt7s/edit#gid=0",
        },
        "lojas_novas": {  # ! 13
            "save_table": "models/br_sou_da_paz_armas_municoes/output/lojas_novas.csv",
            "real_file_id": "1P0qjgINt6zuJw87O0AUMiyP9Q6z7Su-v",
            "sheet_name": "13 - Lojas Novas EB",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1pVAewm1naF1q0dhoUvcH_S63q8eu3VlHq3zerKMeGVU/edit#gid=0",
        },
        "entidades_novas": {  # ! 14
            "save_table": "models/br_sou_da_paz_armas_municoes/output/entidades_novas.csv",
            "real_file_id": "1G-XZEMpREnalv5yRyZQDINVYqw9tRiaz",
            "sheet_name": "14 - Entidades Novas EB",
            "url_architecture": "https://docs.google.com/spreadsheets/d/12o7iAlQ1g7BUyVyc0tSJvKZ8wQcRDC8I1IbxDYgsz_0/edit#gid=0",
        },
        "armas_destruidas": {  # ! 15
            "save_table": "models/br_sou_da_paz_armas_municoes/output/armas_destruidas.csv",
            "real_file_id": "1MxG_juSQBDOK705mLd1SAOxvZkflLE7_",
            "sheet_name": "15- Destruições EB",
            "url_architecture": "https://docs.google.com/spreadsheets/d/1te5do4IUXXhezBegUm1DkC66xnELg-hG0LOxapZow-I/edit#gid=0",
        },
    }
