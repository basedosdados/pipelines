"""
Constantes de br_ms_sinasc.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_ms_sinasc."""

    FTP = (
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/"
        "DN{sigla_uf}{ano}.dbc"
    )
    FTP_DIR = (
        "ftp://ftp.datasus.gov.br/dissemin/publicos/SINASC/1996_/Dados/DNRES/"
    )

    # Área de trabalho do pod. `input/` recebe os .dbc, `output/` o particionado
    # que sobe para o GCS.
    PATH = "/tmp/br_ms_sinasc/"

    SOURCE_FORMAT = "csv"

    # Teto de plausibilidade para o ano de uma data. Acima disso é digitação
    # errada, e o valor não existe no diretório de tempo.
    MAX_YEAR = 2100

    UFS = [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]

    TABLES = {
        "microdados": {
            "file_prefix": "DN",
            "file_name": "data.csv",
            "partition_columns": ["ano", "sigla_uf"],
        },
    }

    DATE_COLUMNS = [
        "data_nascimento",
        "data_nascimento_mae",
        "data_ultima_menstruacao",
        "data_registro_cartorio",
        "data_cadastro",
        "data_recebimento",
        "data_recebimento_original",
        "data_declaracao",
    ]

    # Colunas de município que a fonte grava com 6 dígitos até certo ano e com 7
    # a partir dele. A conversão é por valor, não por coluna.
    MUNICIPIO_COLUMNS = [
        "id_municipio_nascimento",
        "id_municipio_mae",
        "id_municipio_residencia",
    ]

    RENAME = {
        "CONTADOR": "sequencial_nascimento",
        "CODMUNNASC": "id_municipio_nascimento",
        "LOCNASC": "local_nascimento",
        "CODESTAB": "codigo_estabelecimento",
        "DTNASC": "data_nascimento",
        "HORANASC": "hora_nascimento",
        "SEXO": "sexo",
        "PESO": "peso",
        "RACACOR": "raca_cor",
        "APGAR1": "apgar1",
        "APGAR5": "apgar5",
        "IDANOMAL": "id_anomalia",
        "CODANOMAL": "codigo_anomalia",
        "SEMAGESTAC": "semana_gestacao",
        "TPMETESTIM": "semana_gestacao_estimada",
        "GESTACAO": "gestacao_agr",
        "GRAVIDEZ": "tipo_gravidez",
        "PARTO": "tipo_parto",
        "MESPRENAT": "inicio_pre_natal",
        "CONSPRENAT": "pre_natal",
        "CONSULTAS": "pre_natal_agr",
        "KOTELCHUCK": "classificacao_pre_natal",
        "QTDFILVIVO": "quantidade_filhos_vivos",
        "QTDFILMORT": "quantidade_filhos_mortos",
        "NATURALMAE": "id_pais_mae",
        "CODUFNATU": "id_uf_mae",
        "CODMUNNATU": "id_municipio_mae",
        "CODPAISRES": "id_pais_residencia",
        "CODMUNRES": "id_municipio_residencia",
        "DTNASCMAE": "data_nascimento_mae",
        "IDADEMAE": "idade_mae",
        "ESCMAE": "escolaridade_mae",
        "SERIESCMAE": "serie_escolar_mae",
        "ESCMAE2010": "escolaridade_2010_mae",
        "ESCMAEAGR1": "escolaridade_2010_agr_mae",
        "ESTCIVMAE": "estado_civil_mae",
        "CODOCUPMAE": "ocupacao_mae",
        "RACACORMAE": "raca_cor_mae",
        "QTDGESTANT": "gestacoes_ant",
        "QTDPARTNOR": "quantidade_parto_normal",
        "QTDPARTCES": "quantidade_parto_cesareo",
        "DTULTMENST": "data_ultima_menstruacao",
        "TPAPRESENT": "tipo_apresentacao",
        "STTRABPART": "inducao_parto",
        "STCESPARTO": "cesarea_antes_parto",
        "TPROBSON": "tipo_robson",
        "IDADEPAI": "idade_pai",
        "CODCART": "cartorio",
        "NUMREGCART": "registro_cartorio",
        "DTREGCART": "data_registro_cartorio",
        "ORIGEM": "origem",
        "NUMEROLOTE": "numero_lote",
        "VERSAOSIST": "versao_sistema",
        "DTCADASTRO": "data_cadastro",
        "DTRECEBIM": "data_recebimento",
        # A fonte alterna entre os dois nomes conforme o ano; nenhum arquivo traz
        # os dois ao mesmo tempo.
        "DTRECORIGA": "data_recebimento_original",
        "DTRECORIG": "data_recebimento_original",
        "DIFDATA": "diferenca_data",
        "DTDECLARAC": "data_declaracao",
        "TPFUNCRESP": "funcao_responsavel",
        "TPDOCRESP": "documento_responsavel",
        "TPNASCASSI": "formacao_profissional_responsavel",
        "STDNEPIDEM": "status_dn",
        "STDNNOVA": "status_dn_nova",
        "PARIDADE": "paridade",
    }

    COLUMNS = [
        "ano",
        "sigla_uf",
        "sequencial_nascimento",
        "id_municipio_nascimento",
        "local_nascimento",
        "codigo_estabelecimento",
        "data_nascimento",
        "hora_nascimento",
        "sexo",
        "peso",
        "raca_cor",
        "apgar1",
        "apgar5",
        "id_anomalia",
        "codigo_anomalia",
        "semana_gestacao",
        "semana_gestacao_estimada",
        "gestacao_agr",
        "tipo_gravidez",
        "tipo_parto",
        "inicio_pre_natal",
        "pre_natal",
        "pre_natal_agr",
        "classificacao_pre_natal",
        "quantidade_filhos_vivos",
        "quantidade_filhos_mortos",
        "id_pais_mae",
        "id_uf_mae",
        "id_municipio_mae",
        "id_pais_residencia",
        "id_municipio_residencia",
        "data_nascimento_mae",
        "idade_mae",
        "escolaridade_mae",
        "serie_escolar_mae",
        "escolaridade_2010_mae",
        "escolaridade_2010_agr_mae",
        "estado_civil_mae",
        "ocupacao_mae",
        "raca_cor_mae",
        "gestacoes_ant",
        "quantidade_parto_normal",
        "quantidade_parto_cesareo",
        "data_ultima_menstruacao",
        "tipo_apresentacao",
        "inducao_parto",
        "cesarea_antes_parto",
        "tipo_robson",
        "idade_pai",
        "cartorio",
        "registro_cartorio",
        "data_registro_cartorio",
        "origem",
        "numero_lote",
        "versao_sistema",
        "data_cadastro",
        "data_recebimento",
        "data_recebimento_original",
        "diferenca_data",
        "data_declaracao",
        "funcao_responsavel",
        "documento_responsavel",
        "formacao_profissional_responsavel",
        "status_dn",
        "status_dn_nova",
        "paridade",
    ]
