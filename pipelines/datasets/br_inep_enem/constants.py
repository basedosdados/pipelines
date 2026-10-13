"""
Constantes de br_inep_enem.
"""

from enum import Enum


class constants(Enum):
    """Constantes de br_inep_enem."""

    SOURCE_LINK = "https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/enem"
    DOWNLOAD_LINK = (
        "https://download.inep.gov.br/microdados/microdados_enem_{ano}.zip"
    )

    PATH = "/tmp/br_inep_enem/"

    FIRST_YEAR = 2024

    ENCODING = "latin1"
    SEPARATOR = ";"
    CHUNK_SIZE = 200_000

    TABLES = {
        "participantes": {
            "file_prefix": "PARTICIPANTES",
            "partition_columns": ["ano"],
        },
        "resultados": {
            "file_prefix": "RESULTADOS",
            "partition_columns": ["ano"],
        },
        "questionario_socioeconomico": {
            "file_prefix": "PARTICIPANTES",
            "partition_columns": [],
        },
    }

    RENAME = {
        "participantes": {
            "NU_ANO": "ano",
            "NU_INSCRICAO": "id_inscricao",
            "TP_FAIXA_ETARIA": "faixa_etaria",
            "TP_SEXO": "sexo",
            "TP_ESTADO_CIVIL": "estado_civil",
            "TP_COR_RACA": "cor_raca",
            "TP_NACIONALIDADE": "nacionalidade",
            "TP_ST_CONCLUSAO": "situacao_conclusao",
            "TP_ANO_CONCLUIU": "ano_conclusao",
            "TP_ENSINO": "ensino",
            "IN_TREINEIRO": "indicador_treineiro",
            "CO_MUNICIPIO_PROVA": "id_municipio_prova",
            "SG_UF_PROVA": "sigla_uf_prova",
        },
        "resultados": {
            "NU_ANO": "ano",
            "NU_SEQUENCIAL": "id_sequencial",
            "CO_ESCOLA": "id_escola",
            "CO_MUNICIPIO_ESC": "id_municipio_escola",
            "SG_UF_ESC": "sigla_uf_escola",
            "TP_DEPENDENCIA_ADM_ESC": "dependencia_administrativa_escola",
            "TP_LOCALIZACAO_ESC": "localizacao_escola",
            "TP_SIT_FUNC_ESC": "situacao_funcionamento_escola",
            "CO_MUNICIPIO_PROVA": "id_municipio_prova",
            "SG_UF_PROVA": "sigla_uf_prova",
            "TP_PRESENCA_CN": "presenca_ciencias_natureza",
            "TP_PRESENCA_CH": "presenca_ciencias_humanas",
            "TP_PRESENCA_LC": "presenca_linguagens_codigos",
            "TP_PRESENCA_MT": "presenca_matematica",
            "CO_PROVA_CN": "tipo_prova_ciencias_natureza",
            "CO_PROVA_CH": "tipo_prova_ciencias_humanas",
            "CO_PROVA_LC": "tipo_prova_linguagens_codigos",
            "CO_PROVA_MT": "tipo_prova_matematica",
            "NU_NOTA_CN": "nota_ciencias_natureza",
            "NU_NOTA_CH": "nota_ciencias_humanas",
            "NU_NOTA_LC": "nota_linguagens_codigos",
            "NU_NOTA_MT": "nota_matematica",
            "TX_RESPOSTAS_CN": "respostas_ciencias_natureza",
            "TX_RESPOSTAS_CH": "respostas_ciencias_humanas",
            "TX_RESPOSTAS_LC": "respostas_linguagens_codigos",
            "TX_RESPOSTAS_MT": "respostas_matematica",
            "TX_GABARITO_CN": "gabarito_ciencias_natureza",
            "TX_GABARITO_CH": "gabarito_ciencias_humanas",
            "TX_GABARITO_LC": "gabarito_linguagens_codigos",
            "TX_GABARITO_MT": "gabarito_matematica",
            "TP_LINGUA": "lingua_estrangeira",
            "TP_STATUS_REDACAO": "presenca_redacao",
            "NU_NOTA_COMP1": "nota_redacao_competencia_1",
            "NU_NOTA_COMP2": "nota_redacao_competencia_2",
            "NU_NOTA_COMP3": "nota_redacao_competencia_3",
            "NU_NOTA_COMP4": "nota_redacao_competencia_4",
            "NU_NOTA_COMP5": "nota_redacao_competencia_5",
            "NU_NOTA_REDACAO": "nota_redacao",
            **{
                f"TP_STATUS_REDACAO_AV{n}": f"presenca_redacao_avaliador_{n}"
                for n in range(1, 5)
            },
            **{
                f"NU_NOTA_COMP{c}_AV{n}": (
                    f"nota_redacao_competencia_{c}_avaliador_{n}"
                )
                for c in range(1, 6)
                for n in range(1, 5)
            },
            **{
                f"NU_NOTA_AV{n}": f"nota_redacao_avaliador_{n}"
                for n in range(1, 5)
            },
        },
        "questionario_socioeconomico": {
            "NU_INSCRICAO": "id_inscricao",
            **{f"Q{n:03d}": f"q{n:03d}" for n in range(1, 24)},
        },
    }

    COLUMNS = {
        "participantes": [
            "ano",
            "id_inscricao",
            "faixa_etaria",
            "sexo",
            "estado_civil",
            "cor_raca",
            "nacionalidade",
            "situacao_conclusao",
            "ano_conclusao",
            "ensino",
            "indicador_treineiro",
            "id_municipio_prova",
            "sigla_uf_prova",
        ],
        "resultados": [
            "ano",
            "id_sequencial",
            "id_escola",
            "id_municipio_escola",
            "sigla_uf_escola",
            "dependencia_administrativa_escola",
            "localizacao_escola",
            "situacao_funcionamento_escola",
            "id_municipio_prova",
            "sigla_uf_prova",
            "presenca_ciencias_natureza",
            "presenca_ciencias_humanas",
            "presenca_linguagens_codigos",
            "presenca_matematica",
            "tipo_prova_ciencias_natureza",
            "tipo_prova_ciencias_humanas",
            "tipo_prova_linguagens_codigos",
            "tipo_prova_matematica",
            "nota_ciencias_natureza",
            "nota_ciencias_humanas",
            "nota_linguagens_codigos",
            "nota_matematica",
            "respostas_ciencias_natureza",
            "respostas_ciencias_humanas",
            "respostas_linguagens_codigos",
            "respostas_matematica",
            "gabarito_ciencias_natureza",
            "gabarito_ciencias_humanas",
            "gabarito_linguagens_codigos",
            "gabarito_matematica",
            "lingua_estrangeira",
            "presenca_redacao",
            "nota_redacao_competencia_1",
            "nota_redacao_competencia_2",
            "nota_redacao_competencia_3",
            "nota_redacao_competencia_4",
            "nota_redacao_competencia_5",
            "nota_redacao",
            *(f"presenca_redacao_avaliador_{n}" for n in range(1, 5)),
            *(
                f"nota_redacao_competencia_{c}_avaliador_{n}"
                for c in range(1, 6)
                for n in range(1, 5)
            ),
            *(f"nota_redacao_avaliador_{n}" for n in range(1, 5)),
        ],
        "questionario_socioeconomico": [
            "id_inscricao",
            *(f"q{n:03d}" for n in range(1, 24)),
        ],
    }

    BOOLEAN_COLUMNS = {"indicador_treineiro"}
