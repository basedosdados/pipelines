{{
    config(
        alias="resultados",
        schema="br_inep_enem",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2024, "end": 2025, "interval": 1},
        },
        labels={"project_id": "basedosdados", "tema": "educacao"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_sequencial as string) id_sequencial,
    safe_cast(id_escola as string) id_escola,
    safe_cast(id_municipio_escola as string) id_municipio_escola,
    safe_cast(sigla_uf_escola as string) sigla_uf_escola,
    safe_cast(
        dependencia_administrativa_escola as string
    ) dependencia_administrativa_escola,
    safe_cast(localizacao_escola as string) localizacao_escola,
    safe_cast(situacao_funcionamento_escola as string) situacao_funcionamento_escola,
    safe_cast(id_municipio_prova as string) id_municipio_prova,
    safe_cast(sigla_uf_prova as string) sigla_uf_prova,
    safe_cast(presenca_ciencias_natureza as string) presenca_ciencias_natureza,
    safe_cast(presenca_ciencias_humanas as string) presenca_ciencias_humanas,
    safe_cast(presenca_linguagens_codigos as string) presenca_linguagens_codigos,
    safe_cast(presenca_matematica as string) presenca_matematica,
    safe_cast(tipo_prova_ciencias_natureza as string) tipo_prova_ciencias_natureza,
    safe_cast(tipo_prova_ciencias_humanas as string) tipo_prova_ciencias_humanas,
    safe_cast(tipo_prova_linguagens_codigos as string) tipo_prova_linguagens_codigos,
    safe_cast(tipo_prova_matematica as string) tipo_prova_matematica,
    safe_cast(nota_ciencias_natureza as float64) nota_ciencias_natureza,
    safe_cast(nota_ciencias_humanas as float64) nota_ciencias_humanas,
    safe_cast(nota_linguagens_codigos as float64) nota_linguagens_codigos,
    safe_cast(nota_matematica as float64) nota_matematica,
    safe_cast(respostas_ciencias_natureza as string) respostas_ciencias_natureza,
    safe_cast(respostas_ciencias_humanas as string) respostas_ciencias_humanas,
    safe_cast(respostas_linguagens_codigos as string) respostas_linguagens_codigos,
    safe_cast(respostas_matematica as string) respostas_matematica,
    safe_cast(gabarito_ciencias_natureza as string) gabarito_ciencias_natureza,
    safe_cast(gabarito_ciencias_humanas as string) gabarito_ciencias_humanas,
    safe_cast(gabarito_linguagens_codigos as string) gabarito_linguagens_codigos,
    safe_cast(gabarito_matematica as string) gabarito_matematica,
    safe_cast(lingua_estrangeira as string) lingua_estrangeira,
    safe_cast(presenca_redacao as string) presenca_redacao,
    safe_cast(nota_redacao_competencia_1 as float64) nota_redacao_competencia_1,
    safe_cast(nota_redacao_competencia_2 as float64) nota_redacao_competencia_2,
    safe_cast(nota_redacao_competencia_3 as float64) nota_redacao_competencia_3,
    safe_cast(nota_redacao_competencia_4 as float64) nota_redacao_competencia_4,
    safe_cast(nota_redacao_competencia_5 as float64) nota_redacao_competencia_5,
    safe_cast(nota_redacao as float64) nota_redacao
from {{ set_datalake_project("br_inep_enem_staging.resultados") }} as t
