{{
    config(
        alias="participantes",
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
    safe_cast(id_inscricao as string) id_inscricao,
    safe_cast(faixa_etaria as string) faixa_etaria,
    safe_cast(sexo as string) sexo,
    safe_cast(estado_civil as string) estado_civil,
    safe_cast(cor_raca as string) cor_raca,
    safe_cast(nacionalidade as string) nacionalidade,
    safe_cast(situacao_conclusao as string) situacao_conclusao,
    safe_cast(ano_conclusao as string) ano_conclusao,
    safe_cast(ensino as string) ensino,
    safe_cast(indicador_treineiro as boolean) indicador_treineiro,
    safe_cast(id_municipio_prova as string) id_municipio_prova,
    safe_cast(sigla_uf_prova as string) sigla_uf_prova
from {{ set_datalake_project("br_inep_enem_staging.participantes") }} as t
