{{
    config(
        schema="br_tse_eleicoes",
        alias="perfil_eleitorado_local_votacao",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2016, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(turno as int64) turno,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(zona as string) zona,
    safe_cast(secao as string) secao,
    safe_cast(tipo_secao_agregada as string) tipo_secao_agregada,
    safe_cast(numero as string) numero,
    safe_cast(nome as string) nome,
    safe_cast(tipo as string) tipo,
    safe_cast(endereco as string) endereco,
    safe_cast(bairro as string) bairro,
    safe_cast(cep as string) cep,
    safe_cast(telefone as string) telefone,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude,
    safe_cast(situacao as string) situacao,
    safe_cast(situacao_zona as string) situacao_zona,
    safe_cast(situacao_secao as string) situacao_secao,
    safe_cast(situacao_localidade as string) situacao_localidade,
    safe_cast(situacao_secao_acessibilidade as string) situacao_secao_acessibilidade,
    safe_cast(eleitores_secao as int64) eleitores_secao,
from
    {{
        set_datalake_project(
            "br_tse_eleicoes_staging.perfil_eleitorado_local_votacao"
        )
    }} as t
