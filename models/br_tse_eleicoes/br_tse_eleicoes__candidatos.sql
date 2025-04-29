{{
    config(
        schema="br_tse_eleicoes",
        alias="candidatos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1994, "end": 2024, "interval": 2},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(data_eleicao as date) data_eleicao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(titulo_eleitoral as string) titulo_eleitoral,
    cast(split(cpf, '.')[offset(0)] as string) cpf,
    safe_cast(sequencial as string) sequencial,
    safe_cast(numero as string) numero,
    safe_cast(nome as string) nome,
    safe_cast(nome_urna as string) nome_urna,
    safe_cast(numero_partido as string) numero_partido,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(cargo as string) cargo,
    safe_cast(situacao as string) situacao,
    safe_cast(data_nascimento as date) data_nascimento,
    cast(split(idade, '.')[offset(0)] as int64) idade,
    safe_cast(genero as string) genero,
    safe_cast(instrucao as string) instrucao,
    safe_cast(ocupacao as string) ocupacao,
    safe_cast(estado_civil as string) estado_civil,
    safe_cast(nacionalidade as string) nacionalidade,
    safe_cast(sigla_uf_nascimento as string) sigla_uf_nascimento,
    safe_cast(municipio_nascimento as string) municipio_nascimento,
    safe_cast(email as string) email,
    safe_cast(raca as string) raca
from {{ set_datalake_project("br_tse_eleicoes_staging.candidatos") }} as t
