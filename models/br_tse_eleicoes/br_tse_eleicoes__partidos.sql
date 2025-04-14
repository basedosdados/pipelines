{{
    config(
        schema="br_tse_eleicoes",
        alias="partidos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1990, "end": 2024, "interval": 2},
        },
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(turno as int64) turno,
    safe_cast(id_eleicao as string) id_eleicao,
    safe_cast(tipo_eleicao as string) tipo_eleicao,
    safe_cast(data_eleicao as string) data_eleicao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(id_municipio_tse as string) id_municipio_tse,
    safe_cast(cargo as string) cargo,
    safe_cast(numero as string) numero,
    safe_cast(sigla as string) sigla,
    safe_cast(nome as string) nome,
    safe_cast(tipo_agremiacao as string) tipo_agremiacao,
    safe_cast(sequencial_coligacao as string) sequencial_coligacao,
    safe_cast(nome_coligacao as string) nome_coligacao,
    safe_cast(composicao_coligacao as string) composicao_coligacao,
    safe_cast(numero_federacao as string) numero_federacao,
    safe_cast(nome_federacacao as string) nome_federacacao,
    safe_cast(sigla_federacao as string) sigla_federacao,
    safe_cast(composicao_federacao as string) composicao_federacao,
    safe_cast(situacao_legenda as string) situacao_legenda
from {{ set_datalake_project("br_tse_eleicoes_staging.partidos") }} as t
