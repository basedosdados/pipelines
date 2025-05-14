{{
    config(
        alias="licitacao_participante",
        schema="world_wb_mides",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2009, "end": 2021, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(orgao as string) orgao,
    safe_cast(id_unidade_gestora as string) id_unidade_gestora,
    safe_cast(id_licitacao_bd as string) id_licitacao_bd,
    safe_cast(id_licitacao as string) id_licitacao,
    safe_cast(id_dispensa as string) id_dispensa,
    safe_cast(razao_social as string) razao_social,
    safe_cast(documento as string) documento,
    safe_cast(habilitado as int64) habilitado,
    safe_cast(classificado as int64) classificado,
    safe_cast(vencedor as int64) vencedor,
    safe_cast(endereco as string) endereco,
    safe_cast(cep as string) cep,
    safe_cast(municipio_participante as string) municipio_participante,
    safe_cast(tipo as string) tipo
from {{ set_datalake_project("world_wb_mides_staging.licitacao_participante") }} as t
