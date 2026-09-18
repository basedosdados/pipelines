{{
    config(
        alias="microdados_familia_2022",
        schema="br_ibge_censo_demografico",
        materialized="table",
        cluster_by=["sigla_uf"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(controle as string) controle,
    safe_cast(numero_ordem as string) numero_ordem,
    safe_cast(peso_amostral as float64) peso_amostral,
    safe_cast(situacao_setor as string) situacao_setor,
    safe_cast(f0130 as string) f0130,
    safe_cast(situacao_domicilio as string) situacao_domicilio,
    safe_cast(f0150 as string) f0150,
    safe_cast(f0160 as int64) f0160,
    safe_cast(f0170 as string) f0170,
    safe_cast(f0180 as string) f0180,
    safe_cast(f0190 as string) f0190,
    safe_cast(f0200 as string) f0200,
    safe_cast(f0210 as string) f0210,
    safe_cast(f0220 as string) f0220,
    safe_cast(f0230 as int64) f0230,
    safe_cast(f0240 as int64) f0240,
    safe_cast(f0250 as int64) f0250,
    safe_cast(f0260 as float64) f0260,
    safe_cast(f0270 as int64) f0270,
    safe_cast(mf0190 as string) mf0190,
    safe_cast(mf0200 as string) mf0200
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_familia_2022"
        )
    }} as t
