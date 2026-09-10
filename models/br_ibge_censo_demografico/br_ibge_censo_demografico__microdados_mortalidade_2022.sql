{{
    config(
        alias="microdados_mortalidade_2022",
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
    safe_cast(m0130 as string) m0130,
    safe_cast(situacao_domicilio as string) situacao_domicilio,
    safe_cast(m0150 as string) m0150,
    safe_cast(m0160 as string) m0160,
    safe_cast(m0170 as string) m0170,
    safe_cast(mm0150 as string) mm0150,
    safe_cast(mm0160 as string) mm0160,
    safe_cast(mm0170 as string) mm0170
from
    {{
        set_datalake_project(
            "br_ibge_censo_demografico_staging.microdados_mortalidade_2022"
        )
    }} as t
