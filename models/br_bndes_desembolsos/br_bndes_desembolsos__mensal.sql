{{
    config(
        alias="mensal",
        schema="br_bndes_desembolsos",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1995, "end": 2031, "interval": 1},
        },
        labels={"project_id": "basedosdados"},
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(forma_apoio as string) forma_apoio,
    safe_cast(produto as string) produto,
    safe_cast(instrumento_financeiro as string) instrumento_financeiro,
    safe_cast(indicador_inovacao as string) indicador_inovacao,
    safe_cast(porte_empresa as string) porte_empresa,
    safe_cast(setor_cnae as string) setor_cnae,
    safe_cast(subsetor_cnae_agrupado as string) subsetor_cnae_agrupado,
    safe_cast(setor_bndes as string) setor_bndes,
    safe_cast(subsetor_bndes as string) subsetor_bndes,
    safe_cast(valor_desembolsado as float64) valor_desembolsado
from {{ set_datalake_project("br_bndes_desembolsos_staging.mensal") }} as t
