{{
    config(
        schema="br_ms_sisvan",
        alias="microdados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2023, "interval": 1},
        },
        cluster_by=["mes", "sigla_uf"],
    )
}}

select
    safe_cast(ano as int64) as ano,
    safe_cast(mes as int64) as mes,
    safe_cast(sg_uf as string) as sigla_uf,
    safe_cast(co_municipio_ibge as string) as id_municipio,
    safe_cast(co_acompanhamento as string) as acompanhamento,
    safe_cast(co_pessoa_sisvan as string) as id_individuo,
    safe_cast(co_cnes as string) as cnes,
    safe_cast(nu_idade_ano as int64) as idade,
    safe_cast(regexp_replace(nu_fase_vida, r'[.,]0+', '') as string) as fase_vida,
    safe_cast(sg_sexo as string) as sexo,
    safe_cast(regexp_replace(co_raca_cor, r'^0+', '') as string) as raca_cor,
    safe_cast(co_povo_comunidade as string) as povo_comunidade,
    safe_cast(regexp_replace(co_escolaridade, r'^0+', '') as string) as escolaridade,
    safe_cast(
        format_date('%Y-%m-%d', parse_date('%d/%m/%Y', dt_acompanhamento)) as date
    ) as data_acompanhamento,
    safe_cast(replace(nu_peso, ',', '.') as float64) as peso,
    safe_cast(replace(nu_altura, ',', '.') as float64) as altura,
    safe_cast(replace(ds_imc, ',', '.') as float64) as imc,
    safe_cast(replace(ds_imc_pre_gestacional, ',', '.') as float64) as imc_gestacional,
    safe_cast(peso_x_idade as string) as estado_nutricional_peso_idade_crianca,
    safe_cast(peso_x_altura as string) as estado_nutricional_peso_altura_crianca,
    safe_cast(cri_altura_x_idade as string) as estado_nutricional_altura_idade_crianca,
    safe_cast(cri_imc_x_idade as string) as estado_nutricional_imc_idade_crianca,
    safe_cast(
        ado_altura_x_idade as string
    ) as estado_nutricional_altura_idade_adolescente,
    safe_cast(ado_imc_x_idade as string) as estado_nutricional_imc_idade_adolescente,
    safe_cast(co_estado_nutri_adulto as string) as estado_nutricional_adulto,
    safe_cast(co_estado_nutri_idoso as string) as estado_nutricional_idoso,
    safe_cast(co_estado_nutri_imc_semgest as string) as estado_nutricional_gestantes,
    safe_cast(co_sistema_origem_acomp as string) as sistema_origem
from {{ set_datalake_project("br_ms_sisvan_staging.microdados") }} as t
