{{ config(alias="transferencia", schema="br_me_sic") }}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(id_esfera_orcamentaria as string) id_esfera_orcamentaria,
    safe_cast(nome_esfera_orcamentaria as string) nome_esfera_orcamentaria,
    safe_cast(id_natureza_juridica as string) id_natureza_juridica,
    safe_cast(nome_natureza_juridica as string) nome_natureza_juridica,
    safe_cast(id_resultado_primario as string) id_resultado_primario,
    safe_cast(nome_resultado_primario as string) nome_resultado_primario,
    safe_cast(
        id_unidade_organizacional_nivel_0 as string
    ) id_unidade_organizacional_nivel_0,
    safe_cast(
        nome_unidade_organizacional_nivel_0 as string
    ) nome_unidade_organizacional_nivel_0,
    safe_cast(
        id_unidade_organizacional_nivel_1 as string
    ) id_unidade_organizacional_nivel_1,
    safe_cast(
        nome_unidade_organizacional_nivel_1 as string
    ) nome_unidade_organizacional_nivel_1,
    safe_cast(
        id_unidade_organizacional_nivel_2 as string
    ) id_unidade_organizacional_nivel_2,
    safe_cast(
        nome_unidade_organizacional_nivel_2 as string
    ) nome_unidade_organizacional_nivel_2,
    safe_cast(
        id_unidade_organizacional_nivel_3 as string
    ) id_unidade_organizacional_nivel_3,
    safe_cast(
        nome_unidade_organizacional_nivel_3 as string
    ) nome_unidade_organizacional_nivel_3,
    safe_cast(valor_custo_transferencia as float64) valor_custo_transferencia,
from {{ set_datalake_project("br_me_sic_staging.transferencia") }} as t
