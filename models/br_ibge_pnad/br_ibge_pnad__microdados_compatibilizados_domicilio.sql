{{
    config(
        alias="microdados_compatibilizados_domicilio",
        schema="br_ibge_pnad",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1981, "end": 2015, "interval": 1},
        },
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_uf as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_domicilio as string) id_domicilio,
    safe_cast(regiao_metropolitana as int64) regiao_metropolitana,
    safe_cast(zona_urbana as string) zona_urbana,
    safe_cast(tipo_zona_domicilio as string) tipo_zona_domicilio,
    safe_cast(total_pessoas as int64) total_pessoas,
    safe_cast(total_pessoas_10_mais as int64) total_pessoas_10_mais,
    safe_cast(especie_domicilio as string) especie_domicilio,
    safe_cast(tipo_domicilio as string) tipo_domicilio,
    safe_cast(tipo_parede as string) tipo_parede,
    safe_cast(tipo_cobertura as string) tipo_cobertura,
    safe_cast(possui_agua_rede as string) possui_agua_rede,
    safe_cast(tipo_esgoto as string) tipo_esgoto,
    safe_cast(possui_sanitario_exclusivo as string) possui_sanitario_exclusivo,
    safe_cast(lixo_coletado as string) lixo_coletado,
    safe_cast(possui_iluminacao_eletrica as string) possui_iluminacao_eletrica,
    safe_cast(quantidade_comodos as int64) quantidade_comodos,
    safe_cast(quantidade_dormitorios as int64) quantidade_dormitorios,
    safe_cast(possui_sanitario as string) possui_sanitario,
    safe_cast(posse_domicilio as string) posse_domicilio,
    safe_cast(possui_filtro as string) possui_filtro,
    safe_cast(possui_fogao as string) possui_fogao,
    safe_cast(possui_geladeira as string) possui_geladeira,
    safe_cast(possui_radio as string) possui_radio,
    safe_cast(possui_tv as string) possui_tv,
    safe_cast(renda_mensal_domiciliar as float64) renda_mensal_domiciliar,
    safe_cast(
        renda_mensal_domiciliar_compativel_1992 as float64
    ) renda_mensal_domiciliar_compativel_1992,
    safe_cast(aluguel as float64) aluguel,
    safe_cast(prestacao as float64) prestacao,
    safe_cast(deflator as int64) deflator,
    safe_cast(conversor_moeda as int64) conversor_moeda,
    safe_cast(renda_domicilio_deflacionado as float64) renda_domicilio_deflacionado,
    safe_cast(
        renda_mensal_domiciliar_compativel_1992_deflacionado as float64
    ) renda_mensal_domiciliar_compativel_1992_deflacionado,
    safe_cast(aluguel_deflacionado as float64) aluguel_deflacionado,
    safe_cast(prestacao_deflacionado as float64) prestacao_deflacionado,
    safe_cast(peso_amostral as float64) peso_amostral
from
    {{
        set_datalake_project(
            "br_ibge_pnad_staging.microdados_compatibilizados_domicilio"
        )
    }}
where ano > 1996
