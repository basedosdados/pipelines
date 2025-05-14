{{
    config(
        alias="iptu",
        schema="br_mg_belohorizonte_smfa_iptu",
        materialized="incremental",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2022, "end": 2023, "interval": 1},
        },
        cluster_by=["mes"],
        labels={"project_id": "basedosdados"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(indice_cadastral as string) indice_cadastral,
    safe_cast(lote as string) lote,
    safe_cast(zoneamento as string) zoneamento,
    safe_cast(zona_homogenea as string) zona_homogenea,
    safe_cast(cep as string) cep,
    initcap(endereco) endereco,
    initcap(tipo_construtivo) tipo_construtivo,
    initcap(tipo_ocupacao) tipo_ocupacao,
    safe_cast(padrao_acabamento as string) padrao_acabamento,
    initcap(tipologia) tipologia,
    safe_cast(codigo_quantidade_economia as int64) quantidade_economias,
    initcap(frequencia_coleta) frequencia_coleta,
    safe_cast(indicador_rede_telefonica as bool) indicador_rede_telefonica,
    safe_cast(indicador_meio_fio as bool) indicador_meio_fio,
    safe_cast(indicador_pavimentacao as bool) indicador_pavimentacao,
    safe_cast(indicador_arborizacao as bool) indicador_arborizacao,
    safe_cast(indicador_galeria_pluvial as bool) indicador_galeria_pluvial,
    safe_cast(indicador_iluminacao_publica as bool) indicador_iluminacao_publica,
    safe_cast(indicador_rede_esgoto as bool) indicador_rede_esgoto,
    safe_cast(indicador_agua as bool) indicador_agua,
    safe.st_geogfromtext(poligono) poligono,
    safe_cast(fracao_ideal as float64) fracao_ideal,
    safe_cast(area_terreno as float64) area_terreno,
    safe_cast(area_construida as float64) area_construida
from {{ set_datalake_project("br_mg_belohorizonte_smfa_iptu_staging.iptu") }} as t
{% if is_incremental() %}
    where
        date(cast(ano as int64), cast(mes as int64), 1)
        > (select max(date(cast(ano as int64), cast(mes as int64), 1)) from {{ this }})
{% endif %}
