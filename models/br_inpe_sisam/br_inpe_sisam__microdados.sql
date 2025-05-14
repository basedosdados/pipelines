{{
    config(
        alias="microdados",
        schema="br_inpe_sisam",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2000, "end": 2025, "interval": 1},
        },
        cluster_by=["ano", "sigla_uf"],
        labels={"tema": "meio-ambiente"},
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(data_hora as datetime) data_hora,
    safe_cast(co_ppb as float64) co_ppb,
    safe_cast(no2_ppb as float64) no2_ppb,
    safe_cast(o3_ppb as float64) o3_ppb,
    safe_cast(pm25_ugm3 as float64) pm25_ugm3,
    safe_cast(so2_ugm3 as float64) so2_ugm3,
    safe_cast(precipitacao_dia as float64) precipitacao_dia,
    safe_cast(temperatura as float64) temperatura,
    safe_cast(umidade_relativa as float64) umidade_relativa,
    safe_cast(vento_direcao as int64) vento_direcao,
    safe_cast(vento_velocidade as float64) vento_velocidade,
from {{ set_datalake_project("br_inpe_sisam_staging.microdados") }} as t
