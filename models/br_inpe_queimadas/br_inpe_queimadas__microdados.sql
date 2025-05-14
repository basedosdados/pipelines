{{
    config(
        schema="br_inpe_queimadas",
        alias="microdados",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2003, "end": 2025, "interval": 1},
        },
        cluster_by=["mes"],
    )
}}
select distinct
    safe_cast(f.ano as int64) ano,
    safe_cast(f.mes as int64) mes,
    datetime(
        safe_cast(ano as int64),
        safe_cast(mes as int64),
        safe_cast(dia as int64),
        safe_cast(hora as int64),
        safe_cast(minuto as int64),
        safe_cast(segundo as int64)
    ) data_hora,
    safe_cast(f.bioma as string) bioma,
    safe_cast(f.sigla_uf as string) sigla_uf,
    safe_cast(m.id_municipio as string) id_municipio,
    safe_cast(f.latitude as float64) latitude,
    safe_cast(f.longitude as float64) longitude,
    safe_cast(f.satelite as string) satelite,
    safe_cast(f.dias_sem_chuva as float64) dias_sem_chuva,
    safe_cast(f.precipitacao as float64) precipitacao,
    safe_cast(f.risco_fogo as float64) risco_fogo,
    safe_cast(f.potencia_radiativa_fogo as float64) potencia_radiativa_fogo,
from {{ set_datalake_project("br_inpe_queimadas_staging.microdados") }} as f
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` m
    on f.municipio_uf = m.nome || ' - ' || m.sigla_uf
