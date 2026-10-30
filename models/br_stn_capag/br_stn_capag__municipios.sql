{{
    config(
        schema="br_stn_capag",
        alias="municipios",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2018, "end": 2030, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(data_referencia as date) data_referencia,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(populacao as int64) populacao,
    safe_cast(indicador_endividamento as float64) indicador_endividamento,
    safe_cast(nota_endividamento as string) nota_endividamento,
    safe_cast(indicador_poupanca_corrente as float64) indicador_poupanca_corrente,
    safe_cast(nota_poupanca_corrente as string) nota_poupanca_corrente,
    safe_cast(indicador_liquidez as float64) indicador_liquidez,
    safe_cast(nota_liquidez as string) nota_liquidez,
    safe_cast(nota_capag as string) nota_capag,
    safe_cast(ano_base as int64) ano_base,
    safe_cast(
        indicador_endividamento_revisao as float64
    ) indicador_endividamento_revisao,
    safe_cast(nota_endividamento_revisao as string) nota_endividamento_revisao,
    safe_cast(
        indicador_poupanca_corrente_revisao as float64
    ) indicador_poupanca_corrente_revisao,
    safe_cast(nota_poupanca_corrente_revisao as string) nota_poupanca_corrente_revisao,
    safe_cast(indicador_liquidez_revisao as float64) indicador_liquidez_revisao,
    safe_cast(nota_liquidez_revisao as string) nota_liquidez_revisao,
    safe_cast(nota_capag_revisao as string) nota_capag_revisao,
from {{ set_datalake_project("br_stn_capag_staging.municipios") }} as t
