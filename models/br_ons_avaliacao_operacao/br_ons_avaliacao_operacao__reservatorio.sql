{{ config(alias="reservatorio", schema="br_ons_avaliacao_operacao") }}

select
    safe_cast(data as date) data,
    safe_cast(id_subsistema as string) id_subsistema,
    safe_cast(subsistema as string) subsistema,
    safe_cast(id_empreendimento_aneel as string) id_empreendimento_aneel,
    safe_cast(
        replace(id_reservatorio_planejamento, 'nan', '') as string
    ) id_reservatorio_planejamento,
    safe_cast(replace(id_posto_vazao, 'nan', '') as string) id_posto_vazao,
    safe_cast(reservatorio_equivalente as string) reservatorio_equivalente,
    safe_cast(reservatorio as string) reservatorio,
    safe_cast(tipo_reservatorio as string) tipo_reservatorio,
    safe_cast(usina as string) usina,
    safe_cast(bacia as string) bacia,
    safe_cast(rio as string) rio,
    safe_cast(cota_maxima as float64) cota_maxima,
    safe_cast(cota_minima as float64) cota_minima,
    safe_cast(volume_maximo as float64) volume_maximo,
    safe_cast(volume_minimo as float64) volume_minimo,
    safe_cast(volume_util as float64) volume_util,
    safe_cast(produtividade_especifica as float64) produtividade_especifica,
    safe_cast(produtividade_65_volume_util as float64) produtividade_65_volume_util,
    safe_cast(tipo_perda as string) tipo_perda,
    safe_cast(perda_carga as float64) perda_carga,
    safe_cast(latitude as float64) latitude,
    safe_cast(longitude as float64) longitude
from {{ set_datalake_project("br_ons_avaliacao_operacao_staging.reservatorio") }} as t
