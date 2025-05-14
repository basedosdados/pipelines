{{
    config(
        alias="empreendimento",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}
select
    safe_cast(id_empreendimento as string) id_empreendimento,
    safe_cast(data_inicio_empreendimento as date) data_inicio,
    safe_cast(data_fim_empreendimento as date) data_fim,
    safe_cast(finalidade as string) finalidade,
    safe_cast(atividade as string) atividade,
    safe_cast(modalidade as string) modalidade,
    safe_cast(produto as string) produto,
    safe_cast(variedade as string) variedade,
    safe_cast(cesta_safra as string) cesta_safra,
    safe_cast(zoneamento as string) zoneamento,
    safe_cast(unidade_medida as string) unidade_medida,
    safe_cast(
        unidade_medida_previsao_producao as string
    ) unidade_medida_previsao_producao,
    safe_cast(consorcio as string) consorcio,
    safe_cast(cedula_mae as string) cedula_mae,
    safe_cast(id_tipo_cultura as string) id_tipo_cultura
from {{ set_datalake_project("br_bcb_sicor_staging.empreendimento") }} as t
