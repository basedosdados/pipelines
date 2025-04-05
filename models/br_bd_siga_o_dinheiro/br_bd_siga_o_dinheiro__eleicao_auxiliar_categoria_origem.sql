{{
    config(
        alias="eleicao_auxiliar_categoria_origem",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}
select *
from {{ project_path("br_jota_staging.eleicao_auxiliar_categoria_origem") }}
