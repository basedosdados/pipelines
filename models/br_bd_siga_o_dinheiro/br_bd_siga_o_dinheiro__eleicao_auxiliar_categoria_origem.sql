{{
    config(
        alias="eleicao_auxiliar_categoria_origem",
        schema="br_bd_siga_o_dinheiro",
        materialized="table",
    )
}}
select *
from {{ set_datalake_project("br_jota_staging.eleicao_auxiliar_categoria_origem") }}
