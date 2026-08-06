{{ config(alias="bloco", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_bloco as string) id_bloco,
    safe_cast(nome_bloco as string) nome_bloco,
    safe_cast(nome_apelido as string) nome_apelido,
    safe_cast(data_criacao as date) data_criacao,
    safe_cast(data_extincao as date) data_extincao,
from {{ set_datalake_project("br_senado_dados_abertos_staging.bloco") }} as t
