{{ config(alias="partido", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_partido as string) id_partido,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(nome_partido as string) nome_partido,
    safe_cast(data_criacao as date) data_criacao,
    safe_cast(data_extincao as date) data_extincao,
from {{ set_datalake_project("br_senado_dados_abertos_staging.partido") }} as t
