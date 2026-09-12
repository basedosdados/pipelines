{{ config(alias="senador_filiacao", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_senador as string) id_senador,
    safe_cast(id_partido as string) id_partido,
    safe_cast(sigla_partido as string) sigla_partido,
    safe_cast(data_filiacao as date) data_filiacao,
    safe_cast(data_desfiliacao as date) data_desfiliacao,
from {{ set_datalake_project("br_senado_dados_abertos_staging.senador_filiacao") }} as t
