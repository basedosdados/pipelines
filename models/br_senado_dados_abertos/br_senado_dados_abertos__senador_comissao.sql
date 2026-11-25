{{ config(alias="senador_comissao", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_senador as string) id_senador,
    safe_cast(id_comissao as string) id_comissao,
    safe_cast(sigla_comissao as string) sigla_comissao,
    safe_cast(sigla_casa as string) sigla_casa,
    safe_cast(participacao as string) participacao,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
from {{ set_datalake_project("br_senado_dados_abertos_staging.senador_comissao") }} as t
