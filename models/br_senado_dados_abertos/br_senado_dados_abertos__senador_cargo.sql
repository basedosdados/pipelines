{{ config(alias="senador_cargo", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_senador as string) id_senador,
    safe_cast(id_comissao as string) id_comissao,
    safe_cast(sigla_comissao as string) sigla_comissao,
    safe_cast(id_cargo as string) id_cargo,
    safe_cast(descricao_cargo as string) descricao_cargo,
    safe_cast(data_inicio as date) data_inicio,
    safe_cast(data_fim as date) data_fim,
from {{ set_datalake_project("br_senado_dados_abertos_staging.senador_cargo") }} as t
