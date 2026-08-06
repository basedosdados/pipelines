{{ config(alias="comissao", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_comissao as string) id_comissao,
    safe_cast(sigla_comissao as string) sigla_comissao,
    safe_cast(nome_comissao as string) nome_comissao,
    safe_cast(sigla_casa as string) sigla_casa,
    safe_cast(id_tipo_colegiado as string) id_tipo_colegiado,
    safe_cast(sigla_tipo_colegiado as string) sigla_tipo_colegiado,
    safe_cast(descricao_tipo_colegiado as string) descricao_tipo_colegiado,
    safe_cast(publica as string) publica,
    safe_cast(data_inicio as date) data_inicio,
from {{ set_datalake_project("br_senado_dados_abertos_staging.comissao") }} as t
