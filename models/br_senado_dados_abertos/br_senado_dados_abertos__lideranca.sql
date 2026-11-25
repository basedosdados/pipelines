{{ config(alias="lideranca", schema="br_senado_dados_abertos") }}

select
    safe_cast(id_lideranca as string) id_lideranca,
    safe_cast(casa as string) casa,
    safe_cast(id_senador as string) id_senador,
    safe_cast(nome_parlamentar as string) nome_parlamentar,
    safe_cast(sigla_tipo_lideranca as string) sigla_tipo_lideranca,
    safe_cast(descricao_tipo_lideranca as string) descricao_tipo_lideranca,
    safe_cast(sigla_tipo_unidade_lideranca as string) sigla_tipo_unidade_lideranca,
    safe_cast(
        descricao_tipo_unidade_lideranca as string
    ) descricao_tipo_unidade_lideranca,
    safe_cast(id_partido_filiacao as string) id_partido_filiacao,
    safe_cast(sigla_partido_filiacao as string) sigla_partido_filiacao,
    safe_cast(nome_partido_filiacao as string) nome_partido_filiacao,
    safe_cast(id_bloco as string) id_bloco,
    safe_cast(sigla_bloco as string) sigla_bloco,
    safe_cast(nome_bloco as string) nome_bloco,
    safe_cast(numero_ordem_vice_lider as string) numero_ordem_vice_lider,
    safe_cast(data_designacao as date) data_designacao,
from {{ set_datalake_project("br_senado_dados_abertos_staging.lideranca") }} as t
