{{
    config(
        schema="br_senado_dados_abertos_administrativos",
        alias="quadro_pessoal",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_extracao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


select
    safe_cast(data_extracao as date) data_extracao,
    safe_cast(data_referencia as date) data_referencia,
    safe_cast(quadro as string) quadro,
    safe_cast(periodo as string) periodo,
    safe_cast(categoria as string) categoria,
    safe_cast(classe as string) classe,
    safe_cast(grupo as string) grupo,
    safe_cast(cargo as string) cargo,
    safe_cast(nivel as string) nivel,
    safe_cast(especialidade as string) especialidade,
    safe_cast(referencia as string) referencia,
    safe_cast(tabela_vencimento as string) tabela_vencimento,
    safe_cast(plano_carreira as string) plano_carreira,
    safe_cast(nivel_escolaridade as string) nivel_escolaridade,
    safe_cast(padrao_nivel_referencia as string) padrao_nivel_referencia,
    safe_cast(quantidade_cargos as int64) quantidade_cargos,
    safe_cast(quantidade_ocupados as int64) quantidade_ocupados,
    safe_cast(quantidade_vagos as int64) quantidade_vagos,
    safe_cast(quantidade_subtotal as int64) quantidade_subtotal,
    safe_cast(quantidade_total as int64) quantidade_total,
    safe_cast(quantidade_com_opcao as int64) quantidade_com_opcao,
    safe_cast(quantidade_sem_opcao as int64) quantidade_sem_opcao,
    safe_cast(quantidade_sem_vinculo as int64) quantidade_sem_vinculo,
    safe_cast(quantidade_estaveis as int64) quantidade_estaveis,
    safe_cast(quantidade_nao_estaveis as int64) quantidade_nao_estaveis,
    safe_cast(quantidade_total_ativo as int64) quantidade_total_ativo,
    safe_cast(quantidade_aposentados as int64) quantidade_aposentados,
    safe_cast(quantidade_instituidores_pensao as int64) quantidade_instituidores_pensao,
    safe_cast(quantidade_beneficiarios_pensao as int64) quantidade_beneficiarios_pensao,
    safe_cast(quantidade_total_inativo as int64) quantidade_total_inativo
from
    {{
        set_datalake_project(
            "br_senado_dados_abertos_administrativos_staging.quadro_pessoal"
        )
    }} as t
