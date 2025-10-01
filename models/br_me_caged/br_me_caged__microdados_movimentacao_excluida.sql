{{
    config(
        schema="br_me_caged",
        materialized="table",
        alias="microdados_movimentacao_excluida",
        labels={"project_id": "basedosdados", "tema": "economia"},
        pre_hook="DROP ALL ROW ACCESS POLICIES ON {{ this }}",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(substring(competenciamov, 1, 4) as int64) ano_competencia_movimentacao,
    safe_cast(substring(competenciamov, 5, 6) as int64) mes_competencia_movimentacao,
    safe_cast(substring(competenciadec, 1, 4) as int64) ano_declaracao_movimentacao,
    safe_cast(substring(competenciadec, 5, 6) as int64) mes_declaracao_movimentacao,
    safe_cast(a.sigla_uf as string) sigla_uf,
    safe_cast(b.id_municipio as string) id_municipio,
    safe_cast(cnae_2_secao as string) cnae_2_secao,
    safe_cast(lpad(cnae_2_subclasse, 7, '0') as string) cnae_2_subclasse,
    safe_cast(saldo_movimentacao as int64) saldo_movimentacao,
    safe_cast(cbo_2002 as string) cbo_2002,
    safe_cast(categoria as string) categoria,
    safe_cast(grau_instrucao as string) grau_instrucao,
    safe_cast(replace(idade, '.0', '') as int64) idade,
    case
        when horas_contratuais like '%,%'
        then cast(replace(horas_contratuais, ',', '.') as float64)
        else cast(horas_contratuais as float64)
    end as horas_contratuais,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(sexo as string) sexo,
    safe_cast(tipo_empregador as string) tipo_empregador,
    safe_cast(tipo_estabelecimento as string) tipo_estabelecimento,
    safe_cast(tipo_movimentacao as string) tipo_movimentacao,
    safe_cast(tipo_deficiencia as string) tipo_deficiencia,
    safe_cast(
        indicador_trabalho_intermitente as string
    ) indicador_trabalho_intermitente,
    safe_cast(indicador_trabalho_parcial as string) indicador_trabalho_parcial,
    safe_cast(replace(salario_mensal, ',', '.') as float64) salario_mensal,
    safe_cast(
        tamanho_estabelecimento_janeiro as string
    ) tamanho_estabelecimento_janeiro,
    safe_cast(indicador_aprendiz as string) indicador_aprendiz,
    safe_cast(origem_informacao as string) origem_informacao,
    safe_cast(indicador_exclusao as int64) indicador_exclusao,
    safe_cast(indicador_fora_prazo as int64) indicador_fora_prazo
from
    {{ set_datalake_project("br_me_caged_staging.microdados_movimentacao_excluida") }} a
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` b
    on a.id_municipio = b.id_municipio_6
