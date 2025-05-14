select
    safe_cast(ano as string) ano,
    safe_cast(mes as string) mes,
    safe_cast(id_terceirizado as string) id_terceirizado,
    safe_cast(
        sigla_orgao_superior_unidade_gestora as string
    ) sigla_orgao_superior_unidade_gestora,
    safe_cast(codigo_unidade_gestora as string) codigo_unidade_gestora,
    safe_cast(unidade_gestora as string) unidade_gestora,
    safe_cast(sigla_unidade_gestora as string) sigla_unidade_gestora,
    safe_cast(contrato_empresa as string) contrato_empresa,
    safe_cast(cnpj_empresa as string) cnpj_empresa,
    safe_cast(razao_social_empresa as string) razao_social_empresa,
    safe_cast(cpf as string) cpf,
    safe_cast(nome as string) nome,
    safe_cast(categoria_profissional as string) categoria_profissional,
    safe_cast(nivel_escolaridade as string) nivel_escolaridade,
    safe_cast(
        quantidade_horas_trabalhadas_semanais as string
    ) quantidade_horas_trabalhadas_semanais,
    safe_cast(unidade_trabalho as string) unidade_trabalho,
    safe_cast(valor_mensal as string) valor_mensal,
    safe_cast(custo_mensal as string) custo_mensal,
    safe_cast(sigla_orgao_trabalho as string) sigla_orgao_trabalho,
    safe_cast(nome_orgao_trabalho as string) nome_orgao_trabalho,
    safe_cast(codigo_siafi_trabalho as string) codigo_siafi_trabalho,
    safe_cast(codigo_siape_trabalho as string) codigo_siape_trabalho
from
    {{ set_datalake_project("br_cgu_pessoal_executivo_federal_staging.terceirizados") }}
    as t
