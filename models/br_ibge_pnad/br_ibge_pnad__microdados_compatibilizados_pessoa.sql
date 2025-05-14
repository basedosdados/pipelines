{{
    config(
        alias="microdados_compatibilizados_pessoa",
        schema="br_ibge_pnad",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1981, "end": 2015, "interval": 1},
        },
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(id_uf as string) id_uf,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_domicilio as string) id_domicilio,
    safe_cast(regiao_metropolitana as string) regiao_metropolitana,
    safe_cast(numero_familia as int64) numero_familia,
    safe_cast(ordem as int64) ordem,
    safe_cast(condicao_domicilio as string) condicao_domicilio,
    safe_cast(condicao_familia as string) condicao_familia,
    safe_cast(numero_membros_familia as int64) numero_membros_familia,
    safe_cast(sexo as string) sexo,
    safe_cast(dia_nascimento as int64) dia_nascimento,
    safe_cast(mes_nascimento as int64) mes_nascimento,
    safe_cast(ano_nascimento as int64) ano_nascimento,
    safe_cast(idade as int64) idade,
    safe_cast(raca_cor as string) raca_cor,
    safe_cast(sabe_ler_escrever as string) sabe_ler_escrever,
    safe_cast(frequenta_escola as string) frequenta_escola,
    safe_cast(serie_frequentada as string) serie_frequentada,
    safe_cast(grau_frequentado as string) grau_frequentado,
    safe_cast(ultima_serie_frequentada as int64) ultima_serie_frequentada,
    safe_cast(ultimo_grau_frequentado as string) ultimo_grau_frequentado,
    safe_cast(anos_estudo as int64) anos_estudo,
    safe_cast(trabalhou_semana as string) trabalhou_semana,
    safe_cast(tinha_trabalhado_semana as string) tinha_trabalhado_semana,
    safe_cast(tinha_outro_trabalho as string) tinha_outro_trabalho,
    safe_cast(ocupacao_semana as int64) ocupacao_semana,
    safe_cast(atividade_ramo_negocio_semana as int64) atividade_ramo_negocio_semana,
    safe_cast(
        atividade_ramo_negocio_anterior as string
    ) atividade_ramo_negocio_anterior,
    safe_cast(possui_carteira_assinada as string) possui_carteira_assinada,
    safe_cast(renda_mensal_dinheiro as float64) renda_mensal_dinheiro,
    safe_cast(
        renda_mensal_produto_mercadoria as float64
    ) renda_mensal_produto_mercadoria,
    safe_cast(horas_trabalhadas_semana as int64) horas_trabalhadas_semana,
    safe_cast(renda_mensal_dinheiro_outra as float64) renda_mensal_dinheiro_outra,
    safe_cast(renda_mensal_produto_outra as float64) renda_mensal_produto_outra,
    safe_cast(
        horas_trabalhadas_outros_trabalhos as int64
    ) horas_trabalhadas_outros_trabalhos,
    safe_cast(contribui_previdencia as string) contribui_previdencia,
    safe_cast(tipo_instituto_previdencia as string) tipo_instituto_previdencia,
    safe_cast(
        tomou_providencia_conseguir_trabalho_semana as string
    ) tomou_providencia_conseguir_trabalho_semana,
    safe_cast(
        tomou_providencia_ultimos_2_meses as string
    ) tomou_providencia_ultimos_2_meses,
    safe_cast(qual_providencia_tomou as string) qual_providencia_tomou,
    safe_cast(
        tinha_carteira_assinada_ultimo_emprego as string
    ) tinha_carteira_assinada_ultimo_emprego,
    safe_cast(renda_aposentadoria as float64) renda_aposentadoria,
    safe_cast(renda_pensao as float64) renda_pensao,
    safe_cast(renda_abono_permanente as float64) renda_abono_permanente,
    safe_cast(renda_aluguel as float64) renda_aluguel,
    safe_cast(renda_outras as float64) renda_outras,
    safe_cast(
        renda_mensal_ocupacao_principal as float64
    ) renda_mensal_ocupacao_principal,
    safe_cast(renda_mensal_todos_trabalhos as float64) renda_mensal_todos_trabalhos,
    safe_cast(renda_mensal_todas_fontes as float64) renda_mensal_todas_fontes,
    safe_cast(
        atividade_ramo_negocio_agregado as string
    ) atividade_ramo_negocio_agregado,
    safe_cast(
        horas_trabalhadas_todos_trabalhos as int64
    ) horas_trabalhadas_todos_trabalhos,
    safe_cast(posicao_ocupacao as string) posicao_ocupacao,
    safe_cast(grupos_ocupacao as string) grupos_ocupacao,
    safe_cast(renda_mensal_familia as float64) renda_mensal_familia,
    safe_cast(ocupacao_ano_anterior as string) ocupacao_ano_anterior,
    safe_cast(
        renda_mensal_dinheiro_deflacionado as float64
    ) renda_mensal_dinheiro_deflacionado,
    safe_cast(
        renda_mensal_produto_mercadoria_deflacionado as float64
    ) renda_mensal_produto_mercadoria_deflacionado,
    safe_cast(
        renda_mensal_dinheiro_outra_deflacionado as float64
    ) renda_mensal_dinheiro_outra_deflacionado,
    safe_cast(
        renda_mensal_produto_mercadoria_outra_deflacionado as float64
    ) renda_mensal_produto_mercadoria_outra_deflacionado,
    safe_cast(
        renda_mensal_ocupacao_principal_deflacionado as float64
    ) renda_mensal_ocupacao_principal_deflacionado,
    safe_cast(
        renda_mensal_todos_trabalhos_deflacionado as float64
    ) renda_mensal_todos_trabalhos_deflacionado,
    safe_cast(
        renda_mensal_todas_fontes_deflacionado as float64
    ) renda_mensal_todas_fontes_deflacionado,
    safe_cast(
        renda_mensal_familia_deflacionado as float64
    ) renda_mensal_familia_deflacionado,
    safe_cast(
        renda_aposentadoria_deflacionado as float64
    ) renda_aposentadoria_deflacionado,
    safe_cast(renda_pensao_deflacionado as float64) renda_pensao_deflacionado,
    safe_cast(renda_abono_deflacionado as float64) renda_abono_deflacionado,
    safe_cast(renda_aluguel_deflacionado as float64) renda_aluguel_deflacionado,
    safe_cast(renda_outras_deflacionado as float64) renda_outras_deflacionado
from
    {{
        set_datalake_project(
            "br_ibge_pnad_staging.microdados_compatibilizados_pessoa"
        )
    }}
where ano > 1990
