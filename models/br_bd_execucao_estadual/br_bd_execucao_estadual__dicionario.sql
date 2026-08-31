{{
    config(
        alias="dicionario",
        schema="br_bd_execucao_estadual",
        materialized="table",
        labels={"tema": "economia"},
    )
}}

-- Dicionário de valores codificados.
--
-- As colunas codificadas de `despesa` guardam o código da fonte (função, subfunção,
-- programa, ação, elemento, item, fonte de recursos, ...) e não o rótulo. Esta tabela
-- traduz cada código, por estado.
--
-- O par (estado, código) é a chave: os códigos NÃO são comparáveis entre estados.
-- Função e
-- subfunção seguem a classificação federal e coincidem, mas programa, ação, elemento e
-- fonte são definidos por cada estado em sua própria LOA. Juntar por código sem filtrar
-- `sigla_uf` mistura conceitos diferentes.
--
-- MG reemite códigos de função, subfunção, programa e ação a cada PPA, então o mesmo
-- código
-- pode ter rótulos distintos em anos distintos. Mantemos uma linha por (código, rótulo)
-- distinto em vez de escolher o mais recente, e `cobertura_temporal` registra o
-- intervalo de
-- exercícios em que aquele rótulo valeu.
with
    mg as (
        select 'funcao' as nome_coluna, cd_funcao as chave, nome as valor, ano_exercicio
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_funcao") }}
        union all
        select 'subfuncao', cd_subfuncao, nome, ano_exercicio
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_subfuncao"
                )
            }}
        union all
        select 'programa', cd_programa, nome, ano_exercicio
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_programa") }}
        union all
        select 'acao', cd_acao, nome, ano_exercicio
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_acao") }}
        union all
        select 'elemento_despesa', cd_elemento, nome, cast(null as string)
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_elemento") }}
        union all
        select 'item_despesa', cd_item, nome, cast(null as string)
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_item") }}
        union all
        select 'fonte_recurso', cd_fonte, nome, cast(null as string)
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_fonte") }}
        union all
        select 'categoria_economica', cd_categ_econ, nome, cast(null as string)
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_categoria"
                )
            }}
        union all
        select 'grupo_despesa', cd_grupo, nome, cast(null as string)
        from {{ set_datalake_project("br_bd_execucao_estadual_staging.mg_dm_grupo") }}
        union all
        select 'modalidade_aplicacao', cd_modalidade_aplic, nome, cast(null as string)
        from
            {{
                set_datalake_project(
                    "br_bd_execucao_estadual_staging.mg_dm_modalidade_aplic"
                )
            }}
    )

select
    'despesa' as id_tabela,
    'MG' as sigla_uf,
    safe_cast(nome_coluna as string) as nome_coluna,
    safe_cast(chave as string) as chave,
    safe_cast(valor as string) as valor,
    case
        when min(ano_exercicio) is null
        then null
        when min(ano_exercicio) = max(ano_exercicio)
        then min(ano_exercicio)
        else concat(min(ano_exercicio), '(1)', max(ano_exercicio))
    end as cobertura_temporal
from mg
where chave is not null and valor is not null
group by id_tabela, sigla_uf, nome_coluna, chave, valor
