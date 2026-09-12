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
    ),
    -- Espírito Santo needs no dimension tables: SIGEFES ships every classification as
    -- a code+label pair ON the expense row, so the dictionary is the distinct pairs
    -- already present in `es_despesa`.
    --
    -- One `unnest` of an array of structs rather than ten unions, so the 15.3M-row
    -- table is scanned once instead of ten times. Every column is STRING in staging,
    -- which is what lets the structs share a type.
    --
    -- Labels carry padding in the source (' ESTADO ', ' ADMINISTRAÇÃO GERAL A CARGO DA
    -- SEFAZ  '), so they are trimmed here; leaving them would emit the same label
    -- several times under different whitespace.
    es as (
        select par.nome_coluna, par.chave, par.valor, d.ano as ano_exercicio
        from
            {{ set_datalake_project("br_bd_execucao_estadual_staging.es_despesa") }}
            as d,
            unnest(
                [
                    struct(
                        'funcao' as nome_coluna,
                        trim(d.codigofuncao) as chave,
                        trim(d.funcao) as valor
                    ),
                    struct('subfuncao', trim(d.codigosubfuncao), trim(d.subfuncao)),
                    struct('programa', trim(d.codigoprograma), trim(d.programa)),
                    struct('acao', trim(d.codigoacao), trim(d.acao)),
                    struct(
                        'categoria_economica',
                        trim(d.codigocategoriaeconomica),
                        trim(d.categoriaeconomica)
                    ),
                    struct(
                        'grupo_despesa',
                        trim(d.codigogrupodespesa),
                        trim(d.grupodespesa)
                    ),
                    struct(
                        'modalidade_aplicacao',
                        trim(d.codigomodalidade),
                        trim(d.modalidade)
                    ),
                    struct(
                        'elemento_despesa',
                        trim(d.codigoelementodespesa),
                        trim(d.elementodespesa)
                    ),
                    struct(
                        'item_despesa',
                        trim(d.codigosubelementodespesa),
                        trim(d.subelementodespesa)
                    ),
                    struct('fonte_recurso', trim(d.codigofonte), trim(d.fonte))
                ]
            ) as par
    ),
    todos as (
        select 'MG' as sigla_uf, nome_coluna, chave, valor, ano_exercicio
        from mg
        union all
        select 'ES', nome_coluna, chave, valor, ano_exercicio
        from es
    )

select
    'despesa' as id_tabela,
    safe_cast(sigla_uf as string) as sigla_uf,
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
from todos
where nullif(chave, '') is not null and nullif(valor, '') is not null
group by id_tabela, sigla_uf, nome_coluna, chave, valor
