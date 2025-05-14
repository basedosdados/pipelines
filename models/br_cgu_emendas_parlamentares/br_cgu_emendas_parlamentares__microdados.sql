{{
    config(
        alias="microdados",
        schema="br_cgu_emendas_parlamentares",
        materialized="table",
    )
}}
with
    tabela_1 as (
        select
            safe_cast(`Ano da Emenda` as int64) ano_emenda,
            safe_cast(`Código da Emenda` as string) id_emenda,
            safe_cast(
                replace(`Número da emenda`, "S/I", "Sem informação") as string
            ) numero_emenda,
            safe_cast(`Tipo de Emenda` as string) tipo_emenda,
            safe_cast(
                replace(`Código do Autor da Emenda`, "S/I", "Sem informação") as string
            ) id_autor_emenda,
            safe_cast(`Nome do Autor da Emenda` as string) as nome_autor_emenda,
            case
                when
                    `Localidade de aplicação do recurso` in (
                        "Nacional",
                        "Exterior",
                        "Nordeste",
                        "Norte",
                        "Sudeste",
                        "Sul",
                        "Centro-Oeste"
                    )
                then `Localidade de aplicação do recurso`
                else null
            end
            localidade,
            safe_cast(`UF` as string) as sigla_uf_gasto,
            safe_cast(`Código Município IBGE` as string) as id_municipio_gasto,
            safe_cast(`Código Função` as string) as id_funcao,
            safe_cast(`Nome Função` as string) as nome_funcao,
            safe_cast(`Código Subfunção` as string) as id_subfuncao,
            safe_cast(`Nome Subfunção` as string) as nome_subfuncao,
            safe_cast(`Código Programa` as string) as id_programa,
            safe_cast(`Nome Programa` as string) as nome_programa,
            safe_cast(`Código Ação` as string) as id_acao,
            safe_cast(`Nome Ação` as string) as nome_acao,
            safe_cast(`Código Plano Orçamentário` as string) as id_plano_orcamentario,
            safe_cast(`Nome Plano Orçamentário` as string) as nome_plano_orcamentario,
            safe_cast(`Valor Empenhado` as float64) as valor_empenhado,
            safe_cast(`Valor Liquidado` as float64) as valor_liquidado,
            safe_cast(`Valor Pago` as float64) as valor_pago,
            safe_cast(
                `Valor Restos A Pagar Inscritos` as float64
            ) as valor_resto_pagar_inscrito,
            safe_cast(
                `Valor Restos A Pagar Cancelados` as float64
            ) as valor_resto_pagar_cancelado,
            safe_cast(
                `Valor Restos A Pagar Pagos` as float64
            ) as valor_resto_pagar_pagos
        from
            {{
                set_datalake_project(
                    "br_cgu_emendas_parlamentares_staging.microdados"
                )
            }}
    ),
    tabela_2 as (
        select
            microdados.ano_emenda,
            microdados.id_emenda,
            microdados.numero_emenda,
            microdados.tipo_emenda,
            microdados.id_autor_emenda,
            microdados.nome_autor_emenda,
            microdados.localidade,
            sigla_uf_diretorio.sigla as sigla_uf_gasto,
            microdados.id_municipio_gasto,
            microdados.id_funcao,
            microdados.nome_funcao,
            microdados.id_subfuncao,
            microdados.nome_subfuncao,
            microdados.id_programa,
            microdados.nome_programa,
            microdados.id_acao,
            microdados.nome_acao,
            microdados.id_plano_orcamentario,
            microdados.nome_plano_orcamentario,
            microdados.valor_empenhado,
            microdados.valor_liquidado,
            microdados.valor_pago,
            microdados.valor_resto_pagar_inscrito,
            microdados.valor_resto_pagar_cancelado,
            microdados.valor_resto_pagar_pagos,
        from tabela_1 as microdados
        left join
            `basedosdados.br_bd_diretorios_brasil.uf` as sigla_uf_diretorio
            on microdados.sigla_uf_gasto = upper(sigla_uf_diretorio.nome)
    )
select *
from tabela_2
