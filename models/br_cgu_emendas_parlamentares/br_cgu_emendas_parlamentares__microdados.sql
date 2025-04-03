{{
    config(
        alias="microdados", schema="br_cgu_emendas_parlamentares", materialized="table"
    )
}}

with
    tabela_inicial as (
        select
            safe_cast(`Ano da Emenda` as int64) ano_emenda,
            safe_cast(
                replace(`Código da Emenda`, "Sem informaç", "Sem informação") as string
            ) codigo_emenda,
            safe_cast(
                replace(`Número da emenda`, "S/I", "Sem informação") as string
            ) numero_emenda,
            safe_cast(`Tipo de Emenda` as string) tipo_emenda,
            safe_cast(
                replace(`Código do Autor da Emenda`, "S/I", "Sem informação") as string
            ) codigo_autor_emenda,
            safe_cast(`Nome do Autor da Emenda` as string) nome_autor_emenda,
            case
                when `Localidade do gasto` = 'PRESIDENTE JUSCELINO - RN'
                then "SERRA CAIADA - RN"  -- - https://www.serracaiada.rn.gov.br/omunicipio.php
                else `Localidade do gasto`
            end as localidade_gasto,
            safe_cast(`Código Função` as string) codigo_funcao,
            safe_cast(`Nome Função` as string) nome_funcao,
            safe_cast(`Código Subfunção` as string) codigo_subfuncao,
            safe_cast(`Nome Subfunção` as string) nome_subfuncao,
            safe_cast(`Valor Empenhado` as float64) valor_empenhado,
            safe_cast(`Valor Liquidado` as float64) valor_liquidado,
            safe_cast(`Valor Pago` as float64) valor_pago,
            safe_cast(
                `Valor Restos A Pagar Inscritos` as float64
            ) valor_resto_pagar_inscrito,
            safe_cast(
                `Valor Restos A Pagar Cancelados` as float64
            ) valor_resto_pagar_cancelado,
            safe_cast(`Valor Restos A Pagar Pagos` as float64) valor_resto_pagar_pagos,
        from {{ project_path("br_cgu_emendas_parlamentares_staging.microdados") }}
    ),
    tabela_1 as (
        select
            ano_emenda,
            codigo_emenda,
            numero_emenda,
            tipo_emenda,
            codigo_autor_emenda,
            nome_autor_emenda,
            localidade_gasto,
            if
            (
                regexp_contains(localidade_gasto, r'\(UF\)$'),
                null,
                trim(split(localidade_gasto, " - ")[safe_offset(0)])
            ) as localidade_municipio,
            trim(
                split(localidade_gasto, " - ")[safe_offset(1)]
            ) localidade_estado_sigla,
            if
            (
                regexp_contains(localidade_gasto, r'\(UF\)$'),
                trim(split(localidade_gasto, " (UF)")[safe_offset(0)]),
                null
            ) as localidade_estado,
            codigo_funcao,
            nome_funcao,
            codigo_subfuncao,
            nome_subfuncao,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            valor_resto_pagar_inscrito,
            valor_resto_pagar_cancelado,
            valor_resto_pagar_pagos,
        from tabela_inicial
    ),
    tabela_2 as (
        select *, sigla_uf.sigla as sigla_uf,
        from tabela_1
        left join
            `basedosdados.br_bd_diretorios_brasil.uf` as sigla_uf
            on localidade_estado = upper(sigla_uf.nome)
    ),
    tabela_3 as (
        select
            ano_emenda,
            codigo_emenda,
            numero_emenda,
            tipo_emenda,
            codigo_autor_emenda,
            nome_autor_emenda,
            localidade_gasto,
            case
                when localidade_municipio = 'TRAJANO DE MORAIS'
                then "TRAJANO DE MORAES"
                when localidade_municipio = "SANTANA DO LIVRAMENTO"
                then upper("Sant'Ana do Livramento")
                when localidade_municipio = "EMBU"
                then "EMBU DAS ARTES"
                when localidade_municipio = "ELDORADO DOS CARAJÁS"
                then "ELDORADO DO CARAJÁS"
                when localidade_municipio = "AUGUSTO SEVERO"
                then "CAMPO GRANDE"
                -- -
                -- https://g1.globo.com/rn/rio-grande-do-norte/noticia/campo-grande-ou-augusto-severo-populacao-pode-decidir-nas-eleicoes-qual-o-nome-da-cidade.ghtml
                else upper(localidade_municipio)
            end as localidade_municipio,
            coalesce(localidade_estado_sigla, sigla_uf) as localidade_sigla_uf,
            codigo_funcao,
            nome_funcao,
            codigo_subfuncao,
            nome_subfuncao,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            valor_resto_pagar_inscrito,
            valor_resto_pagar_cancelado,
            valor_resto_pagar_pagos
        from tabela_2
    ),
    tabela_4 as (
        select
            ano_emenda,
            codigo_emenda,
            numero_emenda,
            tipo_emenda,
            codigo_autor_emenda,
            nome_autor_emenda,
            localidade_gasto,
            id_municipio.id_municipio as id_municipio_gasto,
            localidade_sigla_uf as sigla_uf_gasto,
            codigo_funcao,
            nome_funcao,
            codigo_subfuncao,
            nome_subfuncao,
            valor_empenhado,
            valor_liquidado,
            valor_pago,
            valor_resto_pagar_inscrito,
            valor_resto_pagar_cancelado,
            valor_resto_pagar_pagos
        from tabela_3
        left join
            `basedosdados.br_bd_diretorios_brasil.municipio` as id_municipio
            on localidade_municipio = upper(id_municipio.nome)
            and localidade_sigla_uf = id_municipio.sigla_uf
    )
select *
from tabela_4
