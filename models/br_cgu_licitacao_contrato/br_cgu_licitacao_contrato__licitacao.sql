{{
    config(
        alias="licitacao", schema="br_cgu_licitacao_contrato", materialized="table"
    )
}}

select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    case when uf = "-3" then null else uf end as sigla_uf,
    dir_municipio.id_municipio,
    safe_cast(numero_licitacao as string) id_licitacao,
    safe_cast(codigo_ug as string) id_unidade_gestora,
    safe_cast(nome_ug as string) nome_unidade_gestora,
    safe_cast(codigo_modalidade_compra as string) id_modalidade_compra,
    safe_cast(modalidade_compra as string) modalidade_compra,
    safe_cast(numero_processo as string) numero_processo,
    safe_cast(objeto as string) objeto,
    safe_cast(situacao_licitacao as string) situacao_licitacao,
    safe_cast(codigo_orgao_superior as string) id_orgao_superior,
    safe_cast(nome_orgao_superior as string) nome_orgao_superior,
    safe_cast(codigo_orgao as string) id_orgao,
    safe_cast(nome_orgao as string) nome_orgao,
    safe_cast(
        parse_date('%d/%m/%Y', data_resultado_compra) as date
    ) data_resultado_compra,
    safe_cast(parse_date('%d/%m/%Y', data_abertura) as date) data_abertura,
    safe_cast(replace(valor_licitacao, ",", ".") as float64) valor_licitacao,
from
    {{ set_datalake_project("br_cgu_licitacao_contrato_staging.licitacao") }}
    as licitacao
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as dir_municipio
    on upper(dir_municipio.nome) = licitacao.municipio
    and dir_municipio.sigla_uf = licitacao.uf
