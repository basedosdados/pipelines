{{ config(alias="cep", schema="br_bd_diretorios_brasil", materialized="table") }}
--
with
    t1 as (
        select
            safe_cast(lpad(cep, 8, '0') as string) cep,
            string_agg(distinct(t.id_municipio)) id_municipio,
            string_agg(distinct(sigla_uf)) sigla_uf,
            string_agg(initcap(descricao_estabelecimento), ", ") estabelecimentos,
            st_centroid_agg(ponto) as centroide
        from `basedosdados.br_ibge_censo_2022.cadastro_enderecos` as t
        group by 1
    ),

    logradouro_n_linhas as (
        select
            cep,
            replace(
                concat(
                    ifnull(tipo_segmento_logradouro, ""),
                    " ",
                    ifnull(titulo_segmento_logradouro, ''),
                    " ",
                    ifnull(nome_logradouro, "")
                ),
                "  ",
                " "
            ) logradouro,
            count(1) n_linhas,
        from `basedosdados.br_ibge_censo_2022.cadastro_enderecos` as t
        group by 1, 2
    ),

    logradouro_max as (
        select cep, max_by(initcap(logradouro), n_linhas) as logradouro
        from logradouro_n_linhas
        group by 1
    ),

    localidade_n_linhas as (
        select cep, localidade, count(1) n_linhas,
        from `basedosdados.br_ibge_censo_2022.cadastro_enderecos` as t
        group by 1, 2
    ),

    localidade_max as (
        select cep, max_by(initcap(localidade), n_linhas) as localidade
        from localidade_n_linhas
        group by 1
    )

select
    t1.cep,
    logradouro,
    localidade,
    t1.id_municipio,
    municipio.nome as nome_municipio,
    t1.sigla_uf,
    estabelecimentos,
    t1.centroide
from t1
left join
    `basedosdados.br_bd_diretorios_brasil.municipio` as municipio
    on t1.id_municipio = municipio.id_municipio
left join logradouro_max on t1.cep = logradouro_max.cep
left join localidade_max on t1.cep = localidade_max.cep
