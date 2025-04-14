{{
    config(
        materialized="table",
        partition_by={
            "field": "data_consulta",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}


with
    tabela_ordenada as (
        select
            parse_date('%Y-%m-%d', dia) as data_consulta,
            time(
                extract(hour from parse_datetime('%Y-%m-%d %H:%M:%S', data_hora)),
                extract(minute from parse_datetime('%Y-%m-%d %H:%M:%S', data_hora)),
                extract(second from parse_datetime('%Y-%m-%d %H:%M:%S', data_hora))
            ) as hora_consulta,
            secao_site,
            lpad(item_id, 12, '0') as id_item,
            case when vendedor = 'None' then null else vendedor end vendedor,
            titulo,
            categorias,
            case
                when categorias = '[]'
                then null
                when trim(json_extract_array(categorias)[offset(1)], '"') = '...'
                then trim(json_extract_array(categorias)[offset(2)], '"')
                when trim(json_extract_array(categorias)[offset(0)], '"') = '...'
                then trim(json_extract_array(categorias)[offset(1)], '"')
                else trim(json_extract_array(categorias)[offset(0)], '"')
            end as categoria_principal,
            case
                when categorias = '[]'
                then null
                when trim(json_extract_array(categorias)[offset(1)], '"') = '...'
                then
                    array_to_string(
                        array(
                            select x
                            from unnest(json_extract_array(categorias)) as x
                            with
                            offset
                            where
                            offset > 3
                        ),
                        ', '
                    )
                when trim(json_extract_array(categorias)[offset(0)], '"') = '...'
                then
                    array_to_string(
                        array(
                            select x
                            from unnest(json_extract_array(categorias)) as x
                            with
                            offset
                            where
                            offset > 1
                        ),
                        ', '
                    )
                else
                    array_to_string(
                        array(
                            select x
                            from unnest(json_extract_array(categorias)) as x
                            with
                            offset
                            where
                            offset > 0
                        ),
                        ', '
                    )
            end as outras_categorias,
            case
                when caracteristicas = '{}' then null else caracteristicas
            end caracteristicas,
            safe_cast(envio_pais as bool) envio_nacional,
            safe_cast(quantidade_avaliacoes as int64) quantidade_avaliacao,
            safe_cast(estrelas as float64) avaliacao,
            safe_cast(
                case
                    when preco_original = 'nan' then null else preco_original
                end as float64
            ) as preco_original,
            safe_cast(desconto as int64) desconto,
            safe_cast(preco as float64) as preco,
        from {{ set_datalake_project("br_mercadolivre_ofertas_staging.item") }}

    ),
    tabela_preco as (
        select
            data_consulta,
            hora_consulta,
            secao_site,
            id_item,
            titulo,
            vendedor,
            categoria_principal,
            regexp_replace(
                trim(outras_categorias, '"'), r'("([^"]+)")', r'\2'
            ) as outras_categorias,
            caracteristicas,
            envio_nacional,
            quantidade_avaliacao,
            avaliacao,
            case
                when preco_original < preco
                then preco
                when preco_original = preco
                then null
                else preco_original
            end preco_original,
            desconto,
            case
                when preco > preco_original
                then preco_original
                when preco = preco_original
                then null
                else preco
            end preco_final,
        from tabela_ordenada

    ),
    tabela_desconto_calculado as

    (
        select
            data_consulta,
            hora_consulta,
            id_item,
            titulo,
            id_vendor as id_vendedor,
            vendedor,
            a.categoria_principal,
            outras_categorias,
            caracteristicas,
            envio_nacional,
            quantidade_avaliacao,
            avaliacao,
            round(
                case
                    when preco_original is null
                    then preco_final * 100 / (100 - desconto)
                    else preco_original
                end,
                2
            ) as preco_original,
            cast(
                case
                    when desconto is null
                    then 100 - (preco_final * 100 / preco_original)
                    else desconto
                end as int
            ) as desconto,
            cast(
                100 - (100 * preco_final / preco_original) as int64
            ) desconto_caclculado,
            round(
                case
                    when preco_final is null
                    then preco_original * (100 - desconto) / 100
                    else preco_final
                end,
                2
            ) as preco_final

        from tabela_preco a

        left join
            (
                select distinct dia, lpad(id_vendor, 12, '0') as id_vendor, nome
                from
                    {{
                        set_datalake_project(
                            "br_mercadolivre_ofertas_staging.vendedor"
                        )
                    }}
            ) b
            on a.vendedor = b.nome
            and data_consulta = parse_date('%Y-%m-%d', dia)
        where
            not (preco_original is null and preco_final is null)
            and not (preco_final is null and desconto is null)
            and not (preco_original is null and desconto is null)
    )
select
    data_consulta,
    hora_consulta,
    id_item,
    titulo,
    id_vendedor,
    vendedor,
    categoria_principal,
    outras_categorias,
    caracteristicas,
    envio_nacional,
    quantidade_avaliacao,
    avaliacao,
    preco_original,
    case
        when abs(desconto_caclculado - desconto) > 3
        then desconto_caclculado
        else desconto
    end as desconto,
    preco_final
from tabela_desconto_calculado
