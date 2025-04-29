{{ config(alias="municipio", schema="br_ibge_censo_2022") }}
with
    domicilio_morador as (
        select
            safe_cast(ano as int64) ano,
            municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'([^\(]+)')) as string
            ) nome_municipio,
            safe_cast(
                trim(regexp_extract(municipio, r'\(([^)]+)\)')) as string
            ) sigla_uf,
            safe_cast(
                domicilios_particulares_permanentes_ocupados_domicilios_ as int64
            ) domicilios,
            safe_cast(
                moradores_em_domicilios_particulares_permanentes_ocupados_pessoas_
                as int64
            ) populacao,
        from {{ set_datalake_project("br_ibge_censo_2022_staging.municipio") }}
    ),

    area as (
        select
            safe_cast(ano as int64) ano,
            municipio,
            safe_cast(area_da_unidade_territorial_quilometros_quadrados_ as int64) area,
        from
            {{ set_datalake_project("br_ibge_censo_2022_staging.municipio_area") }} as t
    ),

    indice_envelhecimento as (
        select
            2022 as ano,
            municipio,
            safe_cast(
                replace(indice_de_envelhecimento_razao_, ",", ".") as float64
            ) indice_envelhecimento,
            safe_cast(replace(idade_mediana_anos_, ",", ".") as float64) idade_mediana,
            safe_cast(replace(razao_de_sexo_razao_, ",", ".") as float64) razao_sexo,
        from
            {{
                set_datalake_project(
                    "br_ibge_censo_2022_staging.municipio_indice_envelhecimento"
                )
            }}
    ),

    indigenas as (
        select
            safe_cast(ano as int64) ano,
            municipio,
            ifnull(
                sum(
                    safe_cast(
                        moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_pessoas_
                        as int64
                    )
                ),
                0
            ) populacao_indigena,
            sum(
                if(
                    localizacao_do_domicilio = 'Em terras indígenas',
                    safe_cast(
                        moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_pessoas_
                        as int64
                    ),
                    0
                )
            ) as populacao_indigena_terra_indigena
        from
            {{ set_datalake_project("br_ibge_censo_2022_staging.municipio_indigenas") }}
        group by 1, 2
    ),

    quilombolas as (
        select
            safe_cast(ano as int64) ano,
            municipio,
            ifnull(
                sum(
                    safe_cast(
                        moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_pessoas_
                        as int64
                    )
                ),
                0
            ) populacao_quilombola,
            sum(
                if(
                    localizacao_do_domicilio = 'Em territórios quilombolas',
                    safe_cast(
                        moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_pessoas_
                        as int64
                    ),
                    0
                )
            ) as populacao_quilombola_territorio_quilombola
        from
            {{
                set_datalake_project(
                    "br_ibge_censo_2022_staging.municipio_quilombolas"
                )
            }}
        group by 1, 2
    ),

    alfabetizacao as (
        select
            2022 as ano,
            safe_cast(munic_pio__c_digo_ as string) id_municipio,
            round(
                sum(if(alfabetiza__o = 'Alfabetizadas', safe_cast(valor as int64), 0))
                / sum(safe_cast(valor as int64)),
                5
            ) as taxa_alfabetizacao
        from
            {{
                set_datalake_project(
                    "br_ibge_censo_2022_staging.alfabetizacao_grupo_idade_sexo_raca"
                )
            }}
        group by 1, 2
    )

select
    auxiliary_table.cod as id_municipio,
    domicilio_morador.sigla_uf,
    domicilio_morador.domicilios,
    domicilio_morador.populacao,
    area.area,
    alfabetizacao.taxa_alfabetizacao,
    indice_envelhecimento.idade_mediana,
    indice_envelhecimento.razao_sexo,
    indice_envelhecimento.indice_envelhecimento,
    indigenas.populacao_indigena,
    indigenas.populacao_indigena_terra_indigena,
    quilombolas.populacao_quilombola,
    quilombolas.populacao_quilombola_territorio_quilombola,
from domicilio_morador
left join
    `basedosdados-staging.br_ibge_censo_2022_staging.auxiliary_table` as auxiliary_table
    on domicilio_morador.municipio = auxiliary_table.municipio
left join
    area
    on domicilio_morador.municipio = area.municipio
    and domicilio_morador.ano = area.ano
left join
    indice_envelhecimento
    on domicilio_morador.municipio = indice_envelhecimento.municipio
    and domicilio_morador.ano = indice_envelhecimento.ano
left join
    indigenas
    on domicilio_morador.municipio = indigenas.municipio
    and domicilio_morador.ano = indigenas.ano
left join
    quilombolas
    on domicilio_morador.municipio = quilombolas.municipio
    and domicilio_morador.ano = quilombolas.ano
left join
    alfabetizacao
    on auxiliary_table.cod = alfabetizacao.id_municipio
    and domicilio_morador.ano = alfabetizacao.ano
