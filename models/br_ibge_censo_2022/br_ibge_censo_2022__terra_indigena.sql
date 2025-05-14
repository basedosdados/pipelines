{{
    config(
        alias="terra_indigena",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(cod_ as string) id_terra_indigena,
    safe_cast(
        trim(
            regexp_extract(terra_indigena_por_unidade_da_federacao, r'([^\(]+)')
        ) as string
    ) terra_indigena,
    case
        when regexp_contains(terra_indigena_por_unidade_da_federacao, r'\(\w{2}\)')
        then
            safe_cast(
                trim(
                    regexp_extract(
                        terra_indigena_por_unidade_da_federacao, r'\((\w{2})\)'
                    )
                ) as string
            )
        else
            safe_cast(
                trim(
                    split(
                        split(terra_indigena_por_unidade_da_federacao, '(')[
                            safe_offset(2)
                        ],
                        ')'
                    )[safe_offset(0)]
                ) as string
            )
    end as sigla_uf,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_domicilios_
        as int64
    ) domicilios,
    safe_cast(
        moradores_em_domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_pessoas_
        as int64
    ) populacao,
    safe_cast(
        moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_localizados_em_terras_indigenas_pessoas_
        as int64
    ) populacao_indigena,
# SAFE_CAST(REPLACE(media_de_moradores_indigenas_em_domicilios_particulares_permanentes_ocupados_com_pelo_menos_um_morador_indigena_localizados_em_terras_indigenas_pessoas_, ",", ".") AS FLOAT64) media_moradores_indigenas_domicilios_terras_indigenas,
from {{ set_datalake_project("br_ibge_censo_2022_staging.terra_indigena") }} t
