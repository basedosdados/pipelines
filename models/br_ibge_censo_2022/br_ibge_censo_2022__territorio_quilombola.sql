{{
    config(
        alias="territorio_quilombola",
        schema="br_ibge_censo_2022",
    )
}}
select
    safe_cast(cod_ as string) id_territorio_quilombola,
    safe_cast(
        trim(
            regexp_extract(territorio_quilombola_por_unidade_da_federacao, r'([^\(]+)')
        ) as string
    ) territorio_quilombola,
    case
        when
            regexp_contains(
                territorio_quilombola_por_unidade_da_federacao, r'\(\w{2}\)'
            )
        then
            safe_cast(
                trim(
                    regexp_extract(
                        territorio_quilombola_por_unidade_da_federacao, r'\((\w{2})\)'
                    )
                ) as string
            )
        else
            safe_cast(
                trim(
                    split(
                        split(territorio_quilombola_por_unidade_da_federacao, '(')[
                            safe_offset(2)
                        ],
                        ')'
                    )[safe_offset(0)]
                ) as string
            )
    end as sigla_uf,
    safe_cast(
        domicilios_particulares_permanentes_ocupados_localizados_em_territorios_quilombolas_domicilios_
        as string
    ) domicilios,
    safe_cast(
        moradores_em_domicilios_particulares_permanentes_ocupados_localizados_em_territorios_quilombolas_pessoas_
        as int64
    ) populacao,
    safe_cast(
        moradores_quilombolas_em_domicilios_particulares_permanentes_ocupados_localizados_em_territorios_quilombolas_pessoas_
        as int64
    ) populacao_quilombola,
# SAFE_CAST(media_moradores_domicilios_pelo_menos_um_territorios_quilombolas AS
# FLOAT64) media_moradores_domicilios_pelo_menos_um_territorios_quilombolas,
# SAFE_CAST(media_moradores_quilombolas_domicilios_pelo_menos_um_territorios_quilombolas AS FLOAT64) media_moradores_quilombolas_domicilios_pelo_menos_um_territorios_quilombolas,
from {{ set_datalake_project("br_ibge_censo_2022_staging.territorio_quilombola") }} as t
