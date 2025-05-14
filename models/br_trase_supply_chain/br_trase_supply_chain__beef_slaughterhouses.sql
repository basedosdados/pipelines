{{ config(alias="beef_slaughterhouses", schema="br_trase_supply_chain") }}


select
    safe_cast(the_geom as string) geom_id,
    safe_cast(cartodb_id as string) cartodb_id,
    safe_cast(the_geom_webmercator as string) geom_webmercator_id,
    safe_cast(geocode as string) municipality_id,
    safe_cast(state as string) state,
    safe_cast(address as string) address,
    safe_cast(id as string) slaugtherhouse_id,
    safe_cast(company as string) company,
    safe_cast(other_names as string) other_company_names,
    safe_cast(multifunctions as string) multifunctions,
    safe_cast(resolution as string) resolution_id,
    safe_cast(subclass as string) subclass,
    safe_cast(inspection_level as string) inspection_level,
    safe_cast(replace(inspection_number, 'NA', '') as string) inspection_number,
    safe_cast(replace(tac, 'NA', '') as string) tac,
    safe_cast(regexp_replace(status, r'(?i)^NA$', '') as string) status,
    safe_cast(
        format_date(
            '%Y-%m-%d', safe.parse_date('%d/%m/%Y', date_sif_registered)
        ) as string
    ) date_sif_registered,
    safe_cast(replace(sif_category, 'NA', '') as string) sif_category,
    safe_cast(
        st_geogpoint(safe_cast(long as float64), safe_cast(lat as float64)) as geography
    ) point
from
    {{ set_datalake_project("br_trase_supply_chain_staging.beef_slaughterhouses") }}
    as t
