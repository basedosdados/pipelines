{{ config(alias="soy_beans_crushing_facilities", schema="br_trase_supply_chain") }}


select
    safe_cast(year as int64) year,
    safe_cast(the_geom as string) geom_id,
    safe_cast(cartodb_id as string) cartodb_id,
    safe_cast(the_geom_webmercator as string) geom_webmercator_id,
    safe_cast(geocode as string) municipality_id,
    safe_cast(uf as string) state,
    safe_cast(cf as string) crushing_facility_id,
    safe_cast(regexp_replace(cnpj, r'[^0-9]', '') as string) cnpj,
    safe_cast(company as string) company,
    safe_cast(capacity as int64) capacity,
    safe_cast(replace(capacity_source, 'NA', '') as string) capacity_source,
    safe_cast(status as string) status,
    safe_cast(
        st_geogpoint(safe_cast(long as float64), safe_cast(lat as float64)) as geography
    ) point,
from
    {{
        set_datalake_project(
            "br_trase_supply_chain_staging.soy_beans_crushing_facilities"
        )
    }} t
