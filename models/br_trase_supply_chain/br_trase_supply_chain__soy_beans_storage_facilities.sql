{{ config(alias="soy_beans_storage_facilities", schema="br_trase_supply_chain") }}


select
    safe_cast(the_geom as string) geom_id,
    safe_cast(cartodb_id as string) cartodb_id,
    safe_cast(the_geom_webmercator as string) geom_webmercator_id,
    safe_cast(geocode as string) municipality_id,
    safe_cast(uf as string) state,
    case
        when length(cnpj) = 18
        then regexp_replace(cnpj, r'[^0-9]', '')
        else concat('***', substr(cnpj, 4, length(cnpj) - 6), '***')
    end as cnpj_cpf,
    safe_cast(company as string) company,
    safe_cast(capacity as int64) capacity,
    safe_cast(
        st_geogpoint(safe_cast(long as float64), safe_cast(lat as float64)) as geography
    ) point,
    safe_cast(safe.parse_date("%Y-%m-%d", date) as date) date,
    safe_cast(subclass as string) subclass,
    safe_cast(dt as string) dt

from
    {{
        set_datalake_project(
            "br_trase_supply_chain_staging.soy_beans_storage_facilities"
        )
    }} t
