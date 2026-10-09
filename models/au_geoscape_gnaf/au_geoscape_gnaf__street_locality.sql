{{
    config(
        schema="au_geoscape_gnaf",
        alias="street_locality",
        materialized="incremental",
        partition_by={
            "field": "snapshot_date",
            "data_type": "date",
        },
        cluster_by=["year", "id_state"],
    )
}}
select
    safe_cast(snapshot_date as date) snapshot_date,
    safe_cast(extract(year from date(snapshot_date)) as int64) year,
    safe_cast(id_state as string) id_state,
    safe_cast(street_locality_pid as string) street_locality_pid,
    safe_cast(date_created as date) date_created,
    safe_cast(date_retired as date) date_retired,
    safe_cast(street_name as string) street_name,
    safe_cast(street_type as string) street_type,
    safe_cast(street_suffix as string) street_suffix,
    safe_cast(street_class as string) street_class,
    safe_cast(locality_pid as string) locality_pid,
    safe_cast(gnaf_street_pid as string) gnaf_street_pid,
    safe_cast(gnaf_reliability as string) gnaf_reliability,
    safe_cast(longitude as float64) longitude,
    safe_cast(latitude as float64) latitude
from {{ set_datalake_project("au_geoscape_gnaf_staging.street_locality") }} as t
{% if is_incremental() %}
    where safe_cast(snapshot_date as date) > (select max(snapshot_date) from {{ this }})
{% endif %}
