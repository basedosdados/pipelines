{{
    config(
        schema="us_epa_tri",
        alias="form",
        materialized="table",
        partition_by={
            "field": "year",
            "data_type": "int64",
            "range": {"start": 1987, "end": 2035, "interval": 1},
        },
        cluster_by=["tri_facility_id", "tri_chemical_id"],
    )
}}

-- Atualizado em 2026-09-03
select
    safe_cast(year as int64) year,
    safe_cast(tri_facility_id as string) tri_facility_id,
    safe_cast(document_control_number as string) document_control_number,
    safe_cast(tri_chemical_id as string) tri_chemical_id,
    safe_cast(chemical_name as string) chemical_name,
    safe_cast(form_type as string) form_type,
    safe_cast(elemental_metal_included as string) elemental_metal_included,
    safe_cast(unit_of_measure as string) unit_of_measure,
    safe_cast(industry_sector_code as string) industry_sector_code,
    safe_cast(industry_sector as string) industry_sector,
    safe_cast(primary_sic as string) primary_sic,
    safe_cast(sic_2 as string) sic_2,
    safe_cast(sic_3 as string) sic_3,
    safe_cast(sic_4 as string) sic_4,
    safe_cast(sic_5 as string) sic_5,
    safe_cast(sic_6 as string) sic_6,
    safe_cast(primary_naics as string) primary_naics,
    safe_cast(naics_2 as string) naics_2,
    safe_cast(naics_3 as string) naics_3,
    safe_cast(naics_4 as string) naics_4,
    safe_cast(naics_5 as string) naics_5,
    safe_cast(naics_6 as string) naics_6,
    safe_cast(naics_version as string) naics_version,
    safe_cast(on_site_release_total as float64) on_site_release_total,
    safe_cast(potw_transfer_total as float64) potw_transfer_total,
    safe_cast(off_site_release_total as float64) off_site_release_total,
    safe_cast(off_site_recycling_total as float64) off_site_recycling_total,
    safe_cast(off_site_energy_recovery_total as float64) off_site_energy_recovery_total,
    safe_cast(off_site_treatment_total as float64) off_site_treatment_total,
    safe_cast(total_transfer as float64) total_transfer,
    safe_cast(total_releases as float64) total_releases,
    safe_cast(waste_released as float64) waste_released,
    safe_cast(
        waste_released_on_site_contained as float64
    ) waste_released_on_site_contained,
    safe_cast(waste_released_on_site_other as float64) waste_released_on_site_other,
    safe_cast(
        waste_released_off_site_contained as float64
    ) waste_released_off_site_contained,
    safe_cast(waste_released_off_site_other as float64) waste_released_off_site_other,
    safe_cast(waste_energy_recovery_on_site as float64) waste_energy_recovery_on_site,
    safe_cast(waste_energy_recovery_off_site as float64) waste_energy_recovery_off_site,
    safe_cast(waste_recycled_on_site as float64) waste_recycled_on_site,
    safe_cast(waste_recycled_off_site as float64) waste_recycled_off_site,
    safe_cast(waste_treated_on_site as float64) waste_treated_on_site,
    safe_cast(waste_treated_off_site as float64) waste_treated_off_site,
    safe_cast(production_related_waste as float64) production_related_waste,
    safe_cast(one_time_release as float64) one_time_release,
    safe_cast(production_ratio_type as string) production_ratio_type,
    safe_cast(production_ratio as float64) production_ratio
from {{ set_datalake_project("us_epa_tri_staging.form") }} as t
