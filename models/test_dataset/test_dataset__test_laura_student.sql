{{
    config(
        schema="test_dataset",
        alias="test_laura_student",
        materialized="table",
    )
}}

select
    safe_cast(year as int64) year,
    safe_cast(country_id_iso_3 as string) country_id_iso_3,
    safe_cast(school_id as string) school_id,
    safe_cast(student_id as string) student_id,
    safe_cast(gender as string) gender,
    safe_cast(mother_education as string) mother_education,
    safe_cast(father_education as string) father_education,
    safe_cast(computer_possession as string) computer_possession,
    safe_cast(internet_access as string) internet_access,
    safe_cast(desk_possession as string) desk_possession,
    safe_cast(room_possession as string) room_possession,
    safe_cast(dishwasher_possession as string) dishwasher_possession,
    safe_cast(television as string) television,
    safe_cast(computer as string) computer,
    safe_cast(car as string) car,
    safe_cast(book as string) book,
    safe_cast(wealth_index as float64) wealth_index,
    safe_cast(
        economic_social_cultural_status as float64
    ) economic_social_cultural_status,
    safe_cast(score_mathematics as float64) score_mathematics,
    safe_cast(score_reading as float64) score_reading,
    safe_cast(score_science as float64) score_science,
    safe_cast(student_weight as float64) student_weight
from {{ set_datalake_project("test_dataset_staging.test_laura_student") }} as t
