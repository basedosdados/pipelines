{{
    config(
        schema="test_dataset",
        alias="test_table",
        materialized="table",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(github as string) github,
    safe_cast(idade as int64) idade,
    safe_cast(sexo as string) sexo
from {{ set_datalake_project("test_dataset_staging.test_table") }} as t
