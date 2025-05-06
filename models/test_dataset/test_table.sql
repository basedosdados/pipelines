{{
    config(
        schema="test_dataset",
        alias="test_table",
        materialized="table",
    )
}}


select
    safe_cast(ano as int64) ano,
    safe_cast(mes as int64) mes,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(dado as string) dado
from `basedosdados-staging.test_dataset_staging.test_table` as t
