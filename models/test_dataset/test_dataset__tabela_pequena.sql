{{
    config(
        schema="test_dataset",
        alias="tabela_pequena",
        materialized="table",
    )
}}

-- ~1 000 linhas × 48 bytes ≈ 48 KB — garante condição < 100 MB sem bdpro_filter
select
    n as id,
    farm_fingerprint(cast(n as string)) as col1,
    farm_fingerprint(cast(n * 31 as string)) as col2,
    farm_fingerprint(cast(n * 97 as string)) as col3,
    farm_fingerprint(cast(n * 257 as string)) as col4,
    farm_fingerprint(cast(n * 1031 as string)) as col5
from unnest(generate_array(1, 1000)) as n
