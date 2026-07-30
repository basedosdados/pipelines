{{
    config(
        schema="test_dataset",
        alias="tabela_pequena_bdpro",
        materialized="table",
        post_hook=[
            'CREATE OR REPLACE ROW ACCESS POLICY bdpro_filter ON `{{ this.database }}.{{ this.schema }}.{{ this.identifier }}` GRANT TO ("group:bd-pro@basedosdados.org", "group:sudo@basedosdados.org") FILTER USING (TRUE)'
        ],
    )
}}

-- Mesmo dado que tabela_pequena; post-hook adiciona bdpro_filter para testar
-- a condição < 100 MB com row access policy
select
    n as id,
    farm_fingerprint(cast(n as string)) as col1,
    farm_fingerprint(cast(n * 31 as string)) as col2,
    farm_fingerprint(cast(n * 97 as string)) as col3,
    farm_fingerprint(cast(n * 257 as string)) as col4,
    farm_fingerprint(cast(n * 1031 as string)) as col5
from unnest(generate_array(1, 1000)) as n
