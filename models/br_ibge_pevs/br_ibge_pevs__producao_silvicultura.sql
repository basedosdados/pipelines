-- - Durante a análise, identificamos que vários produtos não possuíam qualquer
-- registro de produção nos municípios. Isso resultava em uma tabela pouco útil, já
-- que cerca de 90% das linhas estavam vazias. Para melhorar a qualidade dos dados e
-- reduzir o peso da tabela, optamos por remover esses produtos sem produção, mantendo
-- apenas informações realmente relevantes para as consultas e análises.
{{
    config(
        alias="producao_silvicultura",
        schema="br_ibge_pevs",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 1986, "end": 2022, "interval": 1},
        },
        cluster_by=["id_municipio"],
    )
}}
select
    safe_cast(ano as int64) ano,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(categoria_produto as string) categoria_produto,
    safe_cast(tipo_produto as string) tipo_produto,
    safe_cast(subtipo_produto as string) subtipo_produto,
    safe_cast(produto as string) produto,
    safe_cast(unidade as string) unidade,
    safe_cast(quantidade as int64) quantidade,
    round(safe_cast(valor as float64), 4) valor,
from {{ set_datalake_project("br_ibge_pevs_staging.producao_silvicultura") }}
where
    produto is not null  -- isso faz categorias de agregação caírem
    and quantidade is not null  -- isso faz unidade vazia cair
