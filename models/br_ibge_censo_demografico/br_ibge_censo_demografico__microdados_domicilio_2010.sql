{{
    config(
        alias="microdados_domicilio_2010",
        schema="br_ibge_censo_demografico",
        materialized="table",
        partition_by={
            "field": "sigla_uf",
            "data_type": "string",
        },
    )
}}
select
    safe_cast(id_regiao as string) id_regiao,
    safe_cast(sigla_uf as string) sigla_uf,
    safe_cast(id_mesorregiao as string) id_mesorregiao,
    safe_cast(id_microrregiao as string) id_microrregiao,
    safe_cast(id_regiao_metropolitana as string) id_regiao_metropolitana,
    safe_cast(id_municipio as string) id_municipio,
    safe_cast(situacao_setor as int64) situacao_setor,
    safe_cast(situacao_domicilio as int64) situacao_domicilio,
    safe_cast(controle as int64) controle,
    safe_cast(peso_amostral as float64) peso_amostral,
    safe_cast(area_ponderacao as int64) area_ponderacao,
    safe_cast(v4001 as string) v4001,
    safe_cast(v4002 as string) v4002,
    safe_cast(v0201 as string) v0201,
    safe_cast(v2011 as string) v2011,
    safe_cast(v2012 as float64) v2012,
    safe_cast(v0202 as string) v0202,
    safe_cast(v0203 as string) v0203,
    safe_cast(v6203 as float64) v6203,
    safe_cast(v0204 as int64) v0204,
    safe_cast(v6204 as float64) v6204,
    safe_cast(v0205 as string) v0205,
    safe_cast(v0206 as string) v0206,
    safe_cast(v0207 as string) v0207,
    safe_cast(v0208 as string) v0208,
    safe_cast(v0209 as string) v0209,
    safe_cast(v0210 as string) v0210,
    safe_cast(v0211 as string) v0211,
    safe_cast(v0212 as string) v0212,
    safe_cast(v0213 as string) v0213,
    safe_cast(v0214 as string) v0214,
    safe_cast(v0215 as string) v0215,
    safe_cast(v0216 as string) v0216,
    safe_cast(v0217 as string) v0217,
    safe_cast(v0218 as string) v0218,
    safe_cast(v0219 as string) v0219,
    safe_cast(v0220 as string) v0220,
    safe_cast(v0221 as string) v0221,
    safe_cast(v0222 as string) v0222,
    safe_cast(v0301 as string) v0301,
    safe_cast(v0401 as string) v0401,
    safe_cast(v0402 as string) v0402,
    safe_cast(v0701 as string) v0701,
    safe_cast(v6529 as int64) v6529,
    safe_cast(v6530 as float64) v6530,
    safe_cast(v6531 as int64) v6531,
    safe_cast(v6532 as float64) v6532,
    safe_cast(v6600 as int64) v6600,
    safe_cast(v6210 as int64) v6210,
    safe_cast(m0201 as string) m0201,
    safe_cast(m02011 as string) m02011,
    safe_cast(m0202 as string) m0202,
    safe_cast(m0203 as string) m0203,
    safe_cast(m0204 as string) m0204,
    safe_cast(m0205 as string) m0205,
    safe_cast(m0206 as string) m0206,
    safe_cast(m0207 as string) m0207,
    safe_cast(m0208 as string) m0208,
    safe_cast(m0209 as string) m0209,
    safe_cast(m0210 as string) m0210,
    safe_cast(m0211 as string) m0211,
    safe_cast(m0212 as string) m0212,
    safe_cast(m0213 as string) m0213,
    safe_cast(m0214 as string) m0214,
    safe_cast(m0215 as string) m0215,
    safe_cast(m0216 as string) m0216,
    safe_cast(m0217 as string) m0217,
    safe_cast(m0218 as string) m0218,
    safe_cast(m0219 as string) m0219,
    safe_cast(m0220 as string) m0220,
    safe_cast(m0221 as string) m0221,
    safe_cast(m0222 as string) m0222,
    safe_cast(m0301 as string) m0301,
    safe_cast(m0401 as string) m0401,
    safe_cast(m0402 as string) m0402,
    safe_cast(m0701 as string) m0701
from {{ project_path("br_ibge_censo_demografico_staging.microdados_domicilio_2010") }}t
