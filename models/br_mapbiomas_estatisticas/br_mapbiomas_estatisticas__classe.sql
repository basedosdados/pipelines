{{
    config(
        schema="br_mapbiomas_estatisticas",
        alias="classe",
        materialized="table",
    )
}}


select
    safe_cast(chave as string) chave,
    safe_cast(nivel as string) nivel,
    safe_cast(origem as string) origem,
    safe_cast(codigo_hierarquia as string) codigo_hierarquia,
    safe_cast(valor_pt as string) valor_pt,
    safe_cast(valor_en as string) valor_en,
    safe_cast(valor_es as string) valor_es,
    safe_cast(nivel_1_pt as string) nivel_1_pt,
    safe_cast(nivel_1_en as string) nivel_1_en,
    safe_cast(nivel_1_es as string) nivel_1_es,
    safe_cast(nivel_2_pt as string) nivel_2_pt,
    safe_cast(nivel_2_en as string) nivel_2_en,
    safe_cast(nivel_2_es as string) nivel_2_es,
    safe_cast(nivel_3_pt as string) nivel_3_pt,
    safe_cast(nivel_3_en as string) nivel_3_en,
    safe_cast(nivel_3_es as string) nivel_3_es,
    safe_cast(nivel_4_pt as string) nivel_4_pt,
    safe_cast(nivel_4_en as string) nivel_4_en,
    safe_cast(nivel_4_es as string) nivel_4_es,
    safe_cast(cor_hex as string) cor_hex,
    safe_cast(observacoes as string) observacoes
from {{ set_datalake_project("br_mapbiomas_estatisticas_staging.classe") }} as t
