{{
    config(
        schema="br_mgi_pncp",
        alias="dicionario",
        materialized="table",
    )
}}


-- Derived from the fact models rather than from staging, so the
-- dictionary always describes exactly the data it documents. See
-- gen_dbt.dicionario_sql for why.
with
    derived as (
        select
            'contratacao' id_tabela,
            'id_modalidade' nome_coluna,
            safe_cast(id_modalidade as string) chave,
            '' cobertura_temporal,
            safe_cast(modalidade as string) valor
        from {{ ref("br_mgi_pncp__contratacao") }}
        where id_modalidade is not null and modalidade is not null
        union all
        select
            'contratacao' id_tabela,
            'id_modo_disputa' nome_coluna,
            safe_cast(id_modo_disputa as string) chave,
            '' cobertura_temporal,
            safe_cast(modo_disputa as string) valor
        from {{ ref("br_mgi_pncp__contratacao") }}
        where id_modo_disputa is not null and modo_disputa is not null
        union all
        select
            'contratacao' id_tabela,
            'id_situacao_compra' nome_coluna,
            safe_cast(id_situacao_compra as string) chave,
            '' cobertura_temporal,
            safe_cast(situacao_compra as string) valor
        from {{ ref("br_mgi_pncp__contratacao") }}
        where id_situacao_compra is not null and situacao_compra is not null
        union all
        select
            'contratacao' id_tabela,
            'id_tipo_instrumento_convocatorio' nome_coluna,
            safe_cast(id_tipo_instrumento_convocatorio as string) chave,
            '' cobertura_temporal,
            safe_cast(tipo_instrumento_convocatorio as string) valor
        from {{ ref("br_mgi_pncp__contratacao") }}
        where
            id_tipo_instrumento_convocatorio is not null
            and tipo_instrumento_convocatorio is not null
        union all
        select
            'contratacao' id_tabela,
            'codigo_amparo_legal' nome_coluna,
            safe_cast(codigo_amparo_legal as string) chave,
            '' cobertura_temporal,
            safe_cast(nome_amparo_legal as string) valor
        from {{ ref("br_mgi_pncp__contratacao") }}
        where codigo_amparo_legal is not null and nome_amparo_legal is not null
        union all
        select
            'contrato' id_tabela,
            'id_tipo_contrato' nome_coluna,
            safe_cast(id_tipo_contrato as string) chave,
            '' cobertura_temporal,
            safe_cast(tipo_contrato as string) valor
        from {{ ref("br_mgi_pncp__contrato") }}
        where id_tipo_contrato is not null and tipo_contrato is not null
        union all
        select
            'contrato' id_tabela,
            'id_categoria_processo' nome_coluna,
            safe_cast(id_categoria_processo as string) chave,
            '' cobertura_temporal,
            safe_cast(categoria_processo as string) valor
        from {{ ref("br_mgi_pncp__contrato") }}
        where id_categoria_processo is not null and categoria_processo is not null
        union all
        select
            'instrumento_cobranca' id_tabela,
            'id_tipo_instrumento_cobranca' nome_coluna,
            safe_cast(id_tipo_instrumento_cobranca as string) chave,
            '' cobertura_temporal,
            safe_cast(tipo_instrumento_cobranca as string) valor
        from {{ ref("br_mgi_pncp__instrumento_cobranca") }}
        where
            id_tipo_instrumento_cobranca is not null
            and tipo_instrumento_cobranca is not null
    ),
    -- Codes the API never labels; source: PNCP manual de integração.
    hardcoded as (
        select *
        from
            unnest(
                [
                    struct(
                        'contratacao' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'F' as chave,
                        '' as cobertura_temporal,
                        'Federal' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'E' as chave,
                        '' as cobertura_temporal,
                        'Estadual' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'M' as chave,
                        '' as cobertura_temporal,
                        'Municipal' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'D' as chave,
                        '' as cobertura_temporal,
                        'Distrital' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'N' as chave,
                        '' as cobertura_temporal,
                        'Não se aplica' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'F' as chave,
                        '' as cobertura_temporal,
                        'Federal' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'E' as chave,
                        '' as cobertura_temporal,
                        'Estadual' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'M' as chave,
                        '' as cobertura_temporal,
                        'Municipal' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'D' as chave,
                        '' as cobertura_temporal,
                        'Distrital' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_esfera' as nome_coluna,
                        'N' as chave,
                        '' as cobertura_temporal,
                        'Não se aplica' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_poder' as nome_coluna,
                        'E' as chave,
                        '' as cobertura_temporal,
                        'Executivo' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_poder' as nome_coluna,
                        'L' as chave,
                        '' as cobertura_temporal,
                        'Legislativo' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_poder' as nome_coluna,
                        'J' as chave,
                        '' as cobertura_temporal,
                        'Judiciário' as valor
                    ),
                    struct(
                        'contratacao' as id_tabela,
                        'id_poder' as nome_coluna,
                        'N' as chave,
                        '' as cobertura_temporal,
                        'Não se aplica' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_poder' as nome_coluna,
                        'E' as chave,
                        '' as cobertura_temporal,
                        'Executivo' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_poder' as nome_coluna,
                        'L' as chave,
                        '' as cobertura_temporal,
                        'Legislativo' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_poder' as nome_coluna,
                        'J' as chave,
                        '' as cobertura_temporal,
                        'Judiciário' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'id_poder' as nome_coluna,
                        'N' as chave,
                        '' as cobertura_temporal,
                        'Não se aplica' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'tipo_pessoa_fornecedor' as nome_coluna,
                        'PJ' as chave,
                        '' as cobertura_temporal,
                        'Pessoa jurídica' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'tipo_pessoa_fornecedor' as nome_coluna,
                        'PF' as chave,
                        '' as cobertura_temporal,
                        'Pessoa física' as valor
                    ),
                    struct(
                        'contrato' as id_tabela,
                        'tipo_pessoa_fornecedor' as nome_coluna,
                        'PE' as chave,
                        '' as cobertura_temporal,
                        'Pessoa estrangeira' as valor
                    )
                ]
            )
    )
select id_tabela, nome_coluna, chave, cobertura_temporal, valor
from
    (
        select *
        from derived
        union all
        select *
        from hardcoded
    )
qualify
    row_number() over (partition by id_tabela, nome_coluna, chave order by valor) = 1
