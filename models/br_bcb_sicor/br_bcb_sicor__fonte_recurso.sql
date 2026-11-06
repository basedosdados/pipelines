{{
    config(
        alias="fonte_recurso",
        schema="br_bcb_sicor",
        materialized="table",
    )
}}


-- `ltrim(id_fonte_recurso, '0')` reproduz o tratamento aplicado em
-- `br_bcb_sicor__operacao`, onde a coluna homônima também perde os zeros à
-- esquerda. Sem isso a chave publicada aqui ('0100') não casaria com a de lá
-- ('100').
select
    safe_cast(ltrim(id_fonte_recurso, '0') as string) id_fonte_recurso,
    safe_cast(descricao as string) descricao,
    safe_cast(indicador_recurso_publico as string) indicador_recurso_publico,
    safe_cast(parse_date("%d/%m/%Y", data_inicio) as date) data_inicio,
    safe_cast(parse_date("%d/%m/%Y", data_fim) as date) data_fim
from {{ set_datalake_project("br_bcb_sicor_staging.fonte_recurso") }} as t
