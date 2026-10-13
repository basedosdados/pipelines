-- Distrito Federal (DF) contribution to world_wb_mides.pagamento.
-- Split out of the monolithic pagamento model. The SQL is unchanged: the CTE
-- bodies are byte-identical and the union order is preserved, so this model
-- emits exactly the rows and column positions it did before the split.
-- Column names are positional -- the parent model applies the canonical list.
-- Materialisation is set in dbt_project.yml (models/world_wb_mides/states).
with
    pagamento_df as (
        select
            (safe_cast(exercicio as int64)) as ano,
            safe_cast(substring(emissao, -7, 2) as int64) as mes,
            safe_cast(
                concat(
                    substring(emissao, -4),
                    '-',
                    substring(emissao, -7, 2),
                    '-',
                    substring(emissao, 1, 2)
                ) as date
            ) as data,
            'DF' as sigla_uf,
            '5300108' as id_municipio,
            safe_cast(codigo_ug as string) as orgao,
            safe_cast(codigo_gestao as string) as id_unidade_gestora,
            safe_cast(
                concat(
                    right(nota_empenho, length(nota_empenho) - 6),
                    ' ',
                    codigo_ug,
                    ' ',
                    codigo_gestao,
                    ' ',
                    '5300108',
                    ' ',
                    (right(exercicio, 2))
                ) as string
            ) as id_empenho_bd,
            safe_cast(null as string) as id_empenho,
            safe_cast(nota_empenho as string) as numero_empenho,
            case
                when length(nota_lancamento) = 11
                then
                    safe_cast(
                        concat(
                            right(nota_lancamento, length(nota_lancamento) - 6),
                            ' ',
                            codigo_ug,
                            ' ',
                            codigo_gestao,
                            ' ',
                            '5300108',
                            ' ',
                            (right(exercicio, 2))
                        ) as string
                    )
            end as id_liquidacao_bd,
            safe_cast(null as string) as id_liquidacao,
            safe_cast(nota_lancamento as string) as numero_liquidacao,
            case
                when length(numero_ordem_bancaria) = 11
                then
                    safe_cast(
                        concat(
                            right(
                                numero_ordem_bancaria, length(numero_ordem_bancaria) - 6
                            ),
                            ' ',
                            codigo_ug,
                            ' ',
                            codigo_gestao,
                            ' ',
                            '5300108',
                            ' ',
                            (right(exercicio, 2))
                        ) as string
                    )
            end as id_pagamento_bd,
            safe_cast(null as string) as id_pagamento,
            safe_cast(numero_ordem_bancaria as string) as numero,
            safe_cast(credor as string) as nome_credor,
            safe_cast(cnpj_cpf_credor as string) as documento_credor,
            case
                when ano_ordem_bancaria != ano_nota_empenho then true else false
            end as indicador_restos_pagar,
            safe_cast(null as string) as fonte,
            round(
                safe_cast(replace(valor_final_x, ',', '.') as float64), 2
            ) as valor_inicial,
            round(
                safe_cast(replace(valor_cancelado, ',', '.') as float64), 2
            ) as valor_anulacao,
            round(safe_cast(0 as float64), 2) as valor_ajuste,
            round(
                safe_cast(replace(valor_final_x, ',', '.') as float64)
                - safe_cast(replace(valor_cancelado, ',', '.') as float64),
                2
            ) as valor_final,
            round(
                safe_cast(replace(valor_final_x, ',', '.') as float64)
                - safe_cast(replace(valor_cancelado, ',', '.') as float64),
                2
            ) as valor_liquido_recebido,
        from {{ set_datalake_project("world_wb_mides_staging.raw_pagamento_df") }}

    )
select *
from pagamento_df
