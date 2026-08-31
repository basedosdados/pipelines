{{
    config(
        alias="pagamento",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2008, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Pagamentos dos governos estaduais, no nível do documento de pagamento (ordem
-- bancária). Uma linha por lançamento de pagamento, com data, valor, credor e o vínculo
-- com o empenho.
--
-- Esta é a tabela equivalente a `pagamento` do MiDES, e existe apenas para Pernambuco.
-- É a única fonte do conjunto que publica a execução em nível de documento de
-- pagamento: Minas Gerais publica somente a coluna `vr_pago` na linha da despesa, a
-- Bahia não publica documento de pagamento algum, e São Paulo é anual.
--
-- Ligue a `despesa` por `id_empenho_bd`. Note que a soma de `valor_pago` aqui NÃO tem
-- de bater com `despesa.valor_pago`: aquela coluna é o total pago do empenho no
-- exercício, esta tabela traz cada ordem bancária individualmente, e inclui
-- lançamentos devolvidos e cancelados. Filtre por `situacao = 'PAGA'` antes de somar.
select *
from {{ ref("br_bd_execucao_estadual__pagamento_pe") }}
