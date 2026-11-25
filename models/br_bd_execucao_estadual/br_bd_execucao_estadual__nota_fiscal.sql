{{
    config(
        alias="nota_fiscal",
        schema="br_bd_execucao_estadual",
        materialized="table",
        partition_by={
            "field": "ano",
            "data_type": "int64",
            "range": {"start": 2004, "end": 2031, "interval": 1},
        },
        cluster_by=["sigla_uf"],
        labels={"tema": "economia"},
    )
}}

-- Notas fiscais recebidas pelos governos estaduais: o documento que o fornecedor
-- emitiu,
-- com emissor, natureza, datas de emissão, recebimento e registro, e valor total. Uma
-- linha por nota.
--
-- É a fase de ENTREGA, que as outras tabelas não cobrem: `licitacao` é o que se
-- licitou,
-- `contrato` o que se comprometeu, `despesa`/`liquidacao`/`pagamento` o que se
-- gastou, e
-- esta é o que o fornecedor de fato faturou.
--
-- ATENÇÃO, limitação da fonte: a nota NÃO traz número de processo, de contrato nem de
-- empenho, em nenhum dos 114 arquivos do repositório de origem. Não há como ligá-la a
-- `contrato` ou `licitacao` por chave; a única ligação possível é probabilística, por
-- (id_unidade_gestora x documento_emissor x período) e, nos itens, pelo código de
-- catálogo. Use como painel de preços e prazos de entrega, não como elo contratual.
--
-- `ano` e `mes` são o mês de referência do arquivo de origem, não a data de emissão: a
-- data de emissão vai de 1969 a 2026 e 4,3% das linhas têm ano de emissão anterior ao
-- do
-- arquivo, e a tabela de itens não traz data nenhuma. As datas reais estão nas colunas.
--
-- Existe só para Minas Gerais.
select *
from {{ ref("br_bd_execucao_estadual__nota_fiscal_mg") }}
