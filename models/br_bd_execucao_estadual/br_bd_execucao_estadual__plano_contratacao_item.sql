{{
    config(
        alias="plano_contratacao_item",
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

-- Plano anual de contratações dos governos estaduais, item a item: o que o estado
-- declarou que pretendia comprar, com quantidade, preço unitário previsto, ano de
-- entrega e município, antes de comprar. Uma linha por (ano-base, plano, item, versão).
--
-- É a única tabela EX-ANTE do conjunto. Todas as outras registram o que já aconteceu --
-- o que se licitou, contratou, faturou, pagou. Esta registra a intenção, o que torna
-- mensurável a distância entre o planejado e o executado e permite usar o plano inicial
-- como linha de base contra a qual medir discricionariedade posterior. Ligue a
-- `contrato_item`, `licitacao_item` e `nota_fiscal_item` por `codigo_catalogo`, e ao
-- processo que o item virou por `numero_processo`.
--
-- ATENÇÃO às versões. A fonte publica a versão em duas colunas, `versao` e
-- `versao_detalhe`, reproduzidas aqui exatamente como vêm. Em 2023, 2024 e 2025 as duas
-- concordam e há duas versões limpas por ano, inicial e revisão. Em 2026 elas se
-- contradizem: 98.544 linhas trazem versao='revisão' e versao_detalhe='INICIAL' ao
-- mesmo
-- tempo, e outras 109.511 não trazem versao alguma. Por isso NÃO se publica aqui um
-- número de revisão normalizado: construí-lo exigiria escolher em silêncio entre dois
-- rótulos contraditórios em 200 mil linhas. Para 2023-2025 use `versao`; para 2026,
-- examine as duas colunas.
--
-- `id_municipio` é derivado do código de seis dígitos da fonte pelo diretório de
-- municípios, e é nulo nas 156.256 linhas em que a fonte usa o sentinela estadual `14`.
--
-- Existe só para Minas Gerais.
select *
from {{ ref("br_bd_execucao_estadual__plano_contratacao_item_mg") }}
