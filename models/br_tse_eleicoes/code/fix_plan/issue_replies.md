# Draft replies to the open issues (PT)

For Ricardo's review — do not post without approval. Each reply assumes the
header-aware refactor (fix_plan/02) is merged and the full rebuild (05) is
scheduled.

## #1568 — resultados_partido_secao cabeçalhos alterados entre 1994 e 2006

> Diagnóstico concluído. O código de parsing atual está correto — a saída
> bate byte a byte com a referência validada para 1996 e 1998 (incluindo a
> variante legada do arquivo BR), e o harness de diagnóstico
> (`code/python/diagnostics/`) não encontra divergência entre o código e os
> layouts oficiais atuais. O desalinhamento em produção (valores de `cargo`
> em `zona`, etc., anos < 2015) é a assinatura de um build antigo: arquivos
> de uma geração anterior do TSE foram lidos com o mapeamento posicional da
> geração seguinte. Desde o refactor, todos os builders leem por nome de
> coluna do cabeçalho, o que elimina essa classe de erro. A correção dos
> dados é o rebuild completo a partir de downloads atuais, já planejado
> (`code/fix_plan/05`). Fecharemos esta issue quando a tabela for
> rematerializada.

## #1046 — esfera_partidaria_fornecedor totalmente vazia

> Diagnóstico concluído: o campo vem 100% `#NULO#` da fonte. Verificamos os
> arquivos brutos do TSE (`despesas_contratadas_candidatos`): em todas as
> linhas de SP 2020 (632.668) e SP 2022 (309.654), `DS_ESFERA_PART_FORNECEDOR`
> é `#NULO#`; para anos ≤ 2016 o campo nem existe no layout. A esfera
> partidária descreve fornecedores que são órgãos de partido, que não
> ocorrem nas despesas de candidatos. O pipeline mapeia a coluna
> corretamente — ela é vazia porque a fonte é vazia. Opções: (a) manter a
> coluna e registrar em `observations` que o TSE distribui o campo vazio, ou
> (b) removê-la da tabela. Preferência?

## #1463 — nome_candidato ausente em resultados_candidato

> Confirmado e corrigido. O pipeline gera `nome_candidato` e o metadado o
> registra, mas o modelo dbt `br_tse_eleicoes__resultados_candidato.sql` não
> selecionava a coluna — por isso a API a anunciava e o BigQuery não a
> tinha. A coluna foi adicionada ao modelo (após `numero_candidato`, ordem
> do schema validado) e ao schema.yml; entra na próxima materialização.

## #1155 — checar dados de vice-prefeito

> Diagnóstico concluído: não há votos de vice-prefeito na fonte. Os arquivos
> de votação do TSE (`votacao_candidato_munzona`) registram votos apenas
> para os cargos votados — Prefeito e Vereador nos anos municipais
> (verificado em 2000 e 2020); o vice não recebe votos próprios, compõe a
> chapa do titular. Por isso `resultados_candidato_municipio` não tem (e não
> pode ter) `cargo = vice-prefeito`. Os candidatos a vice aparecem
> normalmente na tabela `candidatos` (e.g. 97 candidatos a vice-prefeito no
> AC em 2020). Vamos registrar essa característica na descrição da tabela e
> fechar a issue.
