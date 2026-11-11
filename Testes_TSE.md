- Partição: todos usam ano (INT64, intervalo de 2 em 2). O where: __most_recent_year__ da macro get_where_subquery filtra ano = max(ano). Hoje nenhum modelo do br_tse_eleicoes usa where: nos testes, e os 4 modelos de *_secao têm cluster_by=["sigla_uf"].
- Teste de unicidade: o padrão do dataset é dbt_utils.unique_combination_of_columns. O custom_unique_combinations_of_columns tolera 5% de falhas e só existe em local_secao e pesquisa_eleitoral*.
- Dicionário: o custom_dictionary_coverage lê {{ model }}, que o dbt já substitui pela subquery do where:. O dicionário não é filtrado.
- Sem teste de modelo (4): receitas_comite, receitas_orgao_partidario, resultados_candidato_secao, resultados_partido_secao.
- Sem unicidade (6): bens_candidato, despesas_candidato, receitas_candidato, resultados_candidato, perfil_eleitorado_local_votacao, perfil_eleitorado_secao.

Chaves candidatas (hipóteses a validar)

┌─────────────────────────────────┬──────────────────────────────────────────────────┐
│             Modelo              │                                  Candidatas                                   │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ resultados_candidato_secao      │ ano, tu_tse, zona, secao, cargo,                 │
│                                 │ sequencial_candidato                                                          │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ resultados_partido_secao        │ ano, turno, id_eleicao, id_municipio_tse, zona, secao, cargo, numero_partido  │
├─────────────────────────────────┼──────────────────────────────────────────────────┤
│ resultados_candidato            │ ano, turno, id_eleicao, sigla_uf, id_municipio_tse, cargo,                    │
│                                 │ sequenc                                          │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ perfil_eleitorado_local_votacao │ ano, tusecao                                     │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ perfil_eleitorado_secao         │ ano, idsituacao_biometria, genero, estado_civil, │
│                                 │  grupo_idade, instrucao                                                       │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ receitas_candidato              │ A: ano, sequencial_receita · B: ano,             │
│                                 │ sequencial_receita                                                            │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ despesas_candidato              │ A: ano, turno, sequencial_candidato, sequencial_despesa · B: ano,             │
│                                 │ sequencial_despesa                                                            │
├─────────────────────────────────┼──────────────────────────────────────────────────┤
│ receitas_orgao_partidario       │ ano, sequencial_diretorio, sequencial_receita                                 │
├─────────────────────────────────┼──────────────────────────────────────────────────┤
│ receitas_comite                 │ sem sequencial_receita; testar ano, sequencial_comite, numero_documento,      │
│                                 │ data_re                                          │
├─────────────────────────────────┼───────────────────────────────────────────────────────────────────────────────┤
│ bens_candidato                  │ sem id do item; testar ano, id_eleicao, sigla_uf, sequencial_candidato,       │
│                                 │ tipo_itm                                         │
└─────────────────────────────────┴───────────────────────────────────────────────────────────────────────────────┘

Plano de execução

Fase 0: custos e limites (metadados, sem va
-- Q0: tamanho e linhas das 10 tabelas
SELECT table_id, row_count, ROUND(size_byte
FROM `basedosdados.br_tse_eleicoes.__TABLES__`
WHERE table_id IN ('bens_candidato','despesdato','receitas_comite',
  'receitas_orgao_partidario','resultados_candidato','resultados_candidato_secao',
  'resultados_partido_secao','perfil_eleitorado_local_votacao','perfil_eleitorado_secao')
ORDER BY size_bytes;
Antes de cada query da Fase 1, faço um dry-run para ver os bytes e uso maximum_bytes_billed como teto.

Fase 1: unicidade na partição mais recente (1 query por modelo)
-- Q1.<modelo>: uma varredura, vários candi
SELECT COUNT(*) AS total,
  COUNT(DISTINCT TO_JSON_STRING(STRUCT(<cola,
  COUNT(DISTINCT TO_JSON_STRING(STRUCT(<colunas candidata B>))) AS dist_b
FROM `basedosdados.br_tse_eleicoes.<modelo>
WHERE ano = (SELECT MAX(ano) FROM `basedosdados.br_tse_eleicoes.<modelo>`);
Uma candidata é válida quando dist = total. Em caso de duplicata, rodo a query abaixo com LIMIT 20 para ver os casos:
SELECT <chave>, COUNT(*) AS n FROM `basedos>`
WHERE ano = <ano> GROUP BY <chave> HAVING n > 1 ORDER BY n DESC LIMIT 20;

Fase 2: chave escolhida em todos os anos
Mesma Q1, sem o filtro de ano, só para a ch Define se o teste usa where ou varre tudo.As tabelas *_secao são as mais caras.

Fase 3: proporção de não nulos (só os 4 modelos sem teste)
SELECT COUNT(*) AS total,
  COUNTIF(col_1 IS NOT NULL)/COUNT(*) AS col_1, ...  -- uma por coluna do schema.yml
FROM `basedosdados.br_tse_eleicoes.<modelo>`
WHERE ano = (SELECT MAX(ano) FROM ...);
Serve para escolher o at_least do not_null_proportion_multiple_columns, hoje 0.90 nos outros.

Fase 4: dicionário
SELECT id_tabela, nome_coluna, COUNT(*) AS chaves
FROM `basedosdados.br_tse_eleicoes.dicionario`
WHERE id_tabela IN (<10 modelos>) GROUP BY 1, 2 ORDER BY 1, 2;
Mostra quais colunas têm dicionário. A cobertura em si (valores sem chave) é checada pelo próprio              custom_dictionary_coverage, na Fase 6.
                                                                                                               Fase 5: edição (sem query)
No schema.yml: dbt_utils.unique_combination_of_columns com config: where: __most_recent_year__ nos 10 modelos, not_null_proportion_multiple_columns nos 4 erage onde fizer sentido. Depois dbt parsepara validar a sintaxe.

Fase 6: validação em dev
dbt test --select <modelo> em target=dev, modelo por modelo, com --full-refresh antes se for necessário materializar. Cada dbt test também executa SQL no BigQuery, então peço o seu aval antes. Se algum teste reprovar, ele aparece aqui, e não no flow de produção, onde seria fatal para a materialização.