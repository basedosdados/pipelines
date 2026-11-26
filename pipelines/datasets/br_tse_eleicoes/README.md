# br_tse_eleicoes

Dados do Tribunal Superior Eleitoral (TSE) sobre eleições: candidaturas, bens
declarados, prestação de contas de campanha, resultados, perfil do eleitorado e
locais de votação.

O pipeline recorrente atualiza `candidatos`, `bens_candidato`, `despesas_candidato`
e `receitas_candidato` para o ano da constante `YEAR`, em `constants.py`, que é
trocada à mão a cada eleição.

## Fonte

ZIPs no CDN do TSE, sob `https://cdn.tse.jus.br/estatistica/sead/odsele/`:

| Tabela | ZIP | CSV |
| --- | --- | --- |
| `candidatos` | `consulta_cand/consulta_cand_{ano}.zip` | `consulta_cand_{ano}_BRASIL.csv` |
| `candidatos` | `consulta_cand_complementar/consulta_cand_complementar_{ano}.zip` | `consulta_cand_complementar_{ano}_BRASIL.csv` |
| `bens_candidato` | `bem_candidato/bem_candidato_{ano}.zip` | `bem_candidato_{ano}_BRASIL.csv` |
| `despesas_candidato` | `prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}.zip` | `despesas_contratadas_candidatos_{ano}_BRASIL.csv` |
| `receitas_candidato` | o mesmo de `despesas_candidato` | `receitas_candidatos_{ano}_BRASIL.csv` |

Cada ZIP traz um CSV por UF e um `*_BRASIL.csv`, que é o usado. Os CSVs usam `;`
como separador, codificação ISO-8859-1 e campos entre aspas.

### Bytes NUL no CSV de despesas de 2026

O campo `DS_DESPESA` da despesa de `SQ_DESPESA` 79947890 tem dois bytes NUL
(`\x00`), um no meio de "estratégica" e outro no meio de "política". São os únicos
NUL do arquivo.

| Leitor | Resultado na linha |
| --- | --- |
| `pd.read_csv`, engine C (padrão) | 53 colunas |
| módulo `csv` do Python | 53 colunas |
| polars | lê sem erro |
| `pd.read_csv(engine="pyarrow")` | 52 colunas, erro |
| `pyarrow.csv.read_csv` | 52 colunas, erro |

O leitor de CSV do Arrow conta 52 colunas nessa linha e para. O pandas levanta
`pandas.errors.ParserError` e o `pyarrow.csv.read_csv`, `pyarrow.lib.ArrowInvalid`,
os dois com a mesma mensagem:

```text
CSV parse error: Expected 53 columns, got 52
```

O `read_csv` do pipeline usa a engine C.

## Tratamento dos dados

### Comum

- Células iguais a `#NULO`, `#NULO#`, `#NE`, `NÃO DIVULGÁVEL`, `Não Divulgável`,
  `Não divulgável`, `-1`, `-4` ou `-3` ficam vazias, exceto em `bens_candidato`.
- Colunas de categoria ficam em minúsculas e sem acento.
- Datas `dd/mm/aaaa` viram `aaaa-mm-dd`; data inválida fica vazia.
- Linhas repetidas são removidas.
- Em `candidatos`, `despesas_candidato` e `receitas_candidato`, o `id_municipio`
  vem do diretório de municípios (`br_bd_diretorios_brasil.municipio`), casando
  `SG_UE`, sem zeros à esquerda, com `id_municipio_tse`. Em eleições estaduais e
  federais, `SG_UE` traz a sigla da UF, e a linha fica sem `id_municipio`.

### `candidatos`

- A consulta de candidaturas é unida ao arquivo complementar por `SQ_CANDIDATO`.
- `data_nascimento` fica vazia quando a idade (ano corrente menos ano de
  nascimento) está fora de 18 a 119 anos.
- `nome`, `nome_urna` e `municipio_nascimento` ficam com as iniciais maiúsculas.
- Em `nacionalidade`, `brasileira nata` vira `brasileira`.
- Em `instrucao`, valores com `superior` ganham o prefixo `ensino`.

### `bens_candidato`

- Só `tipo_eleicao` (minúsculas e sem acento) e `data_eleicao` (formato de data)
  são tratadas; valores como `#NULO` ficam como na fonte.
- O valor do bem é lido como número, com vírgula decimal.
- `titulo_eleitoral_candidato` fica vazia.

### `despesas_candidato` e `receitas_candidato`

- Os zeros à esquerda saem do código de município do fornecedor ou do doador.
- Em `tipo_eleicao`, `eleicoes municipais <ano>` vira `eleicao ordinaria`, exceto
  quando seguido de ` - `.
- A vírgula decimal do valor vira ponto.
- As colunas de `VAZIOS_DESPESA` e `VAZIOS_RECEITA` existem na tabela e ficam
  vazias, porque o CSV da fonte não as traz.

## Memória em `despesas_candidato`

Para caber no limite de memória do pod do `extract_and_load` (4Gi),
`despesas_candidato` é montada sem copiar a tabela inteira:

- lê só as 35 colunas do CSV que a tabela usa (`DespesasCandidato.source_columns()`);
  nas outras tabelas, `source_columns()` devolve `None` e todas as colunas são
  lidas;
- busca o `id_municipio` num dicionário `id_municipio_tse → id_municipio`, sem
  `merge`;
- monta a saída coluna a coluna e tira cada coluna de origem da tabela lida no seu
  último uso.

Com o CSV de 2026, o pico de memória fica em 3,11 GiB. Lendo todas as colunas e com
`merge`, chega a 4,10 GiB. O CSV gravado é o mesmo nos dois casos.

## Testes dbt (`models/br_tse_eleicoes/schema.yml`)

### Tabelas sem chave

| Tabela | Motivo |
| --- | --- |
| `receitas_candidato` | As linhas se repetem com o mesmo sequencial na própria fonte do TSE, inclusive linhas 100% idênticas. Não há chave natural. |
| `despesas_candidato` | O mesmo caso de `receitas_candidato`. |
| `bens_candidato` | Sem chave por enquanto. O CSV do TSE traz `NR_ORDEM_BEM_CANDIDATO` e `SQ_CANDIDATO` + ordem não se repete em 2024 nem em 2026, mas a coluna não está na tabela. Os itens "idênticos" só ficam iguais porque a construção descarta a ordem do bem. Ver #2356. |

### `numero_candidato` e não `sequencial_candidato`

Nas tabelas `resultados_candidato*`, a chave usa `numero_candidato`. Em 2026,
`sequencial_candidato` se repete em 216 casos, que são pares de candidatos com
números diferentes e o mesmo sequencial. O número é um identificador mais confiável.

### Testes de unicidade só de 2016 em diante

Os testes de unicidade de `candidatos`, `resultados_candidato_municipio`,
`resultados_candidato_municipio_zona` e `vagas` usam `where: ano >= 2016`. Nas
tabelas inteiras, as chaves repetem em anos antigos (medição de 24/11/2026 em
`basedosdados-dev`, grupos repetidos por ano):

| Tabela | Anos com grupos repetidos |
| --- | --- |
| `candidatos` | 1994 (1.135), 1996 (480), 2000 (1.962), 2002 (1.949), 2004 (1.279), 2006 (2.354), 2008 (61.704) |
| `resultados_candidato_municipio` | 1998 (5.587), 2000 (1.014), 2002 (89), 2004 (88), 2006 (72), 2014 (417) |
| `resultados_candidato_municipio_zona` | 1998 (7.640), 2000 (1.460), 2002 (260), 2004 (92), 2006 (243), 2014 (447) |
| `vagas` | 2008 (162) e 2012 (262), todos com `id_eleicao` nulo |

De 2016 em diante, nenhuma das quatro chaves repete. As duplicatas são do dado
histórico do TSE e não foram tratadas nesta etapa.

### Municípios do exterior e voto em trânsito

Os testes `custom_relationships` de `id_municipio_tse` ignoram as linhas com
`sigla_uf` igual a `ZZ` (exterior) ou `VT`, com `where` no próprio teste. Nessas
linhas, o código TSE não existe em `br_bd_diretorios_brasil.municipio`: são 294
códigos `ZZ` em `perfil_eleitorado_municipio_zona` e 276 `ZZ` mais 27 `VT` (todos
de 2010) em `resultados_candidato_municipio`. O código `73709` (Boa Esperança do
Norte, MT) segue no `ignore_values`. O código `91065`, de `MT` e só de 2002, também
não existe no diretório. Ele está no `ignore_values` de `perfil_eleitorado_municipio_zona`
e dos 6 modelos `resultados_*` com `id_municipio_tse` (de 48 a 179 linhas por tabela).

### `ignore_values` do `not_null_proportion_multiple_columns`

Medição feita em 24/11/2026 nas tabelas de `basedosdados-dev.br_tse_eleicoes`,
considerando todos os anos. As exceções são `resultados_candidato_secao` e
`resultados_partido_secao`, medidas só em 2026 porque o teste usa
`where: __most_recent_year__`.

Uma coluna pouco preenchida por natureza vai para o `ignore_values` e não baixa o
limiar das outras. Os motivos se repetem entre as tabelas:

- **Layout do TSE por ano.** A coluna só existe a partir de certo ano, e nos anos
  anteriores vem vazia.
- **Eleição municipal × geral.** `id_municipio` e `id_municipio_tse` só são
  preenchidos em eleição municipal (1996, 2000, ..., 2024). Em eleição geral,
  `SG_UE` traz a sigla da UF.
- **Campo de doador ou fornecedor.** Só existe para parte das linhas, conforme o
  tipo de doador ou fornecedor (por exemplo, `cnae_2_*`, `*_doador_orig` e os
  campos de candidato ou partido doador).
- **Colunas de fonte de geocodificação** em `local_secao`. Cada coluna cobre só as
  seções localizadas por aquela fonte.

| Tabela | Colunas em `ignore_values` | Motivo |
| --- | --- | --- |
| `bens_candidato` | `tipo_item`, `data_eleicao`, `id_eleicao` | Só preenchidas de 2014 em diante (64% das linhas). |
| `candidatos` | `email`, `raca`, `situacao` | `email` tem 7% de preenchimento. `raca` só existe de 2014 em diante. `situacao` vem vazia em 1994 a 2000 e em 2026. |
| `despesas_candidato` | 8 campos `*_fornecedor` de candidato ou partido (`cargo_fornecedor`, `esfera_partidaria_fornecedor`, `id_municipio_tse_fornecedor`, `numero_candidato_fornecedor`, `numero_partido_fornecedor`, `sequencial_candidato_fornecedor`, `sigla_partido_fornecedor`, `sigla_uf_fornecedor`), preenchidos em até 0,2% | Só existem quando o fornecedor é candidato ou partido. |
| `despesas_candidato` | `id_municipio`, `id_municipio_tse` (67%) | Eleição municipal × geral. |
| `despesas_candidato` | `turno`, `data_eleicao`, `data_prestacao_contas`, `origem_despesa`, `sequencial_despesa`, `sequencial_prestador_contas`, `tipo_fornecedor`, `tipo_prestacao_contas` (41%) | Só preenchidas de 2018 em diante. |
| `despesas_candidato` | `id_eleicao`, `cnpj_prestador_contas` (64%), `descricao_cnae_2_fornecedor`, `nome_fornecedor_rf`, `descricao_despesa` | Só preenchidas de 2014, 2012 ou 2010 em diante, conforme a coluna. |
| `despesas_candidato` | `cnae_2_fornecedor`, `cnae_2_fornecedor_classe`, `cnae_2_fornecedor_subclasse`, `cnpj_candidato`, `cpf_cnpj_fornecedor` (89,7%), `especie_recurso`, `fonte_recurso`, `numero_documento`, `tipo_despesa`, `tipo_documento` | Preenchimento parcial, ou concentrado em poucos anos, que muda de coluna para coluna. `cnae_2_fornecedor*` só existe para fornecedor pessoa jurídica. |
| `local_secao` | `escolas_inep`, `escolas_municipais`, `google`, `melhor_rural`, `melhor_urbano`, `tse_distribuido`, `tse_recente`, além de `ibge_cnefe_*`, `google_centro_geometrico`, `google_relaxado` e `ibge_povoados` | Colunas de fonte de geocodificação. Em 2022, `melhor_rural` e `melhor_urbano` passam de 90%, mas nos anos anteriores ficam entre 70% e 88%. |
| `partidos` | `numero_federacao`, `sigla_federacao`, `nome_federacacao`, `composicao_federacao` | Federação partidária só existe de 2022 em diante. |
| `partidos` | `situacao_legenda`, `data_eleicao`, `id_eleicao`, `nome_coligacao` | `situacao_legenda` só de 2022 em diante e `data_eleicao`/`id_eleicao` só de 2014 em diante. `nome_coligacao` tem preenchimento parcial em quase todos os anos (50% a 88%). |
| `perfil_eleitorado_local_votacao` | `telefone` (65%), `latitude`, `longitude` (69%) | O TSE só informa para parte dos locais. `latitude` e `longitude` vêm vazias em 2026. |
| `perfil_eleitorado_municipio_zona` | `situacao_biometria` (79%) | Vazia em 2024 e 2026. |
| `receitas_candidato` | `id_municipio`, `id_municipio_tse` (84%) | Eleição municipal × geral. |
| `receitas_candidato` | `turno`, `data_eleicao`, `data_prestacao_contas`, `numero_partido`, `sequencial_prestador_contas`, `sequencial_receita`, `tipo_prestacao_contas` | Só preenchidas de 2018 em diante. |
| `receitas_candidato` | `id_eleicao`, `nome_doador_rf`, `fonte_receita`, `natureza_receita`, `cnpj_prestador_contas` | Só preenchidas de 2010, 2012 ou 2014 em diante, conforme a coluna. |
| `receitas_candidato` | 22 campos de doador ou do documento, entre eles `cnae_2_doador*`, `*_candidato_doador`, `*_partido_doador`, `sigla_uf_doador`, `id_municipio_tse_doador`, `cpf_administrador`, `nome_administrador`, `numero_documento*`, `numero_recibo_*`, `descricao_receita`, `especie_receita`, `situacao_receita` e `cnpj_candidato` | Preenchimento parcial. Cada campo só existe para um tipo de doador ou de receita. |
| `receitas_candidato` | `*_doador_orig` (2,5%), `descricao_cnae_2_doador_orig` (0,5%) e `entrega_conjunto` (2,7%) | Só preenchidos quando há doador originário. |
| `receitas_comite` | `id_municipio`, `id_municipio_tse` (88%) | Nulos em eleição geral. |
| `receitas_comite` | `cpf_membro`, `nome_membro` (49%), `situacao_receita`, `numero_partido` (81%), `cnae_2_doador_classe`, `numero_candidato_doador` | Preenchimento parcial ou vazio. |
| `receitas_comite` | `sequencial_comite`, `nome_doador_rf`, `descricao_cnae_2_doador`, `fonte_receita`, `tipo_documento`, `origem_receita` | Só preenchidas de 2006, 2010 ou 2012 em diante, conforme a coluna. |
| `receitas_comite` | `cnae_2_doador`, `cnae_2_doador_subclasse`, `*_doador_orig` (0,4%), `sigla_uf_doador`, `numero_partido_doador`, `cnpj_prestador_contas`, `data_receita`, `descricao_receita`, `numero_documento` | Preenchimento parcial. `*_doador_orig` só existem em 2014. `data_receita` vem vazia em 2012. |
| `receitas_orgao_partidario` | `id_municipio`, `id_municipio_tse` (25%), `id_municipio_doador`, `id_municipio_tse_doador` (4%) | Município só existe para diretório municipal. |
| `receitas_orgao_partidario` | `esfera_partidaria`, `nome_partido`, `numero_partido`, `especie_receita`, `sequencial_receita`, `tipo_prestacao_contas`, `data_prestacao_contas` (28%), `sequencial_prestador_contas`, `cnpj_prestador_contas`, `descricao_cnae_2_doador` | Só preenchidas de 2012, 2014, 2016 ou 2018 em diante, conforme a coluna. |
| `receitas_orgao_partidario` | 15 campos de doador, entre eles `*_candidato_doador`, `*_partido_doador`, `id_municipio*_doador`, `sigla_uf_doador`, `cnae_2_doador*`, `numero_*_doacao` e `descricao_receita`, além de `tipo_diretorio`, `sequencial_diretorio`, `numero_recibo_eleitoral`, `numero_documento`, `tipo_documento` e `*_doador_orig` | Preenchimento parcial. Cada campo só existe para um tipo de doador ou de receita. |
| `pesquisa_eleitoral` | `descricao_area_abrangencia` (84%), `data_divulgacao`, `hora_registro`, `pesquisa_propria`, `nome_fantasia_empresa`, `id_municipio`, `id_municipio_tse` | Pesquisa estadual ou nacional não tem município. Os demais campos o TSE só informa para parte das pesquisas. |
| `pesquisa_eleitoral_contratante` | `valor_pago`, `origem_recurso`, `id_contratante`, `cpf_cnpj_contratante` | O TSE só informa para parte dos contratantes. |
| `pesquisa_eleitoral_pagante` | `origem_recurso`, `id_contratante`, `cpf_cnpj_pagante` | O TSE só informa para parte dos pagantes. |
| `vagas` | `data_eleicao`, `id_eleicao` (50%) | Só preenchidas de 2014 em diante. |
| `dicionario` | `cobertura_temporal` | Vazia por construção. |

Para `sigla_uf`, o `custom_relationships` ignora as siglas que não são UF mas
aparecem no dado do TSE: `ZZ` (exterior), `VT`, `BR`, `GB`, `GP`, `RB`, `LB` e `BH`, conforme a tabela.

### Limiares `at_least` abaixo de `0.99`

O limiar é a menor proporção de preenchimento entre as colunas que ficaram no
teste, arredondada para baixo. Todas as tabelas que não aparecem abaixo têm
`at_least: 0.99`.

| Tabela | `at_least` | Coluna que limita | Medição | Motivo |
| --- | --- | --- | --- | --- |
| `bens_candidato` | 0.97 | `titulo_eleitoral_candidato` | 97,6% | Vazia em 2026. |
| `candidatos` | 0.91 | `cpf` | 91,8% | Vazio em 1996, em 1994 (63% preenchido) e em metade de 2024 (50%). |
| `despesas_candidato` | 0.90 | `data_despesa` | 90,9% | Vazia em 2012. |
| `partidos` | 0.95 | `composicao_coligacao` | 95,3% | Vazia em 1998, 2000, 2006 e 2010, e parcial em 1994, 1996 e 2002. |
| `receitas_candidato` | 0.95 | `sequencial_candidato` | 95,3% | Vazio em 2004. |
| `receitas_comite` | 0.95 | `sigla_uf` | 95,99% | Parcial em 2002, 2006, 2010 e 2014 (59% a 85%). |
| `receitas_orgao_partidario` | 0.93 | `nome_doador_rf` | 93,1% | Vazio em 2010. |
| `resultados_candidato` | 0.92 | `id_municipio` | 92,1% | Nulo em eleição geral, e só 8% das linhas são de eleição geral. |
| `vagas` | 0.98 | `id_municipio` | 98,9% | Nulo em eleição geral. |

As medições dos limiares `0.99` ficaram entre 99,4% e 100%. As menores são
`id_municipio` em `detalhes_votacao_municipio` (99,4%) e em `perfil_eleitorado_municipio_zona`
(99,6%) e `titulo_eleitoral_candidato` em `resultados_candidato_municipio_zona` (99,4%).
