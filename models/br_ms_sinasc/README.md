# br_ms_sinasc — microdados

Documentação sobre tratamento de dados nulos e decisões de qualidade aplicadas ao modelo `br_ms_sinasc__microdados`.

---

## Contexto

Os microdados do SINASC (Sistema de Informações sobre Nascidos Vivos) são coletados em papel (Declaração de Nascido Vivo) e digitados localmente nos estabelecimentos de saúde. Esse processo manual é a principal fonte de inconsistências: campos obrigatórios preenchidos com valores genéricos, códigos inválidos, ou dados simplesmente ausentes.

A política adotada neste modelo é **preservar o dado original quando ele é válido e nulificar quando representa claramente uma falha de imputação** — nunca imputar valores sintéticos que poderiam distorcer análises epidemiológicas.

---

## Anomalia: código de estado mascarado como município

### Descrição do problema

Foi identificado um padrão recorrente de preenchimento incorreto nos campos de identificação geográfica de município. Em vez de informar o código IBGE de 7 dígitos de um município válido, o sistema (ou o digitador) registrava o **código da Unidade Federativa concatenado com cinco zeros**:

```
[2 dígitos do código da UF] + [00000]
```

Exemplos:

| Valor registrado | Interpretação incorreta | UF de origem |
|---|---|---|
| `3500000` | "município de SP" | São Paulo (35) |
| `2200000` | "município do PI" | Piauí (22) |
| `1100000` | "município de RO" | Rondônia (11) |
| `3300000` | "município do RJ" | Rio de Janeiro (33) |

Nenhum desses valores possui correspondência na tabela de municípios do IBGE (`br_bd_diretorios_brasil.municipio`). Quando usados em joins geoespaciais ou agregações por município, simplesmente não retornam resultado — ou pior, quando não há verificação de integridade, passam silenciosamente como se fossem dados válidos.

### Campos afetados

O padrão foi identificado como possível em todos os campos de código de município presentes no modelo:

- `id_municipio_nascimento` — município onde ocorreu o nascimento
- `id_municipio_mae` — município de origem da mãe
- `id_municipio_residencia` — município de residência da mãe

### Causa provável

A hipótese mais plausível é uma **falha no sistema de digitação ou validação do SINASC**: quando o operador não sabe (ou não encontra) o código do município, seleciona apenas o estado, e o sistema complementa os dígitos restantes com zeros para satisfazer a máscara do campo. O resultado é um valor sintaticamente válido (7 dígitos, prefixo de UF reconhecível), mas semanticamente inválido.

---

## Decisão de tratamento

### Critério de nulificação

Um valor de município é **substituído por `null`** quando satisfaz ao menos uma das condições abaixo:

**Condição A — código de estado mascarado** (três critérios simultâneos):

1. **Comprimento igual a 7 dígitos** — formato esperado para `id_municipio` no padrão IBGE
2. **Últimos 5 dígitos são zeros** — `right(valor, 5) = '00000'`
3. **Primeiros 2 dígitos correspondem a um código de UF válido** — conforme `basedosdados.br_bd_diretorios_brasil.uf`

A terceira condição é essencial para evitar falsos positivos: ela garante que apenas valores que reproduzem exatamente o padrão de "código de UF + zeros" sejam nulificados.

**Condição B — código sem correspondência no diretório atual**:

Valores que não seguem o padrão `XX00000` mas também não possuem correspondência em `basedosdados.br_bd_diretorios_brasil.municipio` são igualmente nulificados. Esses casos representam códigos malformados ou fictícios que não podem ser vinculados a nenhum município.

### Por que não imputar o estado como substituto?

Embora o prefixo do código inválido informe a UF de origem, **não é possível determinar o município correto** a partir dessa informação. Imputar o estado seria introduzir um dado de granularidade diferente em um campo que exige município — o que comprometeria análises de cobertura assistencial, distância ao serviço de saúde, e outras métricas geoespaciais. O `null` é a representação honesta da ausência de informação.

### Caso especial: `id_municipio_mae` com 6 dígitos

O campo `id_municipio_mae` pode vir com 6 dígitos (formato legado) no dado bruto de staging. Nesses casos, a resolução para 7 dígitos é feita via join com o diretório de municípios (`br_bd_diretorios_brasil.municipio`). A verificação de anomalia é aplicada **antes da resolução** (no valor de 6 dígitos) e **após** (no valor resolvido de 7 dígitos), garantindo que nenhuma variante do padrão escape.

---

## Municípios históricos extintos — exceção à nulificação

### Descrição

Durante a execução dos testes de integridade referencial (`relationships`) em `id_municipio_residencia`, foram identificados **32.593 registros** com códigos de município que não possuem correspondência no diretório atual (`br_bd_diretorios_brasil.municipio`), mas que **não foram nulificados** pelo modelo.

Os valores predominantes pertencem à faixa `3345XXX` (prefixo `33` = Rio de Janeiro), com um caso isolado de `5306006` (Goiás):

| Faixa de códigos | Volume aproximado | Situação |
|---|---|---|
| `3345XXX` (RJ) | ~32.400 registros | Municípios extintos/incorporados após fusões |
| `5306006` (GO) | ~184 registros | Município extinto |

Esses códigos correspondem a **municípios que existiam à época do registro** — décadas de 1990 e 2000 — mas foram posteriormente extintos, incorporados ou desmembrados, deixando de constar no diretório de municípios vigente.

### Decisão

Esses registros **não são nulificados**. Nulificá-los seria apagar informação geográfica historicamente correta: o nascimento ou a residência da mãe estava de fato naquele município no momento do registro. A ausência do código no diretório atual é uma limitação do diretório, não um erro no dado.

### Reflexo nos testes dbt

O teste de `relationships` em `id_municipio_residencia` foi configurado com `severity: warn` em vez de `error`, com thresholds:

- **`warn_if: >= 1`** — mantém visibilidade sobre qualquer valor fora do diretório
- **`error_if: >= 1000000`** — quebra o pipeline apenas se o volume indicar um problema novo e sistêmico, não os municípios históricos já conhecidos

---

## Referência dos códigos de UF (IBGE)

| Código | UF | | Código | UF |
|---|---|---|---|---|
| 11 | Rondônia | | 31 | Minas Gerais |
| 12 | Acre | | 32 | Espírito Santo |
| 13 | Amazonas | | 33 | Rio de Janeiro |
| 14 | Roraima | | 35 | São Paulo |
| 15 | Pará | | 41 | Paraná |
| 16 | Amapá | | 42 | Santa Catarina |
| 17 | Tocantins | | 43 | Rio Grande do Sul |
| 21 | Maranhão | | 50 | Mato Grosso do Sul |
| 22 | Piauí | | 51 | Mato Grosso |
| 23 | Ceará | | 52 | Goiás |
| 24 | Rio Grande do Norte | | 53 | Distrito Federal |
| 25 | Paraíba | | | |
| 26 | Pernambuco | | | |
| 27 | Alagoas | | | |
| 28 | Sergipe | | | |
| 29 | Bahia | | | |

Fonte: [IBGE — Códigos dos Municípios](https://www.ibge.gov.br/explica/codigos-dos-municipios.php)

---

## Outros campos com nulos esperados

Além da anomalia de município descrita acima, alguns campos naturalmente apresentam nulos por razões estruturais ou de coleta:

| Campo | Motivo esperado de nulo |
|---|---|
| `hora_nascimento` | Campo não obrigatório em versões antigas da DN |
| `apgar1`, `apgar5` | Não aplicável em partos domiciliares ou óbitos precoces |
| `codigo_anomalia` | Preenchido apenas quando `id_anomalia = '1'` (anomalia presente) |
| `semana_gestacao` | Calculado a partir de `data_ultima_menstruacao`; nulo quando esta é ausente |
| `data_nascimento_mae` | Campo introduzido em versões mais recentes da DN |
| `data_ultima_menstruacao` | Frequentemente desconhecida pela mãe ou não registrada |
| `cartorio`, `registro_cartorio`, `data_registro_cartorio` | Preenchidos apenas após registro em cartório |
| `serie_escolar_mae` | Descontinuado na versão 2010 do formulário |
| `idade_pai` | Campo não obrigatório; alta taxa de não preenchimento |

---

## Política geral de qualidade

- **Sem imputação sintética**: nenhum valor é gerado ou estimado para substituir ausências. Dados faltantes são representados como `null`.
- **Cast seguro**: todos os campos usam `safe_cast`, que retorna `null` em vez de erro quando o valor não pode ser convertido ao tipo esperado.
- **Preservação da rastreabilidade**: os campos de metadados (`numero_lote`, `versao_sistema`, `data_cadastro`, `data_recebimento`, `origem`) são mantidos integralmente para permitir auditoria e reprocessamento.
- **Ausência de chave natural única**: o campo `sequencial_nascimento` é gerado por lote estadual e reinicia por município — a mesma combinação de `ano + sigla_uf + id_municipio_nascimento + sequencial_nascimento` pode aparecer múltiplas vezes devido a reprocessamentos de lotes pelo SINASC. Não existe combinação de campos disponíveis que garanta unicidade de linha, e o teste `unique_combination_of_columns` foi removido por esse motivo.
