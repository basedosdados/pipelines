# Censo da Educação Superior (Inep)

Pipeline que mantém atualizadas as tabelas `curso` e `ies` do conjunto
`br_inep_censo_educacao_superior`, a partir dos microdados do Censo da Educação
Superior publicados pelo Inep. Ela verifica todo mês se o Inep publicou uma edição
nova ou republicou uma edição antiga, e recarrega só os anos que mudaram.

## Fonte

O Inep publica um zip por ano, de 1995 até a edição mais recente:

```text
https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{ano}.zip
```

A lista de anos está na [página de microdados do Censo da Educação Superior](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior).
As tabelas da BD começam em 2009. Dentro de cada zip, a pipeline usa o CSV de
cursos (`MICRODADOS_CADASTRO_CURSOS_<ano>.CSV`) e o de instituições (`..._IES_<ano>.CSV`).

O Inep **republica edições antigas**, às vezes com correções. Por isso a data de
publicação de cada ano é lida do cabeçalho `Last-Modified` do zip, e não do texto da
página, que nem sempre acompanha as republicações.

## Como a pipeline funciona

```text
check_update  ──▶  extract_and_load  ──▶  build_and_promote
(dia 15)           (anos que mudaram)      (dbt e publicação)
```

- **`check_update`** (`flows.py`, `tasks.py`): todo dia 15, lê o `Last-Modified` de
  cada ano e a data da última atualização da tabela na BD. Se algum ano foi publicado
  depois dessa data, dispara o próximo estágio com esses anos.
- **`extract_and_load`**: para cada ano, baixa o zip, trata o CSV (`utils.py`) e grava
  `ano=<ano>/data.csv` no staging. Só o arquivo dos anos carregados é substituído.
- **`build_and_promote`**: estágio comum a todos os conjuntos da BD
  (`pipelines/utils/metadata/flows.py`). Roda o dbt
  (`models/br_inep_censo_educacao_superior/`) e publica em produção.

Cada tabela tem o seu próprio par de flows, e as duas são atualizadas de forma
independente.

### Tratamento

O de-para entre as colunas do Inep e as da BD vem das **planilhas de arquitetura**
do conjunto, mantidas pela equipe de dados e lidas pela pipeline em toda execução.
A ordem das linhas da planilha é a ordem das colunas no staging: colunas novas
entram sempre no fim.

O tratamento:

- lê o CSV do Inep em blocos e como texto, para preservar CEPs e códigos com zero à
  esquerda;
- traduz nomes de colunas que o Inep usou em edições antigas (docentes da `ies` em
  2009 e código CINE da `curso` em 2020);
- grava 0 onde o Inep deixa vazio uma quantidade que não se aplica (até 2019) e nulo
  nas colunas que não existiam no ano;
- converte indicadores `1`/`0` em verdadeiro/falso, padroniza `id_municipio` com 7
  dígitos e retira as aspas que o Inep coloca no código CINE.

Antes de carregar, a pipeline compara o CSV do Inep com a arquitetura:

- se faltar uma coluna que deveria existir naquele ano (pela `temporal_coverage` da
  planilha), **a carga falha**. Isso acontece quando o Inep renomeia ou retira uma
  variável; a correção é registrar o nome antigo em `OLD_PREFIXES` (`constants.py`)
  ou ajustar a cobertura na planilha;
- colunas fora do seu período de coleta ficam nulas;
- colunas novas do Inep, ainda fora da arquitetura, são descartadas com aviso no log.

## Como ler a tabela `curso`

**Uma linha não é um curso.** A chave é `id_curso` + `id_municipio` + `tipo_dimensao`,
e o tipo de informação de cada linha depende do `tipo_dimensao`:

| `tipo_dimensao` | A linha representa | Traz |
|---|---|---|
| 1 — presencial | o curso inteiro, no seu município | oferta e alunos |
| 2 — EaD no Brasil | o curso a distância em um município | só alunos |
| 3 — EaD só nível Brasil | o curso a distância no país todo, sem município | só oferta (vagas, inscritos) |
| 4 — EaD no exterior | curso de instituição brasileira no exterior | só alunos |

No EaD, o Inep não divide vagas e inscritos por município: a oferta fica numa linha
nacional e os alunos ficam nos municípios. Para contar cursos, use
`count(distinct id_curso)`, e não o número de linhas.

## Colunas de cor/raça e de reserva de vagas

- **Cor/raça não é cota.** Colunas como `quantidade_matriculas_indigena` contam quem
  se declara daquela cor/raça. As de reserva de vagas contam quem entrou por cota.
- **Os grupos de cota não somam o total.** `quantidade_*_reserva_vaga` é o total de
  cotistas; as colunas `quantidade_*_reserva_vaga_<grupo>` dizem por qual critério eles
  entraram. Um aluno pode cumprir mais de um critério (por exemplo, escola pública,
  renda e PPI), então somar os grupos não dá o total.
- **Mudança de 2024.** A reserva "de cunho étnico" (`..._reserva_vaga_etnico`) vai até
  2023. Em 2024 o Inep passou a abrir as cotas por grupo: pretos, pardos ou indígenas
  (`_ppi`), quilombolas, refugiados, povos tradicionais, idosos, estudantes
  internacionais, medalhistas e pessoas trans. A coluna `_ppi` é a sucessora mais
  próxima da `_etnico`, mas não tem a mesma definição.
- **`indicador_comunitaria` e `indicador_confessional`** existem desde 2023, na
  `curso` e na `ies`, e só se aplicam a instituições privadas.
- **Zeros antes do início da coleta.** Em várias colunas, o Inep publica 0 (ou falso)
  nos anos anteriores à criação da variável. Nesses anos, o valor significa "não
  coletado". O período de cada coluna está nas observações dos metadados.

## Operação

- **Primeira execução e recargas completas:** disparar o deployment de
  `extract_and_load` de cada tabela com
  `{"download_params": {"anos": [2009, ..., <último ano>]}}`. O staging é lido por
  posição, então todos os anos precisam ter sido gravados pela pipeline.
- **Teste em dev:** com o label `deploy-flow` no PR, os flows vão para o pool de dev.
  O `build_and_promote` usa os modelos dbt da `main`; mudanças nos modelos de um PR
  devem ser validadas com `dbt run` e `dbt test` locais.
- **Metadados:** o `check_update` grava em produção, mesmo rodando em dev, o
  registro de consulta à fonte e a data da última publicação do Inep.
- **Instabilidade do Inep:** o servidor derruba conexões e tem a cadeia de
  certificados incompleta. O download (reaproveitado do `br_inep_enem`) tenta de novo
  e retoma de onde parou, mas um ano pode levar vários minutos.
