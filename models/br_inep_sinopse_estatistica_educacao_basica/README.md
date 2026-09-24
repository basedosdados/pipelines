# br_inep_sinopse_estatistica_educacao_basica

## Contexto

A Sinopse Estatística da Educação Básica é a publicação anual do INEP com os
agregados do Censo Escolar: matrículas e docentes por município, etapa de ensino
e rede. A fonte é uma única planilha `.xlsx` de cerca de 200 MB, com 268 abas,
distribuída em um arquivo zip.

O conjunto tem 11 tabelas, em dois grupos:

- **matrícula** — `etapa_ensino_serie`, `faixa_etaria`, `localizacao`,
  `tempo_ensino`, `sexo_raca_cor`
- **docente** — `docente_etapa_ensino`, `docente_localizacao`,
  `docente_escolaridade`, `docente_deficiencia`, `docente_faixa_etaria_sexo`,
  `docente_regime_contrato`

O layout da planilha muda entre edições. Por isso o que descreve uma edição —
nomes de aba, número de linhas de cabeçalho, nomes de bloco — fica em
`code/constants.py`, separado da transformação em `code/utils.py`, que é estável
entre anos.

## Estrutura do código

| Arquivo | Conteúdo |
|---|---|
| `code/constants.py` | Constantes da edição: caminhos, URL, abas, blocos e dicionários de renomeação |
| `code/utils.py` | Funções do tratamento, em seis seções: cabeçalho, fonte, apoio, matrícula, docente e saída |
| `code/run_local.py` | Orquestrador para execução local, sem acesso ao BigQuery |
| `code/upload.py` | Carga do que está em `output/` para o staging de `basedosdados-dev` |
| `code/sinopse_2024.py` | Tratamento de 2024, com todo o código em um único script |
| `code_docente/` | Scripts das tabelas de docente de edições anteriores |

As funções de `utils.py` não dependem de estado global: recebem o caminho da
planilha e o mapa de unidades da federação, e devolvem `DataFrame`. Servem tanto
a um script de edição quanto a uma task de flow, e permitem tratar uma tabela
isolada.

`CLEANERS` associa o nome de cada tabela à função que a produz, e é onde uma
tabela nova é registrada.

Os dados intermediários ficam em
`~/Downloads/br_inep_sinopse_estatistica_educacao_basica_data/`, fora do
repositório. A variável de ambiente `SINOPSE_DATA_DIR` altera esse caminho.

## Como rodar

```bash
cd models/br_inep_sinopse_estatistica_educacao_basica/code

uv run python run_local.py                     # as 11 tabelas
uv run python run_local.py -t faixa_etaria     # uma tabela
uv run python run_local.py --skip-download     # usa a planilha já em disco
uv run python run_local.py --output /tmp/x     # outro destino
```

O download é ignorado quando o zip já existe. A saída é um CSV por unidade da
federação, em `<saída>/<tabela>/ano=<ano>/sigla_uf=<uf>/data.csv`.

`run_local.py` compara as contagens produzidas com `LINHAS_ESPERADAS_2025` e
retorna código de erro quando divergem. Ao alterar a transformação, atualize essa
referência no mesmo commit.

`run_local.py` não envia dados ao BigQuery. O envio é feito por `upload.py`:

```bash
uv run python upload.py                        # as tabelas em output/
uv run python upload.py -t localizacao         # uma tabela
```

### Execução sem service account

O service account de dev não tem permissão `bigquery.jobs.create` em
`basedosdados-dev`, e qualquer chamada a `bd.read_sql` retorna HTTP 403 na
máquina local — incluindo a consulta ao diretório de unidades da federação.

`load_uf_map()` sem argumento devolve o mapa fixo de `UF_NOME_SIGLA`, o que
permite executar o tratamento completo sem credencial. Com `billing_project_id`,
consulta o diretório, que é a referência canônica. As 27 unidades da federação
não mudam, então as duas formas são equivalentes.

## Leitura do cabeçalho da planilha

As abas têm cabeçalho hierárquico em células mescladas. Nenhuma coluna tem nome
próprio: ela é identificada pela combinação dos níveis acima dela. Para o 5º ano
dos anos iniciais:

```text
                          5º ano
              Rede Pública              Rede Privada
    Total  Federal  Estadual  Municipal   Total  Particular  ...
     46      47        48        49        50        51
```

Até 2024 o pandas atribuía nomes distintos a essas colunas (`Federal`,
`Estadual.1`, `Privada.4`), e a seleção era feita por nome, com `errors="raise"`.
Em 2025 o INEP acrescentou o nível `Rede Pública` e desdobrou a rede privada em
quatro categorias (Particular, Comunitária, Confessional e Filantrópica), e os
nomes deixaram de identificar as colunas.

**A seleção de colunas não usa índice de posição.** Um índice fixo
(`Unnamed: 49`) exige contar colunas manualmente e não produz erro quando o
layout muda: a coluna continua existindo, com outro significado. A divergência
resultante não altera contagem de linhas nem conjunto de categorias, e não é
detectada pelos testes dbt.

A seção **cabeçalho** de `code/utils.py` reconstrói o caminho completo de cada
coluna e seleciona as colunas por esse caminho. Duas propriedades são
necessárias:

- A propagação lateral do valor de uma célula mesclada termina quando o caminho
  dos níveis acima muda. Sem isso, o valor de `Filantrópica` do 1º ano seria
  atribuído à primeira coluna do 2º ano.
- `build_renames` falha quando um bloco declarado não existe na aba, quando a aba
  traz um bloco não declarado, ou quando um bloco não tem as quatro redes. Uma
  reorganização do cabeçalho pelo INEP interrompe a execução em vez de produzir
  valores incorretos.

Nas abas de `localizacao` e `tempo_ensino` o mesmo desenho de coluna aparece
duas vezes: antes do nível que abre os blocos, a aba repete o total por rede da
etapa inteira. `NIVEL_LOCALIZACAO` e `NIVEL_TEMPO_ENSINO` recortam o trecho
certo do cabeçalho, e `rede_columns` falha se esse nível não existir.

### A rede privada é `Rede Privada / Total`

`Particular` é uma das quatro categorias que compõem a rede privada, e não a rede
privada inteira. Selecioná-la no lugar de `Rede Privada / Total` subestima a rede
privada em cerca de duas ordens de magnitude.

## Particularidades da fonte

| Comportamento da planilha | Tratamento |
|---|---|
| O nome do arquivo no zip tem "ção" com codificação incorreta e recebe sufixo de revisão (`_V2`) | `find_workbook` localiza o `.xlsx` por busca, e não por nome fixo no código |
| O nível que agrupa as redes é `Rede Pública`/`Rede Privada` nas abas de série, e `Pública`/`Privada` nas de EJA e educação profissional | `NIVEIS_PUBLICA` e `NIVEIS_PRIVADA` aceitam as duas grafias |
| Nomes de bloco incluem a marca de nota de rodapé (`Ensino Fundamental5`) | `clean_block` |
| Os nomes dos cursos alternam hífen e travessão | `clean_block` unifica em hífen |
| Cada aba tem um bloco agregado, com o total da etapa, que não é dado por rede | `NIVEIS_AGREGADOS` |
| A numeração das abas muda a cada edição, e o nome da aba não diz o que ela contém (`1.27`) | As listas `sheets_*` são conferidas contra o título da linha 4 de cada aba |

O padrão da URL de download mudou entre 2022 e 2023, de
`sinopses_estatisticas_censo_escolar_<ano>.zip` para
`sinopse_estatistica_censo_escolar_<ano>.zip`. O padrão antigo retorna HTTP 404.

## Convenções

### Separador

As categorias usam hífen (`-`), não travessão (`–`). É o separador adotado nos
modelos dbt do repositório, e `br_inep_educacao_especial__etapa_ensino.sql` aplica
`replace(etapa_ensino, "–", "-")` na mesma coluna. Na Sinopse, as seis tabelas de
docente já publicam hífen.

A planilha alterna os dois separadores, então a normalização ocorre em dois
pontos: `clean_block`, no tratamento, e um `replace` nos modelos dbt, para os anos
já presentes no staging.

### Formato do staging

A convenção do repositório é parquet com todas as colunas STRING. **Neste conjunto
o staging é CSV**, porque as edições anteriores foram carregadas nesse formato e a
troca faria a tabela externa divergir dos anos já publicados. `upload_tables`
passa `source_format="csv"` de forma explícita.

`upload_tables` também passa `if_storage_data_exists="replace"`, que sobrescreve
arquivo por arquivo, e não o prefixo inteiro: os anos ausentes de `output/`
continuam no bucket como estão. Por isso o nome do arquivo tem de bater com o
que já está lá — a tabela externa lê tudo que houver no prefixo, e um nome
diferente duplicaria a partição em silêncio. Aqui é `data.csv`, nos 19 anos.

## Categorias de educação profissional em 2025

O INEP reorganizou a educação profissional nesta edição:

- `Curso FIC Integrado na Modalidade EJA` foi dividido em Nível Fundamental e
  Nível Médio
- foram criados os Itinerários Formativos Técnico Profissionais, articulados ao
  ensino médio ou exclusivos, cada um com Curso Técnico e Qualificação
  Profissional
- `Associada ao Ensino Médio` deixou de existir
- `docente_deficiencia` passou a ter a categoria Visão Monocular

`ETAPAS_PROFISSIONAL`, em `code/constants.py`, é a única definição dessa
correspondência: associa o nome do bloco na planilha ao prefixo interno e à etapa
de ensino publicada. É usada tanto na seleção das colunas quanto na nomeação da
categoria.

## Limitações conhecidas

- `faixa_etaria` e as demais abas de `docente_*` selecionam colunas por índice
  de posição, com o risco descrito em "Leitura do cabeçalho da planilha".
  `RENAMES_DOCENTE_ETAPA_ENSINO` é o maior desses mapeamentos. Migrá-las exige
  outra função de busca, porque o último nível do cabeçalho dessas abas não é a
  rede, e sim faixa de idade, escolaridade ou regime de contrato.
- A verificação de municípios depende de acesso ao BigQuery. Com
  `billing_project_id`, `clean_all` compara cada tabela com o diretório de
  municípios; sem ele, a primeira tabela define o conjunto de referência e as
  demais são comparadas com ela, o que pega uma tabela que perdeu municípios mas
  não prova que a planilha traz todos. Em 2025 são 5.571 municípios, iguais aos
  do diretório.
- O `end` do `partition_by` é exclusivo no BigQuery. Com `end: 2025`, as linhas de
  2025 ficam na partição `__UNPARTITIONED__`; a convenção do repositório é o
  último ano mais cinco.
