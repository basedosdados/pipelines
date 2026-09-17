# br_inep_censo_escolar

## Contexto

Microdados do Censo Escolar da Educação Básica, publicados anualmente pelo
INEP. O conjunto tem três tabelas: `escola`, uma linha por escola; `turma`, uma
linha por turma; e `dicionario`.

A partir de 2025 o INEP passou a distribuir os microdados em arquivos
separados — Escola, Matrícula, Turma, Docente, Gestor Escolar e Curso Técnico —
onde antes havia um único arquivo largo (`microdados_ed_basica_2024.csv`, com
426 colunas). A tabela `escola` é montada juntando quatro desses arquivos por
`id_escola`.

## Estrutura do código

| Arquivo | Conteúdo |
|---|---|
| `code/constants.py` | Constantes da edição: caminhos, URL, arquivos do microdado |
| `code/utils.py` | Funções do tratamento, em cinco seções: fonte, arquitetura, leitura, montagem e saída |
| `code/run_local.py` | Orquestrador para execução local |
| `code/turma_2024.py` | Tratamento da tabela `turma` |
| `code/main.py` | Tratamento de `turma` a partir do que já está no bucket |
| `code/join_tables_escola.py` | Junta `escola_2023` e `escola_2024` na `escola` |
| `code/br_inep_censo_escolar_2024.ipynb` | Tratamento da edição 2024 |

As funções de `utils.py` não dependem de estado global: recebem o caminho do
arquivo e a arquitetura, e devolvem `DataFrame`. Servem tanto a um script de
edição quanto a uma task de flow.

Os dados intermediários ficam em `~/Downloads/br_inep_censo_escolar_data/`,
fora do repositório. A variável de ambiente `CENSO_DATA_DIR` altera esse
caminho.

## Como rodar

```bash
cd models/br_inep_censo_escolar/code

uv run python run_local.py                  # trata
uv run python run_local.py --output /tmp/x  # outro destino
```

O download e a extração acontecem só se os CSVs não estiverem em disco.

A saída é um CSV por unidade da federação, em
`<saída>/escola/ano=<ano>/sigla_uf=<uf>/escola.csv`. `run_local.py` não envia
nada ao BigQuery; o envio é feito por `utils.upload_table`.

O schema da tabela publicada é lido pela API de metadados (`get_table`), que
não cria job no BigQuery e por isso funciona com a credencial de
desenvolvimento — ao contrário de `bd.read_sql`, que falha na máquina local.

## Particularidades da fonte

| Comportamento | Tratamento |
|---|---|
| A URL de 2025 termina em sublinhado (`microdados_censo_escolar_2025_.zip`), que é a republicação de julho de 2026 | `URL` em `censo_escolar_2025.py` |
| Os arquivos dessa republicação ganharam sufixo `_V2`, e no Gestor Escolar o sufixo veio minúsculo (`_v2`) | `find_csv` procura pelo começo do nome, sem diferenciar caixa |
| O zip tem 512 MB, e o servidor aceita requisição por faixa | Dá para ler o dicionário de dados e os cabeçalhos sem baixar o arquivo inteiro |

## A arquitetura descreve todos os anos

A tabela de arquitetura guarda também as colunas que a Base dos Dados já
publicou e aposentou, e algumas delas vêm da mesma coluna de origem que uma
coluna corrente. `IN_PODER_PUBLICO_PARCERIA` alimenta `poder_publico_parceria`,
que é a atual, e `conveniada_poder_publico`, publicada até 2023.

`colunas_da_edicao` descarta as linhas cuja cobertura temporal termina antes do
ano tratado, e `renames_da_fonte` levanta erro se, ainda assim, um nome de
origem mapear para dois nomes da Base dos Dados. Sem esse corte a coluna cai no
nome aposentado e a corrente sai inteiramente nula.

## Colunas sem origem em 2025

Trinta e oito colunas da tabela não têm coluna correspondente em nenhum dos
arquivos de 2025 e são preenchidas com nulo. Elas se dividem em três grupos:

- **16 colunas de profissionais** (`profissional_psicologo`,
  `profissional_bibliotecario`, `profissional_tradutor_libras` e as demais
  `IN_PROF_*`). O dicionário de dados do INEP descreve essas colunas, mas
  nenhum dos arquivos publicados as traz. Estavam preenchidas em 2024.
- **17 colunas descontinuadas**, ausentes tanto do dicionário quanto dos
  arquivos: `diurno`, `noturno`, `ead`, as sete `etapa_ensino_*` de indicador
  (`IN_INF`, `IN_FUND`, `IN_PROF_TEC`, `IN_EJA_FUND`, `IN_EJA_MED`, `IN_ESP`,
  `IN_ESP_CC`, `IN_ESP_CE`) e as seis `quantidade_matricula_medio_tecnico*`.
- **5 colunas paradas há mais tempo**: `material_especifico_quilombola`,
  `material_especifico_indigena`, `material_especifico_nao_utiliza`,
  `programa_brasil_alfabetizado` e `final_semana`. A arquitetura só declara
  coluna de origem até 2022, e o último ano com valor publicado é 2018 — mesmo
  assim a cobertura temporal delas segue declarada como corrente.

A lista é impressa a cada execução. Vale conferi-la: em 2025 as duas colunas de
poder público apareciam nela por defeito do tratamento, e não por ausência na
fonte.

## Verificação de colunas

A comparação com a tabela do BigQuery é por conjunto, e não por quantidade. Com
`len(a) == len(b)`, uma coluna que entra compensa uma que sai e a verificação
passa com a tabela errada.

A ordem das colunas da tabela publicada é exatamente a da arquitetura, com uma
exceção: a arquitetura declara `id_distrito` (origem `CO_DISTRITO`, cobertura
corrente) e a tabela não a tem. O tratamento segue o schema da tabela, e não a
arquitetura, porque o staging é CSV e a tabela externa casa coluna por posição
— acrescentar uma coluna quebraria a leitura das partições de 2007 a 2024, que
já estão no bucket com o layout antigo. Publicar `id_distrito` exige recarregar
todos os anos.
