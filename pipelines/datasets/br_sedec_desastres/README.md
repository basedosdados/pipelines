# Documentação do Conjunto de Dados: br_sedec_desastres

Status: **esqueleto**. Estrutura criada em 2026-08-04 a partir da issue
[#1747](https://github.com/basedosdados/pipelines/issues/1747); a raspagem, a
arquitetura e as decisões de desenho abaixo ainda não foram feitas.

## Sobre o Sistema

O **S2ID (Sistema Integrado de Informações sobre Desastres)** é a base
administrativa oficial do Governo Federal para registro e gestão de desastres
ocorridos no país. É mantido pela **SEDEC (Secretaria Nacional de Proteção e
Defesa Civil)**, vinculada ao **MIDR (Ministério da Integração e do
Desenvolvimento Regional)**.

A tabela desta task vem do relatório gerencial **"Reconhecimentos vigentes"**:
os reconhecimentos federais de situação de emergência e estado de calamidade
pública **em vigor**.

- **Página da fonte:** `https://s2id.mi.gov.br/paginas/relatorios/`
- **Tabela:** `reconhecimentos_vigentes`

## Decisões

Cada uma tem um `TODO` correspondente no código enquanto estiver aberta. Em
2026-08-05: as decisões 2 e 5 estão fechadas; a 1 é provisória; a 3 e a 4 seguem
abertas, e as duas dependem da frequência de atualização, que continua em branco
na issue.

### 1. `dataset_id`: `br_sedec_desastres` × o `s2id` que já existe

A issue pede `br_sedec_desastres`, mas o backend **de prod** já tem um dataset de
slug **`s2id`** (organização `midr`, temas `agriculture` + `environment`, tags
`disaster`/`natural-disaster`/`nature`), **sem nenhuma tabela**. Pela convenção
`<org>_<slug>` o id GCP dele seria `br_midr_s2id`.

**Decidido em 2026-08-04: seguir com `br_sedec_desastres` "por enquanto"** — ou
seja, provisório, ainda sujeito a revisão antes do registro de metadados. Se a
decisão mudar para reaproveitar o dataset existente, é um `git mv` dos dois
diretórios mais o `DATASET_ID` em `constants.py`, o `schema`/`alias` no `.sql` e
a entrada no `dbt_project.yml`.

Vale reconciliar antes de registrar metadados: registrar `br_sedec_desastres`
como dataset novo deixa o `s2id` de prod órfão (ele já existe, sem tabelas), e
dois datasets para a mesma fonte no backend.

### 2. Retrato × histórico — DECIDIDO: histórico

"Vigentes" é um **retrato do momento**: quando um reconhecimento vence, a linha
desaparece da fonte.

**Decidido em 2026-08-05 (confirmado com a supervisão): acumular retratos.** Cada
execução grava o retrato inteiro com a data da extração, e a tabela é a série
desses retratos.

O que isso fixa:

| Aspecto | Valor |
| --- | --- |
| `dump_mode` | `append` |
| partição | `data_extracao` (DATE) |
| chave única | `data_extracao` + a chave do reconhecimento |
| volume | ~1239 linhas por retrato (medido em 2026-08-05) |

Duas consequências que valem estar ditas:

**A série começa agora.** Não há como recuperar retratos passados — a fonte só
mostra o presente. No primeiro run a tabela tem um dia de história, e o valor
dela acumula daí pra frente. Isso é expectativa a alinhar com quem vai usar.

**O guarda do poll fica sem sentido claro.** `poll_source_for_update_task`
compara a data máxima da fonte contra o `Update` registrado, para não reingerir
dado que não mudou. Numa série de retratos, todo run legitimamente produz linhas
novas (outra `data_extracao`), e a fonte não publica data nenhuma. Ver a decisão
4 abaixo.

### 3. Frequência de atualização

Está em branco na issue. Trava duas coisas:

- o `cron` do `deploy_schedules` (deixado comentado em `flows.py`, para o deploy
  de prod não armar uma cadência errada);
- o **tier de cobertura**: pela regra da BD, tabela que atualiza mensalmente ou
  mais fica com a janela recente atrás do BD Pro (`PartBdpro`); menos que isso
  fica `AllFree`. Sem a frequência, `_COVERAGE` não pode ser preenchido.

Se cair em `PartBdpro`, as **duas** Coverages (free com `is_closed=False` e pro
com `is_closed=True`) precisam existir na tabela antes do primeiro run, senão ele
falha em `assert_coverage_topology` antes de escrever nada.

### 4. Qual data alimenta o poll

`poll_source_for_update_task` compara a data máxima da fonte contra o `Update`
registrado no backend. Numa tabela de retrato não existe "data máxima do dado"
óbvia — pode ser a data de extração, ou alguma data de publicação do relatório.
`clean_all` devolve `max_date=None` como marcador até isso ser definido.

### 5. Onde fica o join do município — DECIDIDO: no dbt

A fonte dá o **nome** do município, e a convenção exige `id_municipio` com FK
para o diretório.

**Decidido em 2026-08-05: o join fica no modelo dbt**, que é o padrão do repo
para join contra produção. O staging carrega `nome_municipio`; `id_municipio` só
existe na tabela final, e o nome não sobrevive a ela (já vive no diretório).

Consequências no código:

- `constants.STAGING_SUBSTITUICOES` declara a divergência
  (`id_municipio` → `nome_municipio`), e o `write_partitioned` a aplica ao montar
  a ordem das colunas do parquet a partir da arquitetura;
- a normalização de nome e o mapa de municípios renomeados **não** ficam em
  Python: viram SQL, no formato do modelo do BNDES;
- o `not_null` em `id_municipio` no `schema.yml` é o que impede perda silenciosa,
  porque o `left join` devolve NULL para quem não casar.

A taxa de casamento medida, as 6 exceções na forma do `case` e a pegadinha do
acento grave estão no roadmap da task, Etapa 4.

## Notas sobre a fonte

O `/paginas/relatorios/` é uma aplicação **JSF/PrimeFaces**: o export não sai de
uma URL de download estável, e sim de um POST no formulário da página, com
sessão e `ViewState`. Os ids dos componentes são gerados pelo framework e mudam
quando a página muda, então convém ancorar a raspagem no texto do painel/botão em
vez de fixar id.

O painel "Reconhecimentos vigentes" oferece PDF, XLS e CSV.

### Raspagem: selenium headless (decidido em 2026-08-04)

A alternativa era `requests.Session` + BeautifulSoup, montando o postback à mão
(ler o `ViewState`, remontar os campos do formulário). Optamos pelo browser: o
`ViewState`, a sessão e o postback ficam sendo problema do Chrome.

A imagem do repo já suporta: o `Dockerfile` instala `google-chrome-stable` e
`webdriver-manager` é dependência pinada. Precedentes de selenium no repo:
`pipelines/crawler/stf_corte_aberta/utils.py` e `pipelines/crawler/bcb/utils.py`
— mas os dois esperam com `time.sleep` fixo, o que os deixa frágeis. Aqui
usamos `WebDriverWait` e `_wait_for_download`, que sonda o `.crdownload` até o
arquivo fechar.

Custos que vêm com a escolha, para não esquecer:

- `--disable-dev-shm-usage` é obrigatório no k8s (o `/dev/shm` do pod é pequeno);
- `job_variables = {"memory": "4Gi"}` em `flows.py` é um palpite — medir num run
  de dev e ajustar;
- a página tem **vários** botões "Exportar CSV", um por relatório. O seletor
  precisa ser relativo ao painel certo, senão baixa o relatório errado sem
  levantar erro nenhum.

## Estrutura

```text
pipelines/datasets/br_sedec_desastres/
├── constants.py   URLs, XPaths, timeouts, COLUNAS (o schema)
├── utils.py       download + limpeza (funções puras, sem Prefect)
├── tasks.py       @task envolvendo utils (é onde ficam os retries)
├── flows.py       o @flow: ordem das etapas + schedule
└── README.md      este arquivo

models/br_sedec_desastres/
├── br_sedec_desastres__reconhecimentos_vigentes.sql modelo dbt
└── schema.yml                                       testes
```

**É só isso.** Uma base brasileira não tem `code/` sob `models/` — medido em
2026-08-05: dos 158 diretórios em `models/`, 18 têm `code/architecture/`, e
nenhum deles é base de dados brasileira (dois são diretórios de outros países).
Arquitetura commitada, script de limpeza local e `upload.py` são o padrão de
onboarding **internacional**, e foram removidos daqui.

Em consequência:

- **a planilha de arquitetura fica em `task_davi/bndes/`**, fora do repo. Como o
  código não pode lê-la, o schema (ordem, tipos, `original_name`) vive em
  `constants.COLUNAS` — ao mexer na planilha, mexer lá também;
- **não há carga inicial separada.** A primeira carga é o próprio flow rodando em
  dev com `materialize_to_prod=False`;
- o teste local das etapas é o `run_flow_local.py` na raiz, que escreve em
  `tmp/br_sedec_desastres/` (já ignorado pelo git).

## Pendências antes de abrir PR

- [ ] `.sql` está com `select *` e sem o join do município — trocar por
      `safe_cast` explícito por coluna. A action table-approve materializa em
      prod todo `.sql` alterado, e o staging é todo STRING: a tabela de prod
      sairia sem tipo nenhum.
- [ ] `schema.yml` com testes comentados.
- [ ] `build_reconhecimentos_vigentes` e o `max_date` do `clean_all`.
- [ ] `_COVERAGE`, `DATE_FORMAT` e o cron em `flows.py` — todos dependem da
      frequência de atualização, ainda em branco na issue.
