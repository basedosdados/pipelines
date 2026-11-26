# Documentação do Conjunto de Dados: CNES (Cadastro Nacional de Estabelecimentos de Saúde)

Este documento registra o contexto, os problemas conhecidos e as decisões tomadas
no conjunto `br_ms_cnes`, para quem for mantê-lo depois.

---

## Sobre o Sistema

O CNES é o cadastro oficial dos estabelecimentos de saúde do país, mantido pelo
DATASUS. Os dados são publicados mensalmente no FTP público, em arquivos `.dbc`
por grupo e por UF, sob `dissemin/publicos/CNES/200508_/Dados/<GRUPO>/`.

- [Portal CNES](https://cnes.datasus.gov.br/)
- [FTP DATASUS](ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/)
- Tabelas auxiliares: `TAB_CNES.zip` (ver ressalva na seção do dicionário)

O nome do arquivo carrega a competência em `YYMM` nas posições 4–8 do basename —
`STAC2606.dbc` é o grupo ST, UF AC, competência 2026-06. É daí que o crawler tira
a cobertura da fonte (`get_datasus_source_max_date`).

## Estrutura no repositório

O crawler é compartilhado com os outros conjuntos do DATASUS:

| Arquivo | Papel |
|---|---|
| `pipelines/datasets/br_ms_cnes/flows.py` | 13 flows, um por tabela, montados por `_cnes_flow(table_id, cron)` |
| `pipelines/shared/datasus/flows.py` | `_run_cnes` — a receita de fato (poll → download → clean → upload → dbt) |
| `pipelines/shared/datasus/tasks.py` | tasks de FTP, descompressão `.dbc`, limpeza |
| `models/br_ms_cnes/` | 14 modelos dbt (13 tabelas + `dicionario`) |
| `models/br_ms_cnes/code/update_dicionario.py` | manutenção do dicionário (ver abaixo) |

O `dicionario` **não tem flow**. É a única tabela do conjunto que não é
atualizada por nada automaticamente.

### O upload para o staging é parquet

O `pre_process_files` grava **parquet**, e as tabelas externas de staging, em dev e em prod,
estão declaradas como `PARQUET`. Por isso as duas chamadas de `upload_to_gcs` no `_run_cnes`
passam `source_format="parquet"` — o default do parâmetro é `"csv"`.

Isso ficou implícito por muito tempo e quebrou em 2026-07-31. Com `dump_mode="append"` o
formato só era usado quando a staging **não** existia, então o default nunca importava. O
PR #1677 acrescentou o `_sync_staging_schema` ao ramo "tabela já existe", que chama
`dump_header(data_path, source_format)` — e aí os 13 flows do conjunto passaram a falhar no
upload com `Nenhum arquivo csv encontrado em /tmp/br_ms_cnes/output/<tabela>`, antes de
subir qualquer coisa. O `_run_dbf_to_parquet` (SIA e SIH) sempre declarou o formato; o
`_run_sinan` tem o mesmo defeito e ficou fora desta correção.

## Modelo de poll (PR #1707)

Até 2026-07 as 11 tabelas com cron estavam paradas em 2026-05 com a fonte já em
2026-06. A causa: o poll comparava a cobertura da fonte com o `Table.Update`, que
guarda **quando** rodamos a materialização, não a competência coberta pelos dados.
Quando a materialização caía depois do dia 1º, esse carimbo ultrapassava o
primeiro dia do mês seguinte e o poll respondia "sem novidade" para sempre.

A correção trocou o processo por `pipelines/utils/metadata/poll.py`, que compara
**cobertura publicada pela fonte** × **cobertura já materializada**, e faz o
`Table.Update` passar a guardar cobertura. O fluxo em `_run_cnes` é:

```
register_source_coverage → check_source_is_ahead_of_table → materialização → sync_table_coverage
```

Só o CNES adota esse modelo; os outros ~25 flows do repo seguem no poll antigo.

Mergeado em 2026-07-28 (`ad0391f8`).

**Consequência prática:** um run que loga `Não há novas atualizações na fonte
original` está rodando o poll **antigo** — é a mensagem do processo anterior. Todos
os runs agendados de 2026-07-28 são anteriores ao merge e fecharam verdes sem
ingerir nada; a partir do dia seguinte a `main` já carrega o poll novo.

### O destravamento manual de 2026-08-03

O modelo novo consertou a comparação, mas nada migrou o valor antigo. As 11 tabelas
com cron seguiam com o horário da materialização no `Table.Update.latest`
(`2026-06-17`/`18`), e o `check_source_is_ahead_of_table` comparava a cobertura da
fonte (`2026-06-01`) contra esse horário — sempre menor. E o único ponto que troca esse
valor por cobertura é o `sync_table_coverage`, que roda **depois** dessa checagem: para
ela passar, o flow precisava ter rodado; para rodar, ela precisava passar.

A saída foi escrever o valor à mão, uma vez por tabela. Em 2026-08-03, via MCP
(`create_update_update`, env prod), as 11 tabelas mensais receberam
`latest = 2026-05-01` — o fim da cobertura registrada —, preservando `entity=month`,
`frequency=1` e `lag=None`. Daí em diante o `sync_table_coverage` mantém o campo como
cobertura e a checagem se sustenta sozinha.

O log de um flow travado não acusa nada disso: ele mostra
`The year_month_to_parse (YYMM) is 2606` (o `check_files_to_parse` lê a **cobertura** e
acerta) e, três linhas abaixo, `Tabela CNES já cobre a fonte — encerrando` (o
`check_source_is_ahead_of_table` lê o `Table.Update` e erra). As duas afirmações
contraditórias na mesma run são a assinatura desse problema.

Duas tabelas ficaram de fora de propósito: `estabelecimento_ensino` e
`regra_contratual` têm `entity=day` e `frequency=None`, e o `create_update_update`
exige `frequency` — corrigi-las seria inventar uma cadência que ninguém definiu. Elas
não estão travadas: o `latest` delas é de `2024-05-16`, anterior à cobertura da fonte,
então a checagem já passa — por acidente, não por desenho.

Duas armadilhas do MCP no caminho: ele não lê `latest`, `frequency` nem `lag`, e
devolve o range de cobertura só com ano. Os valores acima foram lidos por GraphQL
direto (`allUpdate`, `allCoverage`, `allDatetimerange`).

Esse destravamento é **uma vez por tabela**. Tabela nova no conjunto, ou outro
conjunto que adote o poll novo, precisa da mesma escrita.

**Pendências vistas na mesma leitura:**

- `estabelecimento_ensino` e `regra_contratual` têm só a cobertura free, mas o
  `_run_cnes` declara `PartBdpro` para toda tabela — um run de prod com
  `update_metadata=True` para em `assert_coverage_topology`
  (`part_bdpro exige Coverage free + pro`) antes de gravar qualquer coisa.
- Em `dados_complementares` e `estabelecimento_filantropico` o range da cobertura pro
  está com `is_closed=False`, divergindo da cobertura que o contém.

---

## O dicionário

### Estado em 2026-07-30

**Dev está fechado; prod ainda não.** As duas correções da issue #1714 foram
aplicadas no staging dev — as 7 chaves novas (`tipo_equipamento` 11–16 e
`tipo_equipe` 82) e os 5 rótulos recuperados (`tipo_equipe` 77–81). O dicionário
passou de 1.591 para 1.598 linhas; `dbt run` OK e `dbt test` de `equipamento` e
`equipe` passando (16/16 e 8/8).

Prod segue com as 1.591 linhas originais e os tapa-buracos até o `table-approve`
rodar no merge do PR — ver "Como isso chega em prod" abaixo.

Em 2026-07-29 a mesma issue ganhou um segundo escopo: o `id_equipamento` não identifica
um equipamento sozinho, e o dicionário serve rótulo errado em 22.313 linhas por mês. Ver
"O código de 4 dígitos" abaixo.

**Dev fechado em 2026-07-29.** Os 141 rótulos de `codigo_equipamento` entraram (dicionário
de 1.598 para **1.739 linhas**), o `equipamento` foi reconstruído com `--full-refresh`
(142,4M linhas, 4,8 GiB, 40 s) e os 8 testes passam. Verificado na tabela nova: zero linhas
em `__UNPARTITIONED__` e 99,85% das linhas de 2026-06 com rótulo.

**Dev completado em 2026-07-30, e isso muda o plano de prod.** O staging de dev estava 20
competências atrás do de prod, o que tornava qualquer `--full-refresh` em produção uma
perda de histórico. As competências foram transferidas de prod para dev (ver "Como isso
chega em prod"), o `equipamento` de dev foi reconstruído sobre as **251 competências**
resultantes — 164.565.160 linhas, 8 testes passando — e o `--full-refresh` em prod passou a
ser o caminho previsto, não uma armadilha.

Na mesma data a issue #1722 corrigiu o tipo de quatro colunas, o que **obriga** esse
full-refresh: troca de tipo não se aplica em modelo incremental. O dicionário foi de 1.739
para **1.743 linhas** — os rótulos `Sim`/`Não` dos dois indicadores, que passaram a ser
STRING cobertos por dicionário. Ver "Tipos das colunas do equipamento" abaixo.

### Origem

O dicionário deste conjunto **não é gerado por nada**: não há crawler, não há
fonte registrada no backend (o conjunto tem 13 fontes, uma por tabela, nenhuma do
dicionário), não há job de CI. O que está no ar é um CSV único subido à mão em
`gs://basedosdados-dev/staging/br_ms_cnes/dicionario/dicionario.csv`, sem registro
de quem manteve nem a partir de qual planilha. Em 2026-07-28 ele tinha 1.591
linhas cobrindo 12 tabelas e não era tocado desde 2026-01-21.

Verificado em 2026-07-28: o CSV do staging dev e a tabela materializada em prod
eram **idênticos** (1.591 linhas, zero divergência, zero duplicatas).

### Como atualizar

Use `models/br_ms_cnes/code/update_dicionario.py`. Ele lê o staging, acrescenta as linhas
pendentes (ignorando as que já existem, pela chave `id_tabela + nome_coluna + chave`) e
regrava o CSV. São três estruturas, todas idempotentes:

| Estrutura | Para quê |
|---|---|
| `NEW_ROWS` | chaves novas de qualquer tabela ou coluna; traz `id_tabela` e `nome_coluna` por linha |
| `CODIGO_EQUIPAMENTO` | os 141 rótulos do código de 4 dígitos; só chave e valor, o resto é preenchido por `candidate_rows()` |
| `REPLACE_ROWS` | reescreve o valor de linhas que já existem |

```bash
cd models/br_ms_cnes/code
export BD_SERVICE_ACCOUNT_DEV="$HOME/.basedosdados/credentials/credentials-dev.json"
uv run --project ../../.. python update_dicionario.py --dry-run
uv run --project ../../.. python update_dicionario.py --apply

cd ../../..
uv run dbt run  --select br_ms_cnes__dicionario
uv run dbt test --select br_ms_cnes__equipamento br_ms_cnes__equipe
```

### Como isso chega em prod

O script escreve **só o bucket de dev**. O `dicionario` não está em flow nenhum,
então nada vai promovê-lo sozinho. Quem leva para prod é a action
`table-approve`, que roda com `--sync-bucket` e copia
`gs://basedosdados-dev/staging/<ds>/<tabela>` → `gs://basedosdados/staging/<ds>/<tabela>`
(com backup em `basedosdados-backup`), recria a staging e roda o dbt em prod.

Duas condições fáceis de esquecer, e sem as duas o prod fica para trás em silêncio:

1. o PR precisa carregar a label **`table-approve`**;
2. o PR precisa **tocar em `models/br_ms_cnes/br_ms_cnes__dicionario.sql`** — a
   action monta o par (dataset, tabela) a partir dos `.sql` modificados no diff.

Atenção também: o `sync_bucket` apaga o destino e copia o dev por cima. Antes de
sincronizar, confira que prod não tem nada que dev não tenha.

**E até 2026-07-30 prod tinha.** Medido em 2026-07-29: o staging de dev do `equipamento`
tinha 231 competências contra 250 em prod — faltavam em dev 2024-02 a 2025-07, mais 2025-09
e 2025-10, 22.164.918 linhas. O staging de dev não é espelho de prod por construção: ele
acumula só o que os runs de dev produziram, e esses são manuais e esporádicos.

#### A transferência de prod para dev (2026-07-30)

As 20 competências foram copiadas do staging de prod para o de dev antes do merge, o que
tirou a armadilha do caminho em vez de conviver com ela:

- **540 arquivos, 75,8 MB**, cópia server-side de
  `gs://basedosdados/staging/br_ms_cnes/equipamento/ano=…/mes=…/sigla_uf=…/equipamento.parquet`
  para o mesmo caminho em `gs://basedosdados-dev`
- **byte a byte, sem reprocessar nada** — são os arquivos que o próprio crawler escreveu.
  Foi por isso que se descartou reconstruí-los com `EXPORT DATA` ou rebaixar 22M linhas do
  FTP: qualquer um dos dois arriscaria um parquet com esquema levemente diferente, e é esse
  arquivo que o `table-approve` empurra para prod depois
- dev saiu de 231 para **251 competências** (164.567.187 linhas no staging) e virou
  **superconjunto** de prod: zero partição `(ano, mes, sigla_uf)` de prod sem par de
  contagem idêntica em dev, mais 27 partições que só dev tem (2026-06, que prod não
  alcançou)

A credencial de dev (`chave-subidores-de-dados@basedosdados-dev`) **lê o bucket de prod**
passando `user_project=basedosdados-dev` — requester-pays cobrado em dev. Não é preciso
credencial de prod para uma transferência nesse sentido.

Consequência: o `--sync-bucket` deixou de ser destrutivo para o `equipamento`, e
`--full-refresh` em produção passou a ser seguro — o que a issue #1722 exige.

Isso não é particularidade do CNES: a premissa do `--sync-bucket` é que dev espelha prod, e
isso vale para qualquer conjunto cujo dev esteja atrás. Nas outras 12 tabelas do conjunto a
comparação nunca foi feita; antes de mergear uma PR com a label `table-approve` que toque
qualquer `.sql` delas, conte as competências dos dois lados primeiro.

### Códigos acrescentados em 2026-07-28 (issue #1714)

A competência 2026-06 trouxe códigos que o dicionário não tinha, quebrando o
`custom_dictionary_coverage` (severity `error`) de `equipamento` e `equipe`.

| Tabela | Coluna | Chave | Valor | Procedência |
|---|---|---|---|---|
| equipamento | tipo_equipamento | 11 | Avaliação Antropométrica e Funcional | Portaria SAES/MS nº 4.109/2026, via terceiros |
| equipamento | tipo_equipamento | 12 | Radioterapia | **Portaria SAES/MS nº 3.695/2026, Art. 2º §1º** |
| equipamento | tipo_equipamento | 13 | Quimioterapia | **Portaria SAES/MS nº 3.695/2026, Art. 2º §1º** |
| equipamento | tipo_equipamento | 14 | Reabilitação | Portaria SAES/MS nº 4.109/2026, via terceiros |
| equipamento | tipo_equipamento | 15 | Procedimentos Clínicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| equipamento | tipo_equipamento | 16 | Procedimentos Cirúrgicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| equipe | tipo_equipe | 82 | E-DOT - Equipe Hospitalar de Doação para Transplantes | CNES 4.8.40, via terceiros |

Só 12 e 13 têm fonte primária. Os outros cinco vêm de terceiros citando a Portaria
4.109/2026 e o CNES 4.8.40 — o texto oficial da 4.109 não foi localizado. A decisão
foi registrar mesmo assim: os códigos já estão nos dados, e um rótulo de segunda
mão é melhor que um teste quebrado que derruba o flow depois de materializar.

**As tabelas auxiliares oficiais não ajudam para estes códigos.** O `TAB_CNES.zip`,
regerado em 2026-07-21, traz só 10 tipos em `TP_EQUIPAM.dbf` e para no 76 em
`EQUIPE.dbf`.

Ressalva levantada em 2026-07-29: isso vale para o `TP_EQUIPAM.dbf` e o `EQUIPE.dbf`,
mas **não** para o resto do pacote. O mesmo `TAB_CNES.zip` traz um diretório `CNV/` que
ninguém tinha aberto, e lá está o dicionário oficial dos equipamentos — ver a seção
seguinte.

### Rótulos recuperados de `tipo_equipe` 77–81 (2026-07-28)

As chaves 77 a 81 estavam no dicionário com o valor literal
`Não encontrado nos dicionários oficiais` — tapa-buracos de alguém que bateu na
mesma parede antes e registrou a chave sem rótulo só para o teste passar. Os
rótulos reais:

| Chave | Sigla | Nome | Norma |
|---|---|---|---|
| 77 | EMAP-R | Equipe Multiprofissional de Apoio para Reabilitação | Portaria SAES/MS nº 1.619/2024, Art. 3º §4º |
| 78 | EACP | Equipe Assistencial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II |
| 79 | EMCP | Equipe Matricial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II |
| 80 | EAP-DESINST | Equipe de Avaliação e Acompanhamento de Medidas Terapêuticas (transtorno mental em conflito com a lei) | Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº 2.070/2024 |
| 81 | EqAE | Equipe de Atenção Especializada | Portaria SAES/MS nº 3.200/2025, Art. 5º |

**Grau de confirmação, honestamente:** o texto oficial foi lido para 77 e 81.
Para 78 e 79 a fonte é o Conass Informa 156/2024 resumindo o Anexo II. Para 80 não
foi possível ler nenhuma das duas portarias — o `bvsms.saude.gov.br` recusou
conexão em todas as tentativas —, então o código vem de buscas mais a evidência
interna descrita abaixo.

**A evidência interna é forte.** A tabela `equipe` tem a coluna `equipe`
(`nome_eqp` na fonte), com o nome que o próprio estabelecimento deu à equipe, e ela
bate com a norma em todos os cinco casos:

```
77 → EMAP-R · MELHOR EM CASA · PMEC · EQUIPE DE APOIO A REALIBITACAO
78 → EACP · CUIDADOS PALIATIVOS · EQUIPE DE CUIDADOS PALIATIVOS
79 → EMCP - CUIDADOS PALIATIVOS · EQUIPE MATRICIAL DE CP
80 → EAP-DESINST ESTADUAL SAO LUIS · EAP - DESINST · SISTEMA PRISIONAL I
81 → AGORA TEM ESPECIALISTAS · JORNADA OFTALMOLOGICA · EQUIPE PRONAS-PCD
```

No caso do 80 os estabelecimentos escrevem a sigla inteira no nome da equipe, o que
fecha o caso na prática mesmo sem a portaria em mãos. **Vale conferir o texto
oficial quando o bvsms voltar.**

O rótulo do 80 foi **abreviado**: o nome completo (`Equipe de Avaliação e
Acompanhamento de Medidas Terapêuticas Aplicáveis à Pessoa com Transtorno Mental em
Conflito com a Lei`) tem 137 caracteres contra ~60 do resto do arquivo.

**Cronologia:** 77–80 entram nos dados em 2025-11 e o 81 em 2025-12 — foram ligados
no CNES na mesma leva, apesar de as portarias serem de 2024 e 2025. O 82 (E-DOT)
aparece só em 2026-06.

Essa recuperação é feita pelo `REPLACE_ROWS` do `update_dicionario.py`, único caminho que
reescreve o valor de linhas existentes — o `NEW_ROWS` e o `CODIGO_EQUIPAMENTO` só
acrescentam chave nova. Os três são idempotentes.

### Convenção de rótulo

O arquivo é inconsistente por acúmulo histórico: as chaves 1–8 de
`tipo_equipamento` são minúsculas e sem acento; as 9 e 10 são Title Case; os
rótulos de `tipo_equipe` seguem o padrão `sigla - descrição` em minúscula. As
adições mais recentes usam Title Case com acento, e foi esse o padrão adotado
para os códigos novos. O legado não foi mexido.

Em `codigo_equipamento` a grafia **não** foi normalizada: cada rótulo vem como está na
sua fonte. O CNV escreve `Gama Câmara` onde o dicionário legado escrevia `gama camara`, e
a Portaria 3.695 renomeia coisas (`raio x dentario` → `Raio X Odontológico`). Preservar a
grafia da fonte foi decisão consciente — reescrever para um padrão interno perderia a
rastreabilidade até a norma. Consequência: o mesmo equipamento aparece com duas grafias no
dicionário, uma sob `id_equipamento` e outra sob `codigo_equipamento`.

### O código de 4 dígitos (2026-07-29)

O CNES identifica um equipamento por um código de 4 dígitos: os 2 primeiros são o tipo,
os 2 últimos são o equipamento **dentro daquele tipo**. A numeração do equipamento
recomeça a cada tipo — existe um equipamento 01 na radiologia, outro na telessaúde,
outro na diálise. Só os 4 dígitos juntos identificam.

A fonte entrega as duas metades separadas (`TIPEQUIP` e `CODEQUIP`) e o modelo as guarda
em `tipo_equipamento` e `id_equipamento`, descartando a junção. Como o dicionário é
consultado só pelo `id_equipamento`, toda linha de qualquer tipo com equipamento 1 recebe
"gama camara". Medido em 2026-06, **22.313 linhas apontam para outro equipamento**:

| Origem | Linhas | Códigos |
|---|---|---|
| Tipos 9–16, onde a numeração reinicia | 15.942 | 23 |
| Tipo 01, códigos criados pela Portaria 3.695 | 6.371 | 17 |

Os tipos 1 a 8 escapam porque o DATASUS embutia o tipo no primeiro dígito do equipamento
— tipo 2 usa 21–23, tipo 3 usa 31–50 —, o que fazia o número de 2 dígitos bastar. Quebrou
no tipo 9, cuja faixa já estava ocupada pelo tipo 8.

O `custom_dictionary_coverage` não pega: ele valida `id_equipamento` e `tipo_equipamento`
separadamente, e a combinação dos dois não é conferida por ninguém.

**Decisão:** criar a coluna `codigo_equipamento` com os 4 dígitos e passar a consultar o
dicionário por ela. `id_equipamento` e `tipo_equipamento` permanecem, para não quebrar
quem já consome a tabela.

**O teste passa a cobrir só `tipo_equipamento`.** `id_equipamento` sai por ser alarme
morto: os números 1 a 99 estão todos no dicionário e sempre estarão, então o teste nunca
falharia, aconteça o que acontecer com a fonte. E `codigo_equipamento` **não entra** —
apontar o `custom_dictionary_coverage` para ele exigiria cobrir os 207 códigos, e 66 não
têm rótulo que exista (ver pendências). Como o teste é `severity: error` e mata o flow
depois de materializar, ligá-lo ali quebraria o `equipamento` todo mês de propósito. Fica
o `tipo_equipamento`, que é o alarme que funciona — foi ele que pegou os tipos 11–16 e
abriu esta issue.

Duas armadilhas medidas na série:

- **`concat` cru não serve.** O staging trocou de formato em 2025: antes `tipequip` vinha
  `'7'`, hoje vem `'07'`, e 94% da série está no formato antigo. Precisa de `lpad` nas
  duas metades. `codequip` tem 2 caracteres em toda a série e nunca é `'00'`.
- **O `table-approve` não faz full-refresh.** `prefect_run_dbt.py` não aceita a flag, e o
  projeto não define `on_schema_change` em lugar nenhum, então vale o padrão do dbt,
  `ignore`: num modelo incremental, coluna nova não aparece em prod — o `dbt run` roda,
  fica verde, e não adiciona nada. Foi o que aconteceu no `br_me_cnpj`.

  O `br_ms_cnes__equipamento` **passou a declarar `on_schema_change="append_new_columns"`**
  justamente por isso. Com ele, a coluna nova é adicionada à tabela de prod no run normal
  da action, com valor nulo nas linhas antigas. Os demais modelos do conjunto seguem no
  padrão `ignore` e teriam o problema se ganhassem coluna.

  O deployment de dbt aceita `flags` (`run_dbt_model_flow`), e **esse é o caminho para prod
  desde 2026-07-30**, quando o staging de dev deixou de estar atrás do de prod — antes disso
  reconstruir a partir do staging sincronizado perderia 20 competências. Com o
  full-refresh, `codigo_equipamento` chega preenchida na série inteira em vez de nula no
  histórico, e o particionamento é refeito de fato.

  **Peça `dbt_command="run"`, não `"run/test"`.** O `run_dbt` anexa a flag aos **dois**
  comandos: a intenção do código é reservá-la ao `run`, mas quando `cmd == "test"` a
  condição cai no `elif` e a flag vai junto, e o dbt aborta com
  `No such option: --full-refresh`. O `run` termina antes disso, então a tabela sai correta
  e só o teste falha — foi o que aconteceu em 2026-07-31. Rode o teste depois, sem flag, ou
  deixe o flow fazê-lo.

### De onde saem os rótulos dos 4 dígitos

A série tem 207 códigos distintos. Cobertura montada em 2026-07-29:

| Procedência | Códigos | Linhas em 2026-06 |
|---|---|---|
| `CNV/Equip_Tp.cnv` | 94 | 969.154 |
| Portaria SAES/MS nº 3.695/2026, Anexo I | 30 | 56.318 |
| Legado transposto do dicionário atual | 17 | 70.702 |
| Sem rótulo | 66 | 1.681 |

Isso põe **99,85% das linhas de 2026-06** com rótulo (99,97% da série).

**O `CNV/` do `TAB_CNES.zip` é o dicionário oficial do DATASUS.** É o formato de conversão
do TabWin: texto puro, latin-1, uma linha por equipamento com o código de 4 dígitos ao
final. O `Equip_Tp.cnv` resolve 97 dos 207 códigos de uma vez. O crawler não usa o
diretório `Auxiliar/` do FTP.

**A Portaria 3.695 tem precedência sobre o CNV**, por ser a norma e ser mais recente. Ela
criou 27 códigos que o CNV **não traz**, mesmo regerado em 2026-07-21 — o DATASUS está
atrás da própria norma que publicou. Ela também renomeia códigos existentes
(`0107` "Raio X Dentário" → "Raio X Odontológico", `0117` "Mamografo computadorizado" →
"Mamógrafo Digital"), e adotamos os nomes da portaria.

**O legado é transposto por regra mecânica:** se o número de 2 dígitos existe no
dicionário atual e o tipo em questão é o dominante daquele número na série, o rótulo é
transposto para o código de 4. Os 17 casos passam no teste de sanidade — tipo 02
(infraestrutura) recebeu ar condicionado, refrigerador, câmaras de conservação e
geradores; tipo 06 (outros) recebeu veículo utilitário, embarcação, empilhadeira.

**Correção de um diagnóstico anterior.** Os códigos `0119` a `0135` chegaram a ser lidos
como erro de digitação da fonte — tipo 01 "diagnóstico por imagem" carregando equipamento
do bloco de infraestrutura. Não são. São códigos novos criados pelo Art. 2º §2º da
Portaria 3.695, que manda reclassificar `05 Raio X de 100 a 500mA` para `20 Raio X
Analógico`, `21 Raio X Digital`, `22 Raio X Telecomandado` e assim por diante. Os 17
entram nos dados em 2026-03, a competência seguinte à implementação. `0121` é "Raio X
Digital", não ar-condicionado.

O PDF oficial está no DOU de 18/05/2026, Edição 91, Seção 1, Página 174. O `WebFetch` não
lê o PDF (devolve binário); `pdftotext -layout` resolve.

### Mudanças no dicionário

Códigos incluídos ou com rótulo trocado, um por linha. "Primeira competência" e "Linhas"
contam a tabela publicada até a competência 2026-10.

Os rótulos com nome oficial seguem a grafia da fonte. Os códigos sem significado nas fontes
oficiais levam o rótulo `Código <chave>: sem significado nas fontes oficiais (nota da BD)` e
estão no pedido via LAI descrito em "Pendências conhecidas do dicionário".

#### `estabelecimento.tipo_unidade` (1)

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `16` | Código 16: sem significado nas fontes oficiais (nota da BD) | 2025-09 | 2.139 |

Fontes consultadas para o `16`, nenhuma com o significado do código:

| Fonte | O que traz |
|---|---|
| `CNV/TP_ESTAB.CNV` do `TAB_CNES.zip`, versão de 15/10/2026 (`ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Auxiliar`) | Lista oficial dos tipos de estabelecimento. Pula do `15` (`UNIDADE MISTA`) para o `20` (`PRONTO SOCORRO GERAL`) |
| [CnesWeb, tipos de estabelecimento](https://cnes2.datasus.gov.br/Mod_Ind_Unidade.asp?VEstado=00) | A mesma lista, com a contagem por tipo, também sem o `16` |
| Ficha do estabelecimento no CnesWeb: `https://cnes2.datasus.gov.br/Exibe_Ficha_Estabelecimento.asp?VCo_Unidade=<6 primeiros dígitos do id_municipio><id_estabelecimento_cnes>` | Em seis estabelecimentos com `16` em 2026-10, os campos "Tipo Estabelecimento" e "Sub Tipo Estabelecimento" estão vazios |
| [Portaria GM/MS nº 2.022/2017](https://bvsms.saude.gov.br/bvs/saudelegis/gm/2017/prt2022_15_08_2017_rep.html) | Cria a nova classificação de tipos de estabelecimento, sem código numérico para os tipos |
| [Wiki do CNES, nova classificação de tipos](https://wiki.saude.gov.br/cnes/index.php/Categoria:Nova_Classifica%C3%A7%C3%A3o_de_Tipos_de_Estabelecimento) | A tabela de códigos só aparece como imagem |
| [Ofício Circular nº 14/2026/CGSI/DRAC/SAES/MS](https://www.conass.org.br/wp-content/uploads/2026/09/9.1-Oficio-Circular-no-14.2026-CGSI-DRAC-SAES-MS-1.pdf) | A partir da competência 10/2026, o campo "Tipo de Estabelecimento" é substituído pela "Classificação de Tipo de Estabelecimento" |
| [Portaria SAES/MS nº 3.200/2025](https://ses.sp.bvs.br/wp-content/uploads/2025/09/U_PT-MS-SAES-3200_020925.pdf) | Exige a classificação "Ambulatório" com estrutura móvel para a habilitação 38.04, sem código numérico |

#### `habilitacao.tipo_habilitacao` (72)

Rótulos da `tb_habilitacao` da Tabela Unificada do SIGTAP, competência 2026-10
(`TabelaUnificada_202610_v2610271043.zip`), com o código sem o zero à esquerda.

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `406` | Serviço de Especialidade em Saúde Bucal | 2024-02 | 4.186 |
| `638` | CENTRO DE CONVIVÊNCIA BÁSICO | 2026-07 | 4 |
| `639` | CENTRO DE CONVIVÊNCIA INTERMEDIÁRIO | 2026-07 | 8 |
| `640` | CENTRO DE CONVIVÊNCIA AMPLIADO | 2026-07 | 20 |
| `815` | Implante Percutâneo de Válvula Aórtica (TAVI) | 2024-04 | 830 |
| `1420` | Hospital de Referência à Gestação e ao Puerpério de Alto Risco -HGPAR | 2025-08 | 3.360 |
| `1515` | Incremento financeiro à Terapia Renal Substitutiva - TRS | 2026-05 | 618 |
| `1618` | Neurocirurgia Endovascular/Trombectomia Mecanica | 2023-12 | 556 |
| `1722` | Tratamentos Integrados Sincrônicos em Oncologia | 2022-08 | 647 |
| `2308` | Serviço de Referência em Tratamento da Pessoa com Falência Intestinal | 2024-11 | 120 |
| `2436` | Incremento Financeiro SNT TMO Nível A | 2023-09 | 516 |
| `2437` | Incremento Financeiro SNT TMO Nível B | 2023-09 | 76 |
| `2438` | Incremento Financeiro SNT TMO Nível C | 2023-09 | 25 |
| `2439` | Incremento Financeiro SNT TMO Nível D | 2023-09 | 190 |
| `2440` | Incremento Financeiro SNT TMO Nível E | 2023-09 | 194 |
| `2441` | Incremento Financeiro SNT Pâncreas Nível A | 2023-09 | 126 |
| `2442` | Incremento Financeiro SNT Pâncreas Nível B | 2023-09 | 123 |
| `2443` | Incremento Financeiro SNT Pâncreas Nível C | 2023-09 | 131 |
| `2446` | Incremento Financeiro SNT Rim Nível A | 2023-09 | 1.624 |
| `2447` | Incremento Financeiro SNT Rim Nível B | 2023-09 | 547 |
| `2448` | Incremento Financeiro SNT Rim Nível C | 2023-09 | 635 |
| `2449` | Incremento Financeiro SNT Rim Nível D | 2023-09 | 154 |
| `2450` | Incremento Financeiro SNT Rim Nível E | 2023-09 | 111 |
| `2451` | Incremento Financeiro SNT Fígado Nível A | 2023-09 | 816 |
| `2452` | Incremento Financeiro SNT Fígado Nível B | 2023-09 | 307 |
| `2453` | Incremento Financeiro SNT Fígado Nível C | 2023-09 | 274 |
| `2454` | Incremento Financeiro SNT Fígado Nível D | 2023-09 | 215 |
| `2455` | Incremento Financeiro SNT Fígado Nível E | 2023-09 | 193 |
| `2456` | Incremento Financeiro SNT Pulmão Nível A | 2023-09 | 96 |
| `2458` | Incremento Financeiro SNT Pulmão Nível C | 2023-09 | 32 |
| `2461` | Incremento Financeiro SNT Coração Nível A | 2023-09 | 526 |
| `2462` | Incremento Financeiro SNT Coração Nível B | 2023-09 | 106 |
| `2463` | Incremento Financeiro SNT Coração Nível C | 2023-09 | 176 |
| `2464` | Incremento Financeiro SNT Coração Nível D | 2026-08 | 3 |
| `2465` | Incremento Financeiro SNT Coração Nível E | 2023-09 | 38 |
| `2466` | Transplante de intestino delgado ou multivisceral | 2025-06 | 96 |
| `2467` | Transplante de Membrana Amniótica | 2025-12 | 64 |
| `2468` | Banco de Multitecidos | 2025-10 | 148 |
| `2469` | Transplante Pediátrico Tipo I | 2025-10 | 307 |
| `2470` | Transplante Pediátrico Tipo II | 2025-10 | 236 |
| `2471` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado com HLA haploidêntico | 2026-01 | 58 |
| `2472` | Transplante de Células Progenitoras Hematopoéticas Autólogo Pediátrico I | 2026-10 | 15 |
| `2473` | Transplante de Células Progenitoras Hematopoéticas Autólogo Pediátrico II | 2026-10 | 9 |
| `2474` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Idêntico Pediátrico I | 2026-10 | 15 |
| `2475` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Idêntico Ped II | 2026-10 | 7 |
| `2476` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Haploidêntico - Ped I | 2026-10 | 3 |
| `2477` | Transplante de Células Progenitoras Hematopoéticas Alogênico Aparentado c/ HLA Haploidêntico Ped II | 2026-10 | 2 |
| `2478` | Transplante de Células Progenitoras Hematopoéticas Não Aparentado Pediátrico I | 2026-10 | 14 |
| `2479` | Transplante de Células Progenitoras Hematopoéticas Não Aparentado Pediátrico II | 2026-10 | 7 |
| `2482` | Transplante de Rim - Pediátrico Tipo I | 2026-10 | 13 |
| `2483` | Transplante de Rim - Pediátrico Tipo II | 2026-10 | 15 |
| `2484` | Transplante de Fígado - Pediátrico Tipo I | 2026-10 | 13 |
| `2485` | Transplante de Fígado - Pediátrico Tipo II | 2026-10 | 9 |
| `2486` | Transplante de Pulmão - Pediátrico Tipo I | 2026-10 | 2 |
| `2487` | Transplante de Pulmão - Pediátrico Tipo II | 2026-10 | 2 |
| `2488` | Transplante de Coração - Pediátrico Tipo I | 2026-10 | 2 |
| `2489` | Transplante de Coração - Pediátrico Tipo II | 2026-10 | 4 |
| `2490` | Transplante de Intestino Delgado ou Multivisceral - Pediátrico Tipo I | 2026-10 | 1 |
| `2902` | Programa Mais Acesso a Especialistas - Componente Cirurgias | 2023-09 | 41.636 |
| `3516` | Terapia Génica | 2025-10 | 103 |
| `3801` | Programa Mais Acesso a Especialistas | 2024-06 | 68.695 |
| `3803` | Agora Tem Especialistas Modalidade 2 Equipes Volantes | 2025-12 | 167 |
| `3804` | Agora Tem Especialistas Modalidade 3 Unidades Móveis | 2025-09 | 1.044 |
| `3805` | Agora Tem Especialistas - Componente Créditos Financeiro | 2025-10 | 1.470 |
| `3806` | Agora Tem Especialistas - Componente Ressarcimento ao SUS | 2025-11 | 60 |
| `3807` | Componente Acesso à Radioterapia do Programa Agora Tem Especialistas | 2025-11 | 2.520 |
| `3808` | Agora Tem Especialistas - Teleatendimento em Saúde Mental | 2026-10 | 1 |
| `3811` | Agora tem Especialistas 3 Unidade Móvel Exame de Imagem | 2026-10 | 56 |
| `3812` | Agora tem Especialistas 3 Unidade Móvel Oftalmologia | 2026-10 | 19 |
| `3813` | Agora tem Especialistas 3 Unidade Móvel Saúde da Mulher | 2026-10 | 66 |
| `3814` | Agora tem Especialistas 3 Unidade Móvel Cardiologia | 2026-10 | 4 |
| `3815` | Agora tem Especialistas 3 Unidade Móvel Saúde Bucal | 2026-10 | 3 |

#### `incentivos.tipo_habilitacao` (38)

Rótulos com o nome do ato que criou cada incentivo. Nenhum deles está no SIGTAP, cuja
`tb_habilitacao` não tem código começando com 8.

| Chave | Rótulo | Primeira competência | Linhas | Ato |
|---|---|---|---|---|
| `8116` | DRC Categoria I | 2023-12 | 10.062 | Portaria GM/MS nº 1.992/2023, Art. 1º |
| `8117` | DRC Categoria II | 2023-12 | 7.015 | Portaria GM/MS nº 1.992/2023, Art. 1º |
| `8119` | Hospital Sem Fins Lucrativos 100% SUS | 2023-08 | 4.541 | Portaria SAES/MS nº 602/2023, Art. 1º |
| `8121` | Qualificação em Doação e Transplantes - IFQ-DOT I | 2026-06 | 49 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8122` | Qualificação em Doação e Transplantes - IFQ-DOT II | 2026-07 | 15 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8123` | Qualificação em Doação e Transplantes - IFQ-DOT III | 2026-06 | 25 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8124` | Qualificação em Doação e Transplantes - IFQ-DOT IV | 2026-06 | 29 | Portaria SAES/MS nº 4.020/2026, Anexo II |
| `8125` | Equipes de Apoio Assistencial em Cuidados Paliativos | 2026-08 | 36 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8126` | Equipes Macrorregionais de Cuidados Paliativos | 2026-08 | 3 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8127` | Equipes Macrorregionais de Cuidados Paliativos com Pediatra | 2026-08 | 3 | Portaria SAES/MS nº 4.288/2026, Anexo III |
| `8256` | UPA 24h Nova - Qualificada Opção I | 2023-10 | 59 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8257` | UPA 24h Nova - Qualificada Opção II | 2018-02 | 147 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8258` | UPA 24h Nova - Qualificada Opção IV | 2019-07 | 538 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8259` | UPA 24h Nova - Qualificada Opção VI | 2018-07 | 413 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8260` | UPA 24h Nova - Qualificada Opção VII | 2018-01 | 624 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8263` | UPA 24h Ampliada - Habilitada/Qualificada Opção IV | 2023-03 | 112 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8264` | UPA 24h Ampliada - Habilitada/Qualificada Opção VI | 2023-11 | 72 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8266` | UPA 24h Nova - Habilitada Opção I | 2017-12 | 791 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8267` | UPA 24h Nova - Habilitada Opção II | 2017-12 | 770 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8268` | UPA 24h Nova - Habilitada Opção IV | 2018-09 | 787 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8269` | UPA 24h Nova - Habilitada Opção VI | 2018-01 | 782 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8270` | UPA 24h Nova - Habilitada Opção VII | 2017-12 | 672 | Portaria SAS/MS nº 1.535/2017, Anexo |
| `8271` | Enfermaria Clínica de Retaguarda - Novos | 2021-06 | 29.416 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8272` | Enfermaria Clínica de Retaguarda - Qualificados | 2021-06 | 26.188 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8273` | UTI Adulto RUE Tipo II - Novos | 2021-06 | 15.018 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8274` | UTI Adulto RUE Tipo II - Qualificados | 2021-06 | 36.920 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8275` | UTI Pediátrico RUE Tipo II - Novos | 2021-06 | 1.963 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8276` | UTI Pediátrico RUE Tipo II - Qualificados | 2021-06 | 9.832 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8277` | UTI Adulto RUE Tipo III - Novos | 2021-06 | 642 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8278` | UTI Adulto RUE Tipo III - Qualificados | 2021-06 | 2.690 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8279` | UTI Pediátrico RUE Tipo III - Novos | 2024-03 | 32 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8280` | UTI Pediátrico RUE Tipo III - Qualificados | 2021-06 | 1.974 | Portaria SAES/MS nº 474/2021, Anexo I |
| `8285` | Atenção especializada ao seguimento do recém-nascido e crianças egressos de unidades neonatais (ANEO) | 2022-11 | 95 | Portaria SAES/MS nº 216/2022, Anexo II |
| `8286` | Transporte Sanitário Adaptado I | 2024-04 | 238 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8287` | Transporte Sanitário Adaptado II | 2024-09 | 105 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8288` | Transporte Sanitário Adaptado III | 2025-09 | 14 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8290` | Núcleo de Atenção à Criança e Adolescente com Transtorno do Espectro Autista | 2025-11 | 91 | Portaria SAES/MS nº 1.148/2023, Anexo IV |
| `8291` | Incentivo de 20% no custeio do CER habilitado com a modalidade de reabilitação intelectual | 2024-09 | 777 | Portaria SAES/MS nº 1.148/2023, Anexo IV |

- O `8285` foi excluído da tabela de incentivos pela Portaria SAES/MS nº 2.902/2025 (Art. 5º
  §2º), mas continua nos dados até 2026-10.
- Os números 81.16 a 81.39 já foram incentivos de equipes de saúde prisional (Portaria SAS/MS
  nº 305/2014, Anexo II), desativados pela Portaria SAES/MS nº 37/2021 (Art. 10). Nos dados,
  o `8116`, o `8117` e o `8119` só aparecem a partir de 2023, e os `8121` a `8127` a partir de
  2026, depois de os números serem reaproveitados. Por isso o rótulo atual vale para todas as
  linhas.

#### `servico_especializado.tipo_servico_especializado` (3)

Os rótulos das três colunas do `servico_especializado` vêm da Tabela Unificada do SIGTAP,
competência 2026-10: da `tb_servico` para o serviço e da `tb_servico_classificacao` para o
subtipo (código do serviço seguido do código da classificação). O `170002` não está no
SIGTAP: lá, o serviço 170 (Comissões e Comitês) só tem a classificação 001 (Núcleo de
Segurança do Paciente).

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `171` | Cuidados Paliativos | 2024-10 | 6.939 |
| `172` | Atenção ao Luto Materno e Parental | 2026-03 | 321 |
| `175` | Cirúrgia Robótica | 2026-08 | 24 |

#### `servico_especializado.tipo_servico_especializado_unico` (3)

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `171` | Cuidados Paliativos | 2024-10 | 3.708 |
| `172` | Atenção ao Luto Materno e Parental | 2026-03 | 321 |
| `175` | Cirúrgia Robótica | 2026-08 | 24 |

#### `servico_especializado.subtipo_servico_especializado` (36)

| Chave | Rótulo | Primeira competência | Linhas |
|---|---|---|---|
| `112007` | Ambulatório de Seguimento do RN e Criança Egresso de Unidade Neonatal | 2022-08 | 911 |
| `112008` | Ambulatório  de Gestação e Puerpério de Alto Risco | 2022-08 | 1.934 |
| `114008` | Odontopediatria | 2024-10 | 4.447 |
| `114009` | Disfunsão temporomandibular | 2024-10 | 459 |
| `114010` | Estomatologia | 2024-10 | 1.529 |
| `114011` | Implantodondia | 2024-10 | 4.249 |
| `114012` | Odontogeriatria | 2024-10 | 363 |
| `114013` | Ortodontia | 2024-10 | 4.826 |
| `114014` | Prótese Dentária | 2024-10 | 3.804 |
| `114015` | Radiologia Odontológica e Imaginologia | 2024-10 | 1.112 |
| `114016` | Acupuntura na Odontologia | 2024-10 | 1.018 |
| `114017` | Ortopedia funcional dos maxilares | 2024-10 | 673 |
| `114018` | Odontologia hospitalar | 2024-10 | 800 |
| `114019` | Patologia Oral e Máxilo Fácial | 2025-08 | 48 |
| `114020` | Prótese Buco Maxilo Facial | 2025-08 | 48 |
| `115010` | DESINST de Pessoas com Transtorno  Mental em Conflito com a Lei | 2024-10 | 811 |
| `115011` | Promoção da Saúde Mental, Protagonismo e Cidadania | 2026-02 | 2.033 |
| `125010` | Manipulação, Reconstituição e Diluição de Medicamentos | 2026-10 | 4 |
| `141005` | Vigilância de Zoonoses | 2023-06 | 16.407 |
| `154003` | Banco de Multitecidos | 2025-10 | 38 |
| `160004` | TELEDIAGNOSTICO | 2023-12 | 94.148 |
| `160005` | TELE-EDUCACAO | 2023-12 | 76.478 |
| `160006` | TELECONSULTA | 2023-12 | 440.453 |
| `160007` | TELEMONITORAMENTO | 2023-12 | 87.083 |
| `160008` | TELEORIENTACAO | 2023-12 | 100.296 |
| `160009` | TELEINTERCONSULTA | 2023-12 | 93.297 |
| `160010` | TELETRIAGEM | 2023-12 | 75.042 |
| `160011` | TELERREGULACAO | 2023-12 | 54.190 |
| `164012` | Confecção de OPM Auxiliares de Locomoção e Ortopédicas | 2024-11 | 691 |
| `168003` | Terapia Génica | 2025-11 | 47 |
| `170002` | Código 170002: sem significado nas fontes oficiais (nota da BD) | 2024-06 | 54.931 |
| `171001` | Abordagem Paliativa | 2024-10 | 3.637 |
| `171002` | Apoio Matricial em Cuidados Paliativos | 2024-10 | 2.280 |
| `171003` | Cuidados Paliativos Especializados | 2026-08 | 1.022 |
| `172001` | Assistência Multiprofissional ao luto materno e parental | 2026-03 | 321 |
| `175001` | Prostatectomia | 2026-08 | 24 |

#### Fontes dos rótulos

| Fonte | Códigos |
|---|---|
| Tabela Unificada do SIGTAP, competência 2026-10, em `ftp2.datasus.gov.br/pub/sistemas/tup/downloads/` | `habilitacao` e `servico_especializado` |
| [Portaria GM/MS nº 1.992, de 24/11/2023](https://bvsms.saude.gov.br/bvs/saudelegis/gm/2023/prt1992_28_11_2023.html) | `8116`, `8117` |
| [Portaria SAES/MS nº 602, de 28/07/2023](https://bvsms.saude.gov.br/bvs/saudelegis/Saes/2023/prt0602_02_08_2023.html) | `8119` |
| Portaria SAES/MS nº 4.020, de 10/04/2026, DOU de 12/05/2026, edição 87, seção 1, p. 161 ([Conass Informa 84/2026](https://www.conass.org.br/conass-informa-n-84-2026-publicada-a-portaria-saes-n-4-020-que-estabelece-regras-para-registro-das-equipes-hospitalares-de-doacao-para-transplantes-no-cadastro-nacional-de-estabelecimentos/)) | `8121` a `8124` |
| Portaria SAES/MS nº 4.288, de 22/06/2026, DOU de 23/07/2026, edição 137, seção 1, p. 339 ([Conass Informa 140/2026](https://www.conass.org.br/conass-informa-n-140-2026-publicada-a-portaria-saes-n-4-288-que-atualiza-o-cadastro-nacional-de-estabelecimentos-de-saude-cnes-e-a-tabela-de-procedimentos-medicamentos-orteses-pr/)) | `8125` a `8127` |
| [Portaria SAS/MS nº 1.535, de 25/09/2017](https://bvsms.saude.gov.br/bvs/saudelegis/sas/2017/prt1535_09_10_2017.html) | `8256` a `8260`, `8263`, `8264`, `8266` a `8270` |
| [Portaria SAES/MS nº 474, de 22/04/2021](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2021/prt0474_27_04_2021.html) | `8271` a `8280` |
| [Portaria SAES/MS nº 216, de 01/07/2022](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2022/prt0216_01_07_2022.html) | `8285` |
| [Portaria SAES/MS nº 2.902, de 26/06/2025](https://bvsms.saude.gov.br/bvs/saudelegis/Saes/2025/prt2902_02_07_2025.html) | exclusão do `8285` |
| [Portaria SAES/MS nº 1.148, de 21/12/2023](https://bvsms.saude.gov.br/bvs/saudelegis/saes/2023/prt1148_22_12_2023.html) | `8286` a `8288`, `8290`, `8291` |
| [Portaria SAS/MS nº 305, de 10/04/2014, Anexo II](https://bvsms.saude.gov.br/bvs/saudelegis/sas/2014/anexo/anexo_prt0305_10_04_2014.pdf) e [Portaria SAES/MS nº 37, de 18/01/2021](https://www.in.gov.br/en/web/dou/-/portaria-n-37-de-18-de-janeiro-de-2021-299987615) | significado anterior de 81.16 a 81.39 |

Consultados sem os códigos desta seção: a `tb_habilitacao` e a `tb_grupo_habilitacao` do
SIGTAP de 2023-01, 2023-12, 2024-06, 2025-06 e 2026-10; o `TAB_CNES.zip` de 15/10/2026, que
não tem CNV de habilitação, serviço ou incentivo; as Portarias SAS/MS nº 461/2014, SAES/MS
nº 1.187/2023, 2.151/2024, 2.747/2025, 3.033/2025, 3.511/2025, 3.695/2026 e 4.399/2026 e
GM/MS nº 11.179/2026.

### Pendências conhecidas do dicionário

- **Pedido via LAI ao Ministério da Saúde**, protocolo 25072.076527/2026-30
  ([#2367](https://github.com/basedosdados/pipelines/issues/2367)), enviado em
  23/11/2026 pelo Fala.BR (prazo de 20 dias, prorrogável por mais 10). Pergunta o
  significado do `estabelecimento.tipo_unidade` `16` e do
  `servico_especializado.subtipo_servico_especializado` `170002`, os dois com o rótulo
  `Código <chave>: sem significado nas fontes oficiais (nota da BD)`. O pedido também
  perguntava pelos 38 incentivos de `incentivos.tipo_habilitacao`, que depois foram
  achados nas portarias listadas em "Mudanças no dicionário". Quando a resposta chegar,
  trocar os dois rótulos pelo significado informado.
- **66 códigos de 4 dígitos seguem sem rótulo**, 1.681 linhas em 2026-06. São os tipos
  11, 14, 15 e 16 (37 códigos, 340 linhas), que dependem da Portaria 4.109 — texto oficial
  nunca localizado, e ausente também do CNV —, mais 29 códigos antigos cujo tipo dominante
  não bate (`0351`, `0912`, `0265`, uma série de `077x`), de 20 a 480 linhas cada.
  Enquanto esses 66 não fecharem, o `custom_dictionary_coverage` **não pode** apontar para
  `codigo_equipamento`: o teste é `severity: error` e derrubaria o flow todo mês. Se
  alguém localizar a 4.109 e fechar a lista, aí a troca vale.
- **O texto oficial do código 80 continua por conferir** — ver a seção dos rótulos
  recuperados. O restante dos tapa-buracos de `tipo_equipe` 77–81 foi resolvido.
- **`cobertura_temporal` é `(1)` em todas as linhas**, em vez da notação
  `INICIO(1)FIM` do manual de estilo.
- **Encoding quebrado** em `id_equipamento` chave 97:
  `sistema completo de reforafaEUR!o visual(vra)`. O equivalente em
  `codigo_equipamento` (`0897`) veio do CNV e está correto, então quem usar a coluna nova
  não vê o problema — a entrada velha segue torta, como legado.
- **Significados repetidos** em `tipo_equipe`: as chaves 1 e 70 são ambas
  `esf - equipe de saude da familia`; 49 e 76 são ambas `eap`.

---

## Tipos das colunas do equipamento (issue #1722, 2026-07-30)

Quatro colunas contrariavam a convenção de tipos da BD, e as duas primeiras reprovavam o
`check-metadata` porque o BigQuery e a API discordavam entre si:

| Coluna | Era | Virou | Por quê |
|---|---|---|---|
| `quantidade_equipamentos` | STRING | **INT64** | contagem; somar e tirar média faz sentido |
| `quantidade_equipamentos_ativos` | STRING | **INT64** | idem |
| `indicador_equipamento_disponivel_sus` | INT64 | **STRING** + dicionário | booleano 0/1, não quantidade |
| `indicador_equipamento_indisponivel_sus` | INT64 | **STRING** + dicionário | idem |

A `equipamento` era a única tabela do conjunto que convertia contagem para texto — `leito` e
`dados_complementares` já convertiam o mesmo campo `qt_exist` para `int64`.

Medido na série inteira do staging de prod (163.469.332 linhas), não só na competência
corrente:

- **zero** valores não numéricos em `QT_EXIST` e `QT_USO` — o `safe_cast` para INT64 não
  perde nada em nenhuma competência (confirmado depois na tabela: zero
  `quantidade_equipamentos` nula em 164,5M linhas)
- `IND_SUS` e `IND_NSUS` só assumem `'0'` e `'1'`, e são **exatamente complementares**
  (93.878.391 contra 69.590.941, invertidos) — uma é a negação da outra. Redundância da
  fonte; ficou registrado, sem ação. O `Equipamento.def` do `TAB_CNES.zip` documenta
  "somente 1=SIM ou 0=NÃO"

**`measurement_unit` ficou em branco, de propósito.** O backend tem 64 unidades e nenhuma
de contagem — não existe `unit`, `item` nem `count`, e o mais próximo é `person`. `leito` e
`dados_complementares` também deixam as quantidades deles em branco. A regra da convenção
("toda coluna numérica carrega unidade") não tem como ser cumprida aqui até o vocabulário
ganhar uma unidade de contagem.

**O `custom_dictionary_coverage` passou a cobrir os dois indicadores** — e isso só é
possível porque eles viraram STRING: o teste faz `model.<coluna> = dicionario.chave`, e
`chave` é STRING, então com INT64 a query nem compila (`No matching signature for operator
= for argument types: INT64, STRING`). O domínio é fechado em 0/1, então o alarme dispara de
verdade se a fonte inventar um sentinela. `codigo_equipamento` continua fora, pelos 66
códigos sem rótulo.

### Troca de tipo exige full-refresh — não é opcional

`on_schema_change` não trata tipo, em nenhum dos valores. Num modelo incremental o run
seguinte simplesmente falha, medido direto no BigQuery:

```text
Query column 1 has type INT64 which cannot be inserted into
column quantidade_equipamentos, which has type STRING
```

Duas consequências práticas no merge:

1. **O `table-approve` vai falhar no `dbt run` da `equipamento`** — ele roda incremental,
   sem a flag. Não é regressão; é isso. A correção é disparar o `run_dbt_model_flow` em prod
   com `flags="--full-refresh"` logo depois.
2. **O full-refresh derruba o paywall.** O `pre_hook` do modelo faz
   `DROP ALL ROW ACCESS POLICIES`, e o `run_dbt_model_flow` não as reaplica — só
   `register_table_materialization_task` reaplica, num run do flow com
   `update_metadata=True`. A tabela é `PartBdpro`, então esse run tem que vir junto, não
   depois. Atenção que o deployment do `equipamento` estava **pausado** em 2026-07-28.

### O `bigquery_type` na API: nenhuma ferramenta do MCP grava em coluna existente

`update_column` não tem o parâmetro; `bulk_upsert_columns` monta `bigqueryType` **só no
ramo de criação** (`if not is_update:`); `upload_columns` responde 500. O
`upload_columns_from_sheet` funciona, mas reescreve **todas** as colunas da tabela a partir
da planilha, o que arrisca descrições EN/ES e vínculos de observation level.

O caminho usado foi a **mutação `CreateUpdateColumn` direta**, que é patch: enviando só
`id`, `name`, `table`, `bigqueryType` e `coveredByDictionary`, o resto fica intacto. A
tabela foi resolvida pela cloud table (`gcpProjectId` + `gcpDatasetId` + `gcpTableId`), com
asserção de resultado único — a duplicata de cloud table foi o que quebrou todos os flows do
`br_rf_cno`. Consertar a ferramenta é issue no repo `mcp`, não trabalho de PR de dados.

### Como conferir

O `check_metadata.py` da CI compara **BigQuery dev × API de prod** e **não roda local**: ele
fatura no projeto `basedosdados`, e o `config.toml` local é só de dev (a versão do Python
também é 3.12, contra 3.10 do venv do projeto). Para validar antes do push, reproduza a
comparação faturando em dev — mesmas duas fontes, mesmas regras de normalização de tipo.

Vale lembrar a ordem, porque a label `test-dev-model` cobra o estado de dev a cada push: o
dbt test só passa depois de o dicionário ter as chaves **e** a tabela de dev ter sido
reconstruída, e o `check-metadata` só fecha depois da mutação na API. Push no meio do
caminho volta vermelho.

---

## Particionamento

Linhas com `ano` fora do range de partição caem em `__UNPARTITIONED__`, e filtrar por `ano`
não poda nada nessa partição. No particionamento por inteiro do BigQuery o range é
`[start, end)` — o fim é **exclusivo**, e `"end": 2024` já deixa 2024 de fora. A convenção
da BD é `end = último ano + 5`, o que dá 2031 para dados até 2026.

Range declarado nos modelos:

| Modelo | `start` | `end` |
|---|---|---|
| `dados_complementares`, `equipamento`, `equipe`, `estabelecimento`, `estabelecimento_filantropico`, `gestao_metas`, `habilitacao`, `incentivos`, `leito`, `profissional`, `servico_especializado` | 2005 | 2031 |
| `estabelecimento_ensino`, `regra_contratual` | 2005 | 2024 |

`estabelecimento_ensino` e `regra_contratual` ficam em 2024. Nas duas:

- nenhuma linha está em `__UNPARTITIONED__`;
- não entra dado novo: o DATASUS descontinuou o grupo EE em 2019-12, e a `regra_contratual`
  está desativada, com o crawler quebrado (#1703, fechada como `NOT_PLANNED`);
- um full-refresh derruba as row access policies, e o flow não chega a reaplicá-las: ele
  encerra antes do `register_table_materialization_task`, no EE porque o FTP não devolve
  arquivos, na `regra_contratual` porque o crawler falha.

**Alterar o config não reparticiona.** O particionamento é fixado quando a tabela é criada,
e só um `--full-refresh` aplica o range novo. O dbt não acusa a divergência: um `dbt run`
incremental com config novo sobre tabela velha passa normalmente. Tabela de prod que não
passou por full-refresh depois da troca do config segue com o range antigo — a consulta de
conferência abaixo mostra o range de cada uma.

### Reconstrução de uma tabela em prod

Todas as tabelas do conjunto são `PartBdpro` (fixo no `_run_cnes`), e o `pre_hook` de todo
modelo faz `DROP ALL ROW ACCESS POLICIES`. O `run_dbt_model_flow`, que aceita
`--full-refresh`, não recria as políticas; quem recria é o
`register_table_materialization_task`, dentro de um run do flow da tabela. Por isso a
reconstrução usa dois runs seguidos, e entre eles a janela BD Pro fica aberta ao público.

Rode fora da janela dos crons (6h30–12h30, ver "Agendamento") e reconstrua `estabelecimento`
antes de `leito` e `profissional` — os dois modelos fazem join com
`basedosdados.br_ms_cnes.estabelecimento`. Antes do passo 1, rode a consulta de conferência
e anote as `linhas` da tabela.

1. Dispare o deployment `run_dbt_model_flow` ("BD template: Executa DBT model") com os
   parâmetros abaixo. Ele clona a `main`, então o config novo precisa estar mergeado.

   ```text
   dataset_id="br_ms_cnes"
   table_id="<tabela>"
   dbt_command="run"
   flags="--full-refresh"
   target="prod"
   dbt_alias=true
   download_csv_file=false
   ```

2. Logo em seguida, rode o flow `br_ms_cnes__<tabela>` com `force_run=true` e
   `year_month_to_extract="<AAMM da última competência da tabela>"` (`"2606"` para
   2026-06). É esse run que reaplica as row access policies. O incremental não insere nada,
   porque o filtro do modelo só pega competência maior que a máxima da tabela. No log,
   confira `BDpro filter was included` e `All users filter was included`.

3. Confira o resultado pela consulta abaixo, não pelo estado dos runs: `Completed` não mostra
   se a tabela foi reparticionada. A tabela reconstruída tem `range_atual` com fim 2031, zero
   em `fora_do_range` e `linhas` igual ao valor anotado antes do passo 1. O total pode cair
   um pouco, porque o modelo faz `select distinct *` sobre o staging e remove duplicatas
   exatas.

```sql
select t.table_name,
       regexp_extract(t.ddl, r'GENERATE_ARRAY\([^)]*\)') as range_atual,
       sum(p.total_rows) as linhas,
       sum(if(p.partition_id = '__UNPARTITIONED__', p.total_rows, 0)) as fora_do_range
from `basedosdados.br_ms_cnes.INFORMATION_SCHEMA.TABLES` t
join `basedosdados.br_ms_cnes.INFORMATION_SCHEMA.PARTITIONS` p using (table_name)
group by 1, 2 order by 1
```

**Não use o rótulo `table-approve`** numa PR que só muda o config desses modelos. Ele roda
`dbt run` incremental em prod nos modelos alterados: o particionamento fica como está, o
`pre_hook` derruba as políticas e nada as reaplica.

---

## Tabelas e Particularidades

Estado do dev run de 2026-07-27 (`force_run=True`, `target=dev`), que validou o
poll novo. Logs em `task_davi/acompanhamento_de_pipelines/br_ms_cnes/`.

### Passaram ponta-a-ponta (`dbt run OK` + `dbt test OK`)

`dados_complementares`, `estabelecimento`, `estabelecimento_filantropico`,
`gestao_metas`, `habilitacao`, `incentivos`, `servico_especializado`,
`profissional` — oito tabelas.

### br_ms_cnes__equipamento e br_ms_cnes__equipe

Falharam no `dbt test` por causa dos códigos novos do dicionário (issue #1714).
Corrigido em dev em 2026-07-28; prod depende do `table-approve` do PR.

O `equipamento` ganhou ainda a coluna `codigo_equipamento` e foi reconstruído em 2026-07-29,
e de novo em 2026-07-30 sobre as 251 competências, com os tipos da #1722 — 164.565.160
linhas, 8 testes passando. O `custom_dictionary_coverage` dele cobre `tipo_equipamento` e os
**dois indicadores**: `id_equipamento` saiu por ser alarme morto e `codigo_equipamento` não
entrou porque 66 códigos não têm rótulo. Ver "O código de 4 dígitos" e "Tipos das colunas do
equipamento" acima.

### br_ms_cnes__leito

Falhou no `dbt test` (`not_null_proportion id_municipio`), mas é **falso-positivo
de ambiente**. O `id_municipio` do leito vem de um join com
`basedosdados.br_ms_cnes.estabelecimento` — tabela de **prod**, que estava parada
em maio. Junho junta contra um estabelecimento sem junho e fica 100% nulo; maio
tem 0% de nulo. Em prod resolve sozinho, porque `estabelecimento` roda às 09h e
`leito` às 10h. **Sem ação** — mas a ordem importa se for disparar à mão.

### br_ms_cnes__profissional

Passou nos testes, mas o poll logou `Fonte sem cobertura nova` — só chegou ao dbt
porque o `force_run` empurrou. Não tem o mesmo carimbo das outras.

### br_ms_cnes__servico_especializado

Passou (`dbt run OK` + `dbt test OK`), mas é a **única** das oito cujo log não tem
nenhuma linha de cobertura: falta a mensagem do `register_source_coverage_task` e
falta o `Finished in state Completed()` correspondente, comparando com um run
normal como o de `incentivos`. Conferido também direto na API do Prefect — não é
artefato do export.

Isso não deveria acontecer: a task é chamada incondicionalmente em `_run_cnes`, e
`register_source_coverage` sempre loga uma das duas mensagens; o único caminho
silencioso é `source_max_date is None`, que só ocorre com lista de FTP vazia — e a
lista não estava vazia (os `SR*2606.dbc` foram baixados). **Pendência aberta do #1707**:
se a task não roda para essa tabela, o `RawDataSource.Update` dela não avança e ela não
dispara sozinha nem depois do fix.

### br_ms_cnes__estabelecimento_ensino

Parada em 2019-12 porque o DATASUS **descontinuou o grupo EE**. O flow existe sem
cron e encerra com `force_run=True mas FTP não retornou arquivos — encerrando`.
Comportamento correto, sem ação.

### br_ms_cnes__regra_contratual

Desativada pela BD. Falha na leitura do CSV no crawler:

```text
ParserError: Error tokenizing data. C error: Expected 31 fields in line 213, saw 32
```

Mesmo erro desde pelo menos 2026-05. A issue #1703 foi fechada como `NOT_PLANNED`
em 2026-07-23. Fora de escopo.

---

## Agendamento

Todos os deployments ficam no pool `basedosdados`. Crons em `America/Sao_Paulo`,
declarados em `flows.py`:

| Tabela | Cron |
|---|---|
| profissional | `30 6 * * *` |
| estabelecimento | `0 9 * * *` |
| equipe | `30 9 * * *` |
| leito | `0 10 * * *` |
| equipamento | `30 10 * * *` |
| dados_complementares | `0 11 * * *` |
| estabelecimento_filantropico | `15 11 * * *` |
| gestao_metas | `30 11 * * *` |
| habilitacao | `45 11 * * *` |
| incentivos | `50 11 * * *` |
| servico_especializado | `30 12 * * *` |
| estabelecimento_ensino | sem cron |
| regra_contratual | sem cron |

A ordem não é decorativa: `estabelecimento` precisa rodar antes de `leito`, pelo
join descrito acima.

**Merge não arma nada.** O deploy em prod cai `paused=True`, e o sync do backend
registra um deployment desconhecido com `is_schedule_active=False`. Armar é um
tique manual em
`https://backend.basedosdados.org/admin/admin_data_tools/disabledflowschedule/`.
Em 2026-07-28 estavam pausados: `dados_complementares`, `equipamento`, `equipe`,
`leito` e `regra_contratual`.
