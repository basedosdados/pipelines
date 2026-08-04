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
| `pipelines/crawler/datasus/flows.py` | `_run_cnes` — a receita de fato (poll → download → clean → upload → dbt) |
| `pipelines/crawler/datasus/tasks.py` | tasks de FTP, descompressão `.dbc`, limpeza |
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

### Pendências conhecidas do dicionário

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

## Particionamento (achado de 2026-07-29)

**Os modelos do conjunto estão com o fim do range de partição curto demais.** A maioria
declara `"end": 2024` e o `profissional` declara `"end": 2026`, com dados indo até 2026.

O fim do range é **exclusivo** no particionamento por inteiro do BigQuery: linhas com
valor fora de `[start, end)` vão para a partição `__UNPARTITIONED__`. Com `end: 2024`,
tudo de 2024 em diante — três anos de dados — está num único bucket, e filtrar por `ano`
não poda nada nesse intervalo. A convenção da BD é `end = último ano + 5`, então o valor
certo hoje é **2031**.

**Alterar o config não reparticiona.** O particionamento é definido na criação da tabela,
então a correção só vale depois de `--full-refresh`. O `dbt` não acusa a divergência entre
o config novo e o particionamento antigo enquanto isso não acontece —
`dbt run --select br_ms_cnes__incentivos` passou normalmente (`MERGE`) nessa situação.

**Só o `equipamento` foi corrigido aqui** (`end: 2031`). O rebuild serviu a três coisas de
uma vez: reparticionar a tabela, preencher `codigo_equipamento` em toda a série e aplicar os
tipos novos da #1722. Em dev está feito (zero linhas em `__UNPARTITIONED__`); em prod chega
com o `--full-refresh` manual descrito em "Tipos das colunas do equipamento", que desde a
transferência de 2026-07-30 não perde mais histórico.

As outras 11 tabelas, e a reconstrução de todas, ficaram em issue própria: são 248 GB e
1,17 bilhão de linhas, com `estabelecimento` e `profissional` somando 222 GB, o que não
cabia numa PR sobre o dicionário.

`regra_contratual` está fora de qualquer reprocessamento: a tabela está desativada, o
crawler falha na leitura do CSV desde pelo menos 2026-05 e a #1703 foi fechada como
`NOT_PLANNED`.

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
