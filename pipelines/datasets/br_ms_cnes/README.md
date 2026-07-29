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

### Estado em 2026-07-28

**Dev está fechado; prod ainda não.** As duas correções da issue #1714 foram
aplicadas no staging dev — as 7 chaves novas (`tipo_equipamento` 11–16 e
`tipo_equipe` 82) e os 5 rótulos recuperados (`tipo_equipe` 77–81). O dicionário
passou de 1.591 para 1.598 linhas; `dbt run` OK e `dbt test` de `equipamento` e
`equipe` passando (16/16 e 8/8).

Prod segue com as 1.591 linhas originais e os tapa-buracos até o `table-approve`
rodar no merge do PR — ver "Como isso chega em prod" abaixo.

Em 2026-07-29 a mesma issue ganhou um segundo escopo: o `id_equipamento` não identifica
um equipamento sozinho, e o dicionário serve rótulo errado em 16.540 linhas por mês. Ver
"O código de 4 dígitos" abaixo. A correção entra na mesma PR e ainda não foi implementada.

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

Use `models/br_ms_cnes/code/update_dicionario.py`. Ele lê o staging, acrescenta as
linhas declaradas em `NEW_ROWS` (ignorando as que já existem, pela chave
`id_tabela + nome_coluna + chave`) e regrava o CSV.

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

Essa recuperação é feita pelo `REPLACE_ROWS` do `update_dicionario.py`, que reescreve
o valor de linhas existentes (o `NEW_ROWS` só acrescenta chave nova). Os dois
caminhos são idempotentes.

### Convenção de rótulo

O arquivo é inconsistente por acúmulo histórico: as chaves 1–8 de
`tipo_equipamento` são minúsculas e sem acento; as 9 e 10 são Title Case; os
rótulos de `tipo_equipe` seguem o padrão `sigla - descrição` em minúscula. As
adições mais recentes usam Title Case com acento, e foi esse o padrão adotado
para os códigos novos. O legado não foi mexido.

### O código de 4 dígitos (2026-07-29)

O CNES identifica um equipamento por um código de 4 dígitos: os 2 primeiros são o tipo,
os 2 últimos são o equipamento **dentro daquele tipo**. A numeração do equipamento
recomeça a cada tipo — existe um equipamento 01 na radiologia, outro na telessaúde,
outro na diálise. Só os 4 dígitos juntos identificam.

A fonte entrega as duas metades separadas (`TIPEQUIP` e `CODEQUIP`) e o modelo as guarda
em `tipo_equipamento` e `id_equipamento`, descartando a junção. Como o dicionário é
consultado só pelo `id_equipamento`, toda linha de qualquer tipo com equipamento 1 recebe
"gama camara": **16.540 linhas por mês com rótulo errado e plausível** em 2026-06 (tipos
9, 10 e 11–16). Os tipos 1 a 8 escapam porque o DATASUS embutia o tipo no primeiro dígito
do equipamento — tipo 2 usa 21–23, tipo 3 usa 31–50 —, o que fazia o número de 2 dígitos
bastar. Quebrou no tipo 9, cuja faixa já estava ocupada pelo tipo 8.

O `custom_dictionary_coverage` não pega: ele valida `id_equipamento` e `tipo_equipamento`
separadamente, e a combinação dos dois não é conferida por ninguém.

**Decisão:** criar a coluna `codigo_equipamento` com os 4 dígitos e passar a consultar o
dicionário por ela. `id_equipamento` e `tipo_equipamento` permanecem, para não quebrar
quem já consome a tabela, e `id_equipamento` sai do `custom_dictionary_coverage`. Plano
completo em `task_davi/br_ms_cnes/roadmap_codigo_equipamento.md`.

Duas armadilhas medidas na série:

- **`concat` cru não serve.** O staging trocou de formato em 2025: antes `tipequip` vinha
  `'7'`, hoje vem `'07'`, e 94% da série está no formato antigo. Precisa de `lpad` nas
  duas metades. `codequip` tem 2 caracteres em toda a série e nunca é `'00'`.
- **O `table-approve` não faz full-refresh.** `prefect_run_dbt.py` não aceita a flag e o
  projeto não define `on_schema_change`, então vale o padrão do dbt, `ignore`. Num modelo
  incremental a coluna nova não aparece em prod — o `dbt run` roda, fica verde, e não
  adiciona nada. Mesmo caso do `br_me_cnpj`.

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
- **O texto oficial do código 80 continua por conferir** — ver a seção dos rótulos
  recuperados. O restante dos tapa-buracos de `tipo_equipe` 77–81 foi resolvido.
- **`cobertura_temporal` é `(1)` em todas as linhas**, em vez da notação
  `INICIO(1)FIM` do manual de estilo.
- **Encoding quebrado** em `id_equipamento` chave 97:
  `sistema completo de reforafaEUR!o visual(vra)`.
- **Significados repetidos** em `tipo_equipe`: as chaves 1 e 70 são ambas
  `esf - equipe de saude da familia`; 49 e 76 são ambas `eap`.

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

Note que o teste de `equipamento` valida `id_equipamento` e `tipo_equipamento`
separadamente — ele passa mesmo com a combinação errada. Ver "O código de 4 dígitos"
acima.

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
