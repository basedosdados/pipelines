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
- Tabelas auxiliares: `TAB_CNES.zip`, em `dissemin/publicos/CNES/200508_/Auxiliar` (ver a
  seção do dicionário)

O nome do arquivo carrega a competência em `YYMM` nas posições 4–8 do basename:
`STAC2606.dbc` é o grupo ST, UF AC, competência 2026-06.

---

## O dicionário

### Origem

O dicionário deste conjunto não é gerado por nada: não há crawler nem fonte registrada no
backend, e é a única tabela do conjunto sem flow. É um CSV único, mantido à mão, em
`gs://basedosdados-dev/staging/br_ms_cnes/dicionario/dicionario.csv`.

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

O script escreve só o bucket de dev. Quem leva o dicionário para prod é a action
`table-approve`, que copia `gs://basedosdados-dev/staging/<ds>/<tabela>` para
`gs://basedosdados/staging/<ds>/<tabela>` (com backup em `basedosdados-backup`), recria a
staging e roda o dbt em prod. Para isso:

1. a PR precisa carregar o rótulo `table-approve`;
2. a PR precisa alterar `models/br_ms_cnes/br_ms_cnes__dicionario.sql`, porque a action monta
   o par (conjunto, tabela) a partir dos `.sql` modificados.

A cópia apaga o destino antes. Confira que o staging de prod não tem nada que o de dev não
tenha.

### `tipo_equipamento` 11–16 e `tipo_equipe` 82

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
4.109/2026 e o CNES 4.8.40; o texto oficial da 4.109 não foi localizado. O `TP_EQUIPAM.dbf`
(10 tipos) e o `EQUIPE.dbf` (até o 76) do `TAB_CNES.zip` não trazem esses códigos.

### `tipo_equipe` 77–81

| Chave | Sigla | Nome | Norma |
|---|---|---|---|
| 77 | EMAP-R | Equipe Multiprofissional de Apoio para Reabilitação | Portaria SAES/MS nº 1.619/2024, Art. 3º §4º |
| 78 | EACP | Equipe Assistencial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II |
| 79 | EMCP | Equipe Matricial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II |
| 80 | EAP-DESINST | Equipe de Avaliação e Acompanhamento de Medidas Terapêuticas (transtorno mental em conflito com a lei) | Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº 2.070/2024 |
| 81 | EqAE | Equipe de Atenção Especializada | Portaria SAES/MS nº 3.200/2025, Art. 5º |

O texto oficial foi lido para 77 e 81. Para 78 e 79, a fonte é o Conass Informa 156/2024,
que resume o Anexo II. Para 80, nenhuma das duas portarias foi lida.

O nome que o próprio estabelecimento dá à equipe (coluna `equipe`, `nome_eqp` na fonte) bate
com a norma nos cinco casos:

```text
77 → EMAP-R · MELHOR EM CASA · PMEC · EQUIPE DE APOIO A REALIBITACAO
78 → EACP · CUIDADOS PALIATIVOS · EQUIPE DE CUIDADOS PALIATIVOS
79 → EMCP - CUIDADOS PALIATIVOS · EQUIPE MATRICIAL DE CP
80 → EAP-DESINST ESTADUAL SAO LUIS · EAP - DESINST · SISTEMA PRISIONAL I
81 → AGORA TEM ESPECIALISTAS · JORNADA OFTALMOLOGICA · EQUIPE PRONAS-PCD
```

O rótulo do 80 é abreviado: o nome completo (`Equipe de Avaliação e Acompanhamento de
Medidas Terapêuticas Aplicáveis à Pessoa com Transtorno Mental em Conflito com a Lei`) tem
137 caracteres.

### `tipo_unidade` 16 do `estabelecimento`

A chave `16` de `tipo_unidade` aparece nos dados a partir de 2025-09, e nenhuma fonte do
CNES diz o que ela significa. O dicionário a registra com o valor `Significado não
encontrado nas fontes relacionadas ao CNES`, que não vem da fonte.

| Fonte consultada | O que traz |
|---|---|
| `CNV/TP_ESTAB.CNV` do `TAB_CNES.zip`, versão de 15/10/2026 (`ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Auxiliar`) | Lista oficial dos tipos de estabelecimento. Pula do `15` (`UNIDADE MISTA`) para o `20` (`PRONTO SOCORRO GERAL`). O código é o número no fim da linha; o do início é só a posição |
| [CnesWeb, tipos de estabelecimento](https://cnes2.datasus.gov.br/Mod_Ind_Unidade.asp?VEstado=00) | A mesma lista, com a contagem por tipo, também sem o `16` |
| Ficha do estabelecimento no CnesWeb: `https://cnes2.datasus.gov.br/Exibe_Ficha_Estabelecimento.asp?VCo_Unidade=<6 primeiros dígitos do id_municipio><id_estabelecimento_cnes>` | Em seis estabelecimentos com `16` em 2026-10, os campos "Tipo Estabelecimento" e "Sub Tipo Estabelecimento" estão vazios. Na ficha de um estabelecimento de outro tipo (`3301700777994`, oficina ortopédica), o mesmo campo traz o nome do tipo |
| [Portaria GM/MS nº 2.022/2017](https://bvsms.saude.gov.br/bvs/saudelegis/gm/2017/prt2022_15_08_2017_rep.html) | Cria a nova classificação de tipos de estabelecimento, sem código numérico para os tipos |
| [Wiki do CNES, nova classificação de tipos](https://wiki.saude.gov.br/cnes/index.php/Categoria:Nova_Classifica%C3%A7%C3%A3o_de_Tipos_de_Estabelecimento) | O tipo é atribuído a partir das atividades informadas no cadastro. A tabela de códigos só aparece como imagem |
| [Ofício Circular nº 14/2026/CGSI/DRAC/SAES/MS](https://www.conass.org.br/wp-content/uploads/2026/09/9.1-Oficio-Circular-no-14.2026-CGSI-DRAC-SAES-MS-1.pdf) | A partir da competência 10/2026, o campo "Tipo de Estabelecimento" é substituído pela "Classificação de Tipo de Estabelecimento" |
| [Portaria SAES/MS nº 3.200/2025](https://ses.sp.bvs.br/wp-content/uploads/2025/09/U_PT-MS-SAES-3200_020925.pdf) | Exige a classificação "Ambulatório" com estrutura móvel para a habilitação 38.04, sem código numérico |

Competências em que o código aparece:

```sql
select date(ano, mes, 1) competencia, count(*) estabelecimentos
from `basedosdados.br_ms_cnes.estabelecimento`
where tipo_unidade = '16'
group by 1 order by 1
```

### `tipo_especialidade_leito` 96 do `leito`

A chave `96` corresponde ao leito "Suporte Ventilatório Pulmonar". Ele foi criado pela Portaria
SAES/MS nº 510/2020 e reativado pela Portaria SAES/MS nº 550, de 03/07/2023. A chave não está
no `CNV/Esp_leit.CNV` do `TAB_CNES.zip` (versão de 15/10/2026), que termina no `95`.

Fonte: [Conass Informa nº 94/2023](https://www.conass.org.br/conass-informa-n-94-2023-republicada-a-portaria-saes-n-550-que-reativa-o-leito-96-suporte-ventilatorio-pulmonar-no-cadastro-nacional-de-estabelecimentos-de-saude-cnes-e-inclui-pro/), que reproduz a republicação da Portaria SAES/MS nº 550.

### Convenção de rótulo

O arquivo é inconsistente por acúmulo histórico: as chaves 1–8 de `tipo_equipamento` são
minúsculas e sem acento; as 9 e 10 são Title Case; os rótulos de `tipo_equipe` seguem o padrão
`sigla - descrição` em minúscula. As chaves novas usam Title Case com acento. O legado não foi
mexido.

Em `codigo_equipamento` a grafia não é normalizada: cada rótulo vem como está na sua fonte. O
CNV escreve `Gama Câmara` onde o dicionário legado escrevia `gama camara`, e a Portaria 3.695
renomeia coisas (`raio x dentario` → `Raio X Odontológico`). Por isso o mesmo equipamento
aparece com duas grafias no dicionário, uma sob `id_equipamento` e outra sob
`codigo_equipamento`.

### O código de 4 dígitos do equipamento

O CNES identifica um equipamento por um código de 4 dígitos: os 2 primeiros são o tipo, os 2
últimos são o equipamento dentro daquele tipo. A numeração do equipamento recomeça a cada tipo
— existe um equipamento 01 na radiologia, outro na telessaúde, outro na diálise. Só os 4
dígitos juntos identificam.

A fonte entrega as duas metades separadas (`TIPEQUIP` e `CODEQUIP`), que o modelo guarda em
`tipo_equipamento` e `id_equipamento`, e junta em `codigo_equipamento`. O dicionário sob
`id_equipamento` só serve para os tipos 1 a 8: o DATASUS embutia o tipo no primeiro dígito do
equipamento (tipo 2 usa 21–23, tipo 3 usa 31–50), o que fazia o número de 2 dígitos bastar até
o tipo 9, cuja faixa já estava ocupada pelo tipo 8. Para os demais, o rótulo certo está sob
`codigo_equipamento`.

O `tipequip` vem `'7'` no staging até 2025 e `'07'` depois; `codequip` tem sempre 2
caracteres. Por isso o modelo aplica `lpad` nas duas metades.

### De onde saem os rótulos dos 4 dígitos

A série tem 207 códigos distintos:

| Procedência | Códigos | Linhas em 2026-06 |
|---|---|---|
| `CNV/Equip_Tp.cnv` | 94 | 969.154 |
| Portaria SAES/MS nº 3.695/2026, Anexo I | 30 | 56.318 |
| Legado transposto do dicionário | 17 | 70.702 |
| Sem rótulo | 66 | 1.681 |

O `CNV/` do `TAB_CNES.zip` é o dicionário oficial do DATASUS: formato de conversão do TabWin,
texto puro em latin-1, uma linha por equipamento com o código de 4 dígitos ao final.

A Portaria 3.695 tem precedência sobre o CNV, por ser a norma e mais recente. Ela criou 27
códigos que o CNV, regerado em 2026-07-21, não traz, e renomeia códigos existentes (`0107`
"Raio X Dentário" → "Raio X Odontológico", `0117` "Mamografo computadorizado" → "Mamógrafo
Digital"); o dicionário usa os nomes da portaria. Os códigos `0119` a `0135` vêm do Art. 2º
§2º, que reclassifica o `05 Raio X de 100 a 500mA` em `20 Raio X Analógico`, `21 Raio X
Digital`, `22 Raio X Telecomandado` e assim por diante; entram nos dados em 2026-03. O texto
oficial está no DOU de 18/05/2026, Edição 91, Seção 1, Página 174.

O legado é transposto por regra: se o número de 2 dígitos existe no dicionário e o tipo em
questão é o dominante daquele número na série, o rótulo vai para o código de 4 dígitos. Os 17
casos são coerentes com o tipo: o tipo 02 (infraestrutura) recebeu ar condicionado,
refrigerador, câmaras de conservação e geradores; o tipo 06 (outros) recebeu veículo
utilitário, embarcação e empilhadeira.

### Pendências conhecidas do dicionário

- **66 códigos de 4 dígitos sem rótulo**, 1.681 linhas em 2026-06: os tipos 11, 14, 15 e 16
  (37 códigos, 340 linhas), que dependem da Portaria 4.109, cujo texto oficial não foi
  localizado e que também não está no CNV, e 29 códigos antigos cujo tipo dominante não bate
  (`0351`, `0912`, `0265`, uma série de `077x`), de 20 a 480 linhas cada.
- **O texto oficial do `tipo_equipe` 80 não foi conferido.**
- **`cobertura_temporal` é `(1)` em todas as linhas**, em vez da notação `INICIO(1)FIM` do
  manual de estilo.
- **Encoding quebrado** em `id_equipamento` chave 97:
  `sistema completo de reforafaEUR!o visual(vra)`. O equivalente em `codigo_equipamento`
  (`0897`) está correto.
- **Significados repetidos** em `tipo_equipe`: as chaves 1 e 70 são ambas
  `esf - equipe de saude da familia`; 49 e 76 são ambas `eap`.

---

## Particionamento

Linhas com `ano` fora do range de partição caem em `__UNPARTITIONED__`, e filtrar por `ano`
não poda nada nessa partição. No particionamento por inteiro do BigQuery o range é
`[start, end)` — o fim é exclusivo, e `"end": 2024` já deixa 2024 de fora. A convenção da BD é
`end = último ano + 5`, o que dá 2031 para dados até 2026.

| Modelo | `start` | `end` |
|---|---|---|
| `dados_complementares`, `equipamento`, `equipe`, `estabelecimento`, `estabelecimento_filantropico`, `gestao_metas`, `habilitacao`, `incentivos`, `leito`, `profissional`, `servico_especializado` | 2005 | 2031 |
| `estabelecimento_ensino`, `regra_contratual` | 2005 | 2024 |

`estabelecimento_ensino` e `regra_contratual` ficam em 2024: nenhuma linha está em
`__UNPARTITIONED__` e não entra dado novo nas duas (ver "Tabelas e particularidades").

**Alterar o config não reparticiona.** O particionamento é fixado quando a tabela é criada, e
só um `--full-refresh` aplica o range novo. O dbt não acusa a divergência: um `dbt run`
incremental com config novo sobre tabela velha passa normalmente.

### Reconstrução de uma tabela em prod

Todas as tabelas do conjunto são `PartBdpro`, e o `pre_hook` de todo modelo faz
`DROP ALL ROW ACCESS POLICIES`. O `run_dbt_model_flow`, que aceita `--full-refresh`, não
recria as políticas; quem recria é o deployment `update_temporal_coverage`. Por isso a
reconstrução usa dois runs seguidos, e entre eles a janela BD Pro fica aberta ao público.

Rode fora da janela dos crons (9h–12h30, ver "Agendamento") e reconstrua `estabelecimento`
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
   download_csv_file=false
   ```

2. Logo em seguida, dispare o deployment `update_temporal_coverage` com
   `{"dataset_id": "br_ms_cnes", "table_id": "<tabela>"}`. É esse run que reaplica as row
   access policies. No log, confira `BDpro filter was included` e
   `All users filter was included`.

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

### Backfill

O `extract_and_load` de cada tabela (`extract_and_load: br_ms_cnes__<tabela>`) aceita uma
lista de competências no formato `AAAAMM`:

```json
{"download_params": {"year_months": ["202609", "202610"]}}
```

Ele baixa do FTP os arquivos dessas competências, sobe para a staging e dispara o
`build_and_promote`. Competência sem arquivo no FTP faz a carga falhar antes do download.

O modelo é incremental e só insere competência maior que a máxima da tabela. Uma competência
que a tabela já tem, ou anterior a ela, só entra com o full-refresh: depois do backfill, faça a
reconstrução descrita acima.

---

## Tabelas e particularidades

### br_ms_cnes__equipamento

`indicador_equipamento_disponivel_sus` e `indicador_equipamento_indisponivel_sus` são STRING
cobertos pelo dicionário. Só assumem `'0'` e `'1'` e são exatamente complementares: uma é a
negação da outra. O `Equipamento.def` do `TAB_CNES.zip` documenta "somente 1=SIM ou 0=NÃO".

### br_ms_cnes__servico_especializado

O campo `SRVUNICO` chega à staging com o texto `nan` onde não há valor. O modelo converte esse
texto em nulo no `tipo_servico_especializado_unico`.

### br_ms_cnes__estabelecimento_ensino

Parada em 2019-12 porque o DATASUS descontinuou o grupo EE. Não tem cron.

### br_ms_cnes__regra_contratual

Desativada. A leitura do CSV da fonte falha:

```text
ParserError: Error tokenizing data. C error: Expected 31 fields in line 213, saw 32
```

A issue #1703 foi fechada como `NOT_PLANNED`.

---

## Agendamento

Crons em `America/Sao_Paulo`, declarados em `flows.py`:

| Tabela | Cron |
|---|---|
| estabelecimento | `0 9 * * *` |
| equipe | `30 9 * * *` |
| leito | `0 10 * * *` |
| equipamento | `30 10 * * *` |
| profissional | `40 10 * * *` |
| dados_complementares | `0 11 * * *` |
| estabelecimento_filantropico | `15 11 * * *` |
| gestao_metas | `30 11 * * *` |
| habilitacao | `45 11 * * *` |
| incentivos | `50 11 * * *` |
| servico_especializado | `30 12 * * *` |
| estabelecimento_ensino | sem cron |
| regra_contratual | sem cron |

`estabelecimento` roda antes de `leito` e `profissional` porque os dois buscam nele o
`id_municipio` da mesma competência.
