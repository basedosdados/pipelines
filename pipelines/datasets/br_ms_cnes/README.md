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

## Tabelas e particularidades

### br_ms_cnes__equipamento

`indicador_equipamento_disponivel_sus` e `indicador_equipamento_indisponivel_sus` são STRING
cobertos pelo dicionário. Só assumem `'0'` e `'1'` e são exatamente complementares: uma é a
negação da outra. O `Equipamento.def` do `TAB_CNES.zip` documenta "somente 1=SIM ou 0=NÃO".

### br_ms_cnes__estabelecimento_ensino

Parada em 2019-12 porque o DATASUS descontinuou o grupo EE. Não tem cron.

### br_ms_cnes__regra_contratual

Desativada. A leitura do CSV da fonte falha:

```text
ParserError: Error tokenizing data. C error: Expected 31 fields in line 213, saw 32
```

A issue #1703 foi fechada como `NOT_PLANNED`.

---

## Valores ausentes

Nos arquivos mais antigos da staging, o valor ausente está gravado como o texto `nan`, efeito
de um `astype(str)` no tratamento. O tratamento atual grava nulo. Os modelos leem a staging pelo
macro `cnes_staging_nan_as_null` (`macros/cnes_columns_standartization.sql`), que troca `nan` por
nulo em toda coluna de texto, menos `ano`, `mes` e `sigla_uf`: são as partições da staging e
ficam de fora para não perder a poda. O macro troca só `nan`, e o texto em branco passa como
está, porque vários modelos convertem `''` no código `'0'`.

Três colunas usam zeros para valor ausente, e os modelos as convertem em nulo: `cpf_cnpj` e
`cnpj_mantenedora` (14 zeros) no `estabelecimento` e `id_municipio_6_residencia` (6 zeros) no
`profissional`.

---

## Colunas fora do teste de nulos

O `not_null_proportion_multiple_columns` de cada modelo exige pelo menos 99% de valores
preenchidos em cada coluna, na competência mais recente. As colunas abaixo ficam fora dele
(`ignore_values`). Os percentuais foram medidos em prod em 23/11/2026, na competência 2026-10,
com o texto `nan` e o texto vazio contados como nulo.

Para conferir os valores de uma tabela, troque `estabelecimento` nos dois lugares da consulta.
Ela devolve a porcentagem de nulos de cada coluna na competência mais recente; coluna sem nenhum
nulo não aparece. A consulta lê todas as colunas do ano mais recente: uns 7 GiB no
`estabelecimento` e 11 GiB no `profissional`.

```sql
with ultima_competencia as (
  select *
  from `basedosdados.br_ms_cnes.estabelecimento`
  where date(ano, mes, 1) = (
    select max(date(ano, mes, 1)) from `basedosdados.br_ms_cnes.estabelecimento`
  )
)
select
  coluna,
  round(100 * count(*) / (select count(*) from ultima_competencia), 2) as percentual_nulos
from ultima_competencia as t,
  unnest(regexp_extract_all(to_json_string(t), r'"(\w+)":null')) as coluna
group by coluna
order by percentual_nulos desc
```

Colunas vazias. As marcadas com `*` estão vazias em todas as competências de 2024-01 a 2026-10;
as outras foram medidas só em 2026-10.

| Tabela | Coluna | Nulos |
|---|---|---|
| `dados_complementares` | `cns_medico_responsavel_administrador_responsavel_tecnico` | 99,74% |
| `dados_complementares` | `cns_medico_responsavel_oncologista_pediatrico` | 99,95% |
| `dados_complementares` | `cns_medico_responsavel_cirurgia_oncologica` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_oncologista_clinico` | 99,74% |
| `dados_complementares` | `cns_medico_responsavel_radioterapeuta` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_fisico_nuclear` | 99,89% |
| `dados_complementares` | `cns_medico_responsavel_nefrologista` | 100% |
| `estabelecimento` | `ano_avaliacao_pnass` `*` | 100% |
| `estabelecimento` | `mes_avaliacao_pnass` `*` | 100% |
| `estabelecimento` | `data_publicacao_contrato_estadual` `*` | 100% |
| `estabelecimento` | `data_publicacao_contrato_municipal` `*` | 100% |
| `estabelecimento` | `tipo_avaliacao_acreditacao_hospitalar` `*` | 100% |
| `estabelecimento` | `tipo_classificacao_acreditacao_hospitalar` `*` | 100% |
| `estabelecimento` | `tipo_natureza_administrativa` `*` | 100% |
| `estabelecimento` | `tipo_retencao_tributos` `*` | 100% |
| `estabelecimento` | `tipo_retencao_tributos_mantenedora` `*` | 100% |
| `estabelecimento` | `id_microrregiao_saude` | 100% |
| `estabelecimento` | `id_distrito_administrativo` | 100% |
| `estabelecimento` | `tipo_nivel_hierarquia` | 100% |
| `estabelecimento` | `id_contrato_municipio_sus` | 100% |
| `estabelecimento` | `id_contrato_estado_sus` | 100% |
| `profissional` | `cbo_1994` `*` | 100% |
| `profissional` | `indicador_estabelecimento_terceiro` `*` | 100% |
| `profissional` | `id_municipio_6_residencia` | 99,98% |
| `servico_especializado` | `quantidade_nacional_estabelecimento_saude_terceiro` `*` | 100% |

Colunas parcialmente preenchidas. Os nulos andam em grupos de colunas ligadas entre si: `banco`,
`agencia` e `conta_corrente` têm o mesmo número, assim como as duas do conselho e as duas do
serviço único.

| Tabela | Coluna | Nulos |
|---|---|---|
| `dados_complementares` | `cns_medico_responsavel_hemoterapeuta` | 54,92% |
| `dados_complementares` | `cns_medico_capacitado_responsavel` | 55,56% |
| `dados_complementares` | `cns_medico_responsavel_hematologista` | 66,56% |
| `dados_complementares` | `cns_responsavel_tecnico_sorologia` | 71,56% |
| `estabelecimento` | `cpf_cnpj` | 20,32% |
| `estabelecimento` | `tipo_orgao_expedidor` | 34,76% |
| `estabelecimento` | `numero_alvara` | 35,55% |
| `estabelecimento` | `data_expedicao_alvara` | 35,59% |
| `estabelecimento` | `id_regiao_saude` | 60,80% |
| `estabelecimento` | `cnpj_mantenedora` | 79,33% |
| `estabelecimento` | `banco` | 91,96% |
| `estabelecimento` | `agencia` | 91,96% |
| `estabelecimento` | `conta_corrente` | 91,96% |
| `estabelecimento` | `id_distrito_sanitario` | 92,39% |
| `profissional` | `id_registro_conselho` | 26,79% |
| `profissional` | `tipo_conselho` | 26,92% |
| `servico_especializado` | `tipo_servico_especializado_unico` | 47,11% |
| `servico_especializado` | `indicador_servico_especializado_unico` | 47,11% |

Colunas com poucos nulos, mas acima do 1% que o teste aceita:

| Tabela | Coluna | Nulos |
|---|---|---|
| `equipe` | `area` | 2,14% |
| `equipe` | `descricao_segmento` | 2,14% |
| `estabelecimento` | `tipo_fluxo_atendimento` | 1,44% |
| `habilitacao` | `portaria` | 1,49% |

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

### Backfill

O `extract_and_load` de cada tabela (`extract_and_load: br_ms_cnes__<tabela>`) aceita uma
lista de competências no formato `AAAAMM`:

```json
{"download_params": {"year_months": ["202609", "202610"]}}
```

Ele baixa do FTP os arquivos dessas competências, sobe para a staging e dispara o
`build_and_promote`. Competência sem arquivo no FTP faz a carga falhar antes do download.

O modelo é incremental e só insere competência maior que a máxima da tabela. Uma competência
que a tabela já tem, ou anterior a ela, só entra com um `--full-refresh` do modelo depois do
backfill.

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

---

## O dicionário

### Como funciona

#### Origem

O dicionário deste conjunto não é gerado por nada: não há crawler nem fonte registrada no
backend, e é a única tabela do conjunto sem flow. É um CSV único, mantido à mão, em
`gs://basedosdados-dev/staging/br_ms_cnes/dicionario/dicionario.csv`.

#### Como atualizar

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

#### Como chega em prod

O script escreve só o bucket de dev. Quem leva o dicionário para prod é a action
`table-approve`, que copia `gs://basedosdados-dev/staging/<ds>/<tabela>` para
`gs://basedosdados/staging/<ds>/<tabela>` (com backup em `basedosdados-backup`), recria a
staging e roda o dbt em prod. Para isso:

1. a PR precisa carregar o rótulo `table-approve`;
2. a PR precisa alterar `models/br_ms_cnes/br_ms_cnes__dicionario.sql`, porque a action monta
   o par (conjunto, tabela) a partir dos `.sql` modificados.

A cópia apaga o destino antes. Confira que o staging de prod não tem nada que o de dev não
tenha.

#### Convenção de rótulo

O arquivo é inconsistente por acúmulo histórico: as chaves 1–8 de `tipo_equipamento` são
minúsculas e sem acento; as 9 e 10 são Title Case; os rótulos de `tipo_equipe` seguem o padrão
`sigla - descrição` em minúscula. As chaves novas usam Title Case com acento. O legado não foi
mexido.

### Mudanças no dicionário

As chaves 77 a 81 de `tipo_equipe` já existiam com o valor provisório "Não encontrado nos
dicionários oficiais" e ganharam o rótulo da norma; as outras linhas são chaves novas.

| PR | Coluna | Chave | Rótulo | Fonte |
|---|---|---|---|---|
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 11 | Avaliação Antropométrica e Funcional | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 12 | Radioterapia | Portaria SAES/MS nº 3.695/2026, Art. 2º §1º |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 13 | Quimioterapia | Portaria SAES/MS nº 3.695/2026, Art. 2º §1º |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 14 | Reabilitação | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 15 | Procedimentos Clínicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.tipo_equipamento` | 16 | Procedimentos Cirúrgicos | Portaria SAES/MS nº 4.109/2026, via terceiros |
| #1721 (31/07/2026) | `equipamento.codigo_equipamento` | 141 códigos | ver "Particularidades por coluna" | `CNV/Equip_Tp.cnv`, Portaria SAES/MS nº 3.695/2026 e legado |
| #1721 (31/07/2026) | `equipamento.indicador_equipamento_disponivel_sus` | 0, 1 | Não, Sim | `Equipamento.def` do `TAB_CNES.zip` |
| #1721 (31/07/2026) | `equipamento.indicador_equipamento_indisponivel_sus` | 0, 1 | Não, Sim | `Equipamento.def` do `TAB_CNES.zip` |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 77 | EMAP-R - Equipe Multiprofissional de Apoio para Reabilitação | Portaria SAES/MS nº 1.619/2024, Art. 3º §4º |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 78 | EACP - Equipe Assistencial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 79 | EMCP - Equipe Matricial de Cuidados Paliativos | Portaria SAES/MS nº 2.085/2024, Anexo II, via Conass Informa 156/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 80 | EAP-DESINST - Equipe de Avaliação e Acompanhamento de Medidas Terapêuticas (transtorno mental em conflito com a lei) | Portaria GM/MS nº 4.876/2024, operacionalizada pela SAES/MS nº 2.070/2024 |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 81 | EqAE - Equipe de Atenção Especializada | Portaria SAES/MS nº 3.200/2025, Art. 5º |
| #1721 (31/07/2026) | `equipe.tipo_equipe` | 82 | E-DOT - Equipe Hospitalar de Doação para Transplantes | CNES 4.8.40, via terceiros |
| #2320 (18/11/2026) | `equipe.tipo_equipe` | 83 | EMCP-Ped - Equipe Macrorregional de Cuidados Paliativos com Pediatra | Portaria SAES/MS nº 4.288/2026, Anexo II, via Conass Informa 140/2026 |
| #2331 | `estabelecimento.tipo_unidade` | 16 | Significado não encontrado nas fontes relacionadas ao CNES | nenhuma (ver "Particularidades por coluna") |
| #2331 | `leito.tipo_especialidade_leito` | 96 | Suporte Ventilatório Pulmonar | Portaria SAES/MS nº 550/2023, via [Conass Informa nº 94/2023](https://www.conass.org.br/conass-informa-n-94-2023-republicada-a-portaria-saes-n-550-que-reativa-o-leito-96-suporte-ventilatorio-pulmonar-no-cadastro-nacional-de-estabelecimentos-de-saude-cnes-e-inclui-pro/) |

### Particularidades por coluna

#### `equipamento.tipo_equipamento`

Das chaves 11 a 16, só 12 e 13 têm fonte primária. As outras quatro vêm de terceiros citando a
Portaria 4.109/2026, cujo texto oficial não foi localizado. O `TP_EQUIPAM.dbf` do
`TAB_CNES.zip` traz só 10 tipos.

#### `equipamento.codigo_equipamento`

**O código de 4 dígitos.** O CNES identifica um equipamento por um código de 4 dígitos: os 2
primeiros são o tipo, os 2 últimos são o equipamento dentro daquele tipo. A numeração do
equipamento recomeça a cada tipo — existe um equipamento 01 na radiologia, outro na
telessaúde, outro na diálise. Só os 4 dígitos juntos identificam.

A fonte entrega as duas metades separadas (`TIPEQUIP` e `CODEQUIP`), que o modelo guarda em
`tipo_equipamento` e `id_equipamento`, e junta em `codigo_equipamento`. O dicionário sob
`id_equipamento` só serve para os tipos 1 a 8: o DATASUS embutia o tipo no primeiro dígito do
equipamento (tipo 2 usa 21–23, tipo 3 usa 31–50), o que fazia o número de 2 dígitos bastar até
o tipo 9, cuja faixa já estava ocupada pelo tipo 8. Para os demais, o rótulo certo está sob
`codigo_equipamento`.

O `tipequip` vem `'7'` no staging até 2025 e `'07'` depois; `codequip` tem sempre 2
caracteres. Por isso o modelo aplica `lpad` nas duas metades.

**De onde saem os rótulos dos 4 dígitos.** A série tem 207 códigos distintos:

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

A grafia não é normalizada: cada rótulo vem como está na sua fonte. O CNV escreve
`Gama Câmara` onde o dicionário legado escrevia `gama camara`, e a Portaria 3.695 renomeia
coisas (`raio x dentario` → `Raio X Odontológico`). Por isso o mesmo equipamento aparece com
duas grafias no dicionário, uma sob `id_equipamento` e outra sob `codigo_equipamento`.

#### `equipe.tipo_equipe`

O texto oficial foi lido para 77 e 81. Para 78 e 79, a fonte é o Conass Informa 156/2024,
que resume o Anexo II. Para 80, nenhuma das duas portarias foi lida. O 82 vem de terceiros
citando o CNES 4.8.40. O `EQUIPE.dbf` do `TAB_CNES.zip` vai até o 76.

O nome que o próprio estabelecimento dá à equipe (coluna `equipe`, `nome_eqp` na fonte) bate
com a norma nos cinco casos de 77 a 81:

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

#### `estabelecimento.tipo_unidade`

A chave `16` aparece nos dados a partir de 2025-09, e nenhuma fonte do CNES diz o que ela
significa. O dicionário a registra com o valor `Significado não encontrado nas fontes
relacionadas ao CNES`, que não vem da fonte.

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

Resultado em prod, em 23/11/2026 (o código não aparece antes de 2025-09):

| Competência | Estabelecimentos |
|---|---|
| 2025-09 | 10 |
| 2025-10 | 21 |
| 2025-11 | 43 |
| 2025-12 | 54 |
| 2026-01 | 59 |
| 2026-02 | 86 |
| 2026-03 | 125 |
| 2026-04 | 149 |
| 2026-05 | 209 |
| 2026-06 | 256 |
| 2026-07 | 293 |
| 2026-08 | 328 |
| 2026-09 | 358 |
| 2026-10 | 148 |

#### `leito.tipo_especialidade_leito`

A chave `96` é o leito "Suporte Ventilatório Pulmonar", criado pela Portaria SAES/MS nº
510/2020 e reativado pela Portaria SAES/MS nº 550, de 03/07/2023. Ela não está no
`CNV/Esp_leit.CNV` do `TAB_CNES.zip` (versão de 15/10/2026), que termina no `95`.

### Pendências

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
