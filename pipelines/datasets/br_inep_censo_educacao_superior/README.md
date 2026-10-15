# Documentação do Conjunto de Dados: Censo da Educação Superior (Inep)

Este documento registra o contexto, as decisões e os problemas conhecidos do
conjunto `br_inep_censo_educacao_superior`, para quem for mantê-lo depois.

> **Em construção** (PR #2204, issue #2181). As seções marcadas com *(a fazer)*
> descrevem o que foi decidido mas ainda não está implementado.

---

## Sobre a fonte

O Censo da Educação Superior é a pesquisa anual do Inep sobre instituições,
cursos, alunos e docentes de graduação. Os microdados de cada edição saem num
zip por ano:

```text
https://download.inep.gov.br/microdados/microdados_censo_da_educacao_superior_{ano}.zip
```

A [página do Inep](https://www.gov.br/inep/pt-br/acesso-a-informacao/dados-abertos/microdados/censo-da-educacao-superior)
lista os links de 1995 até a edição mais recente. Visualmente ela destaca só o
último ano, mas o HTML traz todos.

Dentro do zip:

| Caminho | Conteúdo |
|---|---|
| `dados/MICRODADOS_CADASTRO_CURSOS_<ano>.CSV` | tabela de cursos, origem da `curso` |
| `dados/MICRODADOS_ED_SUP_IES_<ano>.CSV` | tabela de instituições, origem da `ies` |
| `Anexos/ANEXO I - Dicionário de Dados/*.xlsx` | dicionário, com a matriz de coleta por ano de cada variável |
| `Anexos/ANEXO II - Questionários.../*.pdf` | questionários por módulo |
| `leia-me/` | leia-me e nota informativa de 2022 |

Os CSVs usam `;` como separador e codificação `windows-1252`. **Os nomes de
arquivo não são estáveis**: a extensão muda de caixa entre anos, e o arquivo
`md5_*.txt` de 2024 cita `MICRODADOS_CADASTRO_IES_2024.csv` enquanto o zip traz
`MICRODADOS_ED_SUP_IES_2024.CSV`. Procure o membro do zip por padrão, nunca pelo
nome exato.

O empacotamento também muda: o zip de 2023 é comprimido (deflate, 36 MB) e o de
2024 não tem compressão (435 MB). Nenhum dos dois usa deflate64, mas o
`br_inep_enem` já encontrou deflate64 em outra pesquisa do Inep. Por isso a
extração reaproveita o `extract_member` dele.

### O servidor do Inep

O `download.inep.gov.br` tem dois comportamentos que qualquer código precisa tratar:

- **Cadeia de certificados incompleta.** A verificação SSL falha com
  `unable to get local issuer certificate`, então as requisições usam `verify=False`.
- **Conexões derrubadas**, às vezes já no handshake (`Connection reset by peer`).

O `build_session` do `br_inep_enem` repete as requisições que falham, mas **só
para GET** (`allowed_methods=("GET",)`). Por isso a leitura de cabeçalhos usa
`GET` com `stream=True`, e não `HEAD`.

## Estrutura no repositório

| Arquivo | Papel | Estado |
|---|---|---|
| `constants.py` | URLs da fonte | feito |
| `utils.py` | funções puras: detecção, download e (a fazer) tratamento | parcial |
| `tasks.py` | `get_latest_update` e `extract_load_data` de cada tabela | *(a fazer)* |
| `flows.py` | ligação com o `CheckThenExtractLoadPipeline`, uma pipeline por tabela | *(a fazer)* |
| `models/br_inep_censo_educacao_superior/` | modelos dbt `curso`, `ies` e `dicionario` | existentes |
| `models/br_inep_censo_educacao_superior/code/architecture/curso.csv` | de-para Inep → BD da `curso` (ver "Arquitetura da `curso`") | feito |
| `models/br_inep_censo_educacao_superior/code/architecture/ies.csv` | de-para Inep → BD da `ies` (ver "Arquitetura da `ies`") | feito |
| `models/br_inep_censo_educacao_superior/code/` | scripts de tratamento manuais, da carga de 2024 | serão substituídos pela pipeline |

A pipeline segue o modelo em estágios: `check_update` → `extract_and_load` →
`build_and_promote` (genérico, em `pipelines/utils/metadata/flows.py`).

## Como ler a tabela `curso`

**Uma linha não é um curso.** A chave é `id_curso` + `id_municipio` +
`tipo_dimensao`, e o tipo de informação que a linha carrega depende do
`tipo_dimensao`:

| `tipo_dimensao` | A linha representa | Município | Traz |
|---|---|---|---|
| 1 — presencial | o curso inteiro | preenchido | oferta **e** alunos |
| 2 — EaD no Brasil | o curso EaD **em um município** | preenchido | só alunos (ingressantes, matrículas, concluintes) |
| 3 — EaD só nível Brasil | o curso EaD no país todo | **nulo** | só oferta (vagas, inscritos, `QT_CURSO`) |
| 4 — EaD no exterior | curso de instituição brasileira no exterior | nulo | só alunos |

O motivo, segundo a nota informativa do Inep de 2022: no EaD não é possível
dividir vagas e inscritos por município. A oferta fica numa linha nacional, e os
alunos ficam espalhados pelos municípios.

Exemplo real (2024): um curso presencial ocupa 1 linha; o curso EaD de Educação
Física `1457109` ocupa 10, sendo 9 do tipo 2 (alunos em cada município) e 1 do
tipo 3 (96 vagas, nacional). As medidas não se repetem entre os tipos, então
somar todas as linhas dá o total correto.

Consequências para quem usa:

1. Para contar cursos, use `count(distinct id_curso)`, não o número de linhas.
2. Vagas e inscritos de EaD não existem por município. Um filtro por município perde a oferta EaD inteira.
3. Ao agregar por UF ou município, as matrículas EaD aparecem nos municípios dos
   alunos, mas as vagas correspondentes não aparecem em município nenhum.

Volume em 2024: 720.349 linhas, sendo 34.824 do tipo 1, 673.756 do tipo 2,
11.319 do tipo 3 e 450 do tipo 4.

## Colunas de cor/raça e de reserva de vagas

Estas colunas são fáceis de confundir. Elas respondem perguntas diferentes sobre
o mesmo aluno.

### Cor/raça não é cota

`quantidade_matriculas_indigena` (e `_preta`, `_parda` etc.) conta os alunos que
**se declaram** daquela cor/raça, entrando ou não por cota. Ela é idêntica a
`QT_MAT_INDIGENA` do Inep em todas as 720.349 linhas de 2024. Não confunda com a
cota para pretos, pardos e indígenas (`QT_MAT_RVPPI`): no curso com mais cotistas
PPI em 2024 há 3 alunos indígenas e 1.097 cotistas PPI.

### Reserva de vagas: total e abertura por grupo

`quantidade_*_reserva_vaga` é o **total** de alunos que entraram por cota. As
colunas `quantidade_*_reserva_vaga_<grupo>` são a **abertura**: quantos desses
cotistas entraram por cada critério.

**As partes não somam o total.** O mesmo aluno pode estar em mais de uma
categoria: uma vaga típica da Lei de Cotas combina "escola pública + renda + PPI".
Em 2024, as categorias de matrículas somam 1.116.988, para 648.225 cotistas.
Cada categoria responde "quantos cotistas têm este critério"; somar categorias
não tem significado. (`QT_MAT_RPFIES` não é cota, é financiamento, e fica fora dessa conta.)

### A mudança de 2024

Em 2024 o Inep mudou a abertura:

| Grupo | 2009–2023 | 2024 |
|---|---|---|
| rede pública, social/renda, deficiência, outros | sim | sim |
| **étnico** (`RVETNICO`) | sim | **descontinuado** |
| pretos, pardos ou indígenas (`RVPPI`) | — | **novo** |
| quilombolas, refugiados/apátridas/visto humanitário, povos e comunidades tradicionais, idosos, internacional, medalhistas em olimpíadas, transgênero e/ou travesti | — | **novos** |

Cada grupo novo existe para ingressantes, matrículas e concluintes: 8 × 3 = 24
colunas. Também desde 2023 há `IN_COMUNITARIA` e `IN_CONFESSIONAL` (se a
instituição privada é comunitária ou confessional).

**Continuidade da série racial:** `..._reserva_vaga_etnico` vale até 2023
(ingressantes: 72.270 em 2023). A sucessora mais próxima é `..._ppi`
(ingressantes: 79.969 em 2024). Elas não são a mesma definição, então quem
emendar as duas séries deve dizer isso.

**Inconsistência no dicionário do Inep:** ele marca `QT_MAT_RVETNICO` e
`QT_CONC_RVETNICO` como coletadas em 2024, mas elas não estão no CSV de 2024, e a
nota ao lado diz "válido para os anos de 2009 a 2023". O CSV é o que vale.

### Estado na BD antes da pipeline

A carga manual de 2024 (PR #1305) não incluiu as 26 colunas novas e deixou
`..._reserva_vaga_etnico` nula em 2024. O PR registra só que "três colunas da
série histórica não existem mais em 2024" e ajusta o teste de nulos. Não há
registro de decisão sobre as colunas novas: o tratamento copia só as colunas da
arquitetura, e as demais são descartadas sem aviso.

**Decisão (2026-10-13, com a Laura):** incluir as 26 colunas, no **fim** da
arquitetura (ver "Staging" abaixo), com descrições que expliquem o significado e
a relação com as colunas existentes. Feito na arquitetura em 2026-10-15 (ver
"Arquitetura da `curso`"); falta o dbt e os metadados.

**Decisão (2026-10-14):** nos anos anteriores à sua criação, as colunas novas
ficam **nulas**, não zeradas: o dado não existe nesses anos. É o comportamento
padrão da pipeline para uma coluna da arquitetura ausente no CSV da fonte. A
descrição de cada coluna deve informar o ano em que ela começa, porque isso
difere das variáveis antigas, em que o Inep usa 0 (ver a seção seguinte).

### Zeros antes do início da coleta

O Inep preenche com **0**, e não com nulo, as variáveis nos anos anteriores à sua
criação, e a BD herdou isso. Exemplo: `quantidade_vagas_novas` é 0 em 2009–2013
porque a variável só existe desde 2014. Um 0 nesses anos significa "não
coletado", não "nenhuma vaga". O início de coleta de cada variável está na
matriz do dicionário do Inep. Os dados não foram alterados, mas a
`temporal_coverage` de cada coluna na arquitetura passou a indicar o período real
(ver abaixo).

**Caso extremo: processo seletivo.** O dicionário diz que `QT_VG_PROC_SELETIVO`,
`QT_INSC_PROC_SELETIVO`, `QT_ING_PROC_SELETIVO` e `QT_ING_OUTRO_TIPO_SELECAO`
foram coletadas de 2009 a 2013, mas os microdados publicados trazem **0 em todos
os anos, inclusive nesse período**. Conferido em 2026-10-15 nos zips de 2009 a
2013: soma 0 no Inep e 0 na BD. A carga da BD está correta; quem publica zero é o
Inep. As quatro colunas têm uma observação na arquitetura dizendo isso.

## Arquitetura da `curso`

`models/br_inep_censo_educacao_superior/code/architecture/curso.csv` é o de-para
entre o CSV do Inep (`original_name`) e a tabela da BD (`name`). O tratamento
copia **só** as colunas listadas aqui, na ordem daqui. O que o Inep publica fora
desta lista é descartado.

### Origem e validação

O arquivo vem da planilha usada na carga manual de 2024 (o `arquitetura_apoio.csv`
dos scripts em `code/`), indicada pela Laura. Antes de adotá-la, ela foi
conferida contra os dados de 2024 (2026-10-15):

- os 193 `name` são exatamente as colunas da tabela de prod, na mesma ordem;
- todos os `original_name` existem no CSV do Inep de 2023; em 2024 faltam só as 3 `RVETNICO`;
- juntando as 720.349 linhas do Inep e da BD pela chave, as 189 colunas comparáveis
  têm **valores idênticos** linha a linha, incluindo as que o tratamento altera
  (`id_municipio`, `indicador_gratuito`, `tipo_grau_academico`).

### O que mudou em relação à planilha

1. **Removidas as duas colunas auxiliares sem nome** (ex.: `'NU_ANO_CENSO':'ano'`),
   que só serviam para montar o dicionário de renomeação no Colab. Nada as lia.
2. **`temporal_coverage` recalculada** pela matriz "coleta por ano" do dicionário
   do Inep. Na planilha, todas as linhas tinham `(1)`. Agora a coluna fica vazia
   quando cobre o mesmo período da tabela (convenção da BD) e explícita nas 50
   que diferem: por exemplo, `quantidade_vagas_novas` = `2014(1)`,
   `..._processos_seletivos` = `2009(1)2013`, `..._reserva_vaga_etnico` = `2009(1)2023`.
3. **Observação nas 3 colunas `..._reserva_vaga_etnico`**, explicando a
   descontinuação e apontando a sucessora `..._ppi`.
4. **26 colunas novas, no fim**, seguindo o padrão de nomes que já existia.

### As 26 colunas novas: como os nomes foram formados

As colunas de reserva de vagas seguem `<medida>_reserva_vaga_<grupo>`, o mesmo
padrão de `..._reserva_vaga_rede_publica` e das demais:

| Medida na BD | No Inep |
|---|---|
| `quantidade_ingressantes_` | `QT_ING_` |
| `quantidade_matriculas_` | `QT_MAT_` |
| `quantidade_concluintes_` | `QT_CONC_` |

| Grupo na BD | No Inep | Significado |
|---|---|---|
| `_ppi` | `RVPPI` | pretos, pardos ou indígenas |
| `_quilombola` | `RVQUILO` | quilombolas |
| `_refugiado` | `RVREFU` | refugiados, apátridas ou portadores de visto humanitário |
| `_povos_tradicionais` | `RVPOVT` | povos e comunidades tradicionais |
| `_idoso` | `RVIDOSO` | idosos |
| `_internacional` | `RVINTERN` | estudantes internacionais |
| `_medalhista` | `RVMEDAL` | medalhistas em olimpíadas científicas e competições de conhecimento |
| `_trans` | `RVTRANS` | pessoas transgênero e/ou travestis |

Exemplo: `QT_MAT_RVPPI` → `quantidade_matriculas_reserva_vaga_ppi`. São 3 medidas
× 8 grupos = 24 colunas, todas `int64`, em pessoas, com cobertura `2024(1)`.

As outras duas são indicadores da instituição, no mesmo formato de
`indicador_gratuito` (`boolean`, `1`/`0` do Inep viram verdadeiro/falso):

| BD | Inep | Cobertura | Observação |
|---|---|---|---|
| `indicador_comunitaria` | `IN_COMUNITARIA` | `2023(1)` | sempre falso em instituições públicas: a classificação só se aplica às privadas |
| `indicador_confessional` | `IN_CONFESSIONAL` | `2023(1)` | idem |

Antes do início da cobertura, as colunas novas ficam **nulas** (decisão acima).

### O que continua fora, de propósito

| Colunas do Inep | Por quê |
|---|---|
| `CO_UF`, `NO_UF`, `CO_REGIAO`, `NO_REGIAO`, `NO_MUNICIPIO`, `IN_CAPITAL` | vêm dos diretórios da BD a partir de `id_municipio` |
| `QT_CURSO` | contagem auxiliar do Inep; a contagem de cursos é `count(distinct id_curso)` |

### Anos antigos conferem com a BD

Na mesma conferência de 2026-10-15, os anos de 2009 a 2013 têm na BD exatamente o
mesmo número de linhas e o mesmo total de vagas que os zips atuais do Inep. As
cargas antigas desses anos estão em dia com a fonte.

## Arquitetura da `ies`

`models/br_inep_censo_educacao_superior/code/architecture/ies.csv`, 71 colunas.
Uma linha da `ies` é uma instituição em um ano (chave `ano` + `id_ies`).

O arquivo vem da planilha usada na carga manual de 2024, indicada pela Laura, e
foi conferido contra os dados de 2024 (2026-10-15):

- os 71 `name` são exatamente as colunas da tabela de prod, na mesma ordem;
- todos os `original_name` existem nos CSVs do Inep de 2023 e de 2024;
- as 2.561 instituições de 2024 têm **valores idênticos** em todas as colunas.

A única mudança em relação à planilha é a `temporal_coverage`, recalculada pelo
dicionário do Inep (aba `cadastro_ies`) com o mesmo critério da `curso`. Ficaram
explícitas 9 colunas de biblioteca: 7 começam em `2015(1)`,
`indicador_biblioteca_assina_outras_bases` começa em `2018(1)` e
`indicador_biblioteca_acesso_outras_bases` vai de `2009(1)2017`.

Colunas do Inep fora da arquitetura:

| Colunas | Situação |
|---|---|
| `CO_UF_IES`, `NO_UF_IES`, `CO_REGIAO_IES`, `NO_REGIAO_IES`, `CO_MESORREGIAO_IES`, `NO_MESORREGIAO_IES`, `CO_MICRORREGIAO_IES`, `NO_MICRORREGIAO_IES`, `NO_MUNICIPIO_IES`, `IN_CAPITAL_IES` | vêm dos diretórios da BD a partir de `id_municipio` |
| `IN_COMUNITARIA`, `IN_CONFESSIONAL` (desde 2023) | *(a decidir)* — na `curso` foram incluídas |
| `TP_REDE` (pública/privada, desde 2009) | *(a decidir)* — a `curso` tem `rede`; a `ies` não |

## Detecção de atualização

O Inep **republica edições antigas**, às vezes com correções. Por isso a
detecção olha o `Last-Modified` do zip de **todos os anos** em toda execução, e
não só o ano mais recente. A leitura é barata: são 16 requisições que leem só o
cabeçalho. Uma edição nova e a revisão de um ano antigo disparam a carga do mesmo
jeito.

Datas de publicação conferidas em 2026-10-13:

| Anos | `Last-Modified` |
|---|---|
| 2009–2021 | 2022-11-04 (republicação em massa, junto com a nota informativa) |
| 2022 | 2023-10-10 |
| 2023 | 2025-12-30 |
| 2024 | 2026-07-10 |

**Não use o texto "Atualizado em" da página.** Em outubro de 2026 a página dizia
"Arquivos atualizados em 30/12/2025" para 2024, mas o servidor dizia 10/07/2026.
O cabeçalho do servidor é a fonte confiável.

**Nem toda republicação muda o dado.** A de 2024 não mudou nada (comparação linha
a linha com a BD: 0 diferenças). A de 2023 corrigiu 106 cursos. A pipeline não
tenta distinguir as duas: recarregar um ano sem mudança custa um download e
deixa o dado igual.

Desenho *(a fazer em `tasks.py`)*:

- `reference_date` = o maior `Last-Modified` entre os anos, comparado pelo
  framework com a última atualização da tabela (`compare_against="table_update"`);
- os anos a recarregar são os de `Last-Modified` posterior a essa data.

### O legado de 2023 e a primeira execução

O staging de 2023 é de 09/10/2024; o Inep revisou 2023 em 30/12/2025; a carga
manual de 2024 atualizou a tabela em jan/2026 **sem recarregar 2023**. A regra
"`Last-Modified` posterior à última atualização da tabela" não pega esse caso. A
**primeira execução** precisa receber os anos explicitamente. Depois dela, toda
execução recarrega todos os anos que mudaram antes de atualizar a data da tabela,
e a regra passa a bastar.

**Decisão (2026-10-15):** a primeira execução recarrega **todos os anos, 2009 a
2024**, das duas tabelas, e não só 2023 e 2024. Motivos: a troca de posição das
colunas CINE (ver "Staging") e os defeitos das cargas manuais (ver "Defeitos de
prod corrigidos pela recarga"). A pipeline não deve depender de soluções
pontuais deixadas pelas cargas anteriores.

O que mudou em 2023 (comparação com a BD em 2026-10-13):

| | Inep (revisado) | BD | diferença |
|---|---|---|---|
| linhas | 671.610 | 671.610 | 0 |
| `tipo_dimensao` 1 / 3 | 35.739 / 10.576 | 35.633 / 10.680 | +106 / −104 |
| vagas | 24.621.965 | 24.687.130 | −65.165 |
| inscritos | 14.514.689 | 14.576.590 | −61.901 |
| ingressantes, matrículas, concluintes | — | — | iguais |

Cento e seis cursos foram reclassificados de EaD nacional (tipo 3) para presencial (tipo 1).

## Staging

O staging é **CSV**, com um arquivo por ano, em dev e em prod:

```text
gs://<bucket>/staging/br_inep_censo_educacao_superior/curso/ano=<ano>/data.csv
```

Isso define três regras para a pipeline:

1. **Grave CSV, e não parquet**, com o nome `data.csv`. Um `data.parquet` ficaria
   ao lado do CSV antigo, e a tabela externa CSV não o leria.
2. **Use `dump_mode="append"`, nunca `"overwrite"`.** O `overwrite` apaga a
   tabela de staging inteira, com todos os anos. O `append` sobe os arquivos com
   `if_exists="replace"`, o que substitui só o ano recarregado, desde que o
   caminho seja idêntico.
3. **Colunas novas entram no fim.** A tabela externa CSV lê por posição, e o
   `_sync_staging_schema` só acrescenta colunas no fim, sem reordenar. Como a
   primeira execução regrava todos os anos, todos os arquivos ficam com as
   mesmas colunas, e não há arquivos com número de colunas diferente.

O CSV de staging não é o CSV do Inep: tem separador `,`, codificação utf-8, os
nomes da BD e as colunas da arquitetura, na ordem dela.

### A troca de posição das colunas CINE (até 2023)

Nos arquivos de staging de 2009 a 2023, `id_curso_cine` e `nome_curso_cine` estão
em posições trocadas (9 e 10) em relação à tabela externa; são as únicas fora de
ordem. Como a leitura é por posição, nesses anos o código caía em
`nome_curso_cine` e o nome em `id_curso_cine`. O PR #1536 compensou isso no
`curso.sql` com um `case when ano <= 2023` que troca as duas colunas.

Essa regra é por ano, não por arquivo: um 2023 regravado na ordem certa seria
trocado de novo. **Decisão (2026-10-15):** regravar todos os anos na ordem da
arquitetura e **remover o `case` do `curso.sql`**. Feito. O modelo só funciona
depois que a pipeline regravar o staging de todos os anos: os arquivos antigos
ainda estão na ordem trocada e sem as colunas novas.

## Tratamento

`tratar_tabela` (em `utils.py`) transforma o CSV do Inep de um ano no `data.csv`
do staging:

1. **Extrai o CSV** do zip, procurando por trecho do nome (`CADASTRO_CURSOS`,
   `_IES_`), porque o nome completo muda entre anos.
2. **Confere o cabeçalho** contra a arquitetura e avisa no log as colunas
   descartadas e as ausentes. Foi por esses avisos que os nomes antigos abaixo
   apareceram.
3. **Lê em blocos de 100 mil linhas, tudo como texto.** Em blocos porque a
   `curso` de 2024 lida de uma vez passa de 12 GB; em blocos, o pico fica em
   1,4 GB. Como texto, porque é o que preserva CEPs e códigos com zero à esquerda.
4. **Traduz nomes antigos do Inep** (`OLD_PREFIXES` em `constants.py`):
   - `ies` 2009: `DOC_EX_*` → `QT_DOC_EX_*` e `QT_DOCENTE_*` → `QT_DOC_*`. Sem
     isso, as 31 colunas de docentes de 2009 ficariam nulas;
   - `curso` 2020: `CO_CINE_ROTULO2` → `CO_CINE_ROTULO`. Sem isso, o código CINE
     de 2020 ficaria nulo.
5. **Aplica a arquitetura**: renomeia, ordena, confere que todas as linhas são do
   ano esperado e tira a coluna `ano`, que vai no caminho da partição.
6. **Vazio em coluna de quantidade vira 0; coluna ausente fica nula.** Até 2019,
   o Inep deixa vazio o que não se aplica (vagas e inscritos nas linhas de EaD por
   município); de 2020 em diante, grava 0. Prod sempre teve 0 em todos os anos, e
   a pipeline mantém isso (**decisão de 2026-10-15**: a `curso` é uma das tabelas
   mais consultadas, e o que não precisa mudar não muda). Colunas que não existem
   no CSV do ano, como as 26 novas antes de 2023/2024, continuam nulas.
7. **Ajusta valores**: booleanos `1`/`0` → `True`/`False`; `id_municipio` com 7
   dígitos; aspas retiradas do `id_curso_cine`.
8. **Grava** `ano=<ano>/data.csv`.

### Conferência com prod, ano a ano

Antes da primeira execução, o tratamento rodou em todos os anos das duas tabelas
e cada coluna foi comparada com prod (linhas, valores preenchidos, somas e
valores distintos). Resultado em 2026-10-15: **30 dos 32 pares tabela-ano são
idênticos a prod**. Os outros dois são as correções intencionais: `curso` 2023
(revisão do Inep, 14 colunas de vagas e inscritos) e `ies` 2024 (os `'nan'` e o
CEP abaixo). Diferenças de formato que os agregados não captam (zero à esquerda,
aspas, `.0`) foram conferidas valor a valor em 2024.

### Defeitos de prod corrigidos pela recarga

Todos vêm das cargas manuais de 2023 e 2024, que leram colunas de texto como
número ou não trataram nulos e aspas:

| Tabela | Coluna | Anos | Em prod | Depois da recarga |
|---|---|---|---|---|
| `curso` | `id_curso_cine` | 2024 | `"0115L07"` (com aspas) | `0115L07` |
| `curso` | `id_area_geral`, `id_area_especifica`, `id_area_detalhada` | 2023, 2024 | `4`, `42`, `421` | `04`, `042`, `0421` |
| `ies` | `cep` | 2024 | `78060900.0`; 278 sem o zero à esquerda | `78060900`, 8 dígitos |
| `ies` | `sigla`, `numero`, `complemento`, `cep` | 2024 | texto `'nan'` (459 + 1 + 4 + 26 = 490 valores) | nulo |
| `curso` | vagas e inscritos (14 colunas) | 2023 | versão anterior à revisão do Inep | versão revisada |

## Pendências

- [x] Tratamento: arquitetura → CSV de staging, com avisos no log; conferido com prod em todos os anos
- [x] Arquitetura da `curso` no repositório, validada, com as 26 colunas novas e a cobertura corrigida
- [x] Arquitetura da `ies` no repositório, validada, com a cobertura corrigida
- [ ] Decidir se `IN_COMUNITARIA`, `IN_CONFESSIONAL` e `TP_REDE` entram na `ies`
- [x] dbt: 26 colunas novas no `curso.sql` e no `schema.yml`, remoção do `case` das colunas CINE e partição até 2029 (`curso` e `ies`)
- [ ] Metadados: colunas novas, cobertura por coluna e observações
- [ ] `tasks.py` e `flows.py`
- [ ] Execução em dev ponta a ponta, incluindo a leitura de arquivos com número de colunas diferente
- [ ] Primeira execução recarregando todos os anos (2009 a 2024)
- [ ] Carga de 2025, quando for publicada
