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
   `_sync_staging_schema` só acrescenta colunas no fim, sem reordenar. Os
   arquivos de anos antigos terão menos colunas que os novos; se o BigQuery aceita
   isso numa tabela CSV, só a execução em dev vai dizer. *(a validar)*

O CSV de staging não é o CSV do Inep: tem separador `,`, codificação utf-8, os
nomes da BD e as colunas da arquitetura, na ordem dela.

## Pendências

- [ ] Tratamento: arquitetura → CSV de staging, com aviso no log para colunas da fonte ignoradas e colunas da arquitetura ausentes na fonte
- [x] Arquitetura da `curso` no repositório, validada, com as 26 colunas novas e a cobertura corrigida
- [ ] Arquitetura da `ies` (planilha sendo localizada)
- [ ] dbt (`curso.sql`, `schema.yml`) e metadados com as 26 colunas novas
- [ ] `tasks.py` e `flows.py`
- [ ] Execução em dev ponta a ponta, incluindo a leitura de arquivos com número de colunas diferente
- [ ] Primeira execução com recarga de 2023 (e 2024)
- [ ] Carga de 2025, quando for publicada
