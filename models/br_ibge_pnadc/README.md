# br_ibge_pnadc — notas de manutenção

PNAD Contínua trimestral (IBGE). Tabelas: `microdados`, `educacao`,
`rendimentos_outras_fontes` e `dicionario`.

## Dicionário — pipeline dentro do repo

O dicionário é gerado por uma pipeline **dentro do repo**:

- `pipelines/datasets/br_ibge_pnadc/utils.py` — funções puras (download + parsing +
  correções), sem Prefect. `build_dicionario()` orquestra tudo.
- `pipelines/datasets/br_ibge_pnadc/tasks.py` / `flows.py` — flow
  `br_ibge_pnadc__dicionario` (quinzenal) que reconstrói o dicionário, sobe para o
  staging (`dump_mode="overwrite"`) e materializa via dbt.

Por isso o modelo `br_ibge_pnadc__dicionario.sql` só faz `safe_cast` das 5 colunas, sem
transformar nada — todas as correções de domínio ficam no `build_dicionario`, não no SQL.

### Fontes (FTP do IBGE)

Documentação em `.../Trimestral/Microdados/Documentacao/`:

1. `dicionario_PNADC_microdados_trimestral.xls` — vem **dentro** de
   `Dicionario_e_input_20221031.zip`.
2. `Estrutura_Ocupacao_COD.xls` — usado para reconstruir a variável `V4010`.

**Pegadinha:** o arquivo de ocupação é `.xls` (formato antigo). O script original usava
`openpyxl`, que **não lê `.xls`** — daí ele exigir um passo manual de conversão. A pipeline
lê com `pandas.read_excel(..., dtype=str)`, que resolve a leitura **e** preserva os zeros à
esquerda (críticos no `V4010`).

### Correções aplicadas na origem (em `build_dicionario`)

1. **Casing** — o dicionário guarda `Capital`/`RM_RIDE` (nome original do IBGE); a lista do
   teste `custom_dictionary_coverage` usa esse casing em `columns_covered_by_dictionary`.
2. **Bloco V3 (escolaridade)** — as colunas `V3001`–`V3014` aparecem no Excel só sob a Parte
   de educação, mas `microdados` também as contém. `duplicar_bloco_v3` replica essas linhas
   sob `id_tabela='microdados'` (a mesma decodificação serve as duas tabelas).
3. **Zero à esquerda** — colunas codificadas (`COLUNAS_STRIP_ZERO`: `V2005`, `V4072`,
   `V4074A` e as V3 acima) têm chave `01`–`09` no dicionário, mas o dado guarda dígito único
   `1`–`9`. `normalizar_chaves` aplica `lstrip('0')` (repondo `"0"` quando a chave era só
   `"0"`). A duplicação do V3 roda **antes** do strip, para que as cópias também sejam
   normalizadas.

### V4010 (código de ocupação) fora da cobertura — #1699

`V4010` está **fora** do `custom_dictionary_coverage` (e fora do `COLUNAS_STRIP_ZERO`). É
código de ocupação hierárquico (COD/IBGE), onde o zero à esquerda é **semântico**
(`0110` ≠ `110`), e a hierarquia (GG/SG/SUB/GB) depende dele. O strip do modelo (ver abaixo)
alcança `V4010` e come um zero, então o dado tem `210`/`110`/`000` onde o dicionário tem
`0210`/`0110`/`0`. Em dev são ~34k linhas em `210`, ~7k em `110` e ~0,4k em `000`. Excluir
`V4010` do strip mudaria dado de produção já publicado — decisão do time é **não** normalizar
por ora. Ver issue **#1699**.

## microdados — strip de zero à esquerda

O `br_ibge_pnadc__microdados.sql` fecha com um bloco que remove o zero à esquerda das colunas
`V*` de tipo STRING (`'05'` → `'5'`). É ele que alinha o dado com as chaves do
`br_ibge_pnadc__dicionario`, e ele cobre as duas famílias de coluna:

- as de `COLUNAS_STRIP_ZERO`, cuja chave o `build_dicionario` normaliza (`lstrip`);
- as que o `.xls` do IBGE já publica sem zero (`VD2002`, `VD4009`, `VD4010`, `VD4011`), onde
  não há normalização nenhuma do lado do dicionário.

O arquivo de largura fixa do IBGE traz os campos com zero à esquerda em toda a série; o strip
é o que mantém o histórico consistente desde 2012.

**O bloco depende de as colunas estarem em maiúscula.** `adapter.get_columns_in_relation`
devolve os nomes como estão na tabela, e as colunas `V*` existem em MAIÚSCULA (`V2005`,
`VD4010`) — só as que o modelo cria minúsculas (`ano`, `capital`, `rm_ride`) vêm assim. É isso que
faz o `column.name.startswith("V")` casar. Remover o bloco por parecer que ele não faz nada
quebra o `custom_dictionary_coverage`.

Mexer nesse `select` **só produz efeito depois**: o modelo é `incremental`, então a alteração
não toca nenhuma linha já materializada e só aparece quando entra um trimestre novo. Um `dbt
run`/`dbt test` verde logo depois da mudança não prova que ela está correta.

## microdados — um trimestre por execução, e o que isso implica

`get_data_source_date_and_url` monta a URL com o ano no caminho
(`.../Microdados/{year}/`) e escolhe **um** arquivo dentro da pasta: o de data de
modificação mais recente. `get_extraction_year` devolve o ano corrente (o anterior, entre
janeiro e abril). Não há laço por ano nem por trimestre.

Daí a consequência estrutural: **quando o ano vira, o trimestre que ficou para trás na
pasta do ano anterior sai do alcance do flow para sempre.** Não é um run que falhou — nenhum
run futuro consegue pedir aquele arquivo. Foi o que aconteceu com 2024 T3 e T4: a carga em
massa de set/2024 parou no T2 (o T3 ainda não existia), os runs voltaram só em out/2025, e o
ano corrente já era 2025.

### Nomes de arquivo no FTP mudam quando o IBGE revisa

A pasta de um ano traz `PNADC_{QQ}{AAAA}.zip`, mas o IBGE **acrescenta a data da revisão ao
nome quando republica** um trimestre. A pasta de 2024 hoje é assim:

```text
PNADC_012024_20250815.zip
PNADC_022024_20260324.zip   revisado em mar/2026
PNADC_032024_20250815.zip
PNADC_042024_20250815.zip
```

Enquanto 2025 e 2026 têm nome limpo. Qualquer seleção por trimestre precisa casar por
**prefixo** `PNADC_{QQ}{AAAA}`, nunca por nome exato.

Revisão também não é capturada pelo modelo: o predicado incremental só aceita trimestre
acima do máximo já materializado, então um trimestre republicado é ignorado. Para
reprocessá-lo é preciso apagar a partição antes de rodar.

## As duas stagings são independentes

O flow escreve nos dois buckets em passos separados, e o de prod fica atrás da porta do
`materialize_after_dump`:

```python
upload_to_gcs(..., bucket_name="basedosdados-dev", ...)   # sempre
if not materialize_after_dump:
    return
upload_to_gcs(..., bucket_name="basedosdados", ...)       # só depois da porta
```

Então um run com `materialize_after_dump=False` deposita o trimestre só em dev, e as duas
divergem. Conferir a contagem de partições nos dois buckets antes de qualquer
`--full-refresh` — a listagem de pastas é a fonte da verdade sobre as colunas `ano` e
`trimestre` da external table, e custa zero:

```bash
gcloud storage ls --billing-project=basedosdados-dev \
  "gs://<bucket>/staging/br_ibge_pnadc/microdados/ano=*/" | grep -c 'trimestre='
```

As duas trazem zero à esquerda só a partir do 4º trimestre de 2021 — antes disso o dado já
foi gravado sem ele. Por consequência o corte de zero à esquerda do modelo é **idempotente**
na série antiga: reconstruir do zero reproduz os mesmos valores, `V4010` incluído.

## Full-refresh: só com a staging completa, e antes dos metadados

`--full-refresh` reconstrói a tabela a partir da staging, e nada mais. Trimestre que está na
tabela mas não na staging **desaparece** — não é o storage que perde dado, é a tabela que é
refeita a partir dele.

E o full-refresh **não reaplica row access policies**. Numa tabela `part_bdpro` como esta,
rodar o full-refresh depois do run de metadados derruba o paywall e expõe a janela BD Pro,
sem nenhum sinal de erro. A ordem é full-refresh primeiro, run com `update_metadata=True`
depois.

Sem credencial local para prod: deployment "BD template: Executa DBT model" com
`flags="--full-refresh"`.

## Pendências

- **Flow do dicionário:** `materialize_after_dump` nasce `False` em
  `br_ibge_pnadc__dicionario`, então prod não recebe dicionário novo desde a migração para o
  Prefect 3.
- **BD Pro:** `microdados` é `PartBdpro` (janela móvel, `free_lag` de 6 meses). A Coverage
  **pro** (`is_closed=True`) e seu DateTimeRange existem em prod, então
  `assert_coverage_topology` passa. O primeiro run com `update_metadata=True` encurta a
  cobertura free para 2025-09 e aplica as row access policies pela primeira vez. Slug no
  backend é `pnadc`, não `br_ibge_pnadc`.
