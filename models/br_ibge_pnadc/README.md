# br_ibge_pnadc — notas de manutenção

PNAD Contínua trimestral (IBGE). Tabelas: `microdados`, `educacao`,
`rendimentos_outras_fontes` e `dicionario`.

## Dicionário — pipeline in-repo

O dicionário é gerado por uma pipeline **dentro do repo**:

- `pipelines/datasets/br_ibge_pnadc/utils.py` — funções puras (download + parsing +
  correções), sem Prefect. `build_dicionario()` orquestra tudo.
- `pipelines/datasets/br_ibge_pnadc/tasks.py` / `flows.py` — flow
  `br_ibge_pnadc__dicionario` (quinzenal) que reconstrói o dicionário, sobe para o
  staging (`dump_mode="overwrite"`) e materializa via dbt.

Por isso o modelo `br_ibge_pnadc__dicionario.sql` é um **passthrough** simples
(`safe_cast` das 5 colunas) — todas as correções de domínio ficam no `build_dicionario`,
não no SQL.

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
(`0110` ≠ `110`) — stripar colidiria códigos e achataria a hierarquia (GG/SG/SUB/GB). Em dev,
3 códigos de forças armadas do dado (`210` ≈ 34k linhas, `110` ≈ 7k, `000` ≈ 0,4k) não têm par
no dicionário, que tem os padded `0210`/`0110`/`0`. O padding inconsistente vem do **dado do
microdados**, não do dicionário; corrigi-lo alteraria dado de produção — decisão do time é
**não** normalizar por ora. Ver issue **#1699**.

## microdados — strip de zero removido

O `br_ibge_pnadc__microdados.sql` tinha um bloco que "limpava" zero à esquerda de colunas
`V*`, mas era **código morto**: `column.name.startswith("V")` nunca casava porque
`get_columns_in_relation` devolve nomes minúsculos. Removido; a normalização de zero à
esquerda é responsabilidade do `build_dicionario`.

## Pendências

- **BD Pro:** `microdados` passa a `PartBdpro` (janela móvel, `free_lag` de 6 meses). Antes de
  ativar o agendamento é preciso criar a Coverage **pro** (`is_closed=True`) + DateTimeRange na
  tabela, senão `assert_coverage_topology` dá hard-fail. Slug no backend é `pnadc` (não
  `br_ibge_pnadc`).
