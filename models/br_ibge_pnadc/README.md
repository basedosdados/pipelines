# br_ibge_pnadc — notas de manutenção

PNAD Contínua trimestral (IBGE). Tabelas: `microdados`, `educacao`,
`rendimentos_outras_fontes` e `dicionario`.

## Dicionário — reconciliação de cobertura

O teste `custom_dictionary_coverage` de `microdados` compara os valores do dado com
as `chave` do dicionário (por `id_tabela` + `nome_coluna`). O dicionário é gerado
hoje **fora do repo** (`code/dicionario_pnadc.py`, apontando para
`/home/ubuntu/upload`), então a reconciliação é feita na materialização do modelo
`br_ibge_pnadc__dicionario.sql`. Decisões:

1. **Casing** — o dicionário guarda `Capital`/`RM_RIDE` (nome original do IBGE); a
   lista do teste usava minúsculas. Corrigido em `columns_covered_by_dictionary`.
2. **Bloco V3 (escolaridade)** — as colunas `V3001`–`V3014` existiam só sob
   `id_tabela='educacao'`, mas `microdados` também as contém. O modelo duplica
   essas linhas sob `id_tabela='microdados'` (a mesma decodificação serve as duas
   tabelas).
3. **Zero à esquerda** — colunas codificadas (`V2005`, `V4072`, `V4074A` e as V3
   acima) têm chave `01`–`09` no dicionário, mas o dado guarda dígito único `1`–`9`.
   O modelo aplica `ltrim(chave, '0')` nessas colunas (lista `colunas_strip_zero`).

### V4010 (código de ocupação) fora da cobertura — #1699

`V4010` foi **removido** do `custom_dictionary_coverage`. É código de ocupação
hierárquico (COD/IBGE), onde o zero à esquerda é **semântico** (`0110` ≠ `110`),
então não pode ser stripado como as demais sem colidir códigos e achatar a
hierarquia (GG/SG/SUB/GB). Em dev, 3 códigos de forças armadas do dado (`210` ≈ 34k
linhas, `110` ≈ 7k, `000` ≈ 0,4k) não têm par no dicionário, que tem os padded
`0210`/`0110`/`0`. Correção definitiva pertence à pipeline do dicionário — ver
issue **#1699**.

## microdados — strip de zero removido

O `br_ibge_pnadc__microdados.sql` tinha um bloco que "limpava" zero à esquerda de
colunas `V*`, mas era **código morto**: `column.name.startswith("V")` nunca casava
porque `get_columns_in_relation` devolve nomes minúsculos. Removido; a normalização
de zero à esquerda passou a ser responsabilidade do modelo do dicionário.

## Pendências

- **Escopo B — pipeline do dicionário:** portar `code/dicionario_pnadc.py` (hoje
  aponta para `/home/ubuntu/upload`, não reproduzível) para uma pipeline in-repo que
  baixa a documentação do IBGE e gera o dicionário (V3 correto, sem zero à esquerda,
  ocupação V4010 normalizada). Resolve #1699.
- **BD Pro:** permissionamento com janela móvel de 6 meses em `microdados`.
