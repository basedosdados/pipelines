# br_tse_eleicoes

Dados do Tribunal Superior Eleitoral (TSE) sobre eleições: candidaturas, bens
declarados, prestação de contas de campanha, resultados, perfil do eleitorado e
locais de votação.

O pipeline recorrente atualiza `candidatos`, `bens_candidato`, `despesas_candidato`
e `receitas_candidato` para o ano da constante `YEAR`, em `constants.py`, que é
trocada à mão a cada eleição.

## Fonte

ZIPs no CDN do TSE, sob `https://cdn.tse.jus.br/estatistica/sead/odsele/`:

| Tabela | ZIP | CSV |
| --- | --- | --- |
| `candidatos` | `consulta_cand/consulta_cand_{ano}.zip` | `consulta_cand_{ano}_BRASIL.csv` |
| `candidatos` | `consulta_cand_complementar/consulta_cand_complementar_{ano}.zip` | `consulta_cand_complementar_{ano}_BRASIL.csv` |
| `bens_candidato` | `bem_candidato/bem_candidato_{ano}.zip` | `bem_candidato_{ano}_BRASIL.csv` |
| `despesas_candidato` | `prestacao_contas/prestacao_de_contas_eleitorais_candidatos_{ano}.zip` | `despesas_contratadas_candidatos_{ano}_BRASIL.csv` |
| `receitas_candidato` | o mesmo de `despesas_candidato` | `receitas_candidatos_{ano}_BRASIL.csv` |

Cada ZIP traz um CSV por UF e um `*_BRASIL.csv`, que é o usado. Os CSVs usam `;`
como separador, codificação ISO-8859-1 e campos entre aspas.

### Bytes NUL no CSV de despesas de 2026

O campo `DS_DESPESA` da despesa de `SQ_DESPESA` 79947890 tem dois bytes NUL
(`\x00`), um no meio de "estratégica" e outro no meio de "política". São os únicos
NUL do arquivo.

| Leitor | Resultado na linha |
| --- | --- |
| `pd.read_csv`, engine C (padrão) | 53 colunas |
| módulo `csv` do Python | 53 colunas |
| polars | lê sem erro |
| `pd.read_csv(engine="pyarrow")` | 52 colunas, erro |
| `pyarrow.csv.read_csv` | 52 colunas, erro |

O leitor de CSV do Arrow conta 52 colunas nessa linha e para. O pandas levanta
`pandas.errors.ParserError` e o `pyarrow.csv.read_csv`, `pyarrow.lib.ArrowInvalid`,
os dois com a mesma mensagem:

```text
CSV parse error: Expected 53 columns, got 52
```

O `read_csv` do pipeline usa a engine C.

## Tratamento dos dados

### Comum

- Células iguais a `#NULO`, `#NULO#`, `#NE`, `NÃO DIVULGÁVEL`, `Não Divulgável`,
  `Não divulgável`, `-1`, `-4` ou `-3` ficam vazias, exceto em `bens_candidato`.
- Colunas de categoria ficam em minúsculas e sem acento.
- Datas `dd/mm/aaaa` viram `aaaa-mm-dd`; data inválida fica vazia.
- Linhas repetidas são removidas.
- Em `candidatos`, `despesas_candidato` e `receitas_candidato`, o `id_municipio`
  vem do diretório de municípios (`br_bd_diretorios_brasil.municipio`), casando
  `SG_UE`, sem zeros à esquerda, com `id_municipio_tse`. Em eleições estaduais e
  federais, `SG_UE` traz a sigla da UF, e a linha fica sem `id_municipio`.

### `candidatos`

- A consulta de candidaturas é unida ao arquivo complementar por `SQ_CANDIDATO`.
- `data_nascimento` fica vazia quando a idade (ano corrente menos ano de
  nascimento) está fora de 18 a 119 anos.
- `nome`, `nome_urna` e `municipio_nascimento` ficam com as iniciais maiúsculas.
- Em `nacionalidade`, `brasileira nata` vira `brasileira`.
- Em `instrucao`, valores com `superior` ganham o prefixo `ensino`.

### `bens_candidato`

- Só `tipo_eleicao` (minúsculas e sem acento) e `data_eleicao` (formato de data)
  são tratadas; valores como `#NULO` ficam como na fonte.
- O valor do bem é lido como número, com vírgula decimal.
- `titulo_eleitoral_candidato` fica vazia.

### `despesas_candidato` e `receitas_candidato`

- Os zeros à esquerda saem do código de município do fornecedor ou do doador.
- Em `tipo_eleicao`, `eleicoes municipais <ano>` vira `eleicao ordinaria`, exceto
  quando seguido de ` - `.
- A vírgula decimal do valor vira ponto.
- As colunas de `VAZIOS_DESPESA` e `VAZIOS_RECEITA` existem na tabela e ficam
  vazias, porque o CSV da fonte não as traz.

## Memória em `despesas_candidato`

Para caber no limite de memória do pod do `extract_and_load` (4Gi),
`despesas_candidato` é montada sem copiar a tabela inteira:

- lê só as 35 colunas do CSV que a tabela usa (`DespesasCandidato.source_columns()`);
  nas outras tabelas, `source_columns()` devolve `None` e todas as colunas são
  lidas;
- busca o `id_municipio` num dicionário `id_municipio_tse → id_municipio`, sem
  `merge`;
- monta a saída coluna a coluna e tira cada coluna de origem da tabela lida no seu
  último uso.

Com o CSV de 2026, o pico de memória fica em 3,11 GiB. Lendo todas as colunas e com
`merge`, chega a 4,10 GiB. O CSV gravado é o mesmo nos dois casos.
