# Base de Dados — Diretórios Brasileiros

## 1. Visão geral

Conjunto de tabelas de diretório: entidades com chave primária (`municipio`,
`uf`, `escola`, `cbo_2002`, `cnae_2`, …) usadas como referência pelos demais
conjuntos da Base dos Dados. Diretório não tem cobertura temporal — é a tabela
que qualquer ano dos dados usa para resolver um `id_`.

- `gcp_dataset_id`: `br_bd_diretorios_brasil`
- slug no backend: `diretorios_brasil` (sem o prefixo `br_bd_`)
- 24 tabelas, nenhuma com flow Prefect

## 2. Como cada tabela é atualizada

| Tabela | Código no repo |
|---|---|
| `escola` | `code/update_escola.py` + `pipelines/datasets/br_bd_diretorios_brasil/utils.py` |
| `cid_10` | `code/cid_10.py` |
| `cnae_2` | `code/cnae_2.py` |
| `instituicao_ensino_superior` | `code/[update]instituicao_ensino_superior.ipynb` |
| demais | sem código no repo; carga manual |

## 3. escola

### Fonte

Catálogo de Escolas do Inep, servido por um portal OBIEE em
`https://anonymousdata.inep.gov.br/analytics/saw.dll`. A ação `Extract` aceita
os cookies anônimos que o portal entrega no primeiro GET, sem login. O download
é feito por `curl` em subprocesso porque o servidor derruba a conexão TLS
aberta pelo `ssl` do Python.

A tabela não tem fonte original registrada no backend — as duas fontes do
conjunto são do IBGE e atendem `municipio`, `cep` e `setor_censitario`.

### O catálogo é o registro corrente, não o histórico

O catálogo traz as escolas que estão no registro do Inep hoje, e é o próprio
Inep que remove do registro as extintas em anos anteriores. Carregar o catálogo
puro, substituindo a tabela, apagaria do diretório as escolas removidas — cujo
`id_escola` continua aparecendo no censo escolar de anos anteriores.

Por isso a carga **une** o catálogo ao diretório já publicado
(`fetch_diretorio_publicado`) e registra o resultado por escola na coluna
`situacao_catalogo`:

| Valor | Significado |
|---|---|
| `Presente` | consta no catálogo mais recente |
| `Ausente` | saiu do registro do Inep; a linha é mantida com os atributos da última vez em que apareceu |

Consequência a conhecer: a memória do que já existiu vive na tabela publicada.
Rodar `clean_catalogo` sem passar `diretorio_publicado` reduz a tabela ao
catálogo do dia — o aviso no log é a única proteção. A alternativa seria montar
o universo a partir de `br_inep_censo_escolar.escola` (2007–2024, 295.916
`id_escola` distintos), que sobrevive a qualquer recarga, mas não traz nome,
endereço nem coordenadas.

### id_municipio

O catálogo traz o nome do município, não o código do IBGE, então `id_municipio`
é derivado de (nome, UF) contra o diretório `municipio`, em duas passagens:
`_MUNICIPIO_NAME_FIXES` para as divergências de grafia conhecidas (renomeações,
hífen, `z`/`s`), depois busca com o nome normalizado sem acento. Nome que não
resolve fica nulo, e a carga registra a lista no log.

### Atualização

```bash
uv run python models/br_bd_diretorios_brasil/code/update_escola.py --upload
uv run dbt run --select br_bd_diretorios_brasil__escola
uv run dbt test --select br_bd_diretorios_brasil__escola
```

O script baixa o catálogo (~85 MB), lê o diretório publicado e o diretório
`municipio` do BigQuery, grava o parquet e substitui a staging de
`basedosdados-dev`. `input/` e `output/` são gitignored.

Depois da carga, confira na tabela em dev:

| Conferência | Valor |
|---|---|
| total | soma das linhas já publicadas com as escolas novas do catálogo |
| `situacao_catalogo = 'Ausente'` | escolas que saíram do registro; deve bater com o anti-join contra a tabela publicada |
| `id_municipio` nulo | 0 |
