# IPTU de São Paulo (GeoSampa)

Pipeline que mantém atualizada a tabela `iptu` do conjunto
`br_sp_saopaulo_geosampa_iptu`, a partir do cadastro fiscal do IPTU publicado pela
Prefeitura de São Paulo no GeoSampa. Cada linha é um imóvel (número de contribuinte) em
um ano de exercício. A pipeline verifica toda semana se saiu um ano novo e, quando sai,
carrega só esse ano.

## Fonte

O [GeoSampa](https://geosampa.prefeitura.sp.gov.br/PaginasPublicas/_SBC.aspx) publica um
ZIP por ano de exercício, desde 1995, com um CSV separado por `;` (cerca de 1 GB por
ano). O endereço de cada ano está em `URL_TEMPLATE` (`constants.py`).

- **Um ano inexistente também responde HTTP 200**, com uma página HTML no lugar do ZIP.
  Por isso a pipeline confere se a resposta começa com `PK`, a assinatura de um ZIP.
- **O servidor exige headers de navegador** (`HEADERS`).
- **O formato muda entre os anos**: vírgula decimal em 1995, zeros à esquerda em 2024,
  arquivos em UTF-8 ou latin-1 e duas grafias da coluna de esquinas.
- **Aspas não delimitam campos.** Toda linha tem 29 campos separados por `;`, mas há
  valores com aspas só de um lado (`"D N LT 12`). Um leitor de CSV padrão junta as
  linhas seguintes até achar outra aspa; em 2024, isso perde 75.065 linhas.
- **A fonte não traz `data_cadastramento` de 1995 a 2015.**
- **Anos publicados não mudam.** Os arquivos baixados em novembro de 2026 são idênticos
  aos carregados em 2024 e 2025, então cada ano é carregado uma vez só.

## Como a pipeline funciona

```text
check_update  ──▶  extract_and_load  ──▶  build_and_promote
(segunda-feira)    (ano novo)              (dbt e publicação)
```

- **`check_update`** (`flows.py`, `tasks.py`): toda segunda-feira às 9h47, procura o
  ano mais recente publicado, registra a consulta à fonte e compara o ano com o fim da
  cobertura da tabela na BD. Se a fonte tiver um ano novo, dispara o próximo estágio.
- **`extract_and_load`**: baixa o ZIP do ano, trata o CSV (`utils.py`) e grava
  `ano=<ano>/data.parquet` no staging, com todas as colunas como texto.
- **`build_and_promote`**: estágio comum a todos os conjuntos da BD
  (`pipelines/utils/metadata/flows.py`). Roda o dbt
  (`models/br_sp_saopaulo_geosampa_iptu/`), publica em produção e atualiza a
  cobertura.

### Tratamento

O tratamento lê o CSV em blocos, sem interpretar aspas (`quoting=csv.QUOTE_NONE`), e:

- detecta a codificação de cada arquivo (UTF-8 ou latin-1);
- tira as aspas que envolvem um valor inteiro (`"AP 321 BL ""C"""` → `AP 321 BL "C"`)
  e os espaços nas pontas;
- troca a vírgula decimal por ponto e remove zeros à esquerda (`0001465,00` →
  `1465.00`, `,89` → `0.89`);
- remove o hífen de `cep` e `numero_contribuinte` e converte a data de `dd/mm/aa` para
  `aaaa-mm-dd`;
- grava nulo onde a fonte deixa o valor vazio.

A carga **falha sem gravar nada** quando uma linha não tem 29 campos, quando o número
de linhas gravadas difere do da fonte ou quando um valor foge do formato da sua coluna.
Essas travas vieram de defeitos achados no reprocessamento do histórico, que passavam
sem erro: linhas perdidas por causa das aspas e valores que viravam nulo no
`safe_cast` do dbt. Linhas com campos vazios a mais (uma em 1995) são cortadas para 29
campos.

## Cobertura BD Pro

A tabela é toda BD Pro (`all_bdpro`). A cobertura é lida do backend, a partir da
coluna `ano` marcada com `coverage_entity = year`, e o backend tem uma única cobertura,
BD Pro (`is_closed=true`). A cada ano novo, o `build_and_promote` estende a faixa até o
ano carregado. Nesse tipo de cobertura, a pipeline não cria Row Access Policies no
BigQuery.

Em novembro de 2026, a tabela chegou a ser configurada no padrão de deixar só o ano mais
recente em BD Pro (`part_bdpro`). A decisão foi revertida, e a tabela continua toda
BD Pro, como antes.

## Correção do histórico

Os anos de 1995 a 2025 tinham sido carregados por um script manual, em duas cargas
(julho de 2024 e dezembro de 2025). Em novembro de 2026, todos foram reprocessados com o tratamento desta pipeline e
comparados com o staging anterior, contribuinte a contribuinte.

| Problema no staging anterior | Correção |
|---|---|
| `valor_terreno` e `valor_construcao` nulos em cerca de 90% das linhas (1995–2009) ou dez vezes maiores (2010–2021), porque o script apagava todo `.0` | valores da fonte, como `FLOAT64` (a fonte tem centavos) |
| 2024 sem 75.065 das 3.724.575 linhas, por causa das aspas | ano completo |
| 2017 com 200 mil linhas repetidas no staging (a tabela final não era afetada) | sem repetições |

Os demais anos têm as mesmas linhas e os mesmos contribuintes de antes. Junto com a
correção, `quantidade_esquina_imovel` passou de `STRING` para `INT64`, como já estava
no backend, e a partição do modelo foi estendida até 2031.

## Operação

- **Carga inicial ou recarga de vários anos:** `models/br_sp_saopaulo_geosampa_iptu/code/iptu.py`
  usa o mesmo tratamento da pipeline e grava em
  `~/Downloads/br_sp_saopaulo_geosampa_iptu_data/` (ou em `IPTU_DATA_DIR`):
  `uv run models/br_sp_saopaulo_geosampa_iptu/code/iptu.py 2024 2025`.
- **Recarregar um ano pela pipeline:** disparar o deployment de `extract_and_load` com
  `{"download_params": {"reference_date": "<ano>-01-01"}}`.
- **Teste em dev:** com o label `deploy-flow` no PR, os flows vão para o pool de dev.
  O `build_and_promote` usa os modelos dbt da `main`; mudanças nos modelos de um PR
  devem ser validadas com `dbt run` e `dbt test` locais.
- **Metadados:** o `check_update` grava em produção, mesmo rodando em dev, o registro
  de consulta à fonte e a data da última publicação. Para testar sem tocar em
  produção, dispare direto o `extract_and_load`.
