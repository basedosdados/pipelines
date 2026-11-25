# br_bndes_estatisticas_operacoes

Aprovações e consultas do Sistema BNDES, por mês, desde janeiro de 1995, em 28 tabelas: 14 recortes
de aprovações e os mesmos 14 de consultas, publicados no Portal de Dados Abertos do BNDES (CKAN).

## Fonte

Pacotes CKAN [`aprovacoes`](https://dadosabertos.bndes.gov.br/dataset/aprovacoes) e
[`consultas`](https://dadosabertos.bndes.gov.br/dataset/consultas). Cada recorte é um CSV com o
histórico inteiro, republicado inteiro a cada versão. O CKAN declara atualização trimestral.

- Windows-1252, `;` como separador e vírgula decimal, como dizem as descrições dos recursos.
- Formato largo: uma linha por mês e uma coluna por categoria do recorte (`acre`, `amapa`… no
  recorte por UF).
- Valores em R$ milhões nos recortes em real e em US$ mil nos recortes em dólar.
- O dicionário de cada pacote é um PDF no próprio pacote e fica na fonte. Ele define os campos (forma
  de apoio, produto, porte, UF, setores, BNDES Exim), mas não lista as categorias de nenhum.
- O mapeamento BNDES → CNAE dos dois pacotes é o mesmo arquivo da tabela
  `br_bndes_desembolsos.mapeamento_bndes_cnae`.
- O portal recusa conexões do cluster: toda chamada passa pelo proxy brasileiro
  (`brasil_proxy_url()`).

## Tabelas

O nome tem a entidade do recorte primeiro e a medida depois; os recortes em dólar terminam em `_dolar`.

| Recorte | Aprovações | Consultas | Colunas de categoria |
|---|---|---|---|
| Forma de apoio direta, por produto | `produto_direta_aprovacao` | `produto_direta_consulta` | `produto` |
| Forma de apoio indireta, por produto | `produto_indireta_aprovacao` | `produto_indireta_consulta` | `produto` |
| Porte da empresa | `porte_aprovacao` | `porte_consulta` | `porte_empresa` |
| UF | `uf_aprovacao` | `uf_consulta` | `sigla_uf` |
| Setor CNAE | `setor_cnae_aprovacao` | `setor_cnae_consulta` | `setor_cnae` |
| Subsetor CNAE agrupado | `subsetor_cnae_aprovacao` | `subsetor_cnae_consulta` | `setor_cnae`, `subsetor_cnae_agrupado` |
| Setor BNDES | `setor_bndes_aprovacao` | `setor_bndes_consulta` | `setor_bndes` |
| Subsetor BNDES | `subsetor_bndes_aprovacao` | `subsetor_bndes_consulta` | `setor_bndes`, `subsetor_bndes` |
| BNDES Exim | `exim_aprovacao` | `exim_consulta` | `modalidade_exim` |
| Forma de apoio direta, por produto, em dólar | `produto_direta_aprovacao_dolar` | `produto_direta_consulta_dolar` | `produto` |
| Forma de apoio indireta, por produto, em dólar | `produto_indireta_aprovacao_dolar` | `produto_indireta_consulta_dolar` | `produto` |
| BNDES Exim, em dólar | `exim_aprovacao_dolar` | `exim_consulta_dolar` | `modalidade_exim` |
| BNDES Exim por setor CNAE, em dólar | `exim_setor_cnae_aprovacao_dolar` | `exim_setor_cnae_consulta_dolar` | `setor_cnae` |
| BNDES Exim por subsetor CNAE agrupado, em dólar | `exim_subsetor_cnae_aprovacao_dolar` | `exim_subsetor_cnae_consulta_dolar` | `setor_cnae`, `subsetor_cnae_agrupado` |

Toda tabela tem `ano`, `mes`, as colunas de categoria e o valor: `valor_aprovado` nas aprovações e
`valor_consulta` nas consultas, em BRL ou em USD. A chave é `ano`, `mes` e as colunas de categoria. A
partição é por `ano`. Todas são abertas (`all_free`): o CKAN declara publicação trimestral, abaixo do
limite mensal a partir do qual a BD reserva a janela mais recente ao BD Pro.

## Do formato largo para o longo

Cada célula do CSV vira uma linha. Nenhuma linha é criada, apagada ou somada, e os zeros da fonte
ficam. Um mês de 45 subsetores vira 45 linhas.

- **Valor.** R$ milhões × 1.000.000 e US$ mil × 1.000, sem arredondar. A conta é feita em `Decimal`
  sobre o texto da fonte: `3404,60163400001` vira `3404601634.00001`, com a fração de centavo que já
  está na fonte.
- **Rótulos.** O nome da coluna vira um rótulo com inicial maiúscula e o resto minúsculo, na grafia do
  `br_bndes_desembolsos.mensal` e do `mapeamento_bndes_cnae`: `bndes_finem` vira "Bndes finem",
  `equip_info_eletronico_otico` vira "Equip info, eletrônico, ótico". As abreviações da fonte ficam
  ("Ativ aux transporte e entrega"). A UF vira a sigla (`sao_paulo` vira `SP`), e o EXIM vira
  `modalidade_exim` ("Pré-embarque", "Pós-embarque"). O de-para de cada recorte está em
  `constants.py`; coluna da fonte sem rótulo lá interrompe a limpeza.
- **Setor nas tabelas por subsetor.** As tabelas por subsetor CNAE e por subsetor BNDES levam também o
  setor, tirado do mapeamento BNDES → CNAE, como o `desembolsos.mensal`. No mapeamento, cada subsetor
  CNAE tem um setor só.

### Setor do subsetor BNDES

O CSV por subsetor BNDES põe o setor no nome da coluna (`mecanica_industria`,
`construcao_infraestrutura`). Duas colunas, `agropecuaria_industria` e `comercio_e_servicos_industria`,
dão o setor como Indústria. A tabela usa o setor do mapeamento, que dá Agropecuária e Comércio e
serviços como setores próprios, porque é o que concorda com o resto da fonte:

- no CSV por setor BNDES, `industria` é a soma das colunas `_industria` sem essas duas, e `agropecuaria`
  e `comercio_e_servicos` são essas duas, nos 375 meses de aprovações e de consultas;
- no mapeamento BNDES → CNAE, os dois são setores próprios;
- no `desembolsos.mensal`, as linhas do subsetor Agropecuária estão no setor Agropecuária, e as de
  Comércio e serviços no setor Comércio e serviços.

Por que o nome da coluna diz Indústria, a fonte não explica.

## O que a fonte publica e a tabela mantém

Contagens do arquivo de 13/05/2026, de 1995-01 a 2026-03.

- As tabelas de EXIM não têm linha em 2015-02, 2016-02, 2021-01 e 2021-04 (aprovações) nem em 2014-12,
  2015-01, 2021-01, 2021-03 e 2021-04 (consultas). A fonte não diz por quê.
- Os CSVs de EXIM por subsetor CNAE não têm as colunas de 10 dos 45 subsetores, todos de Comércio e
  serviços (Educação, Transporte terrestre e outros). Por isso, nas tabelas `exim_subsetor_cnae_*`, a soma de Comércio e
  serviços fica abaixo da tabela `exim_setor_cnae_*` correspondente em 42 meses de aprovações e 17 de
  consultas. Nas tabelas comuns por subsetor CNAE, a soma por setor fecha com a tabela por setor em
  todos os meses.
- Os recortes em dólar têm produtos que os em real não têm: Bndes máquinas e serviços nas diretas e
  Bndes finame nas indiretas.

## Pipeline

Cada tabela tem um `check_update` e um `extract_and_load`; o `build_and_promote` é o genérico,
compartilhado por todos os conjuntos. Os 28 `check_update` rodam todo dia, de 0h05 a 3h05 (America/Sao_Paulo),
cinco minutos um do outro.

### Detecção de novidade

O `check_update` lê o `last_modified` do recurso no CKAN, uma chamada só ao `resource_show`, e compara
com a "Última atualização na Base dos Dados" (`Table.Update.latest`), via
`compare_against="table_update"`. O `last_modified` muda quando o arquivo é substituído, não quando
chega um mês novo, então uma correção sem mês novo também dispara a carga.

No primeiro registro de metadados, `Table.Update.latest` precisa ficar antes do `last_modified` do
recurso; do contrário, o primeiro poll não enxerga novidade.

### Carga

O `extract_and_load` baixa o CSV, confere o MD5 com o `hash` do recurso no CKAN, passa para o formato
longo e grava parquet particionado por `ano`, todo em texto. Como a fonte republica o histórico
inteiro, a staging é reescrita (`dump_mode="overwrite"`) e todas as partições são promovidas.
