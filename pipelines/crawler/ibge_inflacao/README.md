# Crawler IBGE Inflação (IPCA / INPC)

Este crawler é compartilhado pelos datasets `br_ibge_ipca` e `br_ibge_inpc`. Os flows em `pipelines/datasets/br_ibge_ipca/flows.py` e `pipelines/datasets/br_ibge_inpc/flows.py` chamam o factory `_run_ibge_inflacao` definido em `flows.py` deste diretório.

## Como atualizar os dados

Os flows são acionados via deployment no Prefect 3 (pool `basedosdados-dev` para teste, `basedosdados` para produção). O fluxo de atualização padrão é:

1. O schedule cron dispara o flow em `target=prod`.
2. A task `check_if_data_is_outdated` consulta o backend Data Basis para descobrir o último mês disponível em `basedosdados.<dataset>.<tabela>` e compara com o mês corrente.
3. Se houver novo dado, o flow segue: baixa o JSON da API SIDRA, transforma em CSV, sobe pro GCS, materializa via dbt e atualiza metadados.
4. Se não houver novo dado, o flow termina cedo (early-return) sem chamar a API.

## Em relação ao parâmetro `force_run`

> **Atenção:** mantenha `force_run=false` (default) nestes flows.

O `force_run=true` ignora o guard `check_if_data_is_outdated` e força a chamada à API mesmo quando não há período novo para baixar. Para a API de agregados do IBGE (SIDRA, `servicodados.ibge.gov.br/api/v3/agregados`), isso é problemático por dois motivos:

1. **Janela inexistente.** Quando não há mês novo publicado, a janela calculada por `json_mes_brasil` pode pedir um período que ainda não existe na API. O retorno vem sem o bloco `resultados` esperado, e o crawler estoura com `IndexError: list index out of range` em `utils.py:238`.
2. **Limite de 100.000 valores por requisição.** A API impõe um teto rígido por chamada (ver FAQ abaixo). Forçar runs fora do schedule aumenta o risco de bater esse limite e receber `HTTP 500`.

Em produção, sempre deixe o guard decidir se há trabalho. Use `force_run=true` apenas para debug local com janela controlada (após patch que torne `json_mes_brasil` defensivo contra `resultados` vazio).

## FAQ — A API de agregados impõe alguma restrição à requisição?

**Sim.** Citação direta da [documentação oficial](https://servicodados.ibge.gov.br/api/docs/agregados?versao=3):

> Para *Variáveis por agregado, períodos pesquisados e identificador da variável* e *Variáveis por agregado e identificador da variável*, cada requisição permite retornar no máximo **100.000 valores**. Para saber a quantidade de valores retornados, use a seguinte fórmula:
>
> `Nº de categorias × Nº de períodos × Nº de localidades <= 100.000`

### Exemplo

URL: `https://servicodados.ibge.gov.br/api/v3/agregados/2654/variaveis?classificacao=244[0]|1836[26877,99818]|2[4,5]|260[5965]&localidades=N6[1100015,1100023,1100031,1100049]`

| Dimensão | Valores | Quantidade |
|---|---|---|
| Classificação 244 | `[0]` | 1 categoria |
| Classificação 1836 | `[26877, 99818]` | 2 categorias |
| Classificação 2 | `[4, 5]` | 2 categorias |
| Classificação 260 | `[5965]` | 1 categoria |
| Períodos | últimos 6 (padrão quando não informado) | 6 |
| Localidades | `1100015, 1100023, 1100031, 1100049` | 4 |

Total: `(1 × 2 × 2 × 1) × 6 × 4 = 96` valores.

> Se a requisição exceder 100.000 valores, a API retorna **HTTP 500**.

### Implicação prática para este crawler

- As tabelas `mes_categoria_municipio` chegam perto do limite: `categorias × meses × municípios` cresce rápido.
- O crawler chunka as requisições por período para se manter abaixo do teto. Ao alterar o cálculo da janela ou o conjunto de localidades, recalcule a estimativa antes de subir.
- Em caso de `HTTP 500` em produção, a primeira suspeita é estouro do teto — não retry cego.
