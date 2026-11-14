---
name: 📝 rfc
about: Task descrita por inteiro, com fonte, tabelas de saída e critérios de aceite
title: '[rfc] '
---

<!--
Um documento só, lido pelo time e entregue ao agente como prompt.
Se a decisão mudar, mude aqui: não existe uma segunda versão "para a IA".
Cada seção serve aos dois leitores. Se alguma parece servir só a um deles,
falta precisão nela. Apague estes comentários ao preencher.
-->

## Problema

<!-- O que está errado hoje, por que precisa ser resolvido e por que agora. -->

## Contexto

<!--
Onde isto entra no sistema, que decisões anteriores pesam sobre esta e links
para RFCs relacionadas. Não conte com quem esteve na reunião: escreva para quem
chegou ao time na semana passada.
-->

## Fonte

<!--
O que muda de uma task para outra. A stack (Python, dbt, Prefect) já está no
AGENTS.md e nas rules; repetir aqui cria uma segunda fonte de verdade.
-->

- URL:
- Formato:
- Como baixar:
- Frequência de publicação:
- Problemas conhecidos:

## Solução proposta

<!-- A abordagem escolhida, por que esta e não outra, e os trade-offs aceitos. -->

## Restrições

<!--
O que não pode ser feito e o padrão que tem de ser seguido, escritos como regra.
"NÃO faça X (use Y)" é mais claro que "siga o padrão do projeto".
-->

- NÃO
-

## Fora do escopo

- O que este documento não resolve
- Funcionalidades relacionadas que ficam para depois
- Decisões adiadas de propósito

## Tabelas de saída

<!--
O contrato é a arquitetura: aponte para ela e resuma o essencial.
Repita o bloco para cada tabela.
-->

### `<gcp_dataset_id>.<tabela>`

- Uma linha é:
- Partição:
- Chave única (testada no dbt):
- Arquitetura: [link]
- Cobertura esperada:
- Acesso: free | part_bdpro

## Critérios de aceite

<!--
Para o time, é a definição de pronto; para o agente, é o sinal de parar.
Cada item tem de ser conferível no dado, com o número esperado quando houver.
-->

- [ ] `dbt test` passa em dev para todas as tabelas
- [ ] Contagem de linhas igual à da fonte em [recorte conhecido]: [N]
- [ ] Cobertura no backend: [início]–[fim]
- [ ] (se houver pipeline) Run em dev com `{"materialize_to_prod": false, "update_metadata": false, "force_run": true}` termina COMPLETED

## Estrutura de arquivos

<!--
Caminho exato de cada arquivo novo ou alterado, com uma linha dizendo o que
ele contém. Nada de "no lugar apropriado".
-->

```text
caminho/do/arquivo          # o que ele contém
tests/caminho/do/teste      # o que ele testa
```

## Referências

<!--
Para quem desenvolve, é ponto de partida; para o agente, é instrução.
-->

- Padrão a seguir: [caminho no repo de um flow ou modelo que já segue o padrão]

Linha bruta da fonte:

```text
```

Como ela fica depois de limpa:

```text
```
