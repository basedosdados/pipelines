# Template Pull Requests - Pipeline

> **Atenção:** o texto deste template é um guia para o Pull Request — **não o copie como mensagem de commit**. Cada commit deve ter sua própria mensagem descritiva.

Boas mensagens de commit facilitam a revisão, o entendimento do histórico e o trabalho em equipe. As seções abaixo são para ajudar a descrever o pull request, você não precisa descrever todas. O mais importante é:

- Nomeação do pull request
- Descrição e explicação de maneira clara e concisa o objetivo desse PR
  - O que mudou?
  - Porque mudou?

## Nomeação do Pull Request

A nomeação de cada Pull Request (PR) deve seguir o seguinte padrão:

- O título de cada Pull Request (PR) deve começar com uma das seguintes palavras-chave, entre colchetes.
  - **[Feature]**: Para novas funcionalidades.
  - **[Data]**: Para subida de novos dados em produção.
  - **[Bugfix]**: Para correções de bugs.
  - **[Refactor]**: Para mudanças no código que não alteram a funcionalidade.
  - **[Docs]**: Para atualizações na documentação.
  - **[Test]**: Para mudanças relacionadas a testes.
  - **[Chore]**: Para tarefas menores e de manutenção.
  - **[Deactivate]**: Para desativar o schedule da Pipeline
---

- Exemplos de título:
  - **[Docs] br_me_caged: add tables descriptions**
  - **[Feature] br_cgu_servidores_publicos add new column**

## Draft:

- Ao abrir o PR, deverá coloca-lo como draft

## Descrição do PR:

- Explique de maneira clara e concisa o objetivo deste PR. Qual o problema que ele resolve? Porque mudou?
  - **Motivação/Contexto:** <!-- Qual a necessidade dessa mudança? -->

## Detalhes Técnicos:

- Detalhe as mudanças mais técnicas, como ajustes na pipeline, scripts ou modelo de dados utilizado.
  - **Principais alterações na pipeline/scripts:** <!-- Cite as principais mudanças na pipeline/script -->
  - **Mudanças nos dados e no schema:** <!-- Cite se há mudanças nos dados e no schema -->
  - **Impacto no desempenho:** <!-- Cite os impactos de desempenho dessas mudanças -->

- Se alguma parte do código precisar de alguma atenção a mais, comente na linha sinalizando para os revisores.

## Teste e Validações:

- Relate os testes e validações relacionado aos dados/script:
  - Testado localmente
  - Testado na Cloud

  **Caso haja algo relacionado aos testes que vale a pena informar:** <!-- Cite informações sobre os testes? -->

## Riscos e Mitigações:

- Identifique os riscos potenciais desta mudança e como mitigar esses Riscos
  - Riscos conhecidos: <!-- Cite problemas que podem surgir -->
  - Planos de rollback: <!-- Explique como reverter as mudanças -->

## Dependências:

- Liste quaisquer dependências externas, como bibliotecas, outros PRs ou mudanças que precisam ser feitas antes deste merge.
  - Dependências: <!-- Cite dependências, bibliotecas e outros PRs que são relacionados a esse Pull Requests antes de mergear -->
  - Nenhuma dependencias adicional


## Revisadores:
- Quando o PR estiver pronto para ser revisado, retire o **Draft** através do **Ready for reviews**, marque os revisadores de repositório, envie o PR no nosso [discord](https://discord.gg/V3yTWRYWZZ) na aba **Correções de PRs, arquiteturas e afins** e marque a **@equipe_dados**:
  - Revisadores recomendados no github:
    - basedosdados/dados
