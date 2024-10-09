
# Template Pull Requests - Pipeline

## Nomeação do Pull Request

A nomeação de cada Pull Request (PR) deve seguir o seguinte padrão:

- O título de cada Pull Request (PR) deve começar com uma das seguintes palavras-chave, entre colchetes. Além disso, **marque a palavra-chave que melhor descreve o seu PR atual**:
  - [ ]  **[Feature]**: Para novas funcionalidades.
  - [ ]  **[Data]**: Para subida de novos dados em produção.
  - [ ]  **[Bugfix]**: Para correções de bugs.
  - [ ]  **[Refactor]**: Para mudanças no código que não alteram a funcionalidade.
  - [ ]  **[Docs]**: Para atualizações na documentação.
  - [ ]  **[Test]**: Para mudanças relacionadas a testes.
  - [ ]  **[Chore]**: Para tarefas menores e de manutenção.
  - [ ]  **[Deactivate]**: Para desativar o schedule da Pipeline
---
  - Exemplos de título:
    - **[docs] br_me_caged**
    - **[Feature] br_cgu_servidores_publicos**

## Draft:
- Ao abrir o PR, deverá coloca-lo como draft

## Descrição do PR:

- Explique de maneira clara e concisa o objetivo deste PR. O que foi alterado? Qual o problema que ele resolve?
    - **Motivação/Contexto:** <!-- Qual a necessidade dessa mudança? -->


## Detalhes Técnicos:

- Detalhe as mudanças mais técnicas, como ajustes na pipeline, scripts ou modelo de dados utilizado.

    - **Principais alterações na pipeline/scripts:** <!-- Cite as principais mudanças na pipeline/script -->
    - **Mudanças nos dados e no schema:** <!-- Cite se há mudanças nos dados e no schema -->
    - **Impacto no desempenho:** <!-- Cite os impactos de desempenho dessas mudanças -->

## Teste e Validações:

- Relate os testes e validações relacionado aos dados/script:
  - [ ]  Testado localmente
  - [ ]  Testado na Cloud

  **Caso haja algo relacionado aos testes que vale a pena informar:** <!-- Cite informações sobre os testes? -->

## Riscos e Mitigações:
- Identifique os riscos potenciais desta mudança e como mitigar esses Riscos

    - Riscos conhecidos: <!-- Cite problemas que podem surgir -->
    - Planos de rollback: <!-- Explique como reverter as mudanças -->

## Dependencias:
- Liste quaisquer dependências externas, como bibliotecas, outros PRs ou mudanças que precisam ser feitas antes deste merge.
    - [ ]  Dependências: <!-- Cite dependencias, bibliotecas e outros PRs que são relacionados a esse Pull Requests antes de mergear -->
    - [ ]  Nenhuma dependencias adicional

## Revisão do código:
- Relate se há algum ponto no código (função, async, download e etc) que precisa de maior atenção:

    - **Ponto de atenção:** <!-- Cite o nome do arquivo, nome da função, número da linha -->
        - Exemplo: task.py, download_file_async, linha: 48


## Revisadores:
- Quando o PR estiver pronto para ser revisado, retire o **Draft** através do **Ready for reviews**, marque os revisadores de repositório, envie o PR no nosso [discord](https://discord.gg/V3yTWRYWZZ) na aba **Correções de PRs, arquiteturas e afins** e marque a **@equipe_dados**:
    - Revisadores recomendados:
        - @tricktx
        - @folhesgabriel
        - @laura-l-amaral
        - @aspeddro
        - @Winzen
        - @vilelaluiza
