<p align="center">
    <a href="https://basedosdados.org">
        <img src="https://storage.googleapis.com/basedosdados-website/logos/bd_minilogo.png" width="340" alt="Base dos Dados">
    </a>
</p>

<p align="center">
    <em>Universalizando o acesso a dados de qualidade.</em>
</p>

# Pipelines

Esse repositório contém fluxos de captura e subida de dados no datalake da Base dos Dados.

## Ingestão assistida por IA

Para fazer o onboarding de um novo conjunto de dados, inicie o agente `orchestrator` com o prompt abaixo. Preencha os campos — quanto mais contexto você fornecer, menos perguntas o agente fará.

```text
Onboard dataset <dataset_slug>.
Sources: <URL or local path to raw files — e.g. https://dados.gov.br/... or ~/Downloads/dados_xyz/>
Drive folder: BD/Dados/Conjuntos/<dataset_slug>/
Architecture suggestion: <brief description — e.g. "one table per year, annual updates, municipal level">
Organization: <source organization name — e.g. "IBGE", "Ministério do Meio Ambiente">
Notes: <anything unusual — e.g. "data is in wide format", "files split by state", "API requires auth">
```

O agente executa uma sequência de 11 etapas (contexto → arquitetura → download → limpeza → upload → dbt → validação → IDs → metadados dev → aprovação → metadados prod → PR), pausando para aprovação humana antes de promover para produção.

## 👥 Como contribuir

Leia nosso [guia de contribuição](./CONTRIBUTING.md)

## 🗺️ Contatos

Sinta-se livre para entrar em contato com projeto por nossas redes sociais.
<br />
Fortemente te incetivamos a participar da nossa comunidade pelo [discord][discord-invite], onde você pode ter um gostinho de toda a [Base Dos Dados][bd]. 🫶

[![][img-discord]][discord-invite]
[![][img-linke]][linkedin]
[![][img-x]][x]
[![][img-youtube]][youtube]

<!-- Referencias -->

[img-discord]: https://img.shields.io/badge/Discord-Comunidade-blue?style=for-the-badge&logo=Discord
[img-x]: https://img.shields.io/badge/Fique%20por%20dentro-blue?style=for-the-badge&logo=x
[img-youtube]: https://img.shields.io/badge/Youtube-Assista-red?style=for-the-badge&logo=youtube
[img-linke]: https://img.shields.io/badge/Linkedin-Acesse-blue?style=for-the-badge&logo=linkedin

[discord-invite]: https://discord.com/invite/huKWpsVYx4
[x]: https://twitter.com/basedosdados
[youtube]: https://www.youtube.com/c/BasedosDados
[bd]: https://basedosdados.org
[linkedin]: https://www.linkedin.com/company/base-dos-dados
