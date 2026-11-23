# Credential request to TCE-MG — draft, not sent

**Status: DRAFT. Nobody has sent this. Confirm the destination address on the TCE-MG
portal before it goes anywhere — the addresses below are described, not asserted.**

## Why this file exists

Every byte of TCE-MG's SICOM bulk data sits behind a token that only a
reCAPTCHA-v3-protected page issues, and that token lives 120 minutes. There is no API
key, no documented service path and no unauthenticated mirror. So the MG half of
`world_wb_mides` cannot run unattended, and `download_mg.py` takes the token as an input
that a human pastes in.

That is a workaround. The fix is a credential, and asking for one is cheap: the data is
already public, already free, already published for bulk download, and the volume we want
is 15 requests a quarter.

## What to ask for, in order of preference

1. **A service credential** — an API key, or a long-lived `AuthorizationProxy` token,
   scoped to the three read-only endpoints listed below. This is the only option that
   makes the pipeline genuinely unattended.
2. **An egress-IP allowlist entry** that exempts one fixed address from the reCAPTCHA
   gate on those endpoints.
3. **A documented bulk mirror** — an S3 bucket, an FTP drop, or signed URLs regenerated
   per exercise. Slower to agree, but it removes the gateway from the path entirely.

If none is granted, MG degrades to a human-in-the-loop quarterly pull. That loses little:
the portal's own `datCarga` was 2026-09-11 and the per-category `dataAtualizacao` for
exercises 2025 and 2026 was 2026-08-07, so the source itself moves monthly at most.

## Where to send it

- The TCE-MG **Ouvidoria / e-SIC** channel, as a request under the Lei de Acesso à
  Informação (Lei 12.527/2011), whose art. 8 §3 requires publication in open,
  machine-readable formats. Framing it as an LAI request gives it a statutory response
  deadline rather than leaving it in a general inbox.
- In parallel, the open-data or SICOM technical contact published on
  `dadosabertos.tce.mg.gov.br`. **Look this up before sending; do not guess it.**

Ask for a named technical contact in the reply. The useful outcome is a conversation with
whoever administers the WSO2 gateway, not a ticket number.

## Technical annex to attach

Endpoints needed, all read-only, all `GET`. Base:

```
https://arabiasaudita.tce.mg.gov.br:8443/TCEMG-proxy-web/publico/wso2amgw/dados-abertos/dadosAbertos
```

| Endpoint | Purpose | Calls per quarter |
|---|---|---|
| `buscarEstatisticas` | read `datCarga` to decide whether anything changed | ~12 (one per week) |
| `buscarCategoriaDownload?exercicio=<Y>&origem=SICOM` | resolve the current `seqZip` for an exercise | 5 |
| `baixarArquivoPct/<seqZip>` | the whole-state package | 10 |

Nothing else is required. We do not need `buscarMunicipios`,
`buscarDetalhesCategorias`, or the per-file `baixarArquivo` route — the bulk package
makes them unnecessary, which is also why the request is small.

Volume: two categories ("Empenhos", "Despesas") across exercises 2022–2026, roughly
**10 GB per full refresh, in 15 requests**. Closed exercises are restated years later
(exercise 2021 carried `dataAtualizacao` 2025-06-10), so a refresh re-pulls the open
years rather than only the newest one. Requests are serial and paced at one per second.
We hold exercises 2014–2021 already, retrieved before the gateway required a token.

## Draft letter (Portuguese)

> **Assunto:** Solicitação de credencial de acesso programático aos dados abertos do
> SICOM (LAI — Lei 12.527/2011)
>
> Prezados,
>
> A Data Basis (Base dos Dados) é uma organização sem fins lucrativos que publica dados
> públicos brasileiros em formato tratado e documentado, com atribuição à fonte original.
> Já republicamos a execução orçamentária municipal de Minas Gerais, exercícios 2014 a
> 2021, obtida do portal de dados abertos do TCE-MG.
>
> Solicitamos uma credencial de serviço para acesso programático a três endpoints
> somente-leitura do portal, listados no anexo. Hoje o acesso depende de um token emitido
> apenas por página protegida por reCAPTCHA, com validade de 120 minutos, o que impede a
> atualização automatizada e nos obriga a intervenção manual a cada coleta.
>
> O volume solicitado é reduzido: aproximadamente 15 requisições por trimestre,
> executadas em série, com intervalo mínimo de um segundo entre elas, totalizando cerca
> de 10 GB por atualização completa dos exercícios de 2022 a 2026. Utilizamos a rota de
> pacote consolidado (`baixarArquivoPct`), que entrega os 853 municípios em uma única
> requisição, justamente para minimizar a carga sobre o servidor.
>
> Atendem à solicitação, em ordem de preferência: (1) uma chave de API ou token de longa
> duração restrito aos endpoints do anexo; (2) a inclusão de um endereço IP fixo em lista
> de exceção; ou (3) a indicação de um espelho de download em massa (S3, FTP ou URLs
> assinadas por exercício).
>
> Registramos que não tentamos contornar o mecanismo de proteção do portal, e não o
> faremos. Esta solicitação é o caminho que entendemos correto.
>
> Solicitamos ainda a indicação de um contato técnico responsável pelo gateway de API,
> para tratar de detalhes de implementação.
>
> Permanecemos à disposição.
>
> Atenciosamente,
> [nome], Data Basis — [e-mail] — https://basedosdados.org

## What we will not do, and should say so

The static `Authorization: Bearer` value is published verbatim in the portal's own
JavaScript bundle and is identical for every anonymous visitor, so quoting it in
correspondence discloses nothing. The per-session `AuthorizationProxy` token is different:
obtaining it automatically means defeating reCAPTCHA. We have not done it, `download_mg.py`
does not do it, and the request above is worth more than a workaround would be — a
credential survives the portal changing its front end, and a bypass does not.
