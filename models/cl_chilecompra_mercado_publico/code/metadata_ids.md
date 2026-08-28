# Resolved backend reference IDs (staging)

Resolved 2026-08-28. IDs differ per backend — re-resolve before registering on prod.

| Kind | Slug | ID |
|---|---|---|
| area | `cl` | `b08e39e7-966c-4d33-ae58-b513c47291d2` |
| account (publishedBy / dataCleanedBy) | rdahis@basedosdados.org | `57` |
| theme | `economics` | `ad6a413a-e882-4dd6-a497-8a62eec8511b` |
| theme | `government` | `6dd730bb-89ab-4dba-a1bf-a25ca1c35003` |
| status | `under_review` | `47208305-325a-4da9-9222-ac6849405b78` |
| status | `published` | `e16221de-ac30-4926-83d3-de219998dab3` |
| availability | `online` | `dd396d7d-0264-4c1f-bf0d-6efe2dc89cbe` |

## Tags — all already exist, none need creating

| Slug | ID |
|---|---|
| `licitacao` | `4b76d0d7-7a4b-4a73-a2c5-33a08853dc77` |
| `compra` | `c6416645-6aeb-43d4-a60c-8a5fddaf959a` |
| `contrato` | `0831b835-2079-44f3-b5e8-3f598435bbe0` |
| `empresa` | `536be6c2-7fc6-4409-a029-fb8e1c771dec` |
| `gasto` | `83b37841-83b0-45e7-9455-b7b1008f1e30` |
| `financas_publicas` | `5dce4b1d-131b-452a-a419-bdd587a8c272` |
| `administracao_publica` | `94b742db-a2c2-468b-b83e-2f223bb98fe7` |
| `concorrencia` | `886e7c5e-65ed-4def-946c-eb3786bf6371` |

A `chile` tag exists (`52e56a56-0c26-4674-9cf0-9d290f226bff`) and is deliberately
**not** attached: tag conventions forbid tagging place names, which are already carried
as the dataset's `Coverage`/`area`. Likewise `governo` is skipped because the
`government` theme already covers it.

## Still to create

- **Organization `chilecompra`** — does not exist. Dirección de Compras y Contratación
  Pública, Ministerio de Hacienda, Chile. `area_id` = the `cl` id above,
  website `https://www.chilecompra.cl`.
- **License `libre_uso_cl`** — does not exist. ChileCompra publishes no CC licence; the
  terms permit reuse with mandatory attribution ("deberán indicar claramente que la
  fuente de los datos es la Dirección ChileCompra") and contemplate commercial use.
  Mirrors the existing `libre_uso_mx` precedent. Approved by the user.

## Blocked

`directory_column` is blank in all three architecture CSVs pending
`br_bd_diretorios_cl`. ChileCompra publishes región and comuna as **names only** (e.g.
`"Región del Maule "`, with a trailing space that the cleaning step strips) and never as
CUT codes, so the FK will have to match on name unless the directory carries name
variants.
