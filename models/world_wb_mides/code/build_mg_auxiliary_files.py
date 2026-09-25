"""Build and upload the per-table auxiliary-file bundles for the MG tables.

    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/build_mg_auxiliary_files.py --dry-run
    ~/.venvs/bd-pipelines/bin/python models/world_wb_mides/code/build_mg_auxiliary_files.py

WHAT GOES IN, AND WHAT IS ONLY LINKED
-------------------------------------
TCE-MG documents SICOM in per-MODULE manuals -- one PDF covering dozens of
streams. Rehosting a 2.8 MB manual into each of 43 bundles would add ~120 MB of
duplication and create 43 copies that go stale independently, so the manual is
**linked**, per the "over ~5 MB and read once -> link it" rule, and the bundle
carries what is genuinely per-table:

  * the exact source stream this table is built from,
  * that stream's pinned header contract -- the field order this table's columns
    were derived from, which is NOT published anywhere by TCE-MG,
  * the caveats that apply to this table specifically.

WHICH BUCKET
------------
The convention is the prod bucket `basedosdados`. Local credentials cannot write
there (403); they are provisioned for `basedosdados-dev` only, and the prod
upload is a deploy-time action. So this writes to `basedosdados-dev` and the URL
recorded on the table points there. Both buckets are requester-pays, so **either
URL returns HTTP 400 `UserProjectMissing` to an anonymous visitor** -- as it does
for all 84 production tables already using this field. That is a bucket setting,
not something this script can fix; it verifies and reports the real status rather
than implying the link works.
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import os
import sys
import zipfile
from pathlib import Path

import requests
from google.cloud import storage
from google.oauth2 import service_account

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(
    0,
    os.path.expanduser("~/Monash Uni Enterprise Dropbox/Ricardo Dahis/BD/mcp"),
)

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import clean_mg

# pyrefly: ignore [missing-import]  # sibling module via sys.path
import mg_table_glossary as tables

# pyrefly: ignore [missing-import]  # the databasis MCP server, via sys.path
import server

ENV = "staging"
DATASET_ID = "d3874769-bcbd-4ece-a38a-157ba1021514"  # slug `mides`
BUCKET = "basedosdados-dev"
PREFIX = "auxiliary_files/world_wb_mides"
CREDENTIALS = Path.home() / ".basedosdados/credentials/staging.json"
HEADERS = Path(__file__).with_name("mg_source_headers.json")

PORTAL = "https://dadosabertos.tce.mg.gov.br"

# Which SICOM module documents each source category, and where that manual lives.
MANUAL = {
    "empenho": (
        "Módulo Acompanhamento Mensal (AM)",
        "https://portalsicom1.tce.mg.gov.br/leiautes/leiautes-2026/modulo-acompanhamento-mensal-2026/",
    ),
    "despesa": (
        "Módulo Acompanhamento Mensal (AM)",
        "https://portalsicom1.tce.mg.gov.br/leiautes/leiautes-2026/modulo-acompanhamento-mensal-2026/",
    ),
    "licitacao": (
        "Módulo Edital e Licitação",
        "https://portalsicom1.tce.mg.gov.br/leiautes/leiautes-2026/modulo-edital-e-licitacao-2026/",
    ),
    "contrato": (
        "Módulo Edital e Licitação",
        "https://portalsicom1.tce.mg.gov.br/leiautes/leiautes-2026/modulo-edital-e-licitacao-2026/",
    ),
}

# Table-specific caveats worth carrying next to the data.
CAVEATS = {
    "restos_pagar_movimentacao": (
        "O stream de origem `movimentacaoRsp` publica um cabeçalho de 9 colunas "
        "copiado de outro stream, enquanto os dados têm 19. Os nomes de coluna "
        "foram reconstruídos e conferidos contra o manual do SICOM e contra os "
        "próprios dados; três campos permanecem inferidos. Ver o comentário em "
        "`_HEADER_OVERRIDE` no código de limpeza."
    ),
}


def bundle(table: str, category: str, member: str) -> bytes:
    headers = json.loads(HEADERS.read_text(encoding="utf-8"))
    key = f"{category}/{member}"
    module, url = MANUAL[category]
    today = datetime.date.today().isoformat()
    readme = f"""# {tables.name(table, "pt")} — arquivos auxiliares

{tables.description(table, "pt")}

## Citação

Tribunal de Contas do Estado de Minas Gerais (TCE-MG), Sistema Informatizado de
Contas dos Municípios (SICOM), dados abertos. Harmonizado por Data Basis para o
MiDES.

## Origem

| | |
|---|---|
| Portal | {PORTAL} |
| Categoria de origem | `{category}` |
| Stream de origem | `{member}` |
| Arquivo bruto | `SICOM.<exercício>.<ibge7>.{category}.zip`, membro `<exercício>.<ibge7>.{category}.{member}.csv` |
| Cobertura | exercícios 2014 a 2026, 853 municípios |
| Coletado em | 2026-09 |
| Bundle gerado em | {today} |

## Documentação (link, não redistribuída)

O leiaute oficial está no **{module}** do Portal SICOM. O PDF é publicado e
mantido pelo TCE-MG; não é copiado para cá para não criar uma cópia que
envelhece de forma independente.

- {url}
- Índice de leiautes: https://portalsicom1.tce.mg.gov.br/category/leiautes/

**Atenção:** o leiaute oficial descreve o arquivo de *remessa* — o formato que o
município envia ao TCE-MG. O CSV de dados abertos é uma exportação derivada, com
nomes e ordem de campos próprios. Os dois coincidem em significado, não em
nomenclatura.

## Contrato de cabeçalho deste stream

A ordem de campos abaixo foi fixada a partir de 16.575 cabeçalhos observados ao
longo de 13 exercícios e 4 categorias, sem nenhuma divergência. É dela que as
colunas desta tabela derivam, e o TCE-MG não a publica em lugar nenhum.

```
{chr(10).join(f"{i + 1:>3}. {name}" for i, name in enumerate(headers[key]))}
```

## Observações
{"- " + CAVEATS[table] if table in CAVEATS else "- Nenhuma específica a esta tabela."}
- Linhas com delimitador não escapado em campo de texto livre são descartadas e
  contadas, nunca descartadas em silêncio. Afeta exercícios de 2020 em diante.
- Os arquivos de origem são UTF-8. Um punhado de municípios envia texto já
  duplamente codificado na origem; isso é reproduzido fielmente, não "corrigido".
"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("README.md", readme)
        zf.writestr(
            "source_header_contract.json",
            json.dumps({key: headers[key]}, ensure_ascii=False, indent=1),
        )
    return buf.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # table slug -> (category, source member), from the cleaner's own registry
    origin = {}
    for phase, (category, member) in clean_mg.WHOLESALE.items():
        origin[phase] = (category, member)
    for members in clean_mg.MEMBERS.values():
        for cat, member, phase in members:
            origin.setdefault(phase, (cat, member))

    mg_dir = Path(__file__).resolve().parent.parent / "mg"
    slugs = sorted(
        p.stem[len("world_wb_mides__") :] for p in mg_dir.glob("*.sql")
    )

    info = json.loads(CREDENTIALS.read_text())
    creds = service_account.Credentials.from_service_account_file(
        str(CREDENTIALS)
    )
    client = storage.Client(credentials=creds, project=info["project_id"])
    bucket = client.bucket(BUCKET, user_project=info["project_id"])

    # NOT `get_dataset`: it returns every column of every table and takes 83s on
    # this dataset, past the client's own 60s read timeout. Ask for just the ids.
    edges = server._gql(
        "query($ds: ID!){ allTable(dataset_Id: $ds, first: 100)"
        "{ edges { node { id slug } } } }",
        {"ds": DATASET_ID},
        env=ENV,
    )["allTable"]["edges"]
    table_id = {
        e["node"]["slug"]: server._strip_id(e["node"]["id"]) for e in edges
    }

    built = 0
    for slug in slugs:
        phase = {"restos_pagar": "rsp"}.get(slug, slug)
        if phase not in origin:
            print(f"  {slug:<34} NO SOURCE MAPPING, skipped")
            continue
        category, member = origin[phase]
        payload = bundle(slug, category, member)
        key = f"{PREFIX}/{slug}/auxiliary_files.zip"
        url = f"https://storage.googleapis.com/{BUCKET}/{key}"
        if args.dry_run:
            print(f"  {slug:<34} {len(payload):>6,} B  <- {category}/{member}")
            built += 1
            continue
        bucket.blob(key).upload_from_string(
            payload, content_type="application/zip"
        )
        server.create_update_table(
            slug=slug,
            name_pt=tables.name(slug, "pt"),
            name_en=tables.name(slug, "en"),
            name_es=tables.name(slug, "es"),
            description_pt=tables.description(slug, "pt"),
            description_en=tables.description(slug, "en"),
            description_es=tables.description(slug, "es"),
            dataset_id=DATASET_ID,
            status_id="e16221de-ac30-4926-83d3-de219998dab3",
            published_by_ids=["57"],
            data_cleaned_by_ids=["57"],
            auxiliary_files_url=url,
            id=table_id.get(slug),
            env=ENV,
        )
        built += 1
        print(f"  {slug:<34} {len(payload):>6,} B  uploaded + linked")

    print(f"\n{built} bundles ({'dry run' if args.dry_run else BUCKET})")
    if not args.dry_run and built:
        probe = f"https://storage.googleapis.com/{BUCKET}/{PREFIX}/{slugs[0]}/auxiliary_files.zip"
        status = requests.get(probe, timeout=30).status_code
        print(f"\nanonymous fetch of {probe}\n  -> HTTP {status}", end="")
        print(
            "  (expected: requester-pays buckets reject anonymous reads; the same"
            " is true of all 84 production tables using this field)"
            if status != 200
            else "  (resolves publicly)"
        )


if __name__ == "__main__":
    main()
