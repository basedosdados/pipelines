"""Build and upload the per-table auxiliary-file bundles for br_mj_sisdepen.

    uv run models/br_mj_sisdepen/code/auxiliary.py [--bucket basedosdados-dev]

The source publishes one document: the collection instrument (the questionnaire
establishments fill in). Every variable in unidade_prisional,
populacao_prisional and populacao_caracteristica traces to a numbered block in
it, so those three tables get the bundle. The derived tables (uf_semestre,
cobertura, unidade_crosswalk, dicionario) do not.
"""

from __future__ import annotations

import argparse
import os
import zipfile
from pathlib import Path

_SDK_KEY = Path.home() / ".basedosdados" / "credentials" / "staging.json"
if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") and _SDK_KEY.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(_SDK_KEY)

from google.cloud import storage  # noqa: E402

GCP_DATASET_ID = "br_mj_sisdepen"
BILLING_PROJECT = "basedosdados-dev"
# Bundles are served from basedosdados-public: the data-lake buckets are
# requester-pays, so anonymous fetches of anything in them return HTTP 400
# UserProjectMissing. Writing to the public bucket needs prod credentials.
AUX_DIR = Path.home() / "Downloads" / "br_mj_sisdepen_data" / "aux"
FORM = "formulario_informacoes_prisionais.pdf"
FORM_URL = (
    "https://www.gov.br/senappen/pt-br/servicos/sisdepen/bases-de-dados/"
    "arquivos/formulario-sobre-informacoes-prisionais.pdf"
)
DOWNLOADED = "2026-09-28"

TABLES = {
    "unidade_prisional": "blocos 1.1 a 1.9 (identificação, capacidade e gestão)",
    "populacao_prisional": "bloco 4.1 (população prisional)",
    "populacao_caracteristica": "blocos 5.1, 5.2, 5.4 e 5.6 (perfil sociodemográfico)",
}

README = """# Arquivos auxiliares — {dataset}.{table}

## Citação

BRASIL. Ministério da Justiça e Segurança Pública. Secretaria Nacional de
Políticas Penais. Sistema de Informações do Departamento Penitenciário Nacional
(SISDEPEN). Brasília, 2016-2025.

## Conteúdo

| Arquivo | O que é | Origem | Baixado em |
|---|---|---|---|
| `{form}` | Formulário de coleta do SISDEPEN, 17 páginas. Instrumento preenchido pelas administrações prisionais estaduais a cada semestre. Define os blocos numerados aos quais as colunas desta tabela correspondem | {url} | {downloaded} |

Esta tabela deriva dos {blocks} do formulário.

## O que é preciso saber para ler a tabela

- A fonte publica apenas respostas validadas. `Situação de Preenchimento` é
  `Validado` em todas as 28.825 linhas-estabelecimento, de modo que uma unidade
  que não respondeu está ausente do arquivo, sem sinalização. A tabela
  `cobertura` reconstrói quantas unidades eram esperadas.
- O identificador `id_unidade` não existe na fonte. Ele é reconstruído por
  pareamento entre ciclos consecutivos; a tabela `unidade_crosswalk` registra
  cada vínculo, seu escore e se é ambíguo.
- Os totais publicados pela fonte nos blocos 1.3, 4.1 e 5.x foram descartados.
  Apenas as células componentes são carregadas, de modo que somar não gera
  dupla contagem.
- Não há quebra de instrumento em 2019. Os ciclos 2 a 9 (2017/1 a 2020/2)
  compartilham um cabeçalho idêntico de 1.333 colunas. A coluna
  `geracao_esquema` registra as quatro gerações reais do questionário.

## Documentos apenas referenciados

Os relatórios analíticos semestrais do SISDEPEN não são reempacotados aqui.
Estão publicados em
https://www.gov.br/senappen/pt-br/servicos/sisdepen e são estáveis na origem.

## Cobertura não incluída

A série histórica do INFOPEN de 2005 a 2015 não faz parte deste conjunto: o
host `dados.mj.gov.br`, onde era publicada, não resolve mais (NXDOMAIN).
"""


def build(table: str, out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    dest = out_dir / f"{table}_auxiliary_files.zip"
    readme = README.format(
        dataset=GCP_DATASET_ID,
        table=table,
        form=FORM,
        url=FORM_URL,
        downloaded=DOWNLOADED,
        blocks=TABLES[table],
    )
    with zipfile.ZipFile(dest, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("README.md", readme)
        z.write(AUX_DIR / FORM, FORM)
    return dest


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", default="basedosdados-public")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    client = storage.Client(project=BILLING_PROJECT)
    bucket = client.bucket(args.bucket, user_project=BILLING_PROJECT)
    for table in TABLES:
        path = build(table, AUX_DIR / "bundles")
        blob_name = (
            f"auxiliary_files/{GCP_DATASET_ID}/{table}/auxiliary_files.zip"
        )
        url = f"https://storage.googleapis.com/{args.bucket}/{blob_name}"
        print(f"{table:26s} {path.stat().st_size / 1024:6.0f} KiB -> {url}")
        if not args.dry_run:
            bucket.blob(blob_name).upload_from_filename(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
