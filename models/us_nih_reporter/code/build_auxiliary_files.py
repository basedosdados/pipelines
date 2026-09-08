"""Build the per-table auxiliary-file bundles for us_nih_reporter.

NIH publishes the column documentation for every ExPORTER family in one page, so
each table's bundle carries that page plus the FAQ that documents the release
cadence and the linkage rules. The project table's bundle additionally carries
the NIH activity code register, which is what turns its ``activity`` column from
a three-character code into a programme name.

Writes to ``<scratch>/auxiliary_files/<table>/auxiliary_files.zip``, then uploads
to ``gs://basedosdados/auxiliary_files/us_nih_reporter/<table>/`` when
credentials allow it.

Note: both ``basedosdados`` and ``basedosdados-public`` are requester-pays and a
Data Basis *dev* service account cannot write to either — the upload step needs a
credential with prod object-write rights. Related: the published
``auxiliaryFilesUrl`` links currently return HTTP 400 (``UserProjectMissing``)
for anonymous readers on every production table that has one, pending the move
to ``gs://basedosdados-public``.

Run: uv run python models/us_nih_reporter/code/build_auxiliary_files.py [--upload]
"""

import shutil
import subprocess
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests
from common import ALL_TABLES, DATA_DIR, DATASET_ID, constants

OUT = DATA_DIR / "auxiliary_files"
DOWNLOADED = date(2026, 9, 8)

# bundle file name -> (url or None for a committed file, what it is)
DOCS = {
    "exporter_data_dictionary.html": (
        "https://report.nih.gov/exporter-data-dictionary",
        "Dicionário de dados do ExPORTER: definição de cada coluna dos arquivos "
        "de projetos, resumos, publicações, ligações, patentes e estudos "
        "clínicos, com os campos correspondentes na busca e na exportação do "
        "RePORTER",
    ),
    "report_nih_faqs.html": (
        "https://report.nih.gov/faqs",
        "Perguntas frequentes do RePORT, cuja seção ExPORTER documenta a cadência "
        "de publicação dos arquivos, quais agências constam deles, por que o "
        "custo é vazio em parte das linhas, e como projetos são ligados a "
        "publicações, patentes e estudos clínicos",
    ),
}

# Committed alongside the code because the page it came from is browser-only.
ACTIVITY_CODES = (
    "activity_codes.csv",
    "Registro oficial de códigos de atividade do NIH, com a categoria de "
    "financiamento e o título de cada um dos 258 códigos de concessão e acordo "
    "de cooperação. Extraído de "
    "https://grants.nih.gov/grants/funding/ac_search_results.htm, que responde "
    "403 a qualquer requisição não interativa e por isso é mantido como cópia",
)

README = """# {table} — arquivos auxiliares

Documentação publicada pelo National Institutes of Health (NIH) para o RePORTER
ExPORTER, reunida para a tabela `{dataset}.{table}` da Base dos Dados.

## Citação

National Institutes of Health, Office of Extramural Research. RePORTER
ExPORTER. Disponível em https://reporter.nih.gov/exporter.

## Arquivos deste pacote

{files}

## Documentos apenas referenciados

Nenhum. Toda a documentação que o NIH publica para o ExPORTER cabe neste pacote.

## O que é preciso saber para ler a tabela

{notes}

## Licença

Obra do governo federal dos Estados Unidos, em domínio público nos termos do
título 17, seção 105, do United States Code. O NIH não impõe restrição de uso
sobre os arquivos do ExPORTER, mas pede que consultas automatizadas respeitem o
robots.txt do sistema e não degradem o serviço para outros usuários.
"""

NOTES = {
    "project": """
- O ano da tabela é o ano FISCAL federal, de 1 de outubro a 30 de setembro,
  nomeado pelo ano em que termina. Não é o ano-calendário.
- A chave lógica é o par (year, application_id).
- `core_project_num` é o que liga esta tabela a `publication_link`,
  `patent_link` e `clinical_study_link`. As colunas de componente do número do
  projeto — `activity`, `administering_ic`, `serial_number` — descrevem a
  concessão como ela está hoje e divergem do número do projeto em 3,09% das
  linhas, porque o instituto administrador muda quando a concessão é
  transferida.
- Os custos dos anos fiscais de 1985 a 1999 vêm do arquivo acessório de custos e
  DUNS que o NIH publica separadamente, ligado por `application_id`.
- As datas foram normalizadas para AAAA-MM-DD; a fonte as publica em até três
  formatos diferentes ao longo do período.
- `org_state` não é um campo restrito a estados dos Estados Unidos: inclui
  províncias canadenses e territórios.
""",
    "project_abstract": """
- O ano da tabela é o ano FISCAL federal e liga a `project` pelo par
  (year, application_id).
- Nas concessões o resumo é fornecido ao NIH pelo beneficiário; nem todo projeto
  tem resumo publicado.
""",
    "publication": """
- O ano da tabela é o ano-CALENDÁRIO do arquivo de divulgação, não o ano fiscal
  usado em `project`.
- A chave lógica é o par (year, pmid): 22.930 PMIDs aparecem em mais de um
  arquivo anual.
- A ligação com os projetos que financiaram cada publicação está em
  `publication_link`.
""",
    "publication_link": """
- Formato longo: uma linha por par de publicação e projeto.
- `core_project_num` liga a `project`; `pmid` liga a `publication`.
- A associação vem dos agradecimentos do artigo ou do sistema de submissão de
  manuscritos do NIH, e não identifica um ano do projeto nem um ano fiscal de
  financiamento.
""",
    "patent_link": """
- Formato longo: uma linha por par de patente e projeto. Sem partição por ano —
  a fonte publica um único arquivo cobrindo todos os anos fiscais.
- O registro é reconhecidamente incompleto: só constam patentes concedidas, não
  pedidos em andamento, e nem toda organização beneficiária cumpre a obrigação
  de reportar ao iEdison depois de encerrado o apoio.
- Patentes só são reportadas para projetos do NIH, não para os das demais
  agências presentes em `project`.
""",
    "clinical_study_link": """
- Formato longo: uma linha por par de estudo clínico e projeto. Sem partição por
  ano — a fonte publica um único arquivo cobrindo todos os anos fiscais.
- A situação do estudo descreve o estágio em que ele se encontrava na data de
  extração do arquivo, e não no ano fiscal do projeto.
""",
    "dicionario": """
- Registro dos valores das colunas codificadas de `project`, com a cobertura
  temporal de cada valor em anos fiscais.
- `valor` fica vazio nos códigos de atividade que o registro oficial do NIH não
  cobre: contratos, projetos intramuros e agências que não o NIH.
""",
}


def fetch(url: str, dest: Path) -> None:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.write_bytes(resp.content)
    print(f"  fetched {dest.name} ({len(resp.content):,} bytes)")


def build(table: str, cache: Path) -> Path:
    bundle = OUT / table
    if bundle.exists():
        shutil.rmtree(bundle)
    bundle.mkdir(parents=True)

    entries = []
    for name, (url, what) in DOCS.items():
        shutil.copy(cache / name, bundle / name)
        entries.append((name, url, what))
    if table == "project":
        name, what = ACTIVITY_CODES
        shutil.copy(Path(constants.REFERENCE_DIR.value) / name, bundle / name)
        entries.append(
            (
                name,
                "https://grants.nih.gov/grants/funding/ac_search_results.htm",
                what,
            )
        )

    files = "\n".join(
        f"- `{name}` — {what}.\n  Origem: {url}\n  Baixado em {DOWNLOADED:%Y-%m-%d}."
        for name, url, what in entries
    )
    (bundle / "README.md").write_text(
        README.format(
            table=table,
            dataset=DATASET_ID,
            files=files,
            notes=NOTES[table].strip(),
        ),
        encoding="utf-8",
    )

    zip_path = bundle / "auxiliary_files.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in sorted(bundle.iterdir()):
            if f.name != "auxiliary_files.zip":
                zf.write(f, f.name)
    print(f"[{table}] {zip_path} ({zip_path.stat().st_size:,} bytes)")
    return zip_path


def main() -> int:
    cache = DATA_DIR / "auxiliary_cache"
    cache.mkdir(parents=True, exist_ok=True)
    print("fetching source documentation ...")
    for name, (url, _) in DOCS.items():
        if not (cache / name).exists():
            fetch(url, cache / name)
        else:
            print(f"  cached {name}")

    OUT.mkdir(parents=True, exist_ok=True)
    built = {t: build(t, cache) for t in ALL_TABLES}

    if "--upload" not in sys.argv:
        print("\nnot uploading (pass --upload)")
        return 0

    for table, zip_path in built.items():
        dest = (
            f"gs://basedosdados/auxiliary_files/{DATASET_ID}/{table}/"
            "auxiliary_files.zip"
        )
        print(f"uploading {table} -> {dest}")
        subprocess.run(
            ["gcloud", "storage", "cp", str(zip_path), dest], check=True
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
