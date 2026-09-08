"""Build the per-table auxiliary-file bundles for us_eia_electricity.

    python build_auxiliary_files.py            # build only
    python build_auxiliary_files.py --upload   # build and upload

The two forms document themselves differently, so the bundles differ:

* **EIA-860** ships its own documentation *inside* every annual ZIP — the blank
  form, the respondent instructions, and a layout workbook that names and defines
  every column of every sheet. Those three are taken from the newest release.
* **EIA-923** ships none: its ZIP holds only data workbooks. Its form and
  instructions are published separately on eia.gov/survey, and the notes that
  explain the pre-2008 EIA-906/920 files are a separate spreadsheet again.

Both bundles also carry the code vocabulary as a CSV, because the codes are what
make these tables readable and a user who has the ZIP should not have to fetch a
second thing to interpret ``BIT`` or ``ST``.

Writes to ``<scratch>/auxiliary_files/<table>/auxiliary_files.zip``, then uploads
to ``gs://basedosdados/auxiliary_files/us_eia_electricity/<table>/`` when credentials allow.

Note: both ``basedosdados`` and ``basedosdados-public`` are requester-pays and a
Data Basis *dev* service account cannot write to either — the upload step needs a
credential with prod object-write rights. The published ``auxiliaryFilesUrl``
links currently return HTTP 400 (``UserProjectMissing``) for anonymous readers on
every production table that has one.
"""

import csv
import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests
from common import DATA_DIR, DATASET_ID, INPUT, codes, constants

OUT = DATA_DIR / "auxiliary_files"
DOWNLOADED = date(2026, 9, 8)
UA = {"User-Agent": constants.USER_AGENT.value}

# Documents fetched from eia.gov: bundle name -> (url, what it is)
WEB_DOCS = {
    "eia860": {
        "eia860_form.xlsx": (
            "https://www.eia.gov/survey/form/eia_860/form.xlsx",
            "Formulário EIA-860 em branco, com todas as perguntas de cada anexo",
        ),
        "eia860_instructions.pdf": (
            "https://www.eia.gov/survey/form/eia_860/instructions.pdf",
            "Instruções da EIA aos respondentes do Formulário EIA-860, incluindo "
            "as definições de cada código",
        ),
        "eia860_faqs.pdf": (
            "https://www.eia.gov/survey/faqs/eia860.pdf",
            "Perguntas frequentes sobre o preenchimento do Formulário EIA-860",
        ),
    },
    "eia923": {
        "eia923_form.xlsx": (
            "https://www.eia.gov/survey/form/eia_923/form.xlsx",
            "Formulário EIA-923 em branco, com todas as perguntas de cada anexo",
        ),
        "eia923_instructions.pdf": (
            "https://www.eia.gov/survey/form/eia_923/instructions.pdf",
            "Instruções da EIA aos respondentes do Formulário EIA-923, incluindo "
            "as definições de cada código e as unidades físicas por combustível",
        ),
        "eia923_technical_notes.pdf": (
            "https://www.eia.gov/electricity/monthly/pdf/technotes.pdf",
            "Notas técnicas do Electric Power Monthly, que descrevem como a EIA "
            "monta as séries publicadas a partir deste formulário",
        ),
        "eia906_database_notes.xls": (
            "https://www.eia.gov/electricity/data/eia923/xls/eia906dbnotes.xls",
            "Notas da EIA sobre os arquivos EIA-906/920 anteriores a 2008, que "
            "são a origem dos anos de 2001 a 2007 desta tabela",
        ),
    },
}

# Documents lifted out of the newest source ZIP: member suffix -> (name, what).
ZIP_DOCS = {
    "eia860": {
        "LayoutY2025_Early_Release.xlsx": (
            "eia860_layout_2025.xlsx",
            "Layout do release mais recente do EIA-860: nome, posição e definição "
            "de cada coluna de cada planilha",
        ),
    },
    "eia923": {},
}

TABLE_FORM = {
    "plant": "eia860",
    "generator": "eia860",
    "generation_fuel": "eia923",
    "fuel_receipts_costs": "eia923",
}

FORM_LABEL = {
    "eia860": "EIA-860 (Annual Electric Generator Report)",
    "eia923": "EIA-923 (Power Plant Operations Report)",
}

README = """# {table} — arquivos auxiliares

Documentação publicada pela U.S. Energy Information Administration para o
Formulário {form_label}, reunida para a tabela `{dataset}.{table}` da Base dos
Dados.

## Citação

A EIA pede que o uso de seus produtos seja acompanhado de um reconhecimento com
a data da publicação, por exemplo:

> Source: U.S. Energy Information Administration ({month})

Publicações do governo dos Estados Unidos são de domínio público e não estão
sujeitas a direito autoral; a EIA autoriza expressamente o uso e a redistribuição
de seus dados. Ver <https://www.eia.gov/about/copyrights_reuse.php>.

## Arquivos deste pacote

{files}

## Como os dados foram tratados

Os arquivos anuais do formulário mudam de layout quase todo ano — arquivos são
renomeados, planilhas são divididas, linhas de cabeçalho mudam de posição e
colunas são renomeadas. Os mapas de extração usados aqui são os do projeto
**Public Utility Data Liberation (PUDL)**, da Catalyst Cooperative, licença MIT
(<https://github.com/catalyst-cooperative/pudl>), reaproveitados literalmente em
vez de reescritos. Do PUDL vêm também os vocabulários de códigos e os reparos que
eles trazem: `code_fixes`, que troca um código sujo pelo canônico, e
`ignored_codes`, que anula um código sem significado.

Dois reparos de valor publicado, ambos documentados pela própria EIA, também são
aplicados: o custo de combustível é publicado em **centavos** de dólar por MMBtu
e está convertido para dólares, e o ponto isolado (`.`) que a EIA usa para "sem
valor" vira nulo.

Não são reproduzidas as transformações *inferenciais* do PUDL. A máquina primária
ausente nos arquivos de 2001 e 2002 do EIA-923 permanece nula em vez de imputada
a partir de outros anos, e linhas que colidem na chave natural permanecem como
duas linhas. Esta é a microdata como declarada.

## Vocabulário de códigos

`code_vocabularies.csv` traz, para cada coluna codificada, o código, o rótulo e a
descrição. É o mesmo conteúdo da tabela `{dataset}.dicionario`, incluído aqui
para que este pacote seja autossuficiente.

Baixado em {downloaded}.
"""


def fetch(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    response = requests.get(url, headers=UA, timeout=300)
    response.raise_for_status()
    dest.write_bytes(response.content)


def code_vocabulary_csv(dest: Path) -> None:
    """Flatten the vendored vocabularies to one CSV of code, label, description."""
    rows = []
    for table, columns in constants.CODED_COLUMNS.value.items():
        for column, vocabulary in columns.items():
            for entry in codes()[vocabulary]["rows"]:
                rows.append(
                    {
                        "table": table,
                        "column": column,
                        "code": entry["code"],
                        "label": entry.get("label", ""),
                        "description": entry.get("description", ""),
                    }
                )
    with open(dest, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["table", "column", "code", "label", "description"]
        )
        writer.writeheader()
        writer.writerows(rows)


def newest_zip(form: str) -> Path:
    return sorted((INPUT / form).glob(f"{form}_*.zip"))[-1]


def build(table: str) -> Path:
    form = TABLE_FORM[table]
    stage = OUT / table
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)
    cache = OUT / "_cache"
    cache.mkdir(parents=True, exist_ok=True)

    entries = []
    for name, (url, what) in WEB_DOCS[form].items():
        cached = cache / name
        fetch(url, cached)
        shutil.copy2(cached, stage / name)
        entries.append(
            f"- `{name}` — {what}. Origem: <{url}>, baixado em {DOWNLOADED}."
        )

    if ZIP_DOCS[form]:
        with zipfile.ZipFile(newest_zip(form)) as archive:
            members = {Path(n).name: n for n in archive.namelist()}
            for member, (name, what) in ZIP_DOCS[form].items():
                if member not in members:
                    raise SystemExit(
                        f"{newest_zip(form).name}: expected document {member!r}, "
                        f"has {sorted(members)}"
                    )
                (stage / name).write_bytes(archive.read(members[member]))
                entries.append(
                    f"- `{name}` — {what}. Origem: o arquivo `{member}` dentro de "
                    f"`{newest_zip(form).name}`, baixado em {DOWNLOADED}."
                )

    code_vocabulary_csv(stage / "code_vocabularies.csv")
    entries.append(
        "- `code_vocabularies.csv` — código, rótulo e descrição de cada coluna "
        "codificada desta tabela."
    )

    (stage / "README.md").write_text(
        README.format(
            table=table,
            dataset=DATASET_ID,
            form_label=FORM_LABEL[form],
            files="\n".join(entries),
            downloaded=DOWNLOADED.isoformat(),
            month=DOWNLOADED.strftime("%b %Y"),
        ),
        encoding="utf-8",
    )

    zip_path = stage / "auxiliary_files.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as archive:
        for file in sorted(stage.iterdir()):
            if file.name != zip_path.name:
                archive.write(file, file.name)
    for file in sorted(stage.iterdir()):
        if file.name != zip_path.name:
            file.unlink()
    return zip_path


def upload(table: str, zip_path: Path) -> None:
    from google.cloud import storage

    client = storage.Client(project="basedosdados-dev")
    bucket = client.bucket("basedosdados", user_project="basedosdados-dev")
    key = f"auxiliary_files/{DATASET_ID}/{table}/auxiliary_files.zip"
    bucket.blob(key).upload_from_filename(zip_path)
    print(f"  uploaded gs://basedosdados/{key}")


def main() -> None:
    do_upload = "--upload" in sys.argv
    for table in TABLE_FORM:
        path = build(table)
        print(f"{table}: {path} ({path.stat().st_size / 1024:.0f} KB)")
        print(
            "  url: https://storage.googleapis.com/basedosdados/auxiliary_files/"
            f"{DATASET_ID}/{table}/auxiliary_files.zip"
        )
        if do_upload:
            upload(table, path)


if __name__ == "__main__":
    main()
