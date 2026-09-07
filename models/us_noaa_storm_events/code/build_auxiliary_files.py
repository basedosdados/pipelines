"""Build the per-table auxiliary-file bundles for us_noaa_storm_events.

NCEI publishes the column documentation for all three CSV families in one
document, so every data table's bundle carries the same two PDFs plus the
directory's own README. Each bundle gets a README.md with the citation, per-file
provenance and download date, and the notes a reader needs to use the table.

Writes to ``<scratch>/auxiliary_files/<table>/auxiliary_files.zip``, then uploads
to ``gs://basedosdados/auxiliary_files/us_noaa_storm_events/<table>/`` when
credentials allow it.

Note: both ``basedosdados`` and ``basedosdados-public`` are requester-pays and a
Data Basis *dev* service account cannot write to either — the upload step needs a
credential with prod object-write rights. Related: the published
``auxiliaryFilesUrl`` links currently return HTTP 400 (``UserProjectMissing``) for
anonymous readers on every production table that has one, pending the move to
``gs://basedosdados-public``.

Run: uv run python models/us_noaa_storm_events/code/build_auxiliary_files.py [--upload]
"""

import shutil
import sys
import zipfile
from datetime import date
from pathlib import Path

from common import DATA_DIR, DATA_TABLES, DATASET_ID, INPUT

OUT = DATA_DIR / "auxiliary_files"
DOWNLOADED = date(2026, 9, 7)

# source file name -> (bundle name, what it is)
FILES = {
    "Storm-Data-Bulk-csv-Format.pdf": (
        "storm_data_bulk_csv_format.pdf",
        "Definição de cada coluna dos três arquivos CSV (details, fatalities, "
        "locations), com o vocabulário de tipos de evento da diretiva NWS 10-1605 "
        "e as tabelas de códigos de escala Fujita e de local da fatalidade",
    ),
    "Storm-Data-Export-Format.pdf": (
        "storm_data_export_format.pdf",
        "Formato de exportação do Storm Data, incluindo a definição dos campos "
        "que não constam do documento de formato em massa",
    ),
    "README": (
        "ncei_directory_readme.txt",
        "README do diretório de download do NCEI, que documenta a convenção de "
        "nomes dos arquivos e o significado do token de data de criação",
    ),
}

README = """# {table} — arquivos auxiliares

Documentação publicada pelo NOAA National Centers for Environmental Information
(NCEI) para o Storm Events Database, reunida para a tabela
`{dataset}.{table}` da Base dos Dados.

## Citação

NOAA National Centers for Environmental Information, *Storm Events Database*.
<https://www.ncei.noaa.gov/access/monitoring/storm-events/>

Os dados são obra do governo dos Estados Unidos e estão em domínio público.

## Arquivos deste pacote

{files}

## Documentos apenas referenciados

- NWS Directive 10-1605, *Storm Data Preparation* — define o vocabulário de
  tipos de evento permitido em Storm Data:
  <https://www.nws.noaa.gov/directives/sym/pd01016005curr.pdf>
- Diretório de download em massa, com um CSV comprimido por família e por ano:
  <https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/>

## O que saber antes de usar a tabela

- **A cobertura por tipo de evento não é uniforme.** Apenas tornados são
  registrados de 1950 a 1954; tornado, vento de tempestade e granizo de 1955 a
  1995; e o conjunto completo de tipos somente a partir de 1996. Uma contagem de
  eventos por ano ao longo de todo o período mede a expansão do registro, não a
  ocorrência de eventos.
- **Os valores de danos foram decodificados.** A fonte publica
  `DAMAGE_PROPERTY` e `DAMAGE_CROPS` como texto com sufixo de magnitude
  (`2.5K`, `10.00B`, e também `.5K` sem o zero à esquerda). As colunas
  `damage_property` e `damage_crops` trazem o valor em dólares correntes do ano
  do evento; a forma textual original está preservada em
  `damage_property_source` e `damage_crops_source`.
- **As datas foram reconstruídas dos campos numéricos.** O campo textual
  `BEGIN_DATE_TIME` usa o formato `DD-MON-AA`, cujo ano de dois dígitos não
  distingue 1950 de 2050, e não o `MM/DD/AAAA` que a documentação da fonte
  descreve. `begin_datetime` e `end_datetime` foram montados a partir de
  `*_YEARMONTH`, `*_DAY` e `*_TIME`.
- **`county_id` só existe quando `cz_type = 'C'`.** Nas demais linhas o campo
  `cz_fips` identifica uma zona de previsão do NWS ou uma zona marítima, e não
  um condado.
- **Códigos de estado dos territórios foram convertidos.** O NWS usa códigos
  próprios (99 Porto Rico, 98 Guam, 97 Samoa Americana, 96 Ilhas Virgens);
  `state_id` traz o código FIPS real e `state_fips_nws` preserva o publicado.
- **`LAT2` e `LON2` foram descartados.** São uma cópia redundante de `LATITUDE`
  e `LONGITUDE`, sem sinal e em duas codificações diferentes conforme a época.

Arquivos baixados de <https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/>
em {downloaded}.
"""


def build(table: str) -> Path:
    stage = OUT / table
    if stage.exists():
        shutil.rmtree(stage)
    stage.mkdir(parents=True)

    entries = []
    for src_name, (bundle_name, what) in FILES.items():
        src = INPUT / src_name
        if not src.exists():
            raise SystemExit(f"missing source document: {src}")
        shutil.copy2(src, stage / bundle_name)
        entries.append(
            f"- `{bundle_name}` — {what}. Origem: "
            f"<https://www.ncei.noaa.gov/pub/data/swdi/stormevents/csvfiles/{src_name}>, "
            f"baixado em {DOWNLOADED.isoformat()}."
        )

    (stage / "README.md").write_text(
        README.format(
            table=table,
            dataset=DATASET_ID,
            files="\n".join(entries),
            downloaded=DOWNLOADED.isoformat(),
        ),
        encoding="utf-8",
    )

    zip_path = stage / "auxiliary_files.zip"
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(stage.iterdir()):
            if f.name != zip_path.name:
                z.write(f, f.name)
    for f in sorted(stage.iterdir()):
        if f.name != zip_path.name:
            f.unlink()
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
    for table in DATA_TABLES:
        z = build(table)
        print(f"{table}: {z} ({z.stat().st_size / 1024:.0f} KB)")
        print(
            "  url: https://storage.googleapis.com/basedosdados/auxiliary_files/"
            f"{DATASET_ID}/{table}/auxiliary_files.zip"
        )
        if do_upload:
            upload(table, z)


if __name__ == "__main__":
    main()
