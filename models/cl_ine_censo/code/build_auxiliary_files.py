#!/usr/bin/env python3
"""Build the per-table auxiliary-file bundles for cl_ine_censo.

One ZIP per table, each with a README giving the citation, per-file provenance
and download dates, plus an index of the documents that are link-only.

Bundling rule: a document a user of THIS table needs in hand goes in the ZIP; a
long-form PDF they would open once, or anything over ~5 MB, is linked instead.
INE's enumerator manual is 28 MB and is therefore linked, not rehosted.

Usage::

    python build_auxiliary_files.py            # build the zips
    python build_auxiliary_files.py --upload   # build, then upload to GCS
"""

from __future__ import annotations

import argparse
import shutil
import sys
import warnings
import zipfile
from datetime import date
from pathlib import Path

warnings.filterwarnings("ignore")

from constants import DATA_ROOT, DATASET_ID  # noqa: E402

AUX_DIR = DATA_ROOT / "aux"
DOCS_DIR = AUX_DIR / "docs"
BUILD_DIR = AUX_DIR / "bundles"

DOWNLOADED = date(2026, 9, 23).isoformat()

# WHERE THESE GO, and why it is not what the rule file says.
#
# The auxiliary-files rule says to use the prod bucket. That is not achievable
# here and would not work anyway:
#
#   gs://basedosdados         403 to both available service accounts (list AND write)
#   gs://basedosdados-public  list OK, WRITE 403 - the house public path, and the
#                             right long-term home, but not writable from here
#   gs://basedosdados-dev     writable; where 70 of the 97 existing rows point
#
# So the bundles go to basedosdados-dev, the only writable location and the one
# the majority of existing rows already use. The published link will return
# HTTP 400 UserProjectMissing to an anonymous visitor, because that bucket is
# REQUESTER-PAYS - a bucket-level setting that cannot be scoped to a prefix.
# This affects every GCS-hosted auxiliaryFilesUrl in production, not just ours.
#
# The real fix is the pending migration to gs://basedosdados-public, which is
# not requester-pays. When that lands, re-run with --bucket basedosdados-public
# and update the URLs. Do not pretend the link works in the meantime: --upload
# fetches each URL anonymously and prints the status it actually got.
DEFAULT_BUCKET = "basedosdados-dev"
MIGRATION_TARGET = "basedosdados-public"

PUBLIC_URL = "https://storage.googleapis.com/{bucket}/auxiliary_files/{ds}/{table}/auxiliary_files.zip"

SOURCE_BUCKET = "https://storage.googleapis.com/bktdescargascenso2024"

# filename -> (what it is, where it came from)
DOCUMENTS = {
    "diccionario_redatam_microdatos.dicX": (
        "Diccionario Redatam del censo, en XML. Es la UNICA fuente publicada de "
        "las etiquetas de valor de los microdatos: define las 2.029 "
        "correspondencias codigo-etiqueta usadas para construir la tabla "
        "dicionario. Extraido de Microdatos_Redatam_Censo2024.zip.",
        f"{SOURCE_BUCKET}/Microdatos_Redatam_Censo2024.zip",
    ),
    "diccionario_variables_agregadas.xlsx": (
        "Diccionario de las 189 variables agregadas de las bases "
        "manzana-entidad y zona-localidad, con la division "
        "politico-administrativa y las glosas geograficas.",
        f"{SOURCE_BUCKET}/Datos_agregados/diccionario_variables_glosas_censo2024.xlsx",
    ),
    "presentacion_microdatos.pdf": (
        "Presentacion oficial de la base de microdatos: estructura de las tres "
        "tablas, identificadores y tratamiento de los operativos de vivienda "
        "colectiva y de personas en situacion de calle.",
        f"{SOURCE_BUCKET}/Datos_agregados/Presentacion_microdatos_CPV2024.pdf",
    ),
    "nota_tecnica_n6_no_respuesta.pdf": (
        "Nota tecnica N6: tratamiento de la no respuesta al item. Explica el "
        "origen de los codigos centinela -99 y -66 presentes en los microdatos.",
        f"{SOURCE_BUCKET}/Datos_agregados/N6_Nota_tecnica_Tratamiento_no_respuesta_al_item.pdf",
    ),
    "manual_uso_cartografia.pdf": (
        "Manual de uso de la cartografia censal: definicion de manzana, "
        "entidad, zona censal, localidad y distrito, y como se relacionan.",
        f"{SOURCE_BUCKET}/Datos_agregados/Manual_uso_cartografia_CPV2024.pdf",
    ),
    "presentacion_cartografia_manzana_entidad.pdf": (
        "Presentacion oficial de la base manzana-entidad y de su cartografia "
        "asociada.",
        f"{SOURCE_BUCKET}/Datos_agregados/Presentacion_cartografia_base manzana_entidad_CPV2024.pdf",
    ),
    "nota_tecnica_n4_ajuste_direcciones.pdf": (
        "Nota tecnica N4: ajuste de direcciones, es decir como se asignaron los "
        "registros censales a la cartografia.",
        f"{SOURCE_BUCKET}/Datos_agregados/N4_Nota_Tecnica_Ajuste direcciones_CPV2024.pdf",
    ),
    "nota_tecnica_n5_indeterminacion_geografica.pdf": (
        "Nota tecnica N5: indeterminacion geografica. Documenta los registros "
        "que no pudieron asignarse a una manzana y que el INE agrupa en un "
        "contenedor comunal; es la explicacion de por que sumar n_per sobre "
        "esta tabla no devuelve el total censal.",
        f"{SOURCE_BUCKET}/Datos_agregados/N5_Nota Tecnica_indeterminacion_geografica_CPV2024.pdf",
    ),
}

BUNDLES = {
    "persona": [
        "diccionario_redatam_microdatos.dicX",
        "presentacion_microdatos.pdf",
        "nota_tecnica_n6_no_respuesta.pdf",
    ],
    "hogar": [
        "diccionario_redatam_microdatos.dicX",
        "presentacion_microdatos.pdf",
        "nota_tecnica_n6_no_respuesta.pdf",
    ],
    "vivienda": [
        "diccionario_redatam_microdatos.dicX",
        "presentacion_microdatos.pdf",
        "nota_tecnica_n6_no_respuesta.pdf",
    ],
    "dicionario": ["diccionario_redatam_microdatos.dicX"],
    "manzana_entidad": [
        "diccionario_variables_agregadas.xlsx",
        "manual_uso_cartografia.pdf",
        "presentacion_cartografia_manzana_entidad.pdf",
        "nota_tecnica_n4_ajuste_direcciones.pdf",
        "nota_tecnica_n5_indeterminacion_geografica.pdf",
    ],
    "zona_localidad": [
        "diccionario_variables_agregadas.xlsx",
        "manual_uso_cartografia.pdf",
        "presentacion_cartografia_manzana_entidad.pdf",
        "nota_tecnica_n4_ajuste_direcciones.pdf",
        "nota_tecnica_n5_indeterminacion_geografica.pdf",
    ],
}

# Too large to rehost, or read once rather than kept to hand. Listed in every
# README so nobody has to go looking.
LINK_ONLY = [
    (
        "Manual Censal CPV 2024 (28 MB)",
        "El cuestionario censal completo y las instrucciones a los censistas. "
        "Es la redaccion exacta de cada pregunta pXX de los microdatos.",
        "https://censo2024.ine.gob.cl/wp-content/uploads/2025/03/Manual_Censal_CPV2024.pdf",
    ),
    (
        "Memoria Censo 2024",
        "Planificacion, gestion logistica y modelo de recoleccion del censo.",
        "https://censo2024.ine.gob.cl/wp-content/uploads/2025/05/MEMORIA-CENSO-2024.pdf",
    ),
    (
        "Sintesis de resultados Censo 2024",
        "Principales resultados publicados por el INE; util para contrastar "
        "totales calculados sobre estas tablas.",
        "https://censo2024.ine.gob.cl/wp-content/uploads/2025/12/sintesis_resultados_censo2024.pdf",
    ),
    (
        "Terminos de uso y licencia de datos abiertos del INE",
        "Licencia CC BY-SA 4.0 y condiciones de uso, incluida la prohibicion de "
        "reidentificar personas.",
        "https://www.ine.gob.cl/terminos-de-uso-y-licencia-de-datos-abiertos",
    ),
]

CITATION = (
    "Instituto Nacional de Estadisticas (INE), Censo de Poblacion y Vivienda "
    "2024, Chile. Fuente: INE, Censo de Poblacion y Vivienda 2024, actualizada "
    "2025. Licencia Creative Commons Reconocimiento-CompartirIgual 4.0 "
    "Internacional (CC BY-SA 4.0)."
)


def readme(table: str, files: list[str]) -> str:
    lines = [
        f"# Archivos auxiliares - {DATASET_ID}.{table}",
        "",
        "## Citacion",
        "",
        CITATION,
        "",
        "## Contenido de este paquete",
        "",
    ]
    for name in files:
        what, origin = DOCUMENTS[name]
        size_mb = (DOCS_DIR / name).stat().st_size / 1e6
        lines += [
            f"### {name}  ({size_mb:.1f} MB)",
            "",
            what,
            "",
            f"- Origen: {origin}",
            f"- Descargado: {DOWNLOADED}",
            "",
        ]

    lines += [
        "## Documentos relacionados, no incluidos aqui",
        "",
        "Se enlazan en vez de rehospedarse: son estables en el sitio del INE y, "
        "en un caso, demasiado grandes para incluirlos en cada paquete.",
        "",
    ]
    for title, what, url in LINK_ONLY:
        lines += [f"- **{title}** - {what}", f"  {url}", ""]

    lines += [
        "## Notas sobre los datos",
        "",
        "- Los microdatos (persona, hogar, vivienda) estan geocodificados solo "
        "hasta la comuna por control de divulgacion estadistica, de modo que "
        "**no se cruzan** con manzana_entidad ni con zona_localidad.",
        "- Codigos centinela en los microdatos: `-99` no respuesta, `-66` valor "
        "suprimido por anonimizacion, `NULL` no aplica. En las columnas "
        "numericas ambos codigos fueron convertidos a NULL para no contaminar "
        "promedios; en las columnas codificadas se conservan y estan "
        "etiquetados en la tabla `dicionario`.",
        "- `cod_caenes` se almacena como letra de seccion CIIU (A-U), mientras "
        "que el diccionario Redatam la codifica con el ordinal (1-21). La "
        "correspondencia se aplico al construir `dicionario`.",
        "- La geometria proviene de la cartografia censal en SIRGAS 2000 "
        "(EPSG:4674), sin reproyectar: la diferencia con WGS 84 en Chile es "
        "inferior a un metro. Use `ST_AREA(geometria)` para superficie en "
        "metros cuadrados; las columnas SHAPE_Length y SHAPE_Area del origen "
        "estaban en grados y fueron descartadas.",
        "- El INE prohibe usar estos datos para reidentificar personas "
        "(ley 19.628; art. 19 N.4 de la Constitucion; ley 20.575).",
        "",
    ]
    return "\n".join(lines)


def build() -> dict[str, Path]:
    if BUILD_DIR.exists():
        shutil.rmtree(BUILD_DIR)
    BUILD_DIR.mkdir(parents=True)

    built = {}
    for table, files in BUNDLES.items():
        missing = [f for f in files if not (DOCS_DIR / f).exists()]
        if missing:
            sys.exit(f"{table}: missing source documents {missing}")

        target = BUILD_DIR / table
        target.mkdir()
        path = target / "auxiliary_files.zip"
        with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("README.md", readme(table, files))
            for name in files:
                archive.write(DOCS_DIR / name, name)
        built[table] = path
        print(
            f"  {table:18s} {len(files) + 1} files  {path.stat().st_size / 1e6:6.1f} MB"
        )
    return built


def upload(built: dict[str, Path], bucket_name: str) -> dict[str, str]:
    import google.cloud.storage as gcs

    client = gcs.Client(project=bucket_name)
    bucket = client.bucket(bucket_name, user_project=bucket_name)
    urls = {}
    for table, path in built.items():
        blob_name = f"auxiliary_files/{DATASET_ID}/{table}/auxiliary_files.zip"
        bucket.blob(blob_name).upload_from_filename(str(path))
        urls[table] = PUBLIC_URL.format(
            bucket=bucket_name, ds=DATASET_ID, table=table
        )
        print(f"  uploaded gs://{bucket_name}/{blob_name}")
    return urls


def verify_anonymous(urls: dict[str, str]) -> None:
    """Fetch each published URL with no credentials and report what it returns.

    Never state that an auxiliary file is "available at" a URL that has not been
    fetched anonymously.
    """
    import urllib.error
    import urllib.request

    for table, url in urls.items():
        try:
            with urllib.request.urlopen(
                urllib.request.Request(url, method="HEAD"), timeout=30
            ) as response:
                print(f"  {table:18s} HTTP {response.status}  {url}")
        except urllib.error.HTTPError as error:
            print(f"  {table:18s} HTTP {error.code}  {url}")
        except Exception as error:
            print(f"  {table:18s} {type(error).__name__}  {url}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--upload", action="store_true")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    args = parser.parse_args()

    print("bundles:")
    built = build()
    if args.upload:
        print(f"\nupload -> gs://{args.bucket}:")
        urls = upload(built, args.bucket)
        print("\nanonymous fetch of each published URL:")
        verify_anonymous(urls)
        (AUX_DIR / "urls.json").write_text(
            __import__("json").dumps(urls, indent=2), encoding="utf-8"
        )
        print(f"\nURLs written to {AUX_DIR / 'urls.json'}")


if __name__ == "__main__":
    main()
