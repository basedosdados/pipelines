"""Build the per-table auxiliary-file bundles for ar_indec_eph.

Downloads INDEC's documentation, sorts it per aux_manifest.py, and writes one
ZIP per table with a README recording the citation, what each file is, the URL it
came from and the date it was fetched.

Upload target, per .claude/rules/auxiliary-files.md -- the public, NON
requester-pays bucket, or every link returns HTTP 400 to an anonymous visitor:

    gs://basedosdados-public/auxiliary_files/ar_indec_eph/<table>/auxiliary_files.zip
"""

import argparse
import json
import re
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from aux_manifest import (
    BROKEN_URL_FIX,
    INDIVIDUO_ONLY,
    SHARED_METHODOLOGY,
    is_record_layout,
)
from constants import (
    BASE_URL,
    CODE_DIR,
    DATA_DIR,
    HTML_ERROR_SIZE,
)

DOCS = DATA_DIR / "docs"
BUNDLES = DATA_DIR / "auxiliary_files"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; basedosdados/1.0)"}
PUBLIC_BUCKET = "basedosdados-public"
GCS_PREFIX = "auxiliary_files/ar_indec_eph"
PUBLIC_URL = f"https://storage.googleapis.com/{PUBLIC_BUCKET}/{GCS_PREFIX}"
CITATION = (
    "Instituto Nacional de Estadistica y Censos (INDEC), Encuesta Permanente de "
    "Hogares (EPH continua). Bases de microdatos y documentacion, "
    "https://www.indec.gob.ar/"
)


def url_map() -> dict[str, str]:
    """basename -> full catalog URL. Not every document is under the EPH path."""
    urls = json.loads(
        (CODE_DIR / "aux_documents.json").read_text(encoding="utf-8")
    )
    return {u.rsplit("/", 1)[-1]: u for u in urls}


def fetch(basename: str, urls: dict[str, str]) -> Path | None:
    """Download one document, substituting a working URL for a broken one."""
    real = BROKEN_URL_FIX.get(basename, basename)
    dest = DOCS / real
    if dest.exists() and dest.stat().st_size > HTML_ERROR_SIZE:
        return dest
    url = urls.get(real) or (BASE_URL + real)
    try:
        response = requests.get(url, headers=HEADERS, timeout=180)
        response.raise_for_status()
    except Exception as exc:
        print(f"  !! {basename}: {exc}")
        return None
    if len(
        response.content
    ) <= HTML_ERROR_SIZE or not response.content.startswith(b"%PDF"):
        print(f"  !! {basename}: served the HTML error page, not a PDF")
        return None
    DOCS.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(response.content)
    return dest


def wave_label(basename: str) -> str:
    """A human-readable wave label from a record-layout filename."""
    patterns = [
        (
            r"EPH_registro_(\d)T(\d{4})",
            lambda m: f"{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_registro_(\d)t(\d{4})",
            lambda m: f"{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_registro_t(\d)(\d{2})",
            lambda m: f"20{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_registro_(\d)t(\d{2})\.",
            lambda m: f"20{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_registro_(\d)_trim_(\d{4})",
            lambda m: f"{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_disenoreg_T(\d)_(\d{4})",
            lambda m: f"{m.group(2)} Q{m.group(1)}",
        ),
        (
            r"EPH_diseno_reg_t(\d)(\d{2})",
            lambda m: f"20{m.group(2)} Q{m.group(1)}",
        ),
    ]
    for pattern, fmt in patterns:
        m = re.search(pattern, basename)
        if m:
            return fmt(m)
    return "sin onda identificada"


def upload(table: str, zip_path: Path) -> str:
    """Upload one bundle to the public, non requester-pays bucket.

    The two data-lake buckets (basedosdados, basedosdados-dev) are
    requester-pays, so anything served from them returns UserProjectMissing to an
    anonymous visitor. basedosdados-public is how the public already reaches Data
    Basis data. See .claude/rules/auxiliary-files.md.
    """
    from google.cloud import storage

    client = storage.Client(project="basedosdados-dev")
    bucket = client.bucket(PUBLIC_BUCKET)
    blob = bucket.blob(f"{GCS_PREFIX}/{table}/auxiliary_files.zip")
    blob.upload_from_filename(str(zip_path), content_type="application/zip")
    return f"{PUBLIC_URL}/{table}/auxiliary_files.zip"


def verify_anonymous(url: str) -> str:
    """Fetch the published URL with no credentials and report what it returns."""
    import urllib.request

    request = urllib.request.Request(url, method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            return f"HTTP {response.status} {response.headers.get('Content-Length')} bytes"
    except Exception as exc:
        return f"FAILED {exc}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--upload",
        action="store_true",
        help=f"upload each bundle to gs://{PUBLIC_BUCKET}/{GCS_PREFIX}/ and verify "
        "the published URL anonymously",
    )
    args = parser.parse_args()
    urls = url_map()
    layouts = [b for b in urls if is_record_layout(b)]
    today = date.today().isoformat()

    plan = {
        "microdatos_individuo": [
            *[
                (b, f"Diseno de registros de la onda {wave_label(b)}")
                for b in layouts
            ],
            *INDIVIDUO_ONLY.items(),
            *SHARED_METHODOLOGY.items(),
        ],
        "microdatos_hogar": [
            *[
                (b, f"Diseno de registros de la onda {wave_label(b)}")
                for b in layouts
            ],
            *SHARED_METHODOLOGY.items(),
        ],
    }

    BUNDLES.mkdir(parents=True, exist_ok=True)
    for table, entries in plan.items():
        got: list[tuple[str, str, Path]] = []
        missing: list[str] = []
        for basename, description in entries:
            path = fetch(basename, urls)
            if path is None:
                missing.append(basename)
                continue
            got.append((basename, description, path))

        readme = [
            f"# Archivos auxiliares de ar_indec_eph / {table}",
            "",
            "## Cita",
            "",
            CITATION,
            "",
            "## Por que este paquete existe",
            "",
            "Las columnas de los microdatos de la EPH son codigos de cuestionario",
            "(CH04, PP04D_COD, V5_01), el diseno de registros cambia entre ondas, y",
            "los codigos de ocupacion, actividad y geografia se resuelven solo contra",
            "clasificadores publicados por separado. Sin esta documentacion la tabla",
            "no es interpretable.",
            "",
            "## Contenido",
            "",
            f"Descargado de www.indec.gob.ar el {today}.",
            "",
        ]
        for basename, description, _path in sorted(got):
            real = BROKEN_URL_FIX.get(basename, basename)
            source = urls.get(real) or (BASE_URL + real)
            note = ""
            if basename in BROKEN_URL_FIX:
                note = (
                    f" (el catalogo datos.gob.ar publica este documento como "
                    f"{basename}, una URL que devuelve la pagina de error de INDEC; "
                    f"se uso {real})"
                )
            readme.append(
                f"- `{real}` — {description}. Fuente: {source}{note}"
            )
        if missing:
            readme += [
                "",
                "## No incluidos",
                "",
                "Los siguientes documentos figuran en el catalogo pero no se pudieron",
                "descargar en la fecha indicada:",
                "",
                *[f"- {b}" for b in sorted(missing)],
            ]
        readme += [
            "",
            "## Notas sobre los datos",
            "",
            "- INDEC no relevo 2007 Q3 y no publico 2015 Q3, 2015 Q4 ni 2016 Q1.",
            "- Las series entre 2007 y 2015 estan sujetas a la advertencia oficial de",
            "  INDEC sobre el periodo de intervencion; ver",
            "  anexo_informe_eph_23_08_16.pdf en este paquete.",
            "- El identificador de vivienda cambia de formato en 2016: hasta 2015 Q2 es",
            "  un numero de 6 digitos, desde 2016 Q2 una cadena alfanumerica de 29",
            "  caracteres. No permite seguir una vivienda a traves de ese corte.",
            "- ch15_cod y ch16_cod usan abreviaturas de tres letras en las tres ondas",
            "  de 2016 y codigos numericos en el resto de la serie.",
            "- En las columnas de montos, el valor -9 indica Ns./Nr., no un monto",
            "  negativo. En ch06 (edad), -1 identifica a los menores de un anio.",
            "",
        ]

        dest = BUNDLES / table
        dest.mkdir(parents=True, exist_ok=True)
        zip_path = dest / "auxiliary_files.zip"
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", "\n".join(readme))
            for basename, _description, path in sorted(got):
                zf.write(path, BROKEN_URL_FIX.get(basename, basename))
        size = zip_path.stat().st_size
        print(
            f"{table}: {len(got)} documents, {size / 1e6:.1f} MB -> {zip_path}"
        )
        if missing:
            print(f"   missing: {missing}")
        if args.upload:
            published = upload(table, zip_path)
            print(f"   uploaded -> {published}")
            print(f"   anonymous fetch: {verify_anonymous(published)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
