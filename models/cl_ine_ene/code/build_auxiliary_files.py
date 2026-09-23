#!/usr/bin/env python3
"""Build the auxiliary-file bundle for cl_ine_ene.microdato.

The ENE microdata is 271 columns of questionnaire codes — `b14_rev4cl_caenes`,
`cae_especifico`, `ina_cal_edu`. Without the codebooks the table is unusable, so
both go in the bundle rather than being left as links.

Two codebooks are needed, not one, and their file names are misleading:

  codigos-ene-2019.pdf   dated July 2019 — documents the schema in force until
                         the NDE 2019 quarter
  codigos-ene-2020.pdf   dated July 2026 — the CURRENT codebook, despite the
                         2020 in its name

Per .claude/rules/auxiliary-files.md the bundle goes to the prod bucket at
auxiliary_files/<gcp_dataset_id>/<table_slug>/auxiliary_files.zip. Note the
published URL will return HTTP 400 to an anonymous fetch — both GCS buckets are
requester-pays — so verify and report the real status rather than claiming the
files are available.
"""

from __future__ import annotations

import datetime as dt
import os
import pathlib
import zipfile

import requests

DATA = pathlib.Path(
    os.environ.get(
        "CL_INE_ENE_DATA", pathlib.Path.home() / "Downloads/cl_ine_ene_data"
    )
)
DOCS = DATA / "docs"
BASE = "https://www.ine.gob.cl/docs/default-source/ocupacion-y-desocupacion/bbdd/libro-de-codigos"
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

BUNDLE = [
    (
        "libro_de_codigos_2010_2019.pdf",
        f"{BASE}/codigos-ene-2019.pdf",
        "Libro de códigos de la ENE para el esquema vigente hasta el trimestre OND 2019 "
        "(INE, julio 2019). Documenta las variables retiradas en el rediseño de 2020.",
    ),
    (
        "libro_de_codigos_vigente.pdf",
        f"{BASE}/codigos-ene-2020.pdf",
        "Libro de códigos vigente de la ENE (INE, julio 2026). Pese al «2020» del nombre "
        "de archivo en la fuente, es la edición actual y documenta también las variables "
        "descontinuadas. Incluye los anexos de comunas, países y grupos ocupacionales.",
    ),
]

README = """# cl_ine_ene — microdato — archivos auxiliares

Encuesta Nacional de Empleo (ENE), Instituto Nacional de Estadísticas de Chile.

## Cita

Instituto Nacional de Estadísticas (INE), Encuesta Nacional de Empleo (ENE).
https://www.ine.gob.cl/estadisticas-por-tema/mercado-laboral/ocupacion-y-desocupacion

Licencia CC BY-SA 4.0, según https://www.ine.gob.cl/terminos-de-uso-y-licencia-de-datos-abiertos

## Contenido

{contents}

Descargados el {today} desde {base}.

## Por qué dos libros de códigos

La ENE fue rediseñada en el trimestre EFM 2020. La serie publicada contiene 16
esquemas distintos entre 2010-02 y 2026-06, con 271 nombres de columna en total y
sólo 110 presentes en los 197 archivos. Las columnas compartidas conservan su
significado a través del rediseño; el cambio sistemático es el código de no
respuesta, que pasa de `999` a `88`/`99`.

`libro_de_codigos_vigente.pdf` documenta el esquema actual y buena parte de las
variables descontinuadas. `libro_de_codigos_2010_2019.pdf` es necesario para las
variables que desaparecieron antes de esa edición, entre ellas `b1_ciuo88`,
`e16_ciuo88`, `b13_ciiu_rev3`, `b14_ciiu_rev3`, `e18_ciiu_rev3`, `id_directorio`,
`region_15`, `estrato_15` y `r_p_c_15`.

## Transformaciones aplicadas al publicar

- `ano_trimestre` → `ano` y `mes_central` → `mes`, que son las columnas de partición.
  Ambas se refieren al mes CENTRAL del trimestre móvil, no al mes de la entrevista.
- `region` → `id_region`, `provincia` → `id_provincia`, `r_p_c` → `id_comuna`,
  rellenados con ceros a la izquierda a 2, 3 y 5 dígitos para empalmar con el
  directorio `br_bd_diretorios_cl`. El resto de las columnas conserva el nombre
  del cuestionario, que es como se leen contra el libro de códigos.
- Los factores de expansión (`fact`, `fact_cal`) se publican en la fuente con coma
  decimal; aquí se convierten a punto.
- Cada período se reindexa sobre el universo completo de 271 columnas: las que un
  período no preguntó quedan nulas.

## Advertencia sobre el dominio de estimación

El diseño muestral de la ENE entrega estimaciones a nivel nacional y regional. NO
contempla estimaciones comunales. `id_comuna` identifica la vivienda seleccionada
y no constituye un dominio de estimación válido.
"""


def fetch(name: str, url: str) -> pathlib.Path:
    DOCS.mkdir(parents=True, exist_ok=True)
    dest = DOCS / name
    if dest.exists() and dest.stat().st_size > 100_000:
        return dest
    response = requests.get(
        url, headers={"User-Agent": USER_AGENT}, timeout=180
    )
    response.raise_for_status()
    dest.write_bytes(response.content)
    return dest


def main():
    today = dt.date.today().isoformat()
    entries, files = [], []
    for name, url, note in BUNDLE:
        path = fetch(name, url)
        files.append(path)
        entries.append(f"- `{name}` — {note}\n  Fuente: {url}")

    readme = README.format(contents="\n".join(entries), today=today, base=BASE)
    out = DATA / "auxiliary_files.zip"
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as bundle:
        bundle.writestr("README.md", readme)
        for path in files:
            bundle.write(path, path.name)
    print(f"wrote {out} ({out.stat().st_size / 1e6:.1f} MB)")
    print(
        "upload to: gs://basedosdados/auxiliary_files/cl_ine_ene/microdato/auxiliary_files.zip"
    )


if __name__ == "__main__":
    main()
