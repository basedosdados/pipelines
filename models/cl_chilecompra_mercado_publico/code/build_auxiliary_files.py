"""Build the per-table auxiliary-file bundles for cl_chilecompra_mercado_publico.

ChileCompra publishes no downloadable codebook -- its only documentation is the
"Definiciones" web page. That page is the sole authority for what the state codes mean
and how the four procurement mechanisms differ, so it is transcribed here rather than
left as a link that may be restructured away.

    uv run python models/cl_chilecompra_mercado_publico/code/build_auxiliary_files.py

Writes <root>/auxiliary_files/<table>/auxiliary_files.zip, ready to upload to
gs://basedosdados-public/auxiliary_files/cl_chilecompra_mercado_publico/<table>/ -- the
public bucket, not the data-lake ones, which are requester-pays and answer an anonymous
request with HTTP 400.
"""

from __future__ import annotations

import argparse
import os
import zipfile
from pathlib import Path

DEFAULT_ROOT = Path(
    os.environ.get(
        "CHILECOMPRA_DATA_DIR",
        Path.home() / "Downloads" / "cl_chilecompra_mercado_publico_data",
    )
)
CAPTURED = "2026-08-28"
DEFINICIONES_URL = (
    "https://datos-abiertos.chilecompra.cl/datos-abiertos/definiciones"
)
DESCARGAS_URL = "https://datos-abiertos.chilecompra.cl/descargas"

ESTADOS_OC = """## Estados de la orden de compra

Fuente: Definiciones, Datos Abiertos ChileCompra. Corresponde à coluna
`codigo_estado` (e `estado`).

| ID | Estado | Definición |
|---|---|---|
| 4 | Enviada a proveedor | El comprador público envía la orden de compra al proveedor. |
| 5 | En proceso | La orden compra enviada todavía no es aceptada por el proveedor. |
| 6 | Aceptada | El proveedor aceptó la orden de compra enviada por el comprador. |
| 7 | Solicitud de cancelación | El proveedor solicita la cancelación de la orden de compra. |
| 12 | Recepción conforme | El comprador indica a través de la plataforma que recibió a conformidad los bienes o servicios por parte del proveedor. |
"""

ESTADOS_LIC = """## Estados de la licitación

Fuente: Definiciones, Datos Abiertos ChileCompra. Corresponde à coluna
`codigo_estado` (e `estado`). Repare que vários códigos colapsam num mesmo rótulo.

| ID | Estado | Definición |
|---|---|---|
| 5 | Publicada | Es una licitación respecto de la cual la respectiva entidad ha realizado el llamado a propuesta pública, mediante la publicación de las bases en www.mercadopublico.cl. A partir de ese momento se podrán recibir ofertas a través de la plataforma. Durante esta etapa las bases pueden ser modificadas, mediante el correspondiente acto administrativo. |
| 6, 11, 12, 13, 14 | Cerrada | Es aquella licitación respecto de la que ya se ha cumplido el plazo para el cierre de recepción de ofertas y se ha practicado la apertura de las respectivas ofertas. Durante esta etapa las ofertas son sometidas al proceso de evaluación. |
| 7 | Desierta | Es aquella licitación en la que no se han presentado ofertas, o bien, éstas no resultaron convenientes a sus intereses. En ambos casos la declaración deberá ser por resolución fundada. |
| 8, 9, 10 | Adjudicada | Es una licitación respecto de la cual ya se ha emitido la resolución de adjudicación, seleccionando al o los proveedores mejor evaluados. |
| 15 | Revocada | Es un proceso que se deja sin efecto. Este acto debe formalizarse por resolución fundada antes de la adjudicación. |
| 16 | Suspendida | Es aquella licitación cuya tramitación ha sido suspendida por disponerlo así el Tribunal de Contratación Pública mediante una resolución judicial. |
"""

PROCEDIMIENTOS = """## Procedimientos de compra (Ley N° 19.886)

- **Tienda Convenio Marco** — catálogo electrónico de productos y servicios altamente
  demandados por el Estado, primera opción de compra que los organismos públicos deben
  consultar antes de hacer una licitación.
- **Licitación pública** — llamado público, abierto y competitivo, si no encuentran el
  producto o servicio en la tienda de convenios marco.
- **Licitación privada** — el llamado a participar es específico a algunas empresas o
  personas, con un mínimo de tres proveedores del rubro.
- **Trato Directo** — procedimiento excepcional, con causales establecidas en la
  normativa, para seleccionar a un proveedor sin llamado público. Requiere acto
  administrativo fundado y, en la mayoría de los casos, tres cotizaciones.

## Modalidades de compra

- **Compra Ágil** — habilitada en 2020, para compras menores a 30 UTM. Corresponde à
  coluna `indicador_compra_agil` em `orden_compra_item`.
- **Bases Tipo de Licitación** — bases administrativas estandarizadas, ya tomadas de
  razón por la Contraloría. Corresponde à coluna `indicador_base_tipo` em
  `licitacion_item`.
- **Compras Coordinadas** — dos o más organismos agregan demanda para obtener
  condiciones más ventajosas.
"""


def readme(table: str, extra_sections: list[str]) -> str:
    return f"""# Auxiliary files — `cl_chilecompra_mercado_publico.{table}`

## Citación

> Dirección ChileCompra, Ministerio de Hacienda, Gobierno de Chile — Datos Abiertos
> Mercado Público.

ChileCompra exige atribución explícita: "deberán indicar claramente que la fuente de los
datos es la Dirección ChileCompra".

## Qué contiene este paquete

| Archivo | Qué es | Origen |
|---|---|---|
| `README.md` | este archivo | — |
| `definiciones.md` | transcripción de la única documentación que ChileCompra publica sobre estos datos | {DEFINICIONES_URL} (capturado {CAPTURED}) |

ChileCompra **no publica un codebook descargable**. Su documentación existe únicamente
como páginas web, por lo que se transcribe aquí en lugar de enlazarla: la página puede
reestructurarse y los códigos dejarían de ser interpretables.

Los códigos que sí tienen etiqueta en los propios datos están además en la tabla
`cl_chilecompra_mercado_publico.dicionario`, que es la forma consultable de lo mismo.

## Origen de los datos

Descarga masiva, un ZIP por mes y por contenedor:

- órdenes de compra: `https://transparenciachc.blob.core.windows.net/oc-da/<año>-<mes>.zip`
- licitaciones: `https://transparenciachc.blob.core.windows.net/lic-da/<año>-<mes>.zip`

`<mes>` va de 1 a 12: **son archivos mensuales, no semestrales**, pese a que `2026-1` y
`2026-2` inviten a leerlos como semestres. Cobertura 2007-01 en adelante, reconstruidos
diariamente entre las 12:00 y 14:00 hora de Chile con un día de desfase.
Página de descargas: {DESCARGAS_URL}

## Advertencias que afectan la lectura de estos datos

1. **Los archivos NO están en UTF-8**, pese a que la página de Definiciones afirma que
   sí. Todos fallan al decodificarse como UTF-8; la codificación real es Windows-1252
   (bytes 0x92-0x97 son comillas tipográficas y guiones). Algunos archivos contienen
   además 0x81, que no está definido en Windows-1252.
2. **El separador decimal es la coma** y el separador de campos es el punto y coma.
3. Los campos entrecomillados contienen puntos y coma y saltos de línea internos.
4. Valores nulos aparecen como `NA`, cadena vacía, un espacio, y la fecha
   `1900-01-01`. En esta tabla todos ellos se convierten a NULL.
5. **Las cifras no coinciden con las estadísticas oficiales de ChileCompra.** El archivo
   de descarga masiva incluye deliberadamente órdenes de compra que ChileCompra excluye
   de sus cifras oficiales por tener errores en los montos o en el tipo de moneda.

## Transformaciones aplicadas al cargar

- Espacios sobrantes eliminados (la fuente entrega, por ejemplo, `"Región del Maule "`).
- Coma decimal convertida a punto. El punto nunca aparece como separador de miles en la
  fuente, por lo que no se toca.
- `criterios_evaluacion` reordenado alfabéticamente: la fuente lista los mismos
  criterios en órdenes distintos para una misma licitación.
- Nueve columnas presentes sólo en `lic-da/2014-3` y `lic-da/2014-4`, con nombre, RUT,
  cargo, correo y teléfono de funcionarios individuales, **no se publican** (datos
  personales, Ley N° 19.628).

{"".join(extra_sections)}
"""


BUNDLES = {
    "orden_compra_item": [ESTADOS_OC, PROCEDIMIENTOS],
    "licitacion_item": [ESTADOS_LIC, PROCEDIMIENTOS],
    "licitacion_oferta": [ESTADOS_LIC, PROCEDIMIENTOS],
}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    args = parser.parse_args()

    for table, sections in BUNDLES.items():
        out_dir = args.root / "auxiliary_files" / table
        out_dir.mkdir(parents=True, exist_ok=True)
        archive = out_dir / "auxiliary_files.zip"
        definiciones = (
            f"# Definiciones — ChileCompra Datos Abiertos\n\n"
            f"Transcrito de {DEFINICIONES_URL} el {CAPTURED}.\n\n"
            + "\n".join(sections)
        )
        with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.writestr("README.md", readme(table, sections))
            zf.writestr("definiciones.md", definiciones)
        print(f"{archive}  ({archive.stat().st_size:,} bytes)")
    print(
        "\nUpload to gs://basedosdados-public/auxiliary_files/"
        "cl_chilecompra_mercado_publico/<table>/auxiliary_files.zip -- the public "
        "bucket, NOT the data-lake buckets. basedosdados and basedosdados-dev are "
        "requester-pays, so a link served from either returns HTTP 400 "
        "UserProjectMissing to an anonymous visitor. Verify with an unauthenticated "
        "curl and report the real status rather than assuming it resolves."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
