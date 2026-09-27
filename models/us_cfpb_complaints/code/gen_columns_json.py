"""Emit the `columns_json` payload for `bulk_upsert_columns`, in PT, EN and ES.

Types, dictionary flags, directory FKs, measurement units and sensitivity come from
the architecture TSVs (the source of truth); the trilingual descriptions and
observations live here. Passing all three languages matters: a bare `description`
or `observations` is stored as Portuguese only, which is how thousands of
production columns ended up PT-only.

    python gen_columns_json.py complaint > /tmp/complaint_columns.json
"""

import argparse
import json
import sys

from common import COMPLAINT, DICIONARIO, load_cols

# name -> (description_pt, description_en, description_es)
DESC = {
    "year": (
        "Ano-calendário de recebimento da reclamação pelo CFPB",
        "Calendar year in which the CFPB received the complaint",
        "Año calendario en que la CFPB recibió la reclamación",
    ),
    "complaint_id": (
        "Identificador único da reclamação atribuído pelo CFPB",
        "Unique complaint identifier assigned by the CFPB",
        "Identificador único de la reclamación asignado por la CFPB",
    ),
    "state_id": (
        "Código FIPS de duas posições do estado, território ou distrito de "
        "residência do consumidor",
        "Two-digit FIPS code of the consumer's state, territory or district",
        "Código FIPS de dos dígitos del estado, territorio o distrito de "
        "residencia del consumidor",
    ),
    "date_received": (
        "Data em que o CFPB recebeu a reclamação",
        "Date on which the CFPB received the complaint",
        "Fecha en que la CFPB recibió la reclamación",
    ),
    "date_sent_to_company": (
        "Data em que o CFPB encaminhou a reclamação à empresa",
        "Date on which the CFPB forwarded the complaint to the company",
        "Fecha en que la CFPB remitió la reclamación a la empresa",
    ),
    "product": (
        "Tipo de produto ou serviço financeiro identificado pelo consumidor",
        "Type of financial product or service the consumer identified",
        "Tipo de producto o servicio financiero identificado por el consumidor",
    ),
    "sub_product": (
        "Subtipo do produto ou serviço financeiro identificado pelo consumidor",
        "Sub-type of the financial product or service the consumer identified",
        "Subtipo del producto o servicio financiero identificado por el consumidor",
    ),
    "issue": (
        "Problema relatado pelo consumidor",
        "Problem the consumer reported",
        "Problema informado por el consumidor",
    ),
    "sub_issue": (
        "Detalhamento do problema relatado pelo consumidor",
        "Further detail of the problem the consumer reported",
        "Detalle adicional del problema informado por el consumidor",
    ),
    "company_name": (
        "Nome da empresa objeto da reclamação, conforme registrado no CFPB",
        "Name of the company the complaint is about, as registered with the CFPB",
        "Nombre de la empresa objeto de la reclamación, según consta en la CFPB",
    ),
    "state_abbreviation": (
        "Sigla de duas letras do estado de residência do consumidor, conforme "
        "publicada pelo CFPB",
        "Two-letter abbreviation of the consumer's state as published by the CFPB",
        "Sigla de dos letras del estado de residencia del consumidor, según la "
        "publica la CFPB",
    ),
    "zip_code": (
        "Código postal (ZIP code) de cinco dígitos informado pelo consumidor",
        "Five-digit ZIP code reported by the consumer",
        "Código postal (ZIP code) de cinco dígitos informado por el consumidor",
    ),
    "tags": (
        "Marcação atribuída pelo CFPB a reclamações de idosos ou de militares",
        "CFPB marking for complaints from older Americans or servicemembers",
        "Marca asignada por la CFPB a reclamaciones de personas mayores o militares",
    ),
    "submitted_via": (
        "Canal pelo qual a reclamação foi enviada ao CFPB",
        "Channel through which the complaint reached the CFPB",
        "Canal por el cual la reclamación llegó a la CFPB",
    ),
    "company_public_response": (
        "Resposta pública opcional da empresa, escolhida entre opções "
        "pré-definidas pelo CFPB",
        "The company's optional public response, chosen from a set of options the "
        "CFPB defines",
        "Respuesta pública opcional de la empresa, elegida entre opciones "
        "predefinidas por la CFPB",
    ),
    "company_response_to_consumer": (
        "Forma de encerramento dada pela empresa à reclamação",
        "How the company closed the complaint",
        "Forma en que la empresa cerró la reclamación",
    ),
    "timely_response": (
        "Indica se a empresa respondeu à reclamação dentro do prazo exigido pelo "
        "CFPB",
        "Whether the company responded within the timeframe the CFPB requires",
        "Indica si la empresa respondió dentro del plazo exigido por la CFPB",
    ),
    "consumer_complaint_narrative": (
        "Relato do consumidor em texto livre, publicado somente com "
        "consentimento do consumidor e após remoção de dados pessoais pelo CFPB",
        "The consumer's account of the complaint in free text, published only with "
        "the consumer's consent and after the CFPB removes personal information",
        "Relato del consumidor en texto libre, publicado solo con su "
        "consentimiento y tras la eliminación de datos personales por la CFPB",
    ),
    # dicionario
    "id_tabela": (
        "Nome da tabela à qual a chave e o valor se referem",
        "Name of the table the key and value refer to",
        "Nombre de la tabla a la que se refieren la clave y el valor",
    ),
    "nome_coluna": (
        "Nome da coluna à qual a chave e o valor se referem",
        "Name of the column the key and value refer to",
        "Nombre de la columna a la que se refieren la clave y el valor",
    ),
    "chave": (
        "Valor da categoria conforme armazenado na coluna",
        "Category value as stored in the column",
        "Valor de la categoría tal como se almacena en la columna",
    ),
    "cobertura_temporal": (
        "Intervalo de anos em que o valor foi observado na base",
        "Range of years over which the value is observed in the data",
        "Intervalo de años en que el valor se observa en los datos",
    ),
    "valor": (
        "Rótulo da categoria conforme publicado pelo CFPB",
        "Category label as published by the CFPB",
        "Etiqueta de la categoría según la publica la CFPB",
    ),
}

TAX_PT = (
    "Taxonomia do formulário de reclamação revisada em abril de 2017 e em agosto "
    "de 2023; os valores são preservados exatamente como publicados pelo CFPB, sem "
    "reclassificação retroativa. A tabela dicionario registra a cobertura temporal "
    "observada de cada valor."
)
TAX_EN = (
    "The complaint form's taxonomy was revised in April 2017 and August 2023; "
    "values are preserved exactly as the CFPB published them, without retroactive "
    "reclassification. The dicionario table records each value's observed temporal "
    "coverage."
)
TAX_ES = (
    "La taxonomía del formulario de reclamación se revisó en abril de 2017 y en "
    "agosto de 2023; los valores se conservan exactamente como los publicó la CFPB, "
    "sin reclasificación retroactiva. La tabla dicionario registra la cobertura "
    "temporal observada de cada valor."
)

# name -> (observations_pt, observations_en, observations_es)
OBS = {
    "year": (
        "Coluna de particionamento; derivada de date_received.",
        "Partition column; derived from date_received.",
        "Columna de partición; derivada de date_received.",
    ),
    "complaint_id": (
        "Chave lógica da tabela; numérico na fonte, mas sem significado aritmético.",
        "Logical key of the table; numeric in the source but with no arithmetic "
        "meaning.",
        "Clave lógica de la tabla; numérico en la fuente, pero sin significado "
        "aritmético.",
    ),
    "state_id": (
        "Derivado de state_abbreviation pela tabela de estados do diretório "
        "diretorios_us; nulo para os códigos postais militares AA, AE e AP "
        "(4.327 registros) e para registros sem estado informado (62.611).",
        "Derived from state_abbreviation via the state table of the diretorios_us "
        "directory; NULL for the military post codes AA, AE and AP (4,327 rows) "
        "and for rows with no state reported (62,611).",
        "Derivado de state_abbreviation mediante la tabla de estados del directorio "
        "diretorios_us; nulo para los códigos postales militares AA, AE y AP "
        "(4.327 filas) y para las filas sin estado informado (62.611).",
    ),
    "date_sent_to_company": (
        "Anterior a date_received em 7.050 registros (0,04%); inconsistência "
        "presente na fonte e preservada.",
        "Earlier than date_received on 7,050 rows (0.04%); an inconsistency present "
        "in the source and preserved.",
        "Anterior a date_received en 7.050 filas (0,04%); inconsistencia presente en "
        "la fuente y conservada.",
    ),
    "product": (TAX_PT, TAX_EN, TAX_ES),
    "sub_product": (
        "Nem todo produto possui subtipo. " + TAX_PT,
        "Not every product has sub-products. " + TAX_EN,
        "No todos los productos tienen subtipo. " + TAX_ES,
    ),
    "issue": (
        "Os valores disponíveis dependem do produto. " + TAX_PT,
        "The available values depend on the product. " + TAX_EN,
        "Los valores disponibles dependen del producto. " + TAX_ES,
    ),
    "sub_issue": (
        "Nem todo problema possui detalhamento; os valores dependem do produto e do "
        "problema. " + TAX_PT,
        "Not every issue has sub-issues; the values depend on the product and the "
        "issue. " + TAX_EN,
        "No todos los problemas tienen detalle; los valores dependen del producto y "
        "del problema. " + TAX_ES,
    ),
    "company_name": (
        "8.101 nomes distintos; a grafia de uma mesma instituição varia ao longo do "
        "tempo e não há identificador de empresa na fonte.",
        "8,101 distinct names; the spelling used for one institution varies over "
        "time and the source carries no company identifier.",
        "8.101 nombres distintos; la grafía de una misma institución varía con el "
        "tiempo y la fuente no incluye un identificador de empresa.",
    ),
    "state_abbreviation": (
        "Preservada exatamente como publicada; inclui os códigos postais militares "
        "AA, AE e AP e o valor literal UNITED STATES MINOR OUTLYING ISLANDS, que não "
        "são siglas do diretório. Sem chave estrangeira: a chave primária do "
        "diretório de estados é o código FIPS, não a sigla — use state_id.",
        "Preserved exactly as published; it also carries the military post codes AA, "
        "AE and AP and the literal value UNITED STATES MINOR OUTLYING ISLANDS, none "
        "of which are directory abbreviations. No foreign key: the state directory's "
        "primary key is the FIPS code, not the abbreviation — join on state_id.",
        "Conservada exactamente como fue publicada; incluye los códigos postales "
        "militares AA, AE y AP y el valor literal UNITED STATES MINOR OUTLYING "
        "ISLANDS, que no son siglas del directorio. Sin clave foránea: la clave "
        "primaria del directorio de estados es el código FIPS, no la sigla — use "
        "state_id.",
    ),
    "zip_code": (
        "Mascarado pelo CFPB quando a área postal tem 20 mil habitantes ou menos: "
        "1.130.458 registros trazem apenas o prefixo de três dígitos seguido de XX e "
        "138.969 vêm inteiramente mascarados como XXXXX; cerca de 65 registros "
        "trazem valores malformados presentes na fonte.",
        "Masked by the CFPB where the postal area has 20,000 inhabitants or fewer: "
        "1,130,458 rows carry only the three-digit prefix followed by XX and 138,969 "
        "are masked entirely as XXXXX; about 65 rows carry malformed values present "
        "in the source.",
        "Enmascarado por la CFPB cuando el área postal tiene 20.000 habitantes o "
        "menos: 1.130.458 filas llevan solo el prefijo de tres dígitos seguido de XX "
        "y 138.969 están enmascaradas por completo como XXXXX; unas 65 filas traen "
        "valores malformados presentes en la fuente.",
    ),
    "tags": (
        "Preenchida em 4,56% dos registros; um mesmo registro pode acumular as duas "
        "marcações, separadas por vírgula.",
        "Filled on 4.56% of rows; one row can carry both markings, separated by a "
        "comma.",
        "Completada en el 4,56% de las filas; una misma fila puede llevar ambas "
        "marcas, separadas por coma.",
    ),
    "company_public_response": (
        "Preenchida em 54,81% dos registros; publicada em até 180 dias quando a "
        "empresa opta por respondê-la.",
        "Filled on 54.81% of rows; published within 180 days where the company "
        "chooses to respond.",
        "Completada en el 54,81% de las filas; publicada en un plazo de 180 días "
        "cuando la empresa opta por responder.",
    ),
    "timely_response": (
        "Valores Yes e No preservados como publicados; não convertido para booleano.",
        "The values Yes and No are preserved as published; not converted to a "
        "boolean.",
        "Los valores Yes y No se conservan como fueron publicados; no se convierten "
        "a booleano.",
    ),
    "consumer_complaint_narrative": (
        "Presente em 3.847.965 registros (21,90%), e em apenas 2,4% das reclamações "
        "recebidas em 2026, porque a publicação ocorre meses após a reclamação. O "
        "CFPB substitui os dados pessoais identificados por sequências de X. Texto "
        "reproduzido literalmente, sem edição adicional.",
        "Present on 3,847,965 rows (21.90%), and on only 2.4% of complaints received "
        "in 2026, because publication trails the complaint by months. The CFPB "
        "replaces the personal information it identifies with runs of X. Text "
        "reproduced verbatim, without further editing.",
        "Presente en 3.847.965 filas (21,90%), y en solo el 2,4% de las "
        "reclamaciones recibidas en 2026, porque la publicación ocurre meses después "
        "de la reclamación. La CFPB sustituye los datos personales identificados por "
        "secuencias de X. Texto reproducido literalmente, sin edición adicional.",
    ),
    "cobertura_temporal": (
        "Registra a vigência observada de cada valor da taxonomia, revisada em 2017 "
        "e em 2023.",
        "Records the observed currency of each taxonomy value, revised in 2017 and "
        "2023.",
        "Registra la vigencia observada de cada valor de la taxonomía, revisada en "
        "2017 y en 2023.",
    ),
    "valor": (
        "As categorias do CFPB já são publicadas como rótulos legíveis, de modo que "
        "valor reproduz chave; a informação adicional está em cobertura_temporal.",
        "The CFPB's categories are already published as readable labels, so valor "
        "repeats chave; the additional information is in cobertura_temporal.",
        "Las categorías de la CFPB ya se publican como etiquetas legibles, por lo "
        "que valor reproduce chave; la información adicional está en "
        "cobertura_temporal.",
    ),
}


def build(table: str) -> list[dict]:
    out = []
    for c in load_cols(table):
        if c.name not in DESC:
            raise SystemExit(
                f"no trilingual description for column {c.name!r}"
            )
        pt, en, es = DESC[c.name]
        for label, text in (("pt", pt), ("en", en), ("es", es)):
            if text.endswith("."):
                raise SystemExit(
                    f"{c.name} description_{label} ends with a period"
                )
        row = {
            "name": c.name,
            "bigquery_type": c.bq_type,
            "description_pt": pt,
            "description_en": en,
            "description_es": es,
            "covered_by_dictionary": c.covered_by_dictionary,
            "has_sensitive_data": c.name == "consumer_complaint_narrative",
        }
        if c.directory_column:
            row["directory_column"] = c.directory_column
        if c.measurement_unit:
            row["measurement_unit"] = c.measurement_unit
        if c.name in OBS:
            o_pt, o_en, o_es = OBS[c.name]
            row["observations_pt"] = o_pt
            row["observations_en"] = o_en
            row["observations_es"] = o_es
        out.append(row)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("table", choices=[COMPLAINT, DICIONARIO])
    a = ap.parse_args()
    json.dump(build(a.table), sys.stdout, ensure_ascii=False)
    sys.stdout.write("\n")
