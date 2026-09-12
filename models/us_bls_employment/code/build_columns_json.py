#!/usr/bin/env python3
"""Emit columns_json payloads for mcp__databasis__bulk_upsert_columns.

Reads the architecture CSVs (English descriptions, types, dictionary and unit
flags) and attaches the Portuguese and Spanish translations kept below, so
columns can be registered without a Google Sheet. Writes
code/columns_json/<table>.json.

A translation key is either a bare column name or ``name:table`` when one table
needs wording the others do not.

Usage:
    uv run python models/us_bls_employment/code/build_columns_json.py
"""

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARCH = ROOT / "code" / "architecture"
OUT = ROOT / "code" / "columns_json"

# name (or name:table) -> (pt, es). English comes from the architecture CSV.
DESC = {
    "year": (
        "Ano de referência da observação",
        "Año de referencia de la observación",
    ),
    "month": (
        "Mês de referência da observação, de 1 a 12",
        "Mes de referencia de la observación, de 1 a 12",
    ),
    "period_id": (
        "Código de período do BLS que identifica o mês ou a média anual",
        "Código de período del BLS que identifica el mes o el promedio anual",
    ),
    "series_id:ces_national": (
        "Identificador completo da série do CES do BLS",
        "Identificador completo de la serie del CES del BLS",
    ),
    "series_id:ces_state_metro": (
        "Identificador completo da série estadual e metropolitana do CES do BLS",
        "Identificador completo de la serie estatal y metropolitana del CES del BLS",
    ),
    "series_id:laus": (
        "Identificador completo da série do LAUS do BLS",
        "Identificador completo de la serie del LAUS del BLS",
    ),
    "series_id:jolts": (
        "Identificador completo da série do JOLTS do BLS",
        "Identificador completo de la serie del JOLTS del BLS",
    ),
    "seasonal_adjustment": (
        "Indica se a série é ajustada sazonalmente (S) ou não ajustada sazonalmente (U)",
        "Indica si la serie está ajustada estacionalmente (S) o no ajustada estacionalmente (U)",
    ),
    "supersector_id": (
        "Supersetor do CES ao qual o setor de atividade pertence",
        "Supersector del CES al que pertenece el sector de actividad",
    ),
    "industry_id:ces_national": (
        "Código de setor de atividade do CES",
        "Código de sector de actividad del CES",
    ),
    "industry_id:ces_state_metro": (
        "Código de setor de atividade do CES",
        "Código de sector de actividad del CES",
    ),
    "industry_id:jolts": (
        "Código de setor de atividade do JOLTS",
        "Código de sector de actividad del JOLTS",
    ),
    "naics_id": (
        "Código NAICS correspondente ao setor de atividade do CES",
        "Código NAICS correspondiente al sector de actividad del CES",
    ),
    "data_type_id": (
        "Tipo de dado do CES, que determina o que o valor mede",
        "Tipo de dato del CES, que determina qué mide el valor",
    ),
    "state_id:ces_state_metro": (
        "Código FIPS do estado ou território",
        "Código FIPS del estado o territorio",
    ),
    "state_id:laus": (
        "Código FIPS do estado ou território ao qual a área pertence",
        "Código FIPS del estado o territorio al que pertenece el área",
    ),
    "state_id:jolts": ("Código FIPS do estado", "Código FIPS del estado"),
    "county_id": (
        "Código FIPS do condado, nas linhas de condado",
        "Código FIPS del condado, en las filas de condado",
    ),
    "cbsa_id:ces_state_metro": (
        "Código CBSA da área metropolitana ou micropolitana",
        "Código CBSA del área metropolitana o micropolitana",
    ),
    "cbsa_id:laus": (
        "Código CBSA, nas linhas de área metropolitana e micropolitana",
        "Código CBSA, en las filas de área metropolitana y micropolitana",
    ),
    "area_id:ces_state_metro": (
        "Código de área estadual e metropolitana do BLS, incluindo o agregado estadual",
        "Código de área estatal y metropolitana del BLS, incluido el agregado estatal",
    ),
    "area_id:laus": (
        "Código de área do LAUS, de 15 caracteres",
        "Código de área del LAUS, de 15 caracteres",
    ),
    "area_type_id": (
        "Tipo de área do LAUS, do nível estadual até municípios e áreas de saldo estadual",
        "Tipo de área del LAUS, desde el nivel estatal hasta municipios y áreas de saldo estatal",
    ),
    "measure_id": (
        "Medida do LAUS que o valor reporta",
        "Medida del LAUS que reporta el valor",
    ),
    "region_id": (
        "Região censitária que a estimativa cobre",
        "Región censal que cubre la estimación",
    ),
    "sizeclass_id": (
        "Classe de tamanho do estabelecimento que a estimativa cobre",
        "Clase de tamaño del establecimiento que cubre la estimación",
    ),
    "dataelement_id": (
        "Elemento de dado do JOLTS que o valor reporta",
        "Elemento de dato del JOLTS que reporta el valor",
    ),
    "ratelevel_id": (
        "Indica se o valor é um nível ou uma taxa",
        "Indica si el valor es un nivel o una tasa",
    ),
    "benchmark_year": (
        "Ano do benchmark ao qual a série está ancorada atualmente",
        "Año del benchmark al que la serie está anclada actualmente",
    ),
    "value": (
        "Valor publicado da série para o período",
        "Valor publicado de la serie para el período",
    ),
    "measurement_unit": (
        "Unidade em que o valor é medido",
        "Unidad en la que se mide el valor",
    ),
    "footnote_id": (
        "Código de nota de rodapé do BLS associado à observação",
        "Código de nota al pie del BLS asociado a la observación",
    ),
    "id_tabela": (
        "Slug da tabela de us_bls_employment que a entrada do dicionário descreve",
        "Slug de la tabla de us_bls_employment que describe la entrada del diccionario",
    ),
    "nome_coluna": (
        "Nome da coluna que a entrada do dicionário descreve",
        "Nombre de la columna que describe la entrada del diccionario",
    ),
    "chave": (
        "Valor codificado (chave) exatamente como armazenado nos dados",
        "Valor codificado (clave) exactamente como se almacena en los datos",
    ),
    "cobertura_temporal": (
        "Cobertura temporal da chave",
        "Cobertura temporal de la clave",
    ),
    "valor": (
        "Rótulo legível correspondente ao valor codificado",
        "Etiqueta legible correspondiente al valor codificado",
    ),
}

# Free-text notes, keyed the same way. English comes from the architecture CSV's
# `observations`; a column absent here ships its notes in English only, so every
# column that has notes must appear.
OBS = {
    "year": ("Coluna de partição", "Columna de partición"),
    "month": (
        "Derivada do código de período do BLS M01-M12; nula nas linhas de média "
        "anual, que carregam period_id M13",
        "Derivada del código de período del BLS M01-M12; nula en las filas de "
        "promedio anual, que llevan period_id M13",
    ),
    "period_id": (
        "Código de período do BLS como publicado. M01-M12 são meses; M13 é a "
        "média anual publicada nos mesmos arquivos, para a qual month é nulo",
        "Código de período del BLS tal como se publica. M01-M12 son meses; M13 "
        "es el promedio anual publicado en los mismos archivos, para el cual "
        "month es nulo",
    ),
    "series_id": (
        "Mantido ao lado das dimensões decompostas para que uma série possa ser "
        "consultada diretamente no BLS",
        "Se mantiene junto a las dimensiones descompuestas para que una serie "
        "pueda consultarse directamente en el BLS",
    ),
    "seasonal_adjustment": (
        "Uma dimensão da série, não uma nota: valores ajustados e não ajustados "
        "para a mesma célula são números diferentes e nunca devem ser misturados",
        "Una dimensión de la serie, no una nota: los valores ajustados y no "
        "ajustados para la misma celda son números distintos y nunca deben "
        "mezclarse",
    ),
    "industry_id:ces_national": (
        "Código de 8 dígitos do BLS cujos dois primeiros dígitos repetem o "
        "supersetor e cujos seis restantes são um código NAICS preenchido com "
        "zeros à direita. É uma hierarquia do BLS e não o NAICS em si, por isso "
        "nenhum vínculo com diretório é declarado; naics_id carrega o código "
        "NAICS resolvível",
        "Código de 8 dígitos del BLS cuyos dos primeros dígitos repiten el "
        "supersector y cuyos seis restantes son un código NAICS completado con "
        "ceros a la derecha. Es una jerarquía del BLS y no el NAICS en sí, por "
        "lo que no se declara ningún vínculo con directorio; naics_id lleva el "
        "código NAICS resoluble",
    ),
    "industry_id:ces_state_metro": None,  # filled below from ces_national
    "data_type_id:ces_state_metro": None,  # filled below from ces_national
    "naics_id": (
        "Extraído do campo naics_code de ce.industry, e nulo onde nenhum código "
        "único se aplica: agregados do CES não têm equivalente NAICS, listam "
        "códigos irmãos, ou marcam parte de um setor. Os códigos seguem a versão "
        "do NAICS vigente na publicação; o vínculo é declarado contra o NAICS 2022",
        "Extraído del campo naics_code de ce.industry, y nulo donde no aplica un "
        "código único: los agregados del CES no tienen equivalente NAICS, listan "
        "códigos hermanos, o marcan parte de un sector. Los códigos siguen la "
        "versión del NAICS vigente en la publicación; el vínculo se declara "
        "contra el NAICS 2022",
    ),
    "data_type_id:ces_national": (
        "Contagens de emprego, horas semanais médias, salários médios por hora e "
        "por semana, horas e folhas de pagamento agregadas, índices de difusão e "
        "taxas de resposta compartilham a coluna value",
        "Recuentos de empleo, horas semanales medias, salarios medios por hora y "
        "por semana, horas y nóminas agregadas, índices de difusión y tasas de "
        "respuesta comparten la columna value",
    ),
    "state_id:ces_state_metro": (
        "Nulo nas linhas de todos os estados, cujo código bruto 00 é um agregado "
        "e não um lugar",
        "Nulo en las filas de todos los estados, cuyo código bruto 00 es un "
        "agregado y no un lugar",
    ),
    "state_id:laus": (
        "Extraído do campo srd_code do catálogo de séries. Nulo nas linhas de "
        "região e divisão censitária, cujo código 80 não é um estado",
        "Extraído del campo srd_code del catálogo de series. Nulo en las filas "
        "de región y división censal, cuyo código 80 no es un estado",
    ),
    "state_id:jolts": (
        "Nulo nas linhas nacionais, cujo código bruto 00 é um agregado e não um "
        "lugar",
        "Nulo en las filas nacionales, cuyo código bruto 00 es un agregado y no "
        "un lugar",
    ),
    "county_id": (
        "Lido dos caracteres 3 a 7 do código de área do LAUS, e apenas no tipo de "
        "área F. Nulo em todos os demais tipos de área. Quatro áreas censitárias "
        "do Alasca extintas antes da delimitação atual de condados (02201, 02232, "
        "02261, 02280) aparecem entre 1990 e 2019 e não resolvem contra o diretório",
        "Leído de los caracteres 3 a 7 del código de área del LAUS, y solo en el "
        "tipo de área F. Nulo en todos los demás tipos de área. Cuatro áreas "
        "censales de Alaska extinguidas antes de la delimitación actual de "
        "condados (02201, 02232, 02261, 02280) aparecen entre 1990 y 2019 y no "
        "resuelven contra el directorio",
    ),
    "cbsa_id:ces_state_metro": (
        "Nulo nas linhas estaduais, cujo código de área bruto 00000 é um agregado "
        "e não um lugar. As áreas seguem a delimitação do OMB vigente na "
        "publicação; o vínculo é declarado contra a delimitação de 2023",
        "Nulo en las filas estatales, cuyo código de área bruto 00000 es un "
        "agregado y no un lugar. Las áreas siguen la delimitación del OMB vigente "
        "en la publicación; el vínculo se declara contra la delimitación de 2023",
    ),
    "cbsa_id:laus": (
        "Lido dos caracteres 5 a 9 do código de área do LAUS, e apenas nos tipos "
        "de área B e D. Divisões metropolitanas e áreas combinadas carregam um "
        "código na mesma posição que não é um código CBSA, por isso são nulas "
        "aqui e legíveis apenas em area_id",
        "Leído de los caracteres 5 a 9 del código de área del LAUS, y solo en los "
        "tipos de área B y D. Las divisiones metropolitanas y las áreas "
        "combinadas llevan un código en la misma posición que no es un código "
        "CBSA, por lo que son nulas aquí y legibles solo en area_id",
    ),
    "area_id:laus": (
        "O único identificador que cobre todos os tipos de área. Os dois "
        "primeiros caracteres indicam o tipo: ST estado, MT metropolitana, DV "
        "divisão metropolitana, MC micropolitana, CA combinada, CN condado, CS e "
        "CT cidades e municípios, PT parte de cidade, SA pequena área de mercado "
        "de trabalho, ID e IM parte interestadual, BS saldo estadual, RD região "
        "ou divisão censitária",
        "El único identificador que cubre todos los tipos de área. Los dos "
        "primeros caracteres indican el tipo: ST estado, MT metropolitana, DV "
        "división metropolitana, MC micropolitana, CA combinada, CN condado, CS y "
        "CT ciudades y municipios, PT parte de ciudad, SA pequeña área de mercado "
        "laboral, ID e IM parte interestatal, BS saldo estatal, RD región o "
        "división censal",
    ),
    "measure_id": (
        "Taxa de desemprego, desemprego, emprego, força de trabalho, razão "
        "emprego-população, taxa de participação na força de trabalho e população "
        "civil não institucional compartilham a coluna value",
        "Tasa de desempleo, desempleo, empleo, fuerza laboral, razón "
        "empleo-población, tasa de participación en la fuerza laboral y población "
        "civil no institucional comparten la columna value",
    ),
    "region_id": (
        "O JOLTS armazena as quatro regiões censitárias no mesmo campo dos códigos "
        "FIPS de estado; elas são separadas aqui. Nulo nas linhas nacionais e "
        "estaduais",
        "El JOLTS almacena las cuatro regiones censales en el mismo campo que los "
        "códigos FIPS de estado; aquí se separan. Nulo en las filas nacionales y "
        "estatales",
    ),
    "industry_id:jolts": (
        "Código de 6 dígitos na hierarquia de setores do JOLTS, mais grosseira que "
        "o NAICS e contendo agregados que abrangem vários setores NAICS, por isso "
        "nenhum vínculo com diretório é declarado",
        "Código de 6 dígitos en la jerarquía de sectores del JOLTS, más gruesa que "
        "el NAICS y con agregados que abarcan varios sectores NAICS, por lo que no "
        "se declara ningún vínculo con directorio",
    ),
    "dataelement_id": (
        "Vagas abertas, contratações, desligamentos totais, pedidos de demissão, "
        "demissões e dispensas, outros desligamentos, a razão entre desempregados "
        "e vagas abertas, e taxas de resposta compartilham a coluna value",
        "Vacantes, contrataciones, separaciones totales, renuncias, despidos y "
        "ceses, otras separaciones, la razón entre desempleados y vacantes, y las "
        "tasas de respuesta comparten la columna value",
    ),
    "ratelevel_id": (
        "Níveis são contagens em milhares; taxas são o elemento como percentual do "
        "emprego",
        "Los niveles son recuentos en miles; las tasas son el elemento como "
        "porcentaje del empleo",
    ),
    "benchmark_year": (
        "O CES revisa a série histórica anualmente contra a contagem de emprego do "
        "QCEW; isto registra a qual benchmark os valores publicados se referem",
        "El CES revisa la serie histórica anualmente contra el recuento de empleo "
        "del QCEW; esto registra a qué benchmark se refieren los valores "
        "publicados",
    ),
    "value": (
        "As unidades diferem por linha e são dadas em measurement_unit, de modo "
        "que nenhuma unidade única no nível da coluna se aplica. O BLS imprime - "
        "para uma observação não publicada; essas são nulas, não zero",
        "Las unidades difieren por fila y se indican en measurement_unit, por lo "
        "que no aplica una unidad única a nivel de columna. El BLS imprime - para "
        "una observación no publicada; esas son nulas, no cero",
    ),
    "measurement_unit": (
        "Derivada pela Data Basis da dimensão que fixa a unidade (tipo de dado no "
        "CES, medida no LAUS, nível ou taxa no JOLTS); não publicada como campo "
        "pelo BLS",
        "Derivada por Data Basis de la dimensión que fija la unidad (tipo de dato "
        "en el CES, medida en el LAUS, nivel o tasa en el JOLTS); no publicada "
        "como campo por el BLS",
    ),
    "footnote_id": (
        "Nulo quando a observação não carrega nota. Os códigos sinalizam valores "
        "preliminares e revisados e, em 2025, observações ausentes por causa da "
        "interrupção de recursos orçamentários",
        "Nulo cuando la observación no lleva nota. Los códigos señalan valores "
        "preliminares y revisados y, en 2025, observaciones ausentes por la "
        "interrupción de recursos presupuestarios",
    ),
}


# CES national and state/metro share the industry code scheme and the data-type
# column, so they share those notes rather than restating them.
for _key in ("industry_id", "data_type_id"):
    OBS[f"{_key}:ces_state_metro"] = OBS[f"{_key}:ces_national"]


def pick(table: str, name: str, mapping: dict):
    """Return the (pt, es) pair for a column, preferring a table-specific key."""
    return mapping.get(f"{name}:{table}") or mapping.get(name)


def main() -> None:
    """Write one columns_json payload per architecture CSV."""
    OUT.mkdir(parents=True, exist_ok=True)
    missing = []
    for path in sorted(ARCH.glob("*.csv")):
        table = path.stem
        cols = []
        with open(path, newline="") as fh:
            rows = list(csv.DictReader(fh))
        for r in rows:
            desc = pick(table, r["name"], DESC)
            if desc is None:
                missing.append(f"{table}.{r['name']} (description)")
                continue
            col = {
                "name": r["name"],
                "bigquery_type": r["bigquery_type"],
                "description_pt": desc[0],
                "description_en": r["description"],
                "description_es": desc[1],
                "covered_by_dictionary": r["covered_by_dictionary"]
                .strip()
                .lower()
                == "yes",
                "has_sensitive_data": r["has_sensitive_data"].strip().lower()
                == "yes",
            }
            if r["directory_column"].strip():
                col["directory_column"] = r["directory_column"].strip()
            if r["measurement_unit"].strip():
                col["measurement_unit"] = r["measurement_unit"].strip()
            if r["observations"].strip():
                obs = pick(table, r["name"], OBS)
                if obs is None:
                    missing.append(f"{table}.{r['name']} (observations)")
                    continue
                col["observations_pt"] = obs[0]
                col["observations_en"] = r["observations"]
                col["observations_es"] = obs[1]
            cols.append(col)
        (OUT / f"{table}.json").write_text(
            json.dumps(cols, ensure_ascii=False, indent=2)
        )
        print(f"{table}: {len(cols)} columns")
    if missing:
        raise SystemExit("missing translations:\n  " + "\n  ".join(missing))


if __name__ == "__main__":
    main()
