"""Build the architecture CSVs and columns_json for br_mapbiomas_estatisticas.

The architecture table is the source of truth for column names, types, order and
descriptions. This script writes it from one declaration so the CSV uploaded to
Drive, the JSON registered in the backend and the dbt models can never drift.

Descriptions are written in Portuguese, English and Spanish. Per the Data Basis
style manual, column descriptions are capitalised and carry no trailing period.
"""

from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parents[2]
sys.path.insert(0, str(REPO_ROOT))

from pipelines.datasets.br_mapbiomas_estatisticas.utils import (  # noqa: E402
    CLASSE_COLUMNS,
    COVERAGE_COLUMNS,
    COVERAGE_UF_COLUMNS,
    TRANSITION_COLUMNS,
)

ARCH_DIR = HERE / "architecture"
JSON_DIR = HERE / "columns_json"

ARCH_FIELDS = [
    "name",
    "bigquery_type",
    "description",
    "description_en",
    "description_es",
    "temporal_coverage",
    "covered_by_dictionary",
    "directory_column",
    "measurement_unit",
    "has_sensitive_data",
    "observations",
    "original_name",
]


def col(
    name: str,
    bigquery_type: str,
    description: str,
    description_en: str,
    description_es: str,
    *,
    original_name: str = "",
    directory_column: str = "",
    measurement_unit: str = "",
    covered_by_dictionary: str = "no",
    observations: str = "",
) -> dict:
    return {
        "name": name,
        "bigquery_type": bigquery_type,
        "description": description,
        "description_en": description_en,
        "description_es": description_es,
        "temporal_coverage": "",
        "covered_by_dictionary": covered_by_dictionary,
        "directory_column": directory_column,
        "measurement_unit": measurement_unit,
        "has_sensitive_data": "no",
        "observations": observations,
        "original_name": original_name,
    }


# --- shared column definitions -------------------------------------------------

ANO = col(
    "ano",
    "INT64",
    "Ano de referência do mapeamento de cobertura e uso da terra",
    "Reference year of the land cover and land use mapping",
    "Año de referencia del mapeo de cobertura y uso del suelo",
    original_name="y<ano>",
    directory_column="br_bd_diretorios_data_tempo.ano:ano",
    measurement_unit="year",
)

ANO_TRANSICAO = col(
    "ano",
    "INT64",
    "Ano final do período de transição",
    "Final year of the transition period",
    "Año final del período de transición",
    original_name="p<ano_inicial>_<ano>",
    directory_column="br_bd_diretorios_data_tempo.ano:ano",
    measurement_unit="year",
)

ANO_INICIAL = col(
    "ano_inicial",
    "INT64",
    "Ano inicial do período de transição",
    "Initial year of the transition period",
    "Año inicial del período de transición",
    original_name="p<ano_inicial>_<ano>",
    directory_column="br_bd_diretorios_data_tempo.ano:ano",
    measurement_unit="year",
)

SIGLA_UF = col(
    "sigla_uf",
    "STRING",
    "Sigla da Unidade da Federação",
    "Abbreviation of the Federative Unit",
    "Sigla de la Unidad Federativa",
    original_name="state",
    directory_column="br_bd_diretorios_brasil.uf:sigla",
)

SIGLA_UF_FROM_CODE = col(
    "sigla_uf",
    "STRING",
    "Sigla da Unidade da Federação",
    "Abbreviation of the Federative Unit",
    "Sigla de la Unidad Federativa",
    original_name="state",
    directory_column="br_bd_diretorios_brasil.uf:sigla",
    observations=(
        "Derivada dos dois primeiros dígitos do código do município, e não da "
        "coluna de estado da fonte. As duas divergem para Ibateguara (2703007, "
        "Alagoas), que a fonte também registra sob Pernambuco para uma faixa de "
        "0,088 ha."
    ),
)

ID_MUNICIPIO = col(
    "id_municipio",
    "STRING",
    "Código IBGE de sete dígitos do município",
    "Seven-digit IBGE code of the municipality",
    "Código IBGE de siete dígitos del municipio",
    original_name="geocode",
    directory_column="br_bd_diretorios_brasil.municipio:id_municipio",
    observations=(
        "Os códigos 4300001 (Lagoa Mirim) e 4300002 (Lagoa dos Patos) são corpos "
        "d'água aos quais o IBGE atribui código na malha municipal, mas que não "
        "são municípios e por isso não constam do diretório. Fernando de Noronha "
        "(2605459) não é mapeado pelo MapBiomas e não aparece nesta tabela."
    ),
)

BIOMA = col(
    "bioma",
    "STRING",
    "Bioma ao qual pertence a porção do território descrita nesta linha",
    "Biome to which the portion of territory described in this row belongs",
    "Bioma al que pertenece la porción de territorio descrita en esta fila",
    original_name="biome",
    observations=(
        "Territórios que se estendem por mais de um bioma aparecem em uma linha "
        "por bioma. A área total é a soma das linhas de todos os biomas e classes."
    ),
)

AREA = col(
    "area",
    "FLOAT64",
    "Área ocupada pela classe no território, no bioma e no ano",
    "Area occupied by the class in the territory, biome and year",
    "Área ocupada por la clase en el territorio, el bioma y el año",
    original_name="y<ano>",
    measurement_unit="hectare",
    observations=(
        "Área do raster de 30 metros recortado pelos limites territoriais, e não "
        "a área legal do território."
    ),
)

AREA_TRANSICAO = col(
    "area",
    "FLOAT64",
    "Área que passou da classe inicial para a classe final no período",
    "Area that changed from the initial class to the final class in the period",
    "Área que pasó de la clase inicial a la clase final en el período",
    original_name="p<ano_inicial>_<ano>",
    measurement_unit="hectare",
    observations=(
        "Linhas em que a classe inicial e a final coincidem medem a área que "
        "permaneceu na mesma classe."
    ),
)

ID_CLASSE = col(
    "id_classe",
    "STRING",
    "Código da classe de cobertura e uso da terra na legenda do MapBiomas",
    "Code of the land cover and land use class in the MapBiomas legend",
    "Código de la clase de cobertura y uso del suelo en la leyenda de MapBiomas",
    original_name="class",
    observations="Rótulos e hierarquia completa na tabela classe.",
)


def nivel_columns(kind: str = "cobertura") -> list[dict]:
    extra = {
        2: (
            "Classes definidas em nível mais alto repetem o próprio rótulo nos "
            "níveis seguintes, como na fonte."
        )
    }
    return [
        col(
            f"nivel_{n}",
            "STRING",
            f"Classe de cobertura e uso da terra no nível {n} da legenda",
            f"Land cover and land use class at legend level {n}",
            f"Clase de cobertura y uso del suelo en el nivel {n} de la leyenda",
            original_name=f"class_level_{n}",
            observations=extra.get(n, ""),
        )
        for n in range(1, 5)
    ]


def transicao_columns(
    suffix: str, side_pt: str, side_en: str, side_es: str
) -> dict:
    return col(
        f"id_classe_{suffix}",
        "STRING",
        f"Código da classe de cobertura e uso da terra {side_pt} da transição",
        f"Code of the land cover and land use class {side_en} the transition",
        f"Código de la clase de cobertura y uso del suelo {side_es} de la transición",
        original_name=f"class_{'from' if suffix == 'de' else 'to'}",
        observations="Rótulos e hierarquia completa na tabela classe.",
    )


# --- table definitions ---------------------------------------------------------

COBERTURA_MUNICIPIO = [
    ANO,
    SIGLA_UF_FROM_CODE,
    ID_MUNICIPIO,
    ID_CLASSE,
    BIOMA,
    *nivel_columns(),
    AREA,
]

COBERTURA_UF = [ANO, SIGLA_UF, ID_CLASSE, BIOMA, *nivel_columns(), AREA]

TRANSICAO = [
    ANO_TRANSICAO,
    ANO_INICIAL,
    SIGLA_UF,
    transicao_columns("de", "inicial", "at the start of", "inicial"),
    transicao_columns("para", "final", "at the end of", "final"),
    BIOMA,
    AREA_TRANSICAO,
]


def _classe_level_columns() -> list[dict]:
    langs = {
        "pt": ("português", "Portuguese", "portugués"),
        "en": ("inglês", "English", "inglés"),
        "es": ("espanhol", "Spanish", "español"),
    }
    out = []
    for n in range(1, 5):
        for lang, (pt_word, en_word, es_word) in langs.items():
            if lang == "es":
                note = (
                    "Tradução da Base dos Dados. O MapBiomas Brasil não publica "
                    "legenda em espanhol."
                )
            elif lang == "pt":
                note = "Rótulo oficial da legenda publicada pelo MapBiomas."
            else:
                note = "Rótulo oficial da legenda publicada pelo MapBiomas."
            out.append(
                col(
                    f"nivel_{n}_{lang}",
                    "STRING",
                    f"Rótulo em {pt_word} da classe no nível {n} da legenda",
                    f"{en_word} label of the class at legend level {n}",
                    f"Etiqueta en {es_word} de la clase en el nivel {n} de la leyenda",
                    original_name=f"class_level_{n}",
                    observations=note,
                )
            )
    return out


def _valor_columns() -> list[dict]:
    langs = {
        "pt": ("português", "Portuguese", "portugués"),
        "en": ("inglês", "English", "inglés"),
        "es": ("espanhol", "Spanish", "español"),
    }
    return [
        col(
            f"valor_{lang}",
            "STRING",
            f"Nome da classe em {pt_word}",
            f"{en_word} name of the class",
            f"Nombre de la clase en {es_word}",
            original_name="class_level_4",
            observations=(
                "Repete o rótulo do nível mais profundo em que a classe é "
                f"definida, isto é, nivel_4_{lang}."
            ),
        )
        for lang, (pt_word, en_word, es_word) in langs.items()
    ]


CLASSE = [
    col(
        "chave",
        "STRING",
        "Código da classe de cobertura e uso da terra na legenda do MapBiomas",
        "Code of the land cover and land use class in the MapBiomas legend",
        "Código de la clase de cobertura y uso del suelo en la leyenda de MapBiomas",
        original_name="class",
    ),
    col(
        "nivel",
        "STRING",
        "Nível da legenda em que a classe é definida, de 1 a 4",
        "Legend level at which the class is defined, from 1 to 4",
        "Nivel de la leyenda en que se define la clase, de 1 a 4",
    ),
    col(
        "origem",
        "STRING",
        "Origem da classe: Natural, Antrópico, Natural/Antrópico ou Indefinido",
        "Origin of the class: natural, anthropic, natural/anthropic or undefined",
        "Origen de la clase: natural, antrópico, natural/antrópico o indefinido",
        original_name="class_level_0",
    ),
    col(
        "codigo_hierarquia",
        "STRING",
        "Código hierárquico da classe na planilha de estatísticas, como 3.2.1.1",
        "Hierarchical code of the class in the statistics workbook, such as 3.2.1.1",
        "Código jerárquico de la clase en la planilla de estadísticas, como 3.2.1.1",
        original_name="class_level_4",
        observations=(
            "Segue a numeração da planilha de estatísticas, que difere da "
            "numeração do PDF de legenda para algumas classes."
        ),
    ),
    *_valor_columns(),
    *_classe_level_columns(),
    col(
        "cor_hex",
        "STRING",
        "Cor hexadecimal da classe na legenda oficial do MapBiomas",
        "Hexadecimal colour of the class in the official MapBiomas legend",
        "Color hexadecimal de la clase en la leyenda oficial de MapBiomas",
        original_name="hex_code",
        observations="Vazio para classes ausentes da legenda publicada.",
    ),
    col(
        "observacoes",
        "STRING",
        "Notas sobre divergências entre a tabela de estatísticas e a legenda publicada",
        "Notes on divergences between the statistics table and the published legend",
        "Notas sobre divergencias entre la tabla de estadísticas y la leyenda publicada",
    ),
]

TABLES = {
    "cobertura_municipio_classe": (COBERTURA_MUNICIPIO, COVERAGE_COLUMNS),
    "cobertura_uf_classe": (COBERTURA_UF, COVERAGE_UF_COLUMNS),
    "transicao_uf_de_para_anual": (TRANSICAO, TRANSITION_COLUMNS),
    "transicao_uf_de_para_quinquenal": (TRANSICAO, TRANSITION_COLUMNS),
    "transicao_uf_de_para_decenal": (TRANSICAO, TRANSITION_COLUMNS),
    "classe": (CLASSE, CLASSE_COLUMNS),
}

PARTITIONS = {
    "cobertura_municipio_classe": "ano",
    "cobertura_uf_classe": "ano",
    "transicao_uf_de_para_anual": "ano",
    "transicao_uf_de_para_quinquenal": "ano",
    "transicao_uf_de_para_decenal": "ano",
}


def write() -> None:
    ARCH_DIR.mkdir(parents=True, exist_ok=True)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    for table, (columns, expected_order) in TABLES.items():
        names = [c["name"] for c in columns]
        if len(names) != len(set(names)):
            dupes = sorted({n for n in names if names.count(n) > 1})
            raise RuntimeError(f"{table}: duplicate column names {dupes}")
        if names != list(expected_order):
            raise RuntimeError(
                f"{table}: architecture order {names} does not match the writer's "
                f"column order {list(expected_order)}"
            )
        for c in columns:
            for field in ("description", "description_en", "description_es"):
                text = c[field]
                if not text or not text[0].isupper():
                    raise RuntimeError(
                        f"{table}.{c['name']}: {field} not capitalised"
                    )
                if text.endswith("."):
                    raise RuntimeError(
                        f"{table}.{c['name']}: {field} ends with a period"
                    )
            if (
                c["bigquery_type"] in ("INT64", "FLOAT64")
                and not c["measurement_unit"]
            ):
                raise RuntimeError(
                    f"{table}.{c['name']}: numeric column with no unit"
                )

        with open(
            ARCH_DIR / f"{table}.csv", "w", newline="", encoding="utf-8"
        ) as fh:
            # csv defaults to CRLF; the repo is LF-only and pre-commit rewrites it.
            writer = csv.DictWriter(
                fh, fieldnames=ARCH_FIELDS, lineterminator="\n"
            )
            writer.writeheader()
            writer.writerows(columns)

        payload = [
            {
                "name": c["name"],
                "bigquery_type": c["bigquery_type"],
                "description": c["description"],
                "description_en": c["description_en"],
                "description_es": c["description_es"],
                "covered_by_dictionary": c["covered_by_dictionary"] == "yes",
                "directory_column": c["directory_column"] or None,
                "measurement_unit": c["measurement_unit"] or None,
                "has_sensitive_data": False,
                "observations": c["observations"] or None,
                "is_partition": c["name"] == PARTITIONS.get(table),
            }
            for c in columns
        ]
        (JSON_DIR / f"{table}.json").write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        print(f"{table}: {len(columns)} columns")


if __name__ == "__main__":
    write()
