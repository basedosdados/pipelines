"""Table names, descriptions and keys for us_oi_opportunity_atlas.

Shared by build_dbt.py (schema.yml) and register_metadata.py (backend).
"""

OUTCOME_NOTE = {
    "pt": (
        "Crianças nascidas entre 1978 e 1983, com renda medida em 2014-2015. A "
        "fonte adiciona um pequeno ruído a cada estimativa para proteger a "
        "privacidade, de modo que algumas frações saem levemente negativas."
    ),
    "en": (
        "Children born 1978-1983, with income measured in 2014-2015. The source "
        "adds a small amount of noise to each estimate to protect privacy, so some "
        "fractions are slightly negative."
    ),
    "es": (
        "Niños nacidos entre 1978 y 1983, con ingresos medidos en 2014-2015. La "
        "fuente añade un pequeño ruido a cada estimación para proteger la "
        "privacidad, por lo que algunas fracciones son levemente negativas."
    ),
}

GEO = {
    "tract": {
        "pt": ("setor censitário", "Setor censitário", "setores censitários de 2010"),
        "en": ("census tract", "Census tract", "2010 census tracts"),
        "es": ("sector censal", "Sector censal", "sectores censales de 2010"),
        "keys": ["tract_id"],
    },
    "county": {
        "pt": ("condado", "Condado", "condados de 2010"),
        "en": ("county", "County", "2010 counties"),
        "es": ("condado", "Condado", "condados de 2010"),
        "keys": ["county_id"],
    },
    "commuting_zone": {
        "pt": ("zona de deslocamento", "Zona de deslocamento", "zonas de deslocamento de 1990"),
        "en": ("commuting zone", "Commuting zone", "1990 commuting zones"),
        "es": ("zona de desplazamiento", "Zona de desplazamiento", "zonas de desplazamiento de 1990"),
        "keys": ["commuting_zone_id"],
    },
}  # fmt: skip


def _atlas_tables() -> dict[str, dict]:
    out = {}
    for geo, g in GEO.items():
        pt, en, es = g["pt"], g["en"], g["es"]
        out[f"{geo}_outcome"] = {
            "name_pt": f"Resultados por {pt[0]}",
            "name_en": f"Outcomes by {en[0]}",
            "name_es": f"Resultados por {es[0]}",
            "description_pt": (
                f"Resultados previstos na idade adulta para crianças que cresceram em "
                f"cada um dos {pt[2]} dos Estados Unidos, por raça, sexo e percentil "
                f"de renda dos pais (1, 10, 25, 50, 75 e 100, além da média). Uma "
                f"linha por {pt[0]}, raça, sexo e percentil; cada coluna de resultado "
                f"traz a estimativa e o erro-padrão. {OUTCOME_NOTE['pt']}"
            ),
            "description_en": (
                f"Predicted adult outcomes for children who grew up in each of the US "
                f"{en[2]}, by race, gender and parental income percentile (1, 10, "
                f"25, 50, 75 and 100, plus the mean). One row per {en[0]}, race, "
                f"gender and percentile; each outcome has an estimate and a standard "
                f"error column. {OUTCOME_NOTE['en']}"
            ),
            "description_es": (
                f"Resultados previstos en la adultez para los niños que crecieron en "
                f"cada uno de los {es[2]} de Estados Unidos, por raza, sexo y "
                f"percentil de ingreso de los padres (1, 10, 25, 50, 75 y 100, además "
                f"de la media). Una fila por {es[0]}, raza, sexo y percentil; cada "
                f"resultado tiene una columna de estimación y otra de error "
                f"estándar. {OUTCOME_NOTE['es']}"
            ),
            "keys": g["keys"] + ["race", "gender", "parent_income_percentile"],
            "geo": geo,
        }
        out[f"{geo}_sample"] = {
            "name_pt": f"Amostra por {pt[0]}",
            "name_en": f"Sample by {en[0]}",
            "name_es": f"Muestra por {es[0]}",
            "description_pt": (
                f"Tamanhos de amostra e características das crianças por trás das "
                f"estimativas da tabela {geo}_outcome, por {pt[0]}, raça e sexo: o "
                f"número de crianças usado em cada resultado, a posição média de "
                f"renda dos pais, o número de crianças no Censo 2000 e a fração com "
                f"pais abaixo da mediana nacional de renda."
            ),
            "description_en": (
                f"Sample sizes and child characteristics behind the estimates in "
                f"{geo}_outcome, by {en[0]}, race and gender: the number of children "
                f"used for each outcome, the parents' mean income rank, the number of "
                f"children in the 2000 Census and the fraction with parents below "
                f"the national income median."
            ),
            "description_es": (
                f"Tamaños de muestra y características de los niños detrás de las "
                f"estimaciones de la tabla {geo}_outcome, por {es[0]}, raza y sexo: "
                f"el número de niños usado en cada resultado, la posición media del "
                f"ingreso de los padres, el número de niños en el Censo 2000 y la "
                f"fracción con padres por debajo de la mediana nacional de ingresos."
            ),
            "keys": g["keys"] + ["race", "gender"],
            "geo": geo,
        }
    return out


TABLES: dict[str, dict] = {
    **_atlas_tables(),
    "commuting_zone_migration": {
        "name_pt": "Migração entre zonas de deslocamento",
        "name_en": "Migration between commuting zones",
        "name_es": "Migración entre zonas de desplazamiento",
        "description_pt": (
            "Fluxos de jovens adultos entre a zona de deslocamento onde moravam aos "
            "16 anos e a zona onde moram aos 26, para pessoas nascidas nos Estados "
            "Unidos entre 1984 e 1992, por raça e quintil de renda dos pais. Uma "
            "linha por par origem-destino, raça e quintil; pooled indica o total da "
            "dimensão não desagregada. A fonte soma ou subtrai algumas pessoas de "
            "cada célula para garantir privacidade diferencial."
        ),
        "description_en": (
            "Flows of young adults between the commuting zone where they lived at "
            "age 16 and the one where they live at age 26, for people born in the US "
            "in 1984-1992, by race and parental income quintile. One row per "
            "origin-destination pair, race and quintile; pooled marks the total of "
            "the dimension not split. The source adds or subtracts a few people "
            "from each cell to satisfy differential privacy."
        ),
        "description_es": (
            "Flujos de jóvenes adultos entre la zona de desplazamiento donde vivían "
            "a los 16 años y aquella donde viven a los 26, para personas nacidas en "
            "Estados Unidos entre 1984 y 1992, por raza y quintil de ingreso de los "
            "padres. Una fila por par origen-destino, raza y quintil; pooled indica "
            "el total de la dimensión no desagregada. La fuente suma o resta algunas "
            "personas en cada celda para garantizar privacidad diferencial."
        ),
        "keys": [
            "origin_commuting_zone_id",
            "destination_commuting_zone_id",
            "race",
            "parent_income_quintile",
        ],
        "geo": "commuting_zone",
    },
}

TABLE_ORDER = [
    "tract_outcome",
    "county_outcome",
    "commuting_zone_outcome",
    "commuting_zone_migration",
    "tract_sample",
    "county_sample",
    "commuting_zone_sample",
]

ACCEPTED = {
    "race": [
        "pooled", "white", "black", "hispanic", "asian", "native_american", "other",
    ],
    "gender": ["pooled", "male", "female"],
    "parent_income_percentile": ["1", "10", "25", "50", "75", "100", "mean"],
    "parent_income_quintile": ["pooled", "1", "2", "3", "4", "5"],
}  # fmt: skip

# 2010 county codes retired before the directory's vintage (Connecticut's eight
# counties became planning regions in 2022; 46113 -> 46102 in 2015; 02270 -> 02158
# in 2015; 02261 split in 2019; Bedford city 51515 merged in 2013).
RETIRED_COUNTIES = [
    "09001", "09003", "09005", "09007", "09009", "09011", "09013", "09015",
    "46113", "02270", "02261", "51515",
]  # fmt: skip
