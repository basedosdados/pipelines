"""MapBiomas Collection 11 class labels in Portuguese, English and Spanish.

The statistics workbook ships the hierarchy in English only (`class_level_1`
through `class_level_4`). Portuguese comes from MapBiomas' own published legend
(the `LEGEND_CODE` sheet and `legend_code_mapbiomas_brazil_collection_11.csv`),
matched on class id for leaf classes and on the label text for grouping nodes.
Spanish is a Data Basis translation: MapBiomas Brasil publishes no Spanish
legend, and the Spanish-language initiatives (Chaco, Amazonia) use a different
class list, so their terms cannot simply be borrowed.

Keys are the English label with the numeric prefix stripped, exactly as it
appears in the workbook. `check_label_coverage` in `utils.py` fails the build if
the workbook ever emits a label that is not in this table, so a new class in a
future collection cannot slip through untranslated.
"""

# English label (prefix stripped) -> (Portuguese, Spanish)
CLASS_LABELS: dict[str, tuple[str, str]] = {
    # Level 1 - grouping nodes
    "Forest": ("Floresta", "Bosque"),
    "Herbaceous and Shrubby Vegetation": (
        "Vegetação Herbácea e Arbustiva",
        "Vegetación Herbácea y Arbustiva",
    ),
    "Farming": ("Agropecuária", "Agropecuaria"),
    "Non vegetated area": ("Área não Vegetada", "Área no Vegetada"),
    "Water": ("Corpo d'Água", "Cuerpo de Agua"),
    "Not Observed": ("Não Observado", "No Observado"),
    # Level 2 - Forest
    "Forest Formation": ("Formação Florestal", "Formación Forestal"),
    "Savanna Formation": ("Formação Savânica", "Formación de Sabana"),
    "Flooded Savanna": ("Savana Alagada", "Sabana Inundada"),
    "Mangrove": ("Mangue", "Manglar"),
    "Floodable Forest": ("Floresta Alagável", "Bosque Inundable"),
    "Wooded Sandbank Vegetation": (
        "Restinga Arbórea",
        "Vegetación Arbórea de Restinga",
    ),
    # Level 2 - Herbaceous and Shrubby Vegetation
    "Wetland": (
        "Campo Alagado e Área Pantanosa",
        "Campo Inundado y Área Pantanosa",
    ),
    "Marisma": ("Marisma", "Marisma"),
    "Grassland Formation": ("Formação Campestre", "Formación Campestre"),
    "Herbaceous-Shrub Mosaic": (
        "Mosaico Herbáceo-Arbustivo",
        "Mosaico Herbáceo-Arbustivo",
    ),
    "Hypersaline Tidal Flat": ("Apicum", "Llanura Mareal Hipersalina"),
    "Rocky Outcrop": ("Afloramento Rochoso", "Afloramiento Rocoso"),
    "Herbaceous Sandbank Vegetation": (
        "Restinga Herbácea ou Arbustiva",
        "Vegetación Herbácea o Arbustiva de Restinga",
    ),
    # Level 2/3/4 - Farming
    "Pasture": ("Pastagem", "Pastura"),
    "Agriculture": ("Agricultura", "Agricultura"),
    "Temporary Crop": ("Lavoura Temporária", "Cultivo Temporal"),
    "Soybean": ("Soja", "Soja"),
    "Sugar cane": ("Cana", "Caña de Azúcar"),
    "Rice": ("Arroz", "Arroz"),
    "Cotton (beta)": ("Algodão (beta)", "Algodón (beta)"),
    "Other Temporary Crops": (
        "Outras Lavouras Temporárias",
        "Otros Cultivos Temporales",
    ),
    "Perennial Crop": ("Lavoura Perene", "Cultivo Perenne"),
    "Coffee": ("Café", "Café"),
    "Citrus": ("Citrus", "Cítricos"),
    "Palm Oil": ("Dendê", "Palma Aceitera"),
    "Other Perennial Crops": (
        "Outras Lavouras Perenes",
        "Otros Cultivos Perennes",
    ),
    "Forest Plantation": ("Silvicultura", "Silvicultura"),
    "Mosaic of Uses": ("Mosaico de Usos", "Mosaico de Usos"),
    # Level 2 - Non vegetated area
    "Beach, Dune and Sand Spot": (
        "Praia, Duna e Areal",
        "Playa, Duna y Arenal",
    ),
    "Urban Area": ("Área Urbanizada", "Área Urbanizada"),
    "Mining": ("Mineração", "Minería"),
    "Photovoltaic Power Plant (beta)": (
        "Usina Fotovoltaica (beta)",
        "Planta Fotovoltaica (beta)",
    ),
    "Wind Farm": ("Parque Eólico", "Parque Eólico"),
    "Other non Vegetated Areas": (
        "Outras Áreas não Vegetadas",
        "Otras Áreas no Vegetadas",
    ),
    # Level 2 - Water
    "River, Lake and Ocean": ("Rio, Lago e Oceano", "Río, Lago y Océano"),
    "Aquaculture": ("Aquicultura", "Acuicultura"),
}

# `class_level_0` in the workbook: whether the class is natural or anthropic.
ORIGIN_LABELS: dict[str, str] = {
    "Natural": "Natural",
    "Antropic": "Antrópico",
    "Natural/Antropic": "Natural/Antrópico",
    "Undefined": "Indefinido",
}

# Classes present in the municipal statistics but absent from the published
# legend, and vice versa. Recorded so the divergence is documented rather than
# silently smoothed over.
CLASS_NOTES: dict[str, str] = {
    "0": (
        "Classe presente na tabela de estatísticas e ausente da legenda "
        "publicada. Corresponde a pixels sem observação válida no ano."
    ),
    "13": (
        "Classe presente na tabela de estatísticas e ausente da legenda "
        "publicada da Coleção 11, que traz no seu lugar a classe 77 "
        "(Formação Herbáceo Arbustiva), a qual por sua vez não ocorre nas "
        "estatísticas municipais."
    ),
}
