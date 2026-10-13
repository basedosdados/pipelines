"""Constants for br_mapbiomas_estatisticas (MapBiomas land cover and land use)."""

from enum import Enum
from pathlib import Path


class constants(Enum):
    """Constants for br_mapbiomas_estatisticas."""

    DATASET_ID = "br_mapbiomas_estatisticas"

    # Collection 11 (30 m, 1985-2025), released 2026-08-12.
    # The municipality-level statistics workbook is published only as a Google
    # Drive download; every other Collection 11 statistic lives on the WordPress
    # CDN. The Drive id is stable across the collection's life; it changes when a
    # new collection is released, so the download helper re-reads the statistics
    # page rather than trusting the id blindly.
    COLLECTION = "11"
    STATISTICS_PAGE = "https://brasil.mapbiomas.org/downloads/estatisticas/"
    MUNICIPALITY_DRIVE_ID = "1otOqymHuixvkRGVl65zTTNyfaHo46Gqk"
    MUNICIPALITY_ZIP_NAME = (
        "MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE_MUNICIPALITY.zip"
    )
    BIOME_STATE_URL = (
        "https://brasil.mapbiomas.org/wp-content/uploads/sites/3/2026/08/"
        "MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx"
    )
    BIOME_STATE_NAME = "MAPBIOMAS_BRAZIL-COL.11-BIOME_STATE.xlsx"
    TRANSITION_SHEET = "TRANSITION_11"

    LEGEND_CSV_URL = (
        "https://brasil.mapbiomas.org/wp-content/uploads/sites/3/2026/08/"
        "legend_code_mapbiomas_brazil_collection_11.csv"
    )

    COVERAGE_SHEET = "COVERAGE_11"
    LEGEND_SHEET = "LEGEND_CODE"

    FIRST_YEAR = 1985
    LAST_YEAR = 2025

    # The nine tables registered for this dataset. Only the six listed here are
    # buildable: MapBiomas publishes transitions by biome and state, never by
    # municipality, so transicao_municipio_de_para_{anual,quinquenal,decenal}
    # have no source and stay empty.
    TABLES = [
        "cobertura_municipio_classe",
        "cobertura_uf_classe",
        "transicao_uf_de_para_anual",
        "transicao_uf_de_para_quinquenal",
        "transicao_uf_de_para_decenal",
        "classe",
    ]

    UNBUILDABLE_TABLES = [
        "transicao_municipio_de_para_anual",
        "transicao_municipio_de_para_quinquenal",
        "transicao_municipio_de_para_decenal",
    ]

    # Scratch location. Never inside the repo or Dropbox: the workbook is 75 MB
    # zipped and the cleaned output is ~1,100 parquet files.
    DEFAULT_DATA_DIR = (
        Path.home() / "Downloads" / "br_mapbiomas_estatisticas_data"
    )

    ARCHITECTURE_DIR = (
        Path(__file__).resolve().parents[2]
        / "models"
        / "br_mapbiomas_estatisticas"
        / "code"
        / "architecture"
    )

    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    )
