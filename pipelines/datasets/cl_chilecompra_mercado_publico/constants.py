"""Constants for the cl_chilecompra_mercado_publico recurring pipeline."""

from enum import Enum


class constants(Enum):
    DATASET_ID = "cl_chilecompra_mercado_publico"

    # One ZIP per month per container. <month> runs 1..12 -- these are MONTHLY files,
    # not semestral, despite the "YYYY-1" / "YYYY-2" naming inviting the other reading.
    BASE_URL = "https://transparenciachc.blob.core.windows.net"
    CONTAINERS = {"orden_compra": "oc-da", "licitacion": "lic-da"}

    FIRST_YEAR = 2007
    FIRST_MONTH = 1

    TABLES = {
        "orden_compra": ["orden_compra_item"],
        "licitacion": ["licitacion_item", "licitacion_oferta"],
    }
    ALL_TABLES = ["orden_compra_item", "licitacion_item", "licitacion_oferta"]

    # ChileCompra rebuilds a rolling trailing window of months every day, rather than
    # only the current one: measured across all 472 blobs, 15 consecutive oc-da months
    # (2025-06..2026-08) and 4 lic-da months carried a Last-Modified inside the last two
    # days, while everything older had been untouched for more than a month. So
    # "refresh the open period, freeze the rest" would miss real revisions to closed
    # months.
    #
    # Selecting months by recency of Last-Modified reproduces the publisher's own window
    # without storing any state, and self-adjusts if they change it.
    DEFAULT_LOOKBACK_DAYS = 10
