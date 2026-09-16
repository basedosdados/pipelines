"""Cleaning transform for br_sfb_sicar (CAR).

DRY: the transform now lives in ONE place — the recurring pipeline's
``pipelines/crawler/sfb_sicar/utils.py`` — and this module re-exports it so the
one-shot onboarding bootstrap (``bootstrap_clean.py``) and the Prefect flow share
identical code. Do not add transform logic here; edit the pipeline utils.
"""

import os
import sys

# Put the repo root on the path so the canonical pipeline utils import resolves
# when this file is run standalone from models/br_sfb_sicar/code/.
_REPO_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..")
)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from pipelines.crawler.sfb_sicar.utils import (  # noqa: E402
    all_string_schema,
    build_table_df,
    filter_to_uf,
    geometry_to_wkt,
    read_theme_zip,
    write_table_partitioned,
)

__all__ = [
    "all_string_schema",
    "build_table_df",
    "filter_to_uf",
    "geometry_to_wkt",
    "read_theme_zip",
    "write_table_partitioned",
]
