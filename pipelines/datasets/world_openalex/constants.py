"""Constants for the world_openalex pipeline (Prefect 3).

OpenAlex publishes its full database as a CC0 snapshot on a public S3 bucket,
once a quarter. Each release rewrites most records in place (81% of works and
88% of authors changed in the 2026-09-23 release), so every run is a full
rebuild rather than an incremental upsert.
"""

from enum import Enum
from pathlib import Path

# Repo root, then the committed architecture CSVs (column order + types).
_REPO_ROOT = Path(__file__).resolve().parents[3]


class constants(Enum):
    """Constants for the world_openalex pipeline."""

    DATASET_ID = "world_openalex"

    S3_BUCKET = "openalex"
    S3_REGION = "us-east-1"
    MANIFEST_URL = (
        "https://openalex.s3.amazonaws.com/data/parquet/manifest.json"
    )

    ARCHITECTURE_DIR = _REPO_ROOT / "models/world_openalex/code/architecture"

    # Snapshot entity -> tables built from it. Entities absent here (concepts,
    # continents, countries, work-types, ...) are not loaded; languages,
    # licenses and sdgs only feed the dicionario.
    ENTITY_TABLES = {
        "works": [
            "work",
            "work_abstract",
            "work_authorship",
            "work_authorship_institution",
            "work_authorship_affiliation",
            "work_authorship_country",
            "work_location",
            "work_topic",
            "work_keyword",
            "work_sdg",
            "work_mesh",
            "work_award",
            "work_funder",
            "work_reference",
            "work_counts_by_year",
            "work_indexed_in",
        ],
        "authors": [
            "author",
            "author_alternative_name",
            "author_affiliation",
            "author_last_known_institution",
            "author_topic",
            "author_counts_by_year",
        ],
        "awards": [
            "award",
            "award_investigator",
            "award_institution",
            "award_topic",
        ],
        "institutions": ["institution", "institution_association"],
        "sources": ["source", "source_issn"],
        "publishers": ["publisher"],
        "funders": ["funder"],
        "topics": ["topic"],
        "subfields": ["subfield"],
        "fields": ["field"],
        "domains": ["domain"],
        "keywords": ["keyword"],
    }

    # Lookup entities whose labels go into the dicionario:
    # entity -> [(table, column), ...] the codes appear in.
    DICTIONARY_ENTITIES = {
        "languages": [("work", "language")],
        "licenses": [("work_location", "license")],
        "sdgs": [("work_sdg", "sdg_id")],
    }

    # Rows per record batch read from a snapshot file. Peak RSS per worker on
    # an 890 MB works file: 2.4 GB at 50k rows, 1.1 GB at 10k, same speed.
    # At 50k, six workers got the pod evicted from a memory-tight node.
    BATCH_SIZE = 10_000
