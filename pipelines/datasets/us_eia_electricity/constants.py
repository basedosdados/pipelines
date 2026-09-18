"""Constants for the us_eia_electricity recurring pipeline (Prefect 3).

U.S. Energy Information Administration electric power survey forms. See
``models/us_eia_electricity/CLAUDE.md`` for the full design, including the source traps the
transform exists to neutralise and what was deliberately deferred.

Two survey forms are shipped:

* **EIA-860** — annual generator and plant inventory. One ZIP per report year.
* **EIA-923** — monthly generation, fuel consumption and fuel receipts. One ZIP
  per report year, republished every month during the reporting year and again
  as a final revision the following year.
"""

from enum import Enum
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
_CODE_DIR = _REPO_ROOT / "models" / "us_eia_electricity" / "code"


class constants(Enum):
    """Constants for the us_eia_electricity pipeline.

    Lowercase class name follows the repo-wide convention for dataset constant
    enums.
    """

    DATASET_ID = "us_eia_electricity"

    PLANT = "plant"
    GENERATOR = "generator"
    GENERATION_FUEL = "generation_fuel"
    FUEL_RECEIPTS_COSTS = "fuel_receipts_costs"
    DICIONARIO = "dicionario"
    # Order matters only in that every table is built before any is tested:
    # the data tables' custom_dictionary_coverage tests read dicionario.
    ALL_TABLES = [
        "plant",
        "generator",
        "generation_fuel",
        "fuel_receipts_costs",
        "dicionario",
    ]
    DATA_TABLES = [
        "plant",
        "generator",
        "generation_fuel",
        "fuel_receipts_costs",
    ]

    # Committed architecture CSVs — the single schema source of truth (column
    # order, bigquery_type, and the PUDL-canonical name each column comes from).
    ARCHITECTURE_DIR = _CODE_DIR / "architecture"
    # Extraction maps and code vocabularies vendored from PUDL; see
    # models/us_eia_electricity/code/pudl/README.md and vendor_pudl.py.
    PUDL_DIR = _CODE_DIR / "pudl"
    # (state abbreviation, normalised county name) -> county FIPS, exported once
    # from basedosdados.br_bd_diretorios_us.county. The forms publish a county
    # *name*, never a FIPS code, so the directory link has to be resolved by
    # name.
    COUNTY_DIRECTORY = _CODE_DIR / "us_county_directory.csv"

    # Which years each form covers. 2001 is the floor for both: it is where
    # PUDL's extraction maps start, and before it the forms are EIA-860A/860B
    # and EIA-906, which are differently shaped surveys rather than earlier
    # vintages of the same one.
    FIRST_YEAR = 2001

    EIA860_PAGE_URL = "https://www.eia.gov/electricity/data/eia860/"
    EIA923_PAGE_URL = "https://www.eia.gov/electricity/data/eia923/"
    EIA_BASE = "https://www.eia.gov/electricity/data"

    # A browser User-Agent. eia.gov serves the ZIPs to a plain client, but the
    # HTML index pages are behind a CDN that has been seen to 403 default
    # library agents.
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    )

    # Clean table -> the (form, PUDL page) it is built from. `generator` unions
    # three pages from 2009 on and a single page before that; see utils.py.
    TABLE_SOURCES = {
        "plant": [("eia860", "plant")],
        "generator": [
            ("eia860", "generator"),
            ("eia860", "generator_existing"),
            ("eia860", "generator_proposed"),
            ("eia860", "generator_retired"),
        ],
        "generation_fuel": [("eia923", "generation_fuel")],
        "fuel_receipts_costs": [("eia923", "fuel_receipts_costs")],
    }

    # PUDL page -> the generator status group it represents. The pre-2009 files
    # carry existing and retired generators in one sheet, distinguished only by
    # operational_status_code, so that page's group is resolved per row.
    GENERATOR_PAGE_GROUP = {
        "generator": "",
        "generator_existing": "existing",
        "generator_proposed": "proposed",
        "generator_retired": "retired",
    }

    # The twelve month suffixes PUDL appends to every EIA-923 page-1 measure.
    # generation_fuel is published WIDE — one row per (plant, fuel, prime mover)
    # with twelve columns per measure — and is melted to LONG here.
    MONTHS = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december",
    ]

    # Wide EIA-923 page-1 measure -> the long column it becomes.
    GENERATION_FUEL_MEASURES = {
        "fuel_consumed_units": "fuel_consumed_units",
        "fuel_consumed_for_electricity_units": "fuel_consumed_for_electricity_units",
        "fuel_mmbtu_per_unit": "fuel_mmbtu_per_unit",
        "fuel_consumed_mmbtu": "fuel_consumed_mmbtu",
        "fuel_consumed_for_electricity_mmbtu": "fuel_consumed_for_electricity_mmbtu",
        "net_generation_mwh": "net_generation_mwh",
    }

    # Clean column -> the PUDL code vocabulary that defines its values. Drives
    # both the code repairs (code_fixes / ignored_codes) and the dicionario.
    CODED_COLUMNS = {
        "plant": {
            "sector_id": "core_eia__codes_sector_consolidated",
            "balancing_authority_code": "core_eia__codes_balancing_authorities",
            "regulatory_status_code": "core_eia__codes_regulations",
        },
        "generator": {
            "prime_mover_code": "core_eia__codes_prime_movers",
            "operational_status_code": "core_eia__codes_operational_status",
            "energy_source_code_1": "core_eia__codes_energy_sources",
            "energy_source_code_2": "core_eia__codes_energy_sources",
            "energy_source_code_3": "core_eia__codes_energy_sources",
            "energy_source_code_4": "core_eia__codes_energy_sources",
            "energy_source_code_5": "core_eia__codes_energy_sources",
            "energy_source_code_6": "core_eia__codes_energy_sources",
            "planned_energy_source_code_1": "core_eia__codes_energy_sources",
            "planned_new_prime_mover_code": "core_eia__codes_prime_movers",
            "sector_id": "core_eia__codes_sector_consolidated",
        },
        "generation_fuel": {
            "prime_mover_code": "core_eia__codes_prime_movers",
            "energy_source_code": "core_eia__codes_energy_sources",
            "fuel_type_code_agg": "core_eia__codes_fuel_types_agg",
            "sector_id": "core_eia__codes_sector_consolidated",
            "balancing_authority_code": "core_eia__codes_balancing_authorities",
            "reporting_frequency_code": "core_eia__codes_reporting_frequencies",
        },
        "fuel_receipts_costs": {
            "energy_source_code": "core_eia__codes_energy_sources",
            "contract_type_code": "core_eia__codes_contract_types",
            "mine_type_code": "core_eia__codes_coalmine_types",
            "primary_transportation_mode_code": "core_eia__codes_fuel_transportation_modes",
            "secondary_transportation_mode_code": "core_eia__codes_fuel_transportation_modes",
            "balancing_authority_code": "core_eia__codes_balancing_authorities",
            "reporting_frequency_code": "core_eia__codes_reporting_frequencies",
        },
    }

    # Columns whose value set is enumerated in the dicionario table. Every coded
    # column above, plus the free-form ones whose vocabulary is small, stable and
    # not self-explanatory.
    DICT_EXTRA_COLUMNS = {
        "generator": ["generator_status_group", "technology_description"],
        "generation_fuel": ["data_maturity"],
        "fuel_receipts_costs": ["data_maturity", "fuel_group_code"],
        "plant": ["data_maturity"],
    }

    # Divergences between PUDL's archived copy of a release and the copy eia.gov
    # currently serves. PUDL reads its own Zenodo archive of the original
    # publication; EIA has since re-issued some archive ZIPs with a renamed
    # header. Each entry is a measured divergence, not a guess — the reader
    # raises rather than silently producing a null column, and every entry here
    # was added after opening the file and confirming the header.
    #
    #   {(form, page, year): {"rename": {raw_header: canonical}, "drop": [canonical]}}
    #
    # A `rename` entry is {raw simplified header: PUDL canonical name} — the same
    # direction as the vendored column maps, whose KEYS are raw headers and whose
    # VALUES are canonical names. So an entry mapping a name to itself is not a
    # no-op: it re-points the canonical column at a different raw header.
    #
    # eia860/plant/2013: PUDL recorded the raw header as "NERC Region Code" for
    # the canonical column `nerc_region`; the ZIP eia.gov currently serves has
    # the raw header "NERC Region", which simplifies to `nerc_region`. The entry
    # below therefore reads "the canonical column nerc_region now comes from the
    # raw header nerc_region". Confirmed by opening 2___Plant_Y2013.xlsx — the
    # column is there and holds SERC, RFC, SPP and the rest, only the header
    # differs — and by the result: nerc_region is populated on 8,041 of the 8,060
    # plants of 2013, in line with 2012 (7,281/7,289) and 2014 (8,501/8,520).
    MAP_OVERRIDES = {
        ("eia860", "plant", 2013): {"rename": {"nerc_region": "nerc_region"}},
    }

    # How to find a page's workbook inside a ZIP when the name PUDL recorded is
    # no longer the name EIA serves.
    #
    # This is not an edge case, it is the normal state of the current year. EIA
    # stamps the publication date and the latest data month into the EIA-923 file
    # name — "EIA923_Schedules_2_3_4_5_M_05_2026_21JUL2026.xlsx" — and renames it
    # every single month. EIA-860 does the same on an annual clock, dropping the
    # "_Early_Release" suffix when the final revision lands. So any hardcoded
    # name for a live year is stale within weeks: PUDL's map, vendored from a
    # checkout taken the day before this dataset was built, already named the
    # July EIA-923 file while eia.gov was serving the August one.
    #
    # The exact name from the map is tried first, because for the twenty-odd
    # settled years it is exact and unambiguous. These patterns are the fallback,
    # matched case-insensitively against the member's base name with {year}
    # substituted. A pattern that matches no member, or more than one, raises
    # rather than guessing.
    MEMBER_PATTERNS = {
        ("eia860", "utility"): r"^1___utility_y{year}",
        ("eia860", "plant"): r"^2___plant_y{year}",
        ("eia860", "generator_existing"): r"^3_1_generator_y{year}",
        ("eia860", "generator_proposed"): r"^3_1_generator_y{year}",
        ("eia860", "generator_retired"): r"^3_1_generator_y{year}",
        # The 2001-2008 single-sheet generator page keeps its archived name; those
        # years are settled and were never renamed.
        ("eia923", "generation_fuel"): r"^eia923_schedules_2_3_4_5.*_{year}_",
        (
            "eia923",
            "fuel_receipts_costs",
        ): r"^eia923_schedules_2_3_4_5.*_{year}_",
    }
