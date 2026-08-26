"""Curated column spec for the FDIC institution master table.

The institutions endpoint publishes 152 fields.  This spec keeps the ones that
identify, locate, classify or date an institution and drops the repetitive
slots: 20 trade-name/web-site pairs (TE01N528..TE10N529), 14 spare structure
change codes (CHANGEC2..CHANGEC15) and 9 spare previous-name slots.

Each entry is (fdic_field, column_name, bigquery_type, description, options).
`options` carries anything non-default: dict for a dictionary-coded column,
unit for a measure, directory for a foreign key.

Coded columns are STRING with covered_by_dictionary set, per the house rule
that a value whose arithmetic is meaningless is not a number.
"""

from __future__ import annotations

SNAPSHOT = (
    "Snapshot as of financial_report_date; the full quarterly series is in the "
    "financials table"
)

SPEC: list[tuple[str, str, str, str, dict]] = [
    # --- identity -------------------------------------------------------
    (
        "CERT",
        "cert",
        "STRING",
        "FDIC certificate number identifying the institution",
        {
            "obs": "Primary key. Stable across name changes and the key used by financials"
        },
    ),
    ("NAME", "name", "STRING", "Legal name of the institution", {}),
    (
        "FED_RSSD",
        "rssd_id",
        "STRING",
        "Federal Reserve RSSD identifier",
        {"obs": "Used to join to Federal Reserve and FFIEC sources"},
    ),
    (
        "UNINUM",
        "uninum",
        "STRING",
        "FDIC unique number for the institution",
        {},
    ),
    ("LEI", "lei", "STRING", "Legal Entity Identifier", {}),
    (
        "CHARTER",
        "occ_charter_number",
        "STRING",
        "Charter number assigned by the OCC",
        {},
    ),
    (
        "DOCKET",
        "ots_docket_number",
        "STRING",
        "Docket number assigned by the former OTS",
        {},
    ),
    (
        "PRIORNAME1",
        "previous_name",
        "STRING",
        "Most recent previous name of the institution",
        {
            "obs": "The FDIC publishes up to ten previous names; only the most recent is kept"
        },
    ),
    # --- ownership ------------------------------------------------------
    (
        "RSSDHCR",
        "holding_company_rssd_id",
        "STRING",
        "RSSD identifier of the regulatory top holding company",
        {},
    ),
    (
        "NAMEHCR",
        "holding_company_name",
        "STRING",
        "Name of the regulatory top holding company",
        {},
    ),
    (
        "CITYHCR",
        "holding_company_city",
        "STRING",
        "City of the regulatory top holding company",
        {},
    ),
    (
        "STALPHCR",
        "holding_company_state",
        "STRING",
        "State abbreviation of the regulatory top holding company",
        {},
    ),
    (
        "HCTMULT",
        "holding_company_type",
        "STRING",
        "Type of bank holding company",
        {"dict": True},
    ),
    (
        "PARCERT",
        "parent_cert",
        "STRING",
        "FDIC certificate number of the bank that directly owns this one",
        {},
    ),
    (
        "ULTCERT",
        "ultimate_cert",
        "STRING",
        "FDIC certificate number the institution ultimately maps to",
        {},
    ),
    (
        "NEWCERT",
        "successor_cert",
        "STRING",
        "FDIC certificate number the institution became after a merger or conversion",
        {},
    ),
    # --- classification -------------------------------------------------
    (
        "BKCLASS",
        "institution_class",
        "STRING",
        "Charter and supervisory class of the institution",
        {"dict": True},
    ),
    (
        "CLCODE",
        "class_code",
        "STRING",
        "Numeric subcategory of the institution class",
        {"dict": True},
    ),
    (
        "CHRTAGNT",
        "chartering_agency",
        "STRING",
        "Agency that granted the institution's charter",
        {"dict": True},
    ),
    (
        "REGAGNT",
        "primary_regulator",
        "STRING",
        "Primary federal regulator",
        {"dict": True},
    ),
    (
        "REGAGENT2",
        "secondary_regulator",
        "STRING",
        "Secondary regulator",
        {"dict": True},
    ),
    (
        "STCHRTR",
        "is_state_chartered",
        "STRING",
        "Whether the institution holds a state charter",
        {"dict": True},
    ),
    (
        "FEDCHRTR",
        "is_federally_chartered",
        "STRING",
        "Whether the institution holds a federal charter",
        {"dict": True},
    ),
    (
        "SPECGRPN",
        "specialization_group",
        "STRING",
        "Asset concentration group the institution falls into",
        {"dict": True},
    ),
    (
        "MUTUAL",
        "ownership_type",
        "STRING",
        "Whether the institution is stock-owned or mutually owned",
        {"dict": True},
    ),
    (
        "SUBCHAPS",
        "is_subchapter_s",
        "STRING",
        "Whether the institution is a Subchapter S corporation",
        {"dict": True},
    ),
    (
        "CB",
        "is_community_bank",
        "STRING",
        "Whether the institution meets the FDIC community bank definition",
        {"dict": True},
    ),
    (
        "MDI_STATUS_DESC",
        "minority_status",
        "STRING",
        "Minority depository institution status",
        {"dict": True},
    ),
    (
        "TRUST",
        "trust_powers",
        "STRING",
        "Trust powers granted to the institution",
        {"dict": True},
    ),
    (
        "INSTAG",
        "is_agricultural_lender",
        "STRING",
        "Whether the institution is classified as an agricultural lender",
        {"dict": True},
    ),
    (
        "INSTCRCD",
        "is_credit_card_institution",
        "STRING",
        "Whether the institution is classified as a credit card institution",
        {"dict": True},
    ),
    (
        "IBA",
        "is_insured_foreign_branch",
        "STRING",
        "Whether the institution is an insured office of a foreign bank",
        {"dict": True},
    ),
    (
        "FORM31",
        "files_call_report_31",
        "STRING",
        "Whether the institution files the FFIEC 031 Call Report",
        {"dict": True},
    ),
    # --- insurance ------------------------------------------------------
    (
        "INSFDIC",
        "is_fdic_insured",
        "STRING",
        "Whether the institution is insured by the FDIC",
        {"dict": True},
    ),
    (
        "INSAGNT1",
        "primary_insurance_fund",
        "STRING",
        "Deposit insurance fund the institution belongs to",
        {"dict": True},
    ),
    (
        "INSDATE",
        "insurance_date",
        "DATE",
        "Date deposit insurance took effect",
        {},
    ),
    (
        "INSDROPDATE",
        "insurance_end_date",
        "DATE",
        "Date deposit insurance ended",
        {},
    ),
    (
        "INSCOML",
        "is_insured_commercial_bank",
        "STRING",
        "Whether the institution is an insured commercial bank",
        {"dict": True},
    ),
    (
        "INSSAVE",
        "is_insured_savings_institution",
        "STRING",
        "Whether the institution is an insured savings institution",
        {"dict": True},
    ),
    # --- status and dates ------------------------------------------------
    (
        "ACTIVE",
        "is_active",
        "STRING",
        "Whether the institution was operating at the extraction date",
        {"dict": True},
    ),
    (
        "ESTYMD",
        "established_date",
        "DATE",
        "Date the institution began operating",
        {},
    ),
    (
        "ENDEFYMD",
        "end_date",
        "DATE",
        "Date the institution ceased to exist as a separate entity",
        {"obs": "Empty for institutions still operating"},
    ),
    (
        "EFFDATE",
        "last_structure_change_date",
        "DATE",
        "Effective date of the most recent structure change",
        {},
    ),
    (
        "PROCDATE",
        "last_structure_change_process_date",
        "DATE",
        "Date the most recent structure change was processed",
        {},
    ),
    (
        "CHANGEC1",
        "structure_change_code",
        "STRING",
        "Code of the most recent structure change",
        {
            "dict": True,
            "obs": "The FDIC records up to fifteen change codes; the first is kept",
        },
    ),
    (
        "CONSERVE",
        "is_in_conservatorship",
        "STRING",
        "Whether the institution is in conservatorship",
        {"dict": True},
    ),
    (
        "DENOVO",
        "is_de_novo",
        "STRING",
        "Whether the institution is newly chartered",
        {"dict": True},
    ),
    (
        "DATEUPDT",
        "last_update_date",
        "DATE",
        "Date the FDIC last updated the record",
        {},
    ),
    # --- location ---------------------------------------------------------
    ("ADDRESS", "address", "STRING", "Street address of the main office", {}),
    ("CITY", "city", "STRING", "City of the main office", {}),
    ("COUNTY", "county", "STRING", "County of the main office", {}),
    (
        "STALP",
        "state_abbreviation",
        "STRING",
        "Two-letter state abbreviation of the main office",
        {"dir": "br_bd_diretorios_us.state:abbreviation"},
    ),
    ("STNAME", "state_name", "STRING", "State name of the main office", {}),
    (
        "STCNTY",
        "county_id",
        "STRING",
        "FIPS state and county code of the main office",
        {"dir": "br_bd_diretorios_us.county:id_county"},
    ),
    ("ZIP", "zip_code", "STRING", "ZIP code of the main office", {}),
    (
        "LATITUDE",
        "latitude",
        "FLOAT64",
        "Latitude of the main office",
        {"unit": "degrees"},
    ),
    (
        "LONGITUDE",
        "longitude",
        "FLOAT64",
        "Longitude of the main office",
        {"unit": "degrees"},
    ),
    (
        "CBSA_NO",
        "cbsa_id",
        "STRING",
        "Core Based Statistical Area code of the main office",
        {
            "dir": "br_bd_diretorios_us.cbsa_2023:id_cbsa",
            "obs": "Delineation vintage follows the FDIC extract, not the reference year",
        },
    ),
    (
        "CBSA",
        "cbsa_name",
        "STRING",
        "Core Based Statistical Area name of the main office",
        {},
    ),
    (
        "CSA_NO",
        "csa_id",
        "STRING",
        "Combined Statistical Area code of the main office",
        {},
    ),
    (
        "CSA",
        "csa_name",
        "STRING",
        "Combined Statistical Area name of the main office",
        {},
    ),
    (
        "FDICDBS",
        "fdic_region",
        "STRING",
        "FDIC geographic region",
        {"dict": True},
    ),
    (
        "FDICSUPV",
        "fdic_supervisory_region",
        "STRING",
        "FDIC supervisory region",
        {"dict": True},
    ),
    (
        "FED",
        "federal_reserve_district",
        "STRING",
        "Federal Reserve district the institution belongs to",
        {"dict": True},
    ),
    (
        "OCCDIST",
        "occ_district",
        "STRING",
        "OCC district the institution belongs to",
        {"dict": True},
    ),
    (
        "WEBADDR",
        "website",
        "STRING",
        "Primary internet address of the institution",
        {},
    ),
    # --- latest financial snapshot ---------------------------------------
    (
        "RISDATE",
        "financial_report_date",
        "DATE",
        "Quarter end the financial snapshot columns refer to",
        {},
    ),
    (
        "ASSET",
        "total_assets",
        "FLOAT64",
        "Total assets",
        {"unit": "USD", "scaled": True},
    ),
    (
        "DEP",
        "total_deposits",
        "FLOAT64",
        "Total deposits",
        {"unit": "USD", "scaled": True},
    ),
    (
        "DEPDOM",
        "domestic_deposits",
        "FLOAT64",
        "Deposits held in domestic offices",
        {"unit": "USD", "scaled": True},
    ),
    (
        "EQ",
        "equity_capital",
        "FLOAT64",
        "Total equity capital",
        {"unit": "USD", "scaled": True},
    ),
    (
        "NETINC",
        "net_income",
        "FLOAT64",
        "Net income for the year to date",
        {"unit": "USD", "scaled": True},
    ),
    (
        "ROA",
        "return_on_assets",
        "FLOAT64",
        "Net income as a percent of average total assets",
        {"unit": "percent"},
    ),
    (
        "ROE",
        "return_on_equity",
        "FLOAT64",
        "Net income as a percent of average equity",
        {"unit": "percent"},
    ),
    (
        "OFFDOM",
        "domestic_offices",
        "FLOAT64",
        "Number of domestic offices",
        {"unit": "unit"},
    ),
    (
        "OFFFOR",
        "foreign_offices",
        "FLOAT64",
        "Number of foreign offices",
        {"unit": "unit"},
    ),
]

DROPPED_NOTE = (
    "Dropped from the source: 20 trade-name and web-site slots "
    "(TE01N528-TE10N529), structure change codes 2-15, previous names 2-10, "
    "and the redundant CBSA flag and division variants."
)
