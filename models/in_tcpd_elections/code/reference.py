"""Reference / crosswalk data for the in_tcpd_elections cleaning pipeline.

These small hand-curated tables live in code (not in a ``data/`` folder, which the
repo's root .gitignore excludes) so the pipeline is fully reproducible from source.

- STATE_COLUMNS / STATE_ROWS: the br_bd_diretorios_in.state directory table.
- STATE_CROSSWALK: raw Lok Dhaba ``State_Name`` -> canonical ``acronym``.
- DICTIONARY / DICT_TABLES: value -> label for the dictionary-covered columns.
"""

# --- br_bd_diretorios_in.state directory -------------------------------------
STATE_COLUMNS = [
    "acronym",
    "name",
    "iso_3166_2",
    "type",
    "is_current",
    "successor_acronym",
]
STATE_ROWS = [
    (
        "AN",
        "Andaman and Nicobar Islands",
        "IN-AN",
        "union_territory",
        "yes",
        "",
    ),
    ("AP", "Andhra Pradesh", "IN-AP", "state", "yes", ""),
    ("AR", "Arunachal Pradesh", "IN-AR", "state", "yes", ""),
    ("AS", "Assam", "IN-AS", "state", "yes", ""),
    ("BR", "Bihar", "IN-BR", "state", "yes", ""),
    ("CH", "Chandigarh", "IN-CH", "union_territory", "yes", ""),
    ("CG", "Chhattisgarh", "IN-CT", "state", "yes", ""),
    (
        "DH",
        "Dadra and Nagar Haveli and Daman and Diu",
        "IN-DH",
        "union_territory",
        "yes",
        "",
    ),
    ("DL", "Delhi", "IN-DL", "union_territory", "yes", ""),
    ("GA", "Goa", "IN-GA", "state", "yes", ""),
    ("GJ", "Gujarat", "IN-GJ", "state", "yes", ""),
    ("HR", "Haryana", "IN-HR", "state", "yes", ""),
    ("HP", "Himachal Pradesh", "IN-HP", "state", "yes", ""),
    ("JK", "Jammu and Kashmir", "IN-JK", "union_territory", "yes", ""),
    ("JH", "Jharkhand", "IN-JH", "state", "yes", ""),
    ("KA", "Karnataka", "IN-KA", "state", "yes", ""),
    ("KL", "Kerala", "IN-KL", "state", "yes", ""),
    ("LD", "Lakshadweep", "IN-LD", "union_territory", "yes", ""),
    ("MP", "Madhya Pradesh", "IN-MP", "state", "yes", ""),
    ("MH", "Maharashtra", "IN-MH", "state", "yes", ""),
    ("MN", "Manipur", "IN-MN", "state", "yes", ""),
    ("ML", "Meghalaya", "IN-ML", "state", "yes", ""),
    ("MZ", "Mizoram", "IN-MZ", "state", "yes", ""),
    ("NL", "Nagaland", "IN-NL", "state", "yes", ""),
    ("OD", "Odisha", "IN-OR", "state", "yes", ""),
    ("PY", "Puducherry", "IN-PY", "union_territory", "yes", ""),
    ("PB", "Punjab", "IN-PB", "state", "yes", ""),
    ("RJ", "Rajasthan", "IN-RJ", "state", "yes", ""),
    ("SK", "Sikkim", "IN-SK", "state", "yes", ""),
    ("TN", "Tamil Nadu", "IN-TN", "state", "yes", ""),
    ("TG", "Telangana", "IN-TG", "state", "yes", ""),
    ("TR", "Tripura", "IN-TR", "state", "yes", ""),
    ("UP", "Uttar Pradesh", "IN-UP", "state", "yes", ""),
    ("UK", "Uttarakhand", "IN-UT", "state", "yes", ""),
    ("WB", "West Bengal", "IN-WB", "state", "yes", ""),
    # erstwhile / renamed
    ("MDR", "Madras", "", "state", "no", "TN"),
    ("MYS", "Mysore", "", "state", "no", "KA"),
    ("GDD", "Goa, Daman and Diu", "", "union_territory", "no", "GA"),
    ("DN", "Dadra and Nagar Haveli", "", "union_territory", "no", "DH"),
    ("DD", "Daman and Diu", "", "union_territory", "no", "DH"),
]

# --- raw Lok Dhaba State_Name -> acronym --------------------------------------
STATE_CROSSWALK = {
    "Andaman_&_Nicobar_Islands": "AN",
    "Andhra_Pradesh": "AP",
    "Arunachal_Pradesh": "AR",
    "Assam": "AS",
    "Bihar": "BR",
    "Chandigarh": "CH",
    "Chhattisgarh": "CG",
    "Dadra & Nagar Haveli And Daman & Diu": "DH",
    "Dadra_&_Nagar_Haveli": "DN",
    "Daman_&_Diu": "DD",
    "Delhi": "DL",
    "Goa": "GA",
    "Goa,_Daman_&_Diu": "GDD",
    "Goa_Daman_&_Diu": "GDD",
    "Gujarat": "GJ",
    "Haryana": "HR",
    "Himachal_Pradesh": "HP",
    "Jammu_&_Kashmir": "JK",
    "Jharkhand": "JH",
    "Karnataka": "KA",
    "Kerala": "KL",
    "Lakshadweep": "LD",
    "Madhya_Pradesh": "MP",
    "Madras": "MDR",
    "Maharashtra": "MH",
    "Manipur": "MN",
    "Meghalaya": "ML",
    "Mizoram": "MZ",
    "Mysore": "MYS",
    "Nagaland": "NL",
    "Odisha": "OD",
    "Puducherry": "PY",
    "Punjab": "PB",
    "Rajasthan": "RJ",
    "Sikkim": "SK",
    "Tamil_Nadu": "TN",
    "Telangana": "TG",
    "Tripura": "TR",
    "Uttar_Pradesh": "UP",
    "Uttarakhand": "UK",
    "West_Bengal": "WB",
}

# --- dictionary (value -> label) for the coded columns ------------------------
DICT_TABLES = ["general_elections", "assembly_elections"]
DICT_COLUMNS = ["table_id", "column_name", "key", "temporal_coverage", "value"]
DICTIONARY = {
    "poll_type": [
        ("0", "Regular election"),
        ("1", "First bye-poll"),
        ("2", "Second bye-poll"),
    ],
    "delimitation_id": [
        ("1", "First delimitation (1962-1963)"),
        ("2", "Second delimitation (1964-1972)"),
        ("3", "Third delimitation (1973-2007)"),
        ("4", "Fourth delimitation (2008-current)"),
    ],
    "constituency_type": [
        ("GEN", "General (unreserved)"),
        ("SC", "Reserved for Scheduled Castes"),
        ("ST", "Reserved for Scheduled Tribes"),
    ],
    "candidate_sex": [("M", "Male"), ("F", "Female"), ("O", "Other")],
    "candidate_type": [
        ("GEN", "General"),
        ("SC", "Scheduled Caste"),
        ("ST", "Scheduled Tribe"),
    ],
}
