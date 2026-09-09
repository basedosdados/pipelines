"""Dictionary content for ``us_osha_enforcement``.

Two sources, both authoritative, and nothing else:

1. ``LOOKUP_TABLE_TO_COLUMN`` maps each of the 14 code tables OSHA ships in
   ``osha_accident_lookup2`` onto the columns it resolves. Labels come from the
   file itself at build time.
2. ``PUBLISHED_CODES`` holds the code lists published in the column
   descriptions of the DOL Enforcement Data Catalog, plus the violation
   classification, which OSHA documents in its Field Operations Manual.

Codes with no published legend are deliberately absent. Inventing labels for
``gravity``, ``abatement_completion_code``, ``related_event_code``,
``hazard_category``, ``event_code``, ``related_type`` and ``program_type``
would put guesses into the dictionary; each of those columns instead records
the gap in its ``observations``.
"""

from __future__ import annotations

# code table in osha_accident_lookup2 -> [(table_slug, column_name), ...]
LOOKUP_TABLE_TO_COLUMN: dict[str, list[tuple[str, str]]] = {
    "IN": [("accident_injury", "nature_of_injury")],
    "BD": [("accident_injury", "part_of_body")],
    "SO": [("accident_injury", "source_of_injury")],
    "FT": [("accident_injury", "event_type")],
    "EN": [("accident_injury", "environmental_factor")],
    "HU": [("accident_injury", "human_factor")],
    "OCC": [("accident_injury", "occupation_code")],
    "DEGR": [("accident_injury", "degree_of_injury")],
    "TASK": [("accident_injury", "task_assigned")],
    "OPER": [
        ("accident_injury", "construction_operation"),
        ("accident_injury", "construction_operation_cause"),
    ],
    "CAUS": [("accident_injury", "fatality_cause")],
    "ENDU": [("accident", "construction_end_use")],
    "COST": [("accident", "project_cost")],
    "PTYP": [("accident", "project_type")],
}

# Flag columns whose only value is "X". Blank means the flag does not apply,
# and a blank is stored as NULL, so only the positive value is dictionary-coded.
_FLAG_X = {
    "X": "Yes",
}

_FLAG_COLUMNS: list[tuple[str, str]] = [
    ("inspection", "safety_program_manufacturing"),
    ("inspection", "safety_program_construction"),
    ("inspection", "safety_program_maritime"),
    ("inspection", "health_program_manufacturing"),
    ("inspection", "health_program_construction"),
    ("inspection", "health_program_maritime"),
    ("inspection", "migrant_labor"),
    ("violation", "deleted"),
    ("violation", "emphasis_program"),
    ("accident", "fatality"),
    ("related_activity", "related_safety"),
    ("related_activity", "related_health"),
]

_VIOLATION_TYPE = {
    "S": "Serious",
    "W": "Willful",
    "R": "Repeat",
    "O": "Other-than-serious",
    "U": "Unclassified",
}

# (table_slug, column_name) -> {code: label}
PUBLISHED_CODES: dict[tuple[str, str], dict[str, str]] = {
    ("inspection", "owner_type"): {
        "A": "Private",
        "B": "Local government",
        "C": "State government",
        "D": "Federal government",
    },
    ("inspection", "safety_health"): {
        "S": "Safety",
        "H": "Health",
    },
    ("inspection", "advance_notice"): {
        "Y": "Yes",
        "N": "No",
    },
    ("inspection", "inspection_type"): {
        "A": "Accident",
        "B": "Complaint",
        "C": "Referral",
        "D": "Monitoring",
        "E": "Variance",
        "F": "Follow-up",
        "G": "Unprogrammed related",
        "H": "Planned",
        "I": "Programmed related",
        "J": "Unprogrammed other",
        "K": "Programmed other",
        "L": "Other",
        "M": "Fatality or catastrophe",
        "N": "Unprogrammed emphasis",
    },
    ("inspection", "inspection_scope"): {
        "A": "Complete",
        "B": "Partial",
        "C": "Records only",
        "D": "No inspection",
    },
    ("inspection", "no_inspection_reason"): {
        "A": "No inspection: establishment not found",
        "B": "No inspection: out of business",
        "C": "No inspection: process inactive",
        "D": "No inspection: ten or fewer employees",
        "E": "No inspection: entry denied",
        "F": "No inspection: SIC not on the planning guide",
        "G": "No inspection: exempt voluntary",
        "H": "No inspection: non-exempt consultation",
        "I": "No inspection: other",
        "J": "No inspection: employer exempted by appropriation act",
    },
    ("inspection", "union_status"): {
        "Y": "Yes",
        "U": "Yes",
        "A": "Yes",
        "N": "No",
        "B": "No",
    },
    ("accident_injury", "sex"): {
        "M": "Male",
        "F": "Female",
    },
    ("violation", "violation_type"): dict(_VIOLATION_TYPE),
    ("violation_event", "violation_type"): dict(_VIOLATION_TYPE),
    ("violation_event", "penalty_or_fta"): {
        "P": "Penalty",
        "F": "Failure to abate",
    },
    ("accident_narrative", "wrap_style"): {
        "fixed_80": "Source wraps every line at exactly 80 characters, splitting words; lines joined with no separator",
        "word_wrap": "Source wraps at word boundaries and drops the trailing space; lines joined with a single space",
        "single_line": "Narrative fits in one source line; no joining needed",
    },
}

for _t, _c in _FLAG_COLUMNS:
    PUBLISHED_CODES[(_t, _c)] = dict(_FLAG_X)
