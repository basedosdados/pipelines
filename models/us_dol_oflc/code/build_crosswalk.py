"""Build the committed year-to-canonical column crosswalk for us_dol_oflc.

Reads the header of every source file, resolves each header against
``canonical_map.ALIASES``, and writes one CSV per program to
``code/crosswalk/<program>.csv`` plus a coverage report.

A source column that is neither aliased nor matched by a DROP rule is reported
as UNMAPPED and the build exits non-zero. The crosswalk is the deliverable's
quality control: nothing enters or leaves the published tables silently.

Usage:
    uv run python models/us_dol_oflc/code/build_crosswalk.py
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from pathlib import Path

import python_calamine as pc

from pipelines.datasets.us_dol_oflc import canonical_map as cm

DATA = Path(
    os.environ.get("OFLC_DATA_DIR", Path.home() / "Downloads/us_dol_oflc_data")
)
INPUT = DATA / "input"
OUT = Path(__file__).resolve().parent / "crosswalk"

# --------------------------------------------------------------------------
# Source columns deliberately not published, as regexes, with the reason.
# --------------------------------------------------------------------------
DROP_RULES: list[tuple[str, str]] = [
    # Personal contact details of named individuals.
    (
        r"^(EMPLOYER_POC|EMP_POC|EMP_CONTACT)_",
        "employer point-of-contact personal details",
    ),
    (
        r"^(AGENT_ATTORNEY|ATTORNEY_AGENT|ATTY_AG)_(?!LAW_FIRM_NAME$|FIRM_NAME$|REP_TYPE$)",
        "attorney/agent personal details",
    ),
    (r"^AGENT_ATTORNEY_NAME$", "attorney/agent personal name"),
    (r"^(PREPARER|DECL_PREP)_", "form preparer personal details"),
    (
        r"^(EMPLOYER_DECL_INFO|EMP_INFO_DECL|PREPARER_INFO)_",
        "declarant personal details",
    ),
    (
        r"^(EMPLOYER|EMP|LAWFIRM_BUSINESS|ATTY_AG)_?(PHONE|PHONEEXT|FEIN)",
        "phone / employer identifier",
    ),
    (r"^EMPLOYER_PHONE(_EXT)?$", "employer phone"),
    (
        r"^(PHONE|EMAIL|WEBSITE)_(TO_APPLY|EXT_TO_APPLY)$",
        "job application contact",
    ),
    (
        r"^FOREIGN_WORKER_(INFO_)?(CITY|STATE)$|^FW_INFO_POSTAL_CODE$",
        "foreign worker residence — personal",
    ),
    (
        r"^FOREIGN_WORKER_ED_INST",
        "foreign worker education institution address",
    ),
    # Second and later worksite / wage blocks: only the primary worksite is published.
    (
        r"_(?:[2-9]|10)$|(?<=[A-Z])[2-9]$",
        "secondary worksite/wage block (primary worksite only)",
    ),
    (
        r"^(PW_NON-OES_YEAR|PW_SURVEY_NAME|PW_SURVEY_PUBLISHER|PW_OTHER_SOURCE|"
        r"PW_SOURCE_NAME_OTHER)(_1)?$",
        "prevailing wage survey detail",
    ),
    (r"^WORKSITE_ADDRESS2(_1)?$", "worksite address line 2"),
    (
        r"^(LCA_CASE_WORKLOC2|WORK_LOCATION_(CITY|STATE)2)",
        "second worksite block (primary worksite only)",
    ),
    (r"^(AGENT_CITY|AGENT_STATE)$", "agent location detail"),
    (r"^(EMP_DECL_TITLE|EMPLOYER_DECL_TITLE)$", "declarant title"),
    (
        r"^(EMP_WORKER_INTEREST|EMP_RELATIONSHIP_WORKER)$",
        "employer-worker relationship attestation",
    ),
    (r"^(PRIMARY|PRMARY)[_/]SUB$", "legacy primary/sub application flag"),
    (
        r"^DOT_(OCCUPATIONAL_CODE|NAME)$",
        "legacy DOT occupation code (SOC is kept)",
    ),
    (r"^NAME_REQUIRED_TRAINING$", "H-2 job-condition detail"),
    (
        r"^(WAGE_RATE|RATE_PER|MAX_RATE|PART_TIME|CITY|STATE|PREVAILING_WAGE|WAGE_SOURCE|"
        r"YR_SOURCE_PUB|OTHER_WAGE_SOURCE)__?2$",
        "second worksite block",
    ),
    # Recruitment / advertising process detail (PERM), not wage or employer facts.
    (
        r"^(RECR_INFO|RECR_OCC|RI_|REC_INFO|NOTICE_POST)",
        "PERM recruitment process detail",
    ),
    (
        r"^(JOB_FAIR|ON_CAMPUS_RECRUITING|EMPLOYER_WEBSITE|PRO_ORG_AD|JOB_SEARCH_WEBSITE|"
        r"PVT_EMPLOYMENT_FIRM|EMPLOYEE_REF|EMPLOYEE_REFERRAL|CAMPUS_PLACEMENT|"
        r"LOCAL_ETHNIC_PAPER|RADIO_TV_AD|SWA_JOB_ORDER|SUNDAY_EDITION|FIRST_NEWSPAPER|"
        r"FIRST_ADVERTISEMENT|SECOND_NEWSPAPER|SECOND_ADVERTISEMENT|SECOND_AD|"
        r"ADD_RECRUIT|TEACHER_|BASIC_RECRUITMENT|COMPETITIVE_PROCESS|"
        r"APP_FOR_COLLEGE|PROFESSIONAL_OCCUPATION)",
        "PERM recruitment process detail",
    ),
    (
        r"^(JOB_INFO_|JI_|FW_INFO_|FOREIGN_WORKER_|ACCEPT_|OTHER_REQ_|EMP_CERTIFY|"
        r"EMP_RECEIVED_PAYMENT|PAYMENT_DETAILS|BARGAINING_REP|POSTED_NOTICE|"
        r"LAYOFF_IN_PAST|US_WORKERS_CONSIDERED|SPECIFIC_SKILLS|COMBINATION_OCCUPATION|"
        r"FOREIGN_LANGUAGE_REQUIRED|OFFERED_TO_APPL|REQUIRED_TRAINING|"
        r"REQUIRED_FIELD_OF_TRAINING|JOB_OPP_REQUIREMENTS_NORMAL|JOB_EDUCATION_MIN_OTHER|"
        r"EMPLOYER_COMPLETED_APPLICATION|PREVIOUS_SWA_CASE|ORIG_CASE_NO|FW_OWNERSHIP)",
        "PERM job-requirement / worker-qualification detail",
    ),
    # Attachment and form-plumbing flags.
    (r"(ATTACHED|_COMPLETED|APPENDIX|ADDENDUM)", "form attachment flag"),
    (
        r"^(STATE_OF_HIGHEST_COURT|NAME_OF_HIGHEST_STATE_COURT)$",
        "attorney bar detail",
    ),
    (
        r"^(PUBLIC_DISCLOSURE|PUBLIC_DISCLOSURE_LOCATION|LABOR_CON_AGREE|"
        r"AGREE_TO_LC_STATEMENT|MASTERS_EXEMPTION)$",
        "LCA attestation plumbing",
    ),
    # H-2 day-by-day schedules and job-condition detail.
    (
        r"^(SUNDAY|MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY)_HOURS$",
        "per-weekday hour schedule",
    ),
    (r"^HOURLY_(WORK_)?SCHEDULE", "hourly work schedule"),
    (
        r"^(ON_CALL_REQUIREMENT|CERTIFICATION_REQUIREMENTS|DRIVER_REQUIREMENTS|"
        r"CRIMINAL_BACKGROUND_CHECK|DRUG_SCREEN|LIFTING_|EXPOSURE_TO_TEMPERATURES|"
        r"EXTENSIVE_|FREQUENT_STOOPING|REPETITIVE_MOVEMENTS|SUPERVISE_|"
        r"ADDITIONAL_JOB_REQUIREMENTS|SPECIAL_REQUIREMENTS|MIN_STANDARDS|"
        r"TRAINING_MONTHS|TRAINING_REQ|NUM_MONTHS_TRAINING|NAME_REQD_TRAINING|"
        r"EMP_EXPERIENCE_REQD|OTHER_EDU|MAJOR$|SECOND_DIPLOMA)",
        "H-2 job-condition detail",
    ),
    (
        r"^(HOUSING_ADDRESS_LOCATION|HOUSING_POSTAL_CODE|HOUSING_COUNTY|"
        r"HOUSING_COMPLIANCE|TOTAL_UNITS|TOTAL_HOUSING_RECORDS|MEALS_CHARGE|"
        r"MEAL_REIMBURSEMENT|TOTAL_ADDENDUM_A_RECORDS)",
        "H-2A housing detail",
    ),
    (
        r"^(DEDUCTIONS_FROM_PAY|OTHER_FREQUENCY_OF_PAY|OVERTIME_PAY|"
        r"PIECE_RATE_ADDITIONAL_INFO|ADDITIONAL_WAGE_CONDITIONS|JOB_OPP_WAGE_CONDITIONS|"
        r"DAILY_TRANSPORTATION|OVERTIME_AVAILABLE|ON_THE_JOB_TRAINING_AVAILABLE|"
        r"EMP_PROVIDED_TOOLS_EQUIPMENT|BOARD_LODGING_OTHER_FACILITIES|"
        r"FOREIGN_LABOR_RECRUITER|JOB_CONTRACT_EXISTS|USING_AGENT_RECRUITER|"
        r"AG_ASSN_OR_AGENCY_STATUS|WORK_CONTRACTS|EMPLOYER_MSPA|EMP_MSPA|AGENT_MSPA|"
        r"SURETY_BOND|HOUSING_TRANSPORTATION|AGENT_AGREEMENT|AGREEMENTS|"
        r"JOB_ORDER_TO_SWA|PREV_TRADE_NAME_DBA)",
        "H-2 contract / attestation detail",
    ),
    (r"^(CAP_SUBJECT_WORKERS|CAP_EXEMPT_WORKERS)$", "H-2B cap breakdown"),
    (
        r"^(2nd|3rd)_PWD_CASE_NUMBER$",
        "additional prevailing-wage determination",
    ),
    (r"^EMERGENCY_FILING_PWD", "emergency filing attachment"),
    (r"^OTHER_WORKSITE_LOCATION$", "free-text secondary worksite note"),
    (
        r"^PRIMARY_WORKSITE_(TYPE|BLS_AREA)$",
        "PERM worksite classification detail",
    ),
    (
        r"^ADDRESS2$|^EMPLOYER_ADDRESS_?2$|^LCA_CASE_EMPLOYER_ADDRESS2$|^EMP_ADDR2$|"
        r"^WORKSITE_ADDRESS2$|^WORKSITE_ADDRESS_2$|^PRIMARY_WORKSITE_ADDR2$",
        "address line 2",
    ),
    (r"^EMPLOYER_PROVINCE$|^EMP_PROVINCE$", "non-US employer province"),
    (r"^EMPLOYER_COUNTY$", "employer county (present only FY2009)"),
    (r"^(2007_)?NAICS_US_TITLE$", "NAICS title (code is kept)"),
    (
        r"^(CERTIFIED_BEGIN_DATE|CERTIFIED_END_DATE)$",
        "legacy certified period",
    ),
    (
        r"^(PW_SOURCE_NAME_OTHER_9089|PW_SOURCE_OTHER|PW_NON-OES_YEAR|PW_OTHER_YEAR|"
        r"PW_SURVEY_PUBLISHER|PW_SURVEY_NAME|OTHER_WAGE_SOURCE_1|PW_WAGE_SOURCE_OTHER)$",
        "prevailing wage survey detail",
    ),
    (
        r"^ALIEN_WORK_CITY$",
        "legacy H-2A worksite city, superseded by WORKSITE_CITY",
    ),
    (
        r"^PART_TIME_1$",
        "legacy part-time flag, superseded by FULL_TIME_POSITION",
    ),
]

DROP_RE = [(re.compile(p, re.IGNORECASE), why) for p, why in DROP_RULES]


def norm(col: str) -> str:
    """Normalise a source header: upper case, spaces to underscores.

    PERM FY2009 ships space-separated headers ("PW AMOUNT 9089") where
    every other year uses underscores.
    """
    return re.sub(r"\s+", "_", col.strip()).upper()


def fiscal_year(key: str) -> int:
    m = re.match(r"[a-z0-9]+_(\d{4})", key)
    if not m:
        raise SystemExit(
            f"Cannot read a fiscal year from the file name {key!r}"
        )
    return int(m.group(1))


def program(key: str) -> str:
    return key.split("_")[0]


CACHE = DATA / "headers_cache.json"
_cache: dict[str, list[str]] = (
    json.loads(CACHE.read_text()) if CACHE.exists() else {}
)


def header(path: Path) -> list[str]:
    """Header row of a source file, cached — parsing a 280 MB xlsx is slow."""
    key = f"{path.name}:{path.stat().st_size}"
    if key not in _cache:
        ws = pc.CalamineWorkbook.from_path(str(path)).get_sheet_by_index(0)
        rows = ws.to_python(nrows=1)
        _cache[key] = [str(c).strip() for c in rows[0]] if rows else []
        CACHE.write_text(json.dumps(_cache, indent=1))
    return _cache[key]


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    files = sorted(
        p
        for p in INPUT.iterdir()
        if p.suffix in (".xls", ".xlsx") and program(p.stem) in cm.ALIASES
    )
    unmapped: list[tuple[str, str]] = []
    per_program: dict[str, list[dict]] = {p: [] for p in cm.ALIASES}

    for path in files:
        key = path.stem
        prog = program(key)
        fy = fiscal_year(key)
        alias_index = {
            norm(a): canon
            for canon, aliases in cm.ALIASES[prog].items()
            for a in aliases
        }
        cols = header(path)
        seen: dict[str, str] = {}
        for col in cols:
            canon = alias_index.get(norm(col))
            if canon and canon not in seen:
                seen[canon] = col
                per_program[prog].append(
                    {
                        "fiscal_year": fy,
                        "source_file": path.name,
                        "source_column": col,
                        "canonical_column": canon,
                        "disposition": "mapped",
                    }
                )
                continue
            if canon:  # duplicate alias hit — keep the first, record the rest
                per_program[prog].append(
                    {
                        "fiscal_year": fy,
                        "source_file": path.name,
                        "source_column": col,
                        "canonical_column": "",
                        "disposition": f"duplicate of {canon}",
                    }
                )
                continue
            why = next((w for rx, w in DROP_RE if rx.search(norm(col))), None)
            if why:
                per_program[prog].append(
                    {
                        "fiscal_year": fy,
                        "source_file": path.name,
                        "source_column": col,
                        "canonical_column": "",
                        "disposition": f"dropped: {why}",
                    }
                )
            else:
                unmapped.append((key, col))
                per_program[prog].append(
                    {
                        "fiscal_year": fy,
                        "source_file": path.name,
                        "source_column": col,
                        "canonical_column": "",
                        "disposition": "UNMAPPED",
                    }
                )
        # canonical columns with no source this year
        for canon, _ in cm.columns(prog):
            if canon in ("year", "source_file") or canon.endswith("_annual"):
                continue
            if canon not in seen:
                per_program[prog].append(
                    {
                        "fiscal_year": fy,
                        "source_file": path.name,
                        "source_column": "",
                        "canonical_column": canon,
                        "disposition": "absent this year",
                    }
                )

    for prog, rows in per_program.items():
        rows.sort(
            key=lambda r: (
                r["fiscal_year"],
                r["source_file"],
                r["canonical_column"] or "zzz",
                r["source_column"],
            )
        )
        with open(OUT / f"{prog}.csv", "w", newline="") as fh:
            w = csv.DictWriter(
                fh,
                fieldnames=[
                    "fiscal_year",
                    "source_file",
                    "source_column",
                    "canonical_column",
                    "disposition",
                ],
            )
            w.writeheader()
            w.writerows(rows)
        n_map = sum(1 for r in rows if r["disposition"] == "mapped")
        n_drop = sum(1 for r in rows if r["disposition"].startswith("dropped"))
        n_abs = sum(1 for r in rows if r["disposition"] == "absent this year")
        print(
            f"{prog}: {n_map} mapped, {n_drop} dropped, {n_abs} absent-year cells"
        )

    if unmapped:
        print(f"\n{len(unmapped)} UNMAPPED source columns:", file=sys.stderr)
        for key, col in unmapped:
            print(f"  {key}: {col}", file=sys.stderr)
        return 1
    print("\nAll source columns are mapped or explicitly dropped.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
