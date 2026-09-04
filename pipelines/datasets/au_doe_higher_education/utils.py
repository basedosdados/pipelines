"""Pure download and cleaning functions for au_doe_higher_education.

Shared by the one-shot onboarding bootstrap in
``models/au_doe_higher_education/code/`` and, later, by the recurring Prefect
flow. No Prefect imports belong here.

The student and staff "pivot table" releases are Excel PivotTables whose source
data lives in the pivot *cache*, not in any worksheet: the visible grid is ~190
rows while ``xl/pivotCache/pivotCacheRecords1.xml`` holds the full long-format
fact table. Everything in the Tier 1 half of this module reads that cache.
"""

from __future__ import annotations

import re
import shutil
import unicodedata
import zipfile
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"

STATE_ABBREVIATION = {
    "New South Wales": "NSW",
    "Victoria": "VIC",
    "Queensland": "QLD",
    "Western Australia": "WA",
    "South Australia": "SA",
    "Tasmania": "TAS",
    "Northern Territory": "NT",
    "Australian Capital Territory": "ACT",
    "Multi-State": "MULTI",
    "Multi-state": "MULTI",
    "Australia": "AUS",
    "NSW": "NSW",
    "Vic.": "VIC",
    "Vic": "VIC",
    "Qld": "QLD",
    "WA": "WA",
    "SA": "SA",
    "Tas.": "TAS",
    "Tas": "TAS",
    "NT": "NT",
    "ACT": "ACT",
}

# Field names drift cosmetically between publication vintages: spaces become
# underscores in 2024, "Programs" becomes "Programmes", a comma appears in
# "Agriculture, Environmental", and the non-award measure is spelled three ways.
# Normalise to the architecture's column names.
PIVOT_FIELD_RENAME = {
    "year": "year",
    "institution": "institution_id",
    "state": "state_abbreviation",
    "citizenship": "citizenship",
    "commencing": "commencing",
    "broad_course_level": "course_level_broad",
    "detailed_course_level": "course_level_detailed",
    "gender": "gender",
    "mode_of_attendance": "attendance_mode",
    "type_of_attendance": "attendance_type",
    "special_course": "special_course",
    "broad_field_of_education_primary": "field_of_education_primary",
    "broad_field_of_education_secondary": "field_of_education_secondary",
    "discipline": "discipline",
    "liability_status": "liability_status",
    "enrolment_count": "enrolments",
    "student_load": "student_load_eftsl",
    "completions": "completions",
    "current_duties_classification": "duties_classification",
    "function": "function",
    "organisational_unit": "organisational_unit",
    "work_contract": "work_contract",
    "staff_count": "staff_headcount",
    "fte_staff_count": "staff_fte",
}

# Category labels that changed between publication vintages. These are renames
# of the same category, not new categories: leaving them unharmonised makes the
# same underlying rows survive de-duplication twice, once under each label.
# Staff "Non-academic classification level group" -> "Professional" alone
# inflated 2021-2024 staff FTE by roughly 62,000 (about 46%) before this map.
CATEGORY_HARMONISE: dict[str, dict[str, str]] = {
    "course_level_broad": {
        "Non-Award": "Non-Award/Microcredentials",
    },
    "course_level_detailed": {
        "Non-Award": "Non-Award/Microcredentials",
    },
    "field_of_education_primary": {
        "Agriculture Environmental and Related Studies": (
            "Agriculture, Environmental and Related Studies"
        ),
        "Food Hospitality and Personal Services": (
            "Food, Hospitality and Personal Services"
        ),
        "Mixed Field Programs": "Mixed Field Programmes",
        "Non-Award course": "Non-Award Courses",
        "Non-Award courses": "Non-Award Courses",
        "": "Not provided",
    },
    "field_of_education_secondary": {
        "Agriculture Environmental and Related Studies": (
            "Agriculture, Environmental and Related Studies"
        ),
        "Food Hospitality and Personal Services": (
            "Food, Hospitality and Personal Services"
        ),
        "Mixed Field Programs": "Mixed Field Programmes",
    },
    "discipline": {
        "Agriculture Environmental and Related Studies": (
            "Agriculture, Environmental and Related Studies"
        ),
        "Food Hospitality and Personal Services": (
            "Food, Hospitality and Personal Services"
        ),
        "Mixed Field Programs": "Mixed Field Programmes",
    },
    "duties_classification": {
        "Non-academic classification level group": "Professional",
    },
}

# Institutions renamed or re-bucketed between vintages, keyed on the slug.
# Without these the same institution survives de-duplication under two keys and
# its rows are counted twice: the Avondale re-bucketing alone added 69,460
# enrolments to 2020, against a published national total of 1,622,867.
INSTITUTION_HARMONISE = {
    # Avondale was renamed twice as it gained university status.
    "avondale_college_of_higher_education": "avondale_university",
    "avondale_university_college": "avondale_university",
    # Truncated at 50 characters in some source vintages.
    "batchelor_institute_of_indigenous_tertiary_educati": (
        "batchelor_institute_of_indigenous_tertiary_education"
    ),
    # Older vintages carved Avondale out of the New South Wales residual bucket
    # and labelled it separately; from 2023 the single bucket is used again. The
    # two labels are disjoint (they differ by state), so merging is a relabel.
    "non_university_higher_education_providers_excluding_avondale": (
        "non_university_higher_education_providers"
    ),
    # The published cross-tabs say "Institutions" where the cubes say
    # "Providers" for the same residual bucket.
    "non_university_higher_education_institutions": (
        "non_university_higher_education_providers"
    ),
    "private_universities_table_c_and_non_university_higher_education_institutions": (
        "private_universities_and_non_university_higher_education_providers"
    ),
}

# Trailing rows in the published cross-tabs that are commentary, not data: the
# previous year's total for comparison, the percentage change, and footnotes.
NON_DATA_ROW = re.compile(
    r"^(Total\s+(19|20)\d{2}|%\s*change|np\b|\(\d)", re.IGNORECASE
)

# Values the source uses for "no secondary field of education".
NOT_COMBINED = "Not a Combined Course"


def snake(text: object) -> str:
    """Normalise a source field name to snake_case for lookup."""
    normalised = unicodedata.normalize("NFKD", str(text))
    normalised = re.sub(r"[^0-9a-zA-Z]+", "_", normalised)
    return normalised.strip("_").lower()


def slugify_institution(name: object) -> str:
    """Stable directory key derived from an institution's published name."""
    text = unicodedata.normalize("NFKD", str(name))
    text = text.encode("ascii", "ignore").decode("ascii")
    text = re.sub(r"[^0-9a-zA-Z]+", "_", text)
    return text.strip("_").lower()


def read_pivot_cache(path: str | Path) -> pd.DataFrame:
    """Return the full pivot cache of an .xlsx as a long DataFrame.

    ``pivotCacheDefinition1.xml`` names the fields and, per field, lists its
    distinct values in ``<sharedItems>``. Each ``<r>`` record in
    ``pivotCacheRecords1.xml`` then stores ``<x v="N"/>`` as an index into that
    list, ``<m/>`` for missing, and ``<n>``/``<s>`` for literal values.
    """
    with zipfile.ZipFile(path) as archive:
        definition = ET.fromstring(
            archive.read("xl/pivotCache/pivotCacheDefinition1.xml")
        )
        cache_fields = definition.find(NS + "cacheFields")
        if cache_fields is None:
            raise ValueError(f"{path} has no pivot cache fields")
        fields: list[tuple[str, list[str] | None]] = []
        for cache_field in cache_fields:
            shared = cache_field.find(NS + "sharedItems")
            items = (
                [str(item.get("v")) for item in list(shared)]
                if shared is not None and shared.get("count")
                else None
            )
            fields.append((str(cache_field.get("name")), items))

        records: list[list[str | None]] = []
        with archive.open("xl/pivotCache/pivotCacheRecords1.xml") as handle:
            for _, element in ET.iterparse(handle, events=("end",)):
                if element.tag != NS + "r":
                    continue
                row: list[str | None] = []
                for index, child in enumerate(element):
                    tag = child.tag[len(NS) :]
                    if tag == "x":
                        items = fields[index][1]
                        row.append(
                            items[int(child.get("v"))]
                            if items
                            else child.get("v")
                        )
                    elif tag == "m":
                        row.append(None)
                    else:
                        row.append(child.get("v"))
                records.append(row)
                element.clear()

    return pd.DataFrame(records, columns=[name for name, _ in fields])


def clean_pivot(path: str | Path, measures: dict[str, str]) -> pd.DataFrame:
    """Read one pivot-table vintage and reshape it to the architecture.

    ``measures`` maps the architecture's measure column to its BigQuery type,
    e.g. ``{"enrolments": "INT64"}``. Every other cache field is treated as a
    dimension. The per-field-of-education ``<field> Count`` columns the source
    ships from the 2021 vintage onward are dropped: they restate the headline
    measure against the row's own field-of-education dimensions and carry no
    information.
    """
    frame = read_pivot_cache(path)
    frame.columns = [snake(column) for column in frame.columns]

    keep = {
        column: PIVOT_FIELD_RENAME[column]
        for column in frame.columns
        if column in PIVOT_FIELD_RENAME
    }
    frame = frame[list(keep)].rename(columns=keep)

    for column in frame.columns:
        if column in measures:
            continue
        values = frame[column].astype("string").str.strip()
        values = values.mask(values == "NULL")
        frame[column] = values.replace(CATEGORY_HARMONISE.get(column, {}))

    # The source writes a blank string for "not a combined course" before 2024.
    if "field_of_education_secondary" in frame.columns:
        secondary = frame["field_of_education_secondary"]
        frame["field_of_education_secondary"] = secondary.mask(
            secondary == "NULL"
        ).replace({"": NOT_COMBINED})
    for column in ("course_level_detailed", "course_level_broad"):
        if column in frame.columns:
            values = frame[column]
            frame[column] = values.mask(values.isin(["", "NULL"]))

    frame["state_abbreviation"] = frame["state_abbreviation"].map(
        STATE_ABBREVIATION
    )
    frame["institution_name"] = frame["institution_id"]
    frame["institution_id"] = (
        frame["institution_id"]
        .map(slugify_institution)
        .replace(INSTITUTION_HARMONISE)
    )
    frame["year"] = pd.to_numeric(frame["year"], errors="coerce").astype(
        "Int64"
    )

    for measure, dtype in measures.items():
        values = pd.to_numeric(frame[measure], errors="coerce")
        # -1 is the source's suppression sentinel, not a count.
        values = values.mask(values < 0)
        frame[measure] = (
            values.astype("Int64")
            if dtype == "INT64"
            else values.astype("Float64")
        )

    return frame.drop(columns=["institution_name"])


def stack_vintages(
    paths: list[Path], measures: dict[str, str], dimensions: list[str]
) -> pd.DataFrame:
    """Stack publication vintages by taking each year from its newest vintage.

    Each vintage is an internally consistent snapshot covering a rolling window
    of recent years, and each reproduces the department's published annual
    totals exactly. Vintages must therefore be combined **whole years at a
    time**, never merged row by row: labels drift between vintages (an
    institution renamed, a classification re-bucketed), so a row-level union
    keyed on the dimensions lets the same underlying population survive twice
    under two spellings. Doing that inflated 2019 enrolments by 2,588 against a
    published 1,609,798.

    Assigning each year to the single newest vintage that covers it keeps the
    published totals exact by construction and gives every year the most current
    classifications.
    """
    frames = []
    claimed: set[int] = set()
    # Newest vintage first, so earlier vintages only fill years nothing covers.
    for path in sorted(paths, reverse=True):
        frame = clean_pivot(path, measures)
        frame = frame[~frame["year"].isin(claimed)]
        if frame.empty:
            continue
        claimed.update(frame["year"].dropna().unique().tolist())
        frames.append(frame)

    stacked = pd.concat(frames, ignore_index=True)
    # A few exact-duplicate dimension keys occur inside a single vintage (8 rows
    # in the 2024 enrolments cube); sum them rather than dropping data.
    stacked = stacked.groupby(dimensions, dropna=False, as_index=False)[
        list(measures)
    ].sum(min_count=1)
    return stacked.sort_values(dimensions).reset_index(drop=True)


# --------------------------------------------------------------------------
# Institution directory
# --------------------------------------------------------------------------

PROVIDER_CATEGORY = {
    "Public Universities (Table A)": "Public Universities (Table A)",
    "Private Universities (Table B)": "Private Universities (Table B)",
    "Private Universities (Table C)": "Private Universities (Table C)",
}

# Rows in the published institution list that are headings or footnotes rather
# than institutions.
NON_INSTITUTION = re.compile(
    r"^(Higher Education Institutions|For more information|Higher Education Support Act"
    r"|Not applicable|Other Approved Higher Education Institutions)",
    re.IGNORECASE,
)

# Buckets the statistics tables publish in place of individual providers.
AGGREGATE_INSTITUTIONS = {
    "non_university_higher_education_providers": (
        "Non-University Higher Education Providers"
    ),
    "private_universities_and_non_university_higher_education_providers": (
        "Private Universities and Non-University Higher Education Providers"
    ),
    "table_a_providers": "Table A Providers",
    "table_b_providers": "Table B Providers",
    "table_c_providers": "Table C Providers",
    "table_a_and_b_providers": "Table A and B Providers",
}


# Provider codes that the Section 15 to 17 tables do not carry, taken from
# br_bd_diretorios_au.higher_education_provider, which is built from the Higher
# Education Research Data Collection. All 42 codes the sections do carry match
# that directory exactly, so the two are the same identifier.
PROVIDER_CODE_OVERRIDES = {
    "batchelor_institute_of_indigenous_tertiary_education": "2246",
}


def collect_provider_codes(
    path: str | Path, sheets: list[str], column: int
) -> dict:
    """Harvest the department's provider code from published table labels.

    The cross-tabs print it in the institution cell — "Monash University (3035)"
    — while the pivot cubes give only the name. It is the source's own
    identifier and worth carrying on the directory even though the directory
    cannot be keyed on it (the aggregate buckets have none).
    """
    codes: dict[str, str] = {}
    for sheet in sheets:
        try:
            rows = _sheet_rows(path, sheet)
        except KeyError:
            continue
        for row in rows:
            if len(row) <= column or row[column] is None:
                continue
            name, code = split_institution_label(row[column])
            if not code or name is None or TOTAL_ROW.match(name):
                continue
            slug = slugify_institution(name)
            codes.setdefault(INSTITUTION_HARMONISE.get(slug, slug), code)
    return codes


def build_institution_directory(
    institution_list_path: str | Path,
    observed: pd.DataFrame,
    provider_codes: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Build br_bd_diretorios_au.higher_education_institution.

    ``institution_list_path`` is the department's published "List of higher
    education institutions" (a single indented column: state, then provider
    category, then names). ``observed`` is a frame of the distinct
    ``institution_id`` / ``state_abbreviation`` pairs actually appearing in the
    statistics tables, so every foreign key resolves even for the aggregate
    buckets, which the published list does not contain.

    The source publishes no provider code, so the key is a slug derived from the
    name; ``teqsa_provider_code`` is reserved for later reconciliation with the
    national register.
    """
    raw = pd.read_excel(institution_list_path, header=None)[0]
    records = []
    state = category = None
    for value in raw.dropna():
        text = str(value).strip()
        if not text or NON_INSTITUTION.match(text):
            continue
        if text in STATE_ABBREVIATION:
            state = STATE_ABBREVIATION[text]
            continue
        if text in PROVIDER_CATEGORY:
            category = PROVIDER_CATEGORY[text]
            continue
        records.append(
            {
                "id_higher_education_institution": slugify_institution(text),
                "name": text,
                "state_abbreviation": state,
                "provider_category": category,
                "is_aggregate": "no",
            }
        )

    listed = pd.DataFrame(records)
    listed["id_higher_education_institution"] = listed[
        "id_higher_education_institution"
    ].replace(INSTITUTION_HARMONISE)
    listed = listed.drop_duplicates(
        subset=["id_higher_education_institution"], keep="first"
    )

    extra: list[dict[str, str | None]] = []
    known = set(listed["id_higher_education_institution"])
    for key, name in AGGREGATE_INSTITUTIONS.items():
        if key in known:
            continue
        known.add(key)
        extra.append(
            {
                "id_higher_education_institution": key,
                "name": name,
                "state_abbreviation": None,
                "provider_category": "Non-University Higher Education Provider"
                if "non_university" in key
                else None,
                "is_aggregate": "yes",
            }
        )
    for row in observed.itertuples():
        key = None if row.institution_id is None else str(row.institution_id)
        if key is None or key in known:
            continue
        known.add(key)
        extra.append(
            {
                "id_higher_education_institution": key,
                "name": AGGREGATE_INSTITUTIONS.get(
                    key, str(row.institution_name)
                ),
                "state_abbreviation": str(row.state_abbreviation),
                "provider_category": (
                    "Non-University Higher Education Provider"
                    if key in AGGREGATE_INSTITUTIONS
                    else None
                ),
                "is_aggregate": "yes"
                if key in AGGREGATE_INSTITUTIONS
                else "no",
            }
        )

    directory = pd.concat([listed, pd.DataFrame(extra)], ignore_index=True)
    directory.loc[
        directory["id_higher_education_institution"].isin(
            AGGREGATE_INSTITUTIONS
        ),
        "is_aggregate",
    ] = "yes"
    codes = dict(PROVIDER_CODE_OVERRIDES)
    codes.update(provider_codes or {})
    directory["provider_code"] = directory[
        "id_higher_education_institution"
    ].map(codes)
    return directory.sort_values(
        "id_higher_education_institution"
    ).reset_index(drop=True)


# --------------------------------------------------------------------------
# Published cross-tabs (Sections 11, 15, 16, 17 and the applications appendices)
# --------------------------------------------------------------------------

SUPPRESSED = re.compile(r"^\s*<\s*\d+\s*$")


def to_number(value: object) -> float | None:
    """Parse a published cell, mapping suppression markers to missing.

    Cells too small to publish appear as ``< 5``; ``np``, ``na`` and ``-`` are
    also used for not-published and not-applicable.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if (
        not text
        or SUPPRESSED.match(text)
        or text.lower() in {"np", "na", "n/a", "-", "..."}
    ):
        return None
    text = text.replace(",", "").replace("%", "")
    try:
        return float(text)
    except ValueError:
        return None


def _sheet_rows(path: str | Path, sheet: str) -> list[tuple]:
    # pyrefly: ignore [untyped-import]
    import openpyxl

    workbook = openpyxl.load_workbook(path, read_only=True, data_only=True)
    try:
        return [
            tuple(row) for row in workbook[sheet].iter_rows(values_only=True)
        ]
    finally:
        workbook.close()


# Published tables append the department's provider code and footnote markers to
# the institution name: "Monash University (3035)", "Avondale University
# (2252)(1.08)". A four-digit group is the provider code; "n.nn" groups are
# footnote references.
PROVIDER_CODE = re.compile(r"\((\d{4})\)")
LABEL_MARKERS = re.compile(r"\((?:\d+(?:\.\d+)?)\)")

# Rows that are the grand total of the state or country they sit in. These carry
# no institution: the state_abbreviation column already identifies them.
TOTAL_ROW = re.compile(
    r"^(National Total|State Total|Australia|Total|All (providers|institutions))\b",
    re.IGNORECASE,
)

# Rows that aggregate a named subset of providers. These are NOT totals — a
# sheet publishes "National Total", "Table A Providers" and "Table B Providers"
# side by side, so collapsing them all to a null institution silently averages
# three different populations into one row.
PROVIDER_GROUP_ROW = {
    "table_a_providers": "Table A Providers",
    "table_b_providers": "Table B Providers",
    "table_c_providers": "Table C Providers",
    "table_a_and_b_providers": "Table A and B Providers",
}


def split_institution_label(name: object) -> tuple[str | None, str | None]:
    """Split a published institution cell into its name and provider code."""
    if name is None:
        return None, None
    text = str(name).strip()
    code_match = PROVIDER_CODE.search(text)
    code = code_match.group(1) if code_match else None
    text = LABEL_MARKERS.sub("", text).strip()
    return (text or None), code


def _institution_key(name: object) -> str | None:
    """Map a published institution cell to a directory key, or None for totals."""
    text, _ = split_institution_label(name)
    if text is None or TOTAL_ROW.match(text):
        return None
    slug = slugify_institution(text)
    if slug in PROVIDER_GROUP_ROW:
        return slug
    return INSTITUTION_HARMONISE.get(slug, slug)


def _year_matrix(
    rows: list[tuple], id_columns: int, header_row: int = 2
) -> pd.DataFrame:
    """Reshape a "labels then one column per year" sheet into long form."""
    header = rows[header_row]
    years = {
        index: int(str(value)[:4])
        for index, value in enumerate(header)
        if value is not None
        and re.fullmatch(r"\s*(19|20)\d{2}\s*", str(value))
    }
    records = []
    for row in rows[header_row + 1 :]:
        labels = [row[i] for i in range(id_columns)]
        if all(label is None for label in labels):
            continue
        for index, year in years.items():
            value = to_number(row[index])
            if value is None:
                continue
            records.append({"labels": labels, "year": year, "value": value})
    return pd.DataFrame(records)


SECTION_15_SHEETS = {
    "15.1": ("Domestic", "Sector", "attrition_rate"),
    "15.2": ("Overseas", "Provider", "attrition_rate"),
    "15.3": ("All", "Provider", "attrition_rate"),
    "15.4": ("Domestic", "Sector", "retention_rate"),
    "15.5": ("Overseas", "Provider", "retention_rate"),
    "15.6": ("Provider", "Provider", "retention_rate"),
    "15.7": ("Domestic", "Sector", "success_rate"),
    "15.8": ("Overseas", "Provider", "success_rate"),
    "15.9": ("All", "Provider", "success_rate"),
}
SECTION_15_SHEETS["15.6"] = ("All", "Provider", "retention_rate")


def clean_attrition(path: str | Path) -> pd.DataFrame:
    """Section 15 -> student_attrition_retention_success.

    Nine sheets, each a state/institution by year matrix carrying one metric for
    one student group. Attrition and retention are published on two different
    bases: "sector" counts a student as retained anywhere in the system,
    "provider" only at the same institution. The basis is fixed by the student
    group — domestic on a sector basis, overseas and all on a provider basis —
    so the three metrics share one row.
    """
    records: list[pd.DataFrame] = []
    for sheet, (group, basis, metric) in SECTION_15_SHEETS.items():
        long = _year_matrix(_sheet_rows(path, sheet), id_columns=2)
        if long.empty:
            continue
        long["state_abbreviation"] = [
            STATE_ABBREVIATION.get(str(label[0]).strip())
            for label in long["labels"]
        ]
        long["institution_id"] = [
            _institution_key(label[1]) for label in long["labels"]
        ]
        long["student_group"] = group
        long["rate_basis"] = basis
        long["metric"] = metric
        records.append(long.drop(columns=["labels"]))

    keys = [
        "year",
        "institution_id",
        "state_abbreviation",
        "student_group",
        "rate_basis",
    ]
    long = pd.concat(records, ignore_index=True)
    wide = _pivot_keeping_nulls(long, keys, "metric", "value")
    for column in ("attrition_rate", "retention_rate", "success_rate"):
        if column not in wide.columns:
            wide[column] = pd.NA
    wide["year"] = wide["year"].astype("Int64")
    metrics = ["attrition_rate", "retention_rate", "success_rate"]
    # pivot_table materialises the full student_group x rate_basis cartesian;
    # only the combinations the source actually publishes carry values.
    wide = wide.dropna(subset=metrics, how="all")
    return wide[[*keys, *metrics]].sort_values(keys).reset_index(drop=True)


# A visible string, not a control character: pandas normalises "\x00" to the
# empty string inside `Series.where`, so the sentinel never round-trips and the
# nulls come back as "" instead of NA.
SENTINEL = "__BD_NULL__"


def _pivot_keeping_nulls(
    long: pd.DataFrame, keys: list[str], measure_column: str, value_column: str
) -> pd.DataFrame:
    """Pivot long -> wide without silently dropping rows that have null keys.

    ``pivot_table`` drops any row whose index contains NaN, which would discard
    every national-total row (null institution) and every group with no
    statistical vintage. Substitute a sentinel for the pivot and restore it
    afterwards.
    """
    frame = long.copy()
    for key in keys:
        frame[key] = (
            frame[key].astype("object").where(frame[key].notna(), SENTINEL)
        )
    wide = frame.pivot_table(
        index=keys, columns=measure_column, values=value_column, aggfunc="mean"
    ).reset_index()
    wide.columns.name = None
    for key in keys:
        wide.loc[wide[key] == SENTINEL, key] = None
    return wide


def parse_equity_label(label: object) -> tuple[str, str | None, str]:
    """Split a published equity-group label into its three components.

    The source encodes three things in one string: the group, whether it was
    measured on the student's term address or first address, and — in Section 11
    — which SEIFA or ASGS vintage delimited it. For example "First Address Low
    SES by SA1 (2021 SEIFA)(5.04)" is the low-SES group, first-address basis,
    2021 SEIFA. Series measured under different vintages are not comparable, so
    the vintage has to survive as its own column.
    """
    text = LABEL_MARKERS.sub("", str(label)).strip()
    if re.fullmatch(r"Total", text, re.IGNORECASE):
        text = "All Domestic Students"
    classification = None
    vintage = re.search(
        r"\((\d{4}\s+(?:SEIFA|ASGS|MCEETYA|Census))\)", text, re.IGNORECASE
    )
    if vintage:
        classification = re.sub(r"\s+", " ", vintage.group(1)).strip()
        text = text[: vintage.start()] + text[vintage.end() :]
    text = re.sub(r"\s+", " ", text).strip()

    address_basis = "Term address"
    if re.match(r"^first address\b", text, re.IGNORECASE):
        address_basis = "First address"
        text = re.sub(r"^first address\s*", "", text, flags=re.IGNORECASE)

    return re.sub(r"\s+", " ", text).strip(), classification, address_basis


def _forward_fill(row: tuple) -> list[str | None]:
    filled: list[str | None] = []
    current: str | None = None
    for value in row:
        text = None if value is None else str(value).strip()
        if text:
            current = text
        filled.append(current)
    return filled


SECTION_16_SHEETS = {
    "16.1": ("Domestic students", "commencing_students"),
    "16.2": ("Domestic undergraduate students", "commencing_students"),
    "16.3": ("Domestic students", "access_rate"),
    "16.4": ("Domestic undergraduate students", "access_rate"),
    "16.5": ("Domestic students", "enrolments"),
    "16.6": ("Domestic students", "participation_rate"),
    "16.7": ("Domestic students", "participation_ratio"),
    "16.8": ("Domestic students", "retention_rate"),
    "16.9": ("Domestic students", "retention_ratio"),
    "16.10": ("Domestic students", "success_rate"),
    "16.11": ("Domestic students", "success_ratio"),
    "16.12": ("Domestic students", "award_course_completions"),
    "16.13": ("Domestic students", "attainment_rate"),
}

EQUITY_MEASURES = [
    "commencing_students",
    "enrolments",
    "award_course_completions",
    "access_rate",
    "participation_rate",
    "participation_ratio",
    "retention_rate",
    "retention_ratio",
    "success_rate",
    "success_ratio",
    "attainment_rate",
]


def clean_equity_performance(path: str | Path) -> pd.DataFrame:
    """Section 16 -> student_equity_performance.

    Thirteen sheets share one shape: two label columns (state, institution) then
    a two-level header of equity group over year. Table 16.14, the equity
    reference values, is *not* read here — it is published on census-vintage
    qualified columns ("2016 (2011 Census)" beside "2016 (2016 Census)") and
    does not fit this grid.
    """
    records: list[dict[str, object]] = []
    for sheet, (group, measure) in SECTION_16_SHEETS.items():
        rows = _sheet_rows(path, sheet)
        bands = _forward_fill(rows[2])
        header = rows[3]
        columns = {}
        for index, value in enumerate(header):
            if value is None or not re.fullmatch(
                r"\s*(19|20)\d{2}\s*", str(value)
            ):
                continue
            band = bands[index]
            if band is None:
                continue
            columns[index] = (int(str(value).strip()), band)

        for row in rows[4:]:
            if row[0] is None and row[1] is None:
                continue
            state = (
                STATE_ABBREVIATION.get(str(row[0]).strip()) if row[0] else None
            )
            institution = _institution_key(row[1])
            for index, (year, band) in columns.items():
                value = to_number(row[index])
                if value is None:
                    continue
                equity, _classification, basis = parse_equity_label(band)
                records.append(
                    {
                        "year": year,
                        "institution_id": institution,
                        "state_abbreviation": state,
                        "student_group": group,
                        "equity_group": equity,
                        "address_basis": basis,
                        "equity_group_label": band.strip(),
                        "measure": measure,
                        "value": value,
                    }
                )

    long = pd.DataFrame(records)
    # The same group is footnoted differently from sheet to sheet ("All
    # Domestic(2.05)(4.02)" in one table, "All Domestic(2.05)" in another), so
    # the raw label cannot be part of the key or one group splits across rows.
    # Key on the parsed triple and keep the first label seen as provenance.
    keys = [
        "year",
        "institution_id",
        "state_abbreviation",
        "student_group",
        "equity_group",
        "address_basis",
    ]
    labels = long.groupby(
        ["equity_group", "address_basis"],
        dropna=False,
        as_index=False,
    )["equity_group_label"].first()
    wide = _pivot_keeping_nulls(long, keys, "measure", "value")
    for column in EQUITY_MEASURES:
        if column not in wide.columns:
            wide[column] = pd.NA
    wide = wide.merge(labels, on=["equity_group", "address_basis"], how="left")
    keys = [*keys, "equity_group_label"]
    wide["year"] = wide["year"].astype("Int64")
    for column in (
        "commencing_students",
        "enrolments",
        "award_course_completions",
    ):
        wide[column] = (
            pd.to_numeric(wide[column], errors="coerce")
            .round()
            .astype("Int64")
        )
    for column in set(EQUITY_MEASURES) - {
        "commencing_students",
        "enrolments",
        "award_course_completions",
    }:
        wide[column] = pd.to_numeric(wide[column], errors="coerce").astype(
            "Float64"
        )
    return (
        wide[keys + EQUITY_MEASURES].sort_values(keys).reset_index(drop=True)
    )


SECTION_11_NATIONAL = {
    "11.1": "All domestic students",
    "11.2": "Commencing domestic students",
    "11.3": "All domestic undergraduate students",
    "11.4": "Commencing domestic undergraduate students",
}
SECTION_11_INSTITUTION = {
    "11.5": "Commencing domestic students",
    "11.6": "Commencing domestic undergraduate students",
    "11.7": "All domestic students",
    "11.8": "All domestic undergraduate students",
}


def _title_year(rows: list[tuple]) -> int | None:
    title = " ".join(str(value) for value in rows[1] if value is not None)
    years = re.findall(r"(19|20)\d{2}", title)
    return int(re.findall(r"((?:19|20)\d{2})", title)[-1]) if years else None


def clean_equity_group(path: str | Path) -> pd.DataFrame:
    """Section 11 -> student_equity_group.

    Two shapes share the section. Tables 11.1-11.4 are national back-series:
    equity group down the side, one column per year from 2011. Tables 11.5-11.8
    give the institution detail for the publication year only, with equity
    groups across the top.

    A zero in the national series usually means "this group was not measured
    under that classification vintage", not that no student belonged to it —
    the source publishes every SEIFA and ASGS vintage as its own row and pads
    the years outside each vintage's life with zeros. Those become nulls;
    genuine zeros inside a vintage's own span are kept. Table 11.9
    (intersectionality) has a different grain and is not read here.
    """
    records: list[dict[str, object]] = []

    for sheet, group in SECTION_11_NATIONAL.items():
        rows = _sheet_rows(path, sheet)
        header = rows[2]
        years = {
            index: int(str(value).strip())
            for index, value in enumerate(header)
            if value is not None
            and re.fullmatch(r"\s*(19|20)\d{2}\s*", str(value))
        }
        for row in rows[3:]:
            if row[0] is None or not str(row[0]).strip():
                continue
            equity, classification, basis = parse_equity_label(row[0])
            for index, year in years.items():
                records.append(
                    {
                        "year": year,
                        "institution_id": None,
                        "state_abbreviation": "AUS",
                        "student_group": group,
                        "equity_group": equity,
                        "equity_group_classification": classification,
                        "address_basis": basis,
                        "equity_group_label": str(row[0]).strip(),
                        "students": to_number(row[index]),
                    }
                )

    for sheet, group in SECTION_11_INSTITUTION.items():
        rows = _sheet_rows(path, sheet)
        year = _title_year(rows)
        header = rows[3]
        columns = {}
        for index, value in enumerate(header[2:], start=2):
            if value is None:
                continue
            text = str(value).strip()
            # The trailing "Total <previous year>" column is a comparison, not a
            # group of the reference year.
            if not text or re.match(
                r"^Total\s+(19|20)\d{2}$", text, re.IGNORECASE
            ):
                continue
            columns[index] = text
        for row in rows[4:]:
            if row[0] is None and row[1] is None:
                continue
            first = str(row[0]).strip() if row[0] else ""
            if NON_DATA_ROW.match(first):
                continue
            # The grand total sits in the state column with no institution.
            if TOTAL_ROW.match(LABEL_MARKERS.sub("", first).strip()):
                state, institution = "AUS", None
            else:
                state = STATE_ABBREVIATION.get(first) if first else None
                institution = _institution_key(row[1])
            for index, label in columns.items():
                equity, classification, basis = parse_equity_label(label)
                records.append(
                    {
                        "year": year,
                        "institution_id": institution,
                        "state_abbreviation": state,
                        "student_group": group,
                        "equity_group": equity,
                        "equity_group_classification": classification,
                        "address_basis": basis,
                        "equity_group_label": label,
                        "students": to_number(row[index]),
                    }
                )

    frame = pd.DataFrame(records)
    keys = [
        "year",
        "institution_id",
        "state_abbreviation",
        "student_group",
        "equity_group",
        "equity_group_classification",
        "address_basis",
    ]
    labels = frame.groupby(
        ["equity_group", "equity_group_classification", "address_basis"],
        dropna=False,
        as_index=False,
    )["equity_group_label"].first()
    frame = frame.drop(columns=["equity_group_label"])
    frame = frame.groupby(keys, dropna=False, as_index=False)["students"].max()

    # A vintage-specific series is padded with zeros outside the years it was
    # actually measured. Null those, keeping zeros that sit inside the span.
    measured = frame[frame["students"].fillna(0) > 0]
    span = measured.groupby(
        ["equity_group", "equity_group_classification", "address_basis"],
        dropna=False,
    )["year"].agg(["min", "max"])
    frame = frame.merge(
        span,
        left_on=[
            "equity_group",
            "equity_group_classification",
            "address_basis",
        ],
        right_index=True,
        how="left",
    )
    outside = (frame["year"] < frame["min"]) | (frame["year"] > frame["max"])
    frame.loc[outside, "students"] = pd.NA
    frame = frame.drop(columns=["min", "max"])

    frame = frame.merge(
        labels,
        on=["equity_group", "equity_group_classification", "address_basis"],
        how="left",
    )
    frame["year"] = frame["year"].astype("Int64")
    frame["students"] = (
        pd.to_numeric(frame["students"], errors="coerce")
        .round()
        .astype("Int64")
    )
    return (
        frame[[*keys, "equity_group_label", "students"]]
        .dropna(subset=["students"])
        .sort_values(keys)
        .reset_index(drop=True)
    )


DURATION_YEARS = {"four years": 4, "six years": 6, "nine years": 9}

SECTION_17_SHEETS = {
    "17.1": ("Table A and B providers", False),
    "17.2": ("Non-university higher education institutions", False),
    "17.3": ("Table A and B providers", True),
}

COMPLETION_MEASURES = [
    "completed_rate",
    "still_enrolled_rate",
    "re_enrolled_dropped_out_rate",
    "never_returned_rate",
]


def clean_completion_rate(path: str | Path) -> pd.DataFrame:
    """Section 17 -> student_completion_rate.

    Three sheets share one layout: two label columns, then the cohort's
    duration and window, then the four outcomes. In 17.1 and 17.2 the labels are
    a demographic breakdown (gender, SES, basis of admission, field of
    education); in 17.3 they are state and institution. The four outcomes are
    exhaustive and sum to about 100 per cent.
    """
    records: list[dict[str, object]] = []
    for sheet, (provider_group, by_institution) in SECTION_17_SHEETS.items():
        for row in _sheet_rows(path, sheet)[3:]:
            if row[0] is None or row[3] is None:
                continue
            first = str(row[0]).strip()
            if NON_DATA_ROW.match(first):
                continue
            window = re.match(
                r"\s*((?:19|20)\d{2})\s*-\s*((?:19|20)\d{2})", str(row[3])
            )
            if not window:
                continue
            duration = (
                DURATION_YEARS.get(str(row[2]).strip().lower())
                if row[2]
                else None
            )

            record: dict[str, object] = {
                "cohort_start_year": int(window.group(1)),
                "cohort_end_year": int(window.group(2)),
                "tracking_years": duration,
                "provider_group": provider_group,
                "institution_id": None,
                "state_abbreviation": None,
                "dimension": None,
                "dimension_value": None,
            }
            if by_institution:
                record["state_abbreviation"] = (
                    "AUS"
                    if TOTAL_ROW.match(first)
                    else STATE_ABBREVIATION.get(first)
                )
                record["institution_id"] = _institution_key(row[1])
                record["dimension"] = "Institution"
                name, _ = split_institution_label(row[1])
                record["dimension_value"] = name
            else:
                record["state_abbreviation"] = "AUS"
                record["dimension"] = LABEL_MARKERS.sub("", first).strip()
                record["dimension_value"] = (
                    LABEL_MARKERS.sub("", str(row[1])).strip()
                    if row[1]
                    else None
                )

            for offset, measure in enumerate(COMPLETION_MEASURES, start=4):
                record[measure] = to_number(row[offset])
            records.append(record)

    frame = pd.DataFrame(records)
    for column in ("cohort_start_year", "cohort_end_year", "tracking_years"):
        frame[column] = frame[column].astype("Int64")
    for column in COMPLETION_MEASURES:
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype(
            "Float64"
        )
    keys = [
        "cohort_start_year",
        "cohort_end_year",
        "tracking_years",
        "institution_id",
        "state_abbreviation",
        "provider_group",
        "dimension",
        "dimension_value",
    ]
    return (
        frame.dropna(subset=COMPLETION_MEASURES, how="all")
        .drop_duplicates(subset=keys)
        .sort_values(keys)
        .reset_index(drop=True)[[*keys, *COMPLETION_MEASURES]]
    )


UAO_MEASURE = {
    "applicants": "applicants",
    "applications": "applicants",
    "offers": "offers",
    "offer rate": "offer_rate",
    "acceptances": "acceptances",
}
UAO_MEASURES = ["applicants", "offers", "offer_rate", "acceptances"]


def _uao_year_columns(header: tuple) -> dict[int, tuple[int, str]]:
    """Map column index to (year, series) for an appendix year header.

    The department republished 2019-2021 on a revised basis and prints the
    revised columns as "2019a", "2020a", "2021a" beside the originals. Both must
    survive, distinguished, because they are not chainable.
    """
    columns = {}
    for index, value in enumerate(header):
        if value is None:
            continue
        match = re.fullmatch(r"\s*((?:19|20)\d{2})\s*([a-z]?)\s*", str(value))
        if not match:
            continue
        columns[index] = (
            int(match.group(1)),
            "Revised" if match.group(2) else "Original",
        )
    return columns


def _uao_band_table(
    rows: list[tuple], header_row: int, measures_start: int
) -> list[dict[str, object]]:
    """Read an appendix where measure rows sit under a state band row."""
    columns = _uao_year_columns(rows[header_row])
    records: list[dict[str, object]] = []
    state: str | None = None
    state_name: str | None = None
    for row in rows[measures_start:]:
        label = str(row[0]).strip() if row[0] else ""
        if not label:
            # A band row names the state in some later cell and carries no
            # numbers; a measure row always has its label in the first cell.
            text = next(
                (
                    value.strip()
                    for value in row[1:]
                    if isinstance(value, str) and value.strip()
                ),
                None,
            )
            if text and text in STATE_ABBREVIATION:
                state, state_name = STATE_ABBREVIATION[text], text
            continue
        if NON_DATA_ROW.match(label):
            continue
        measure = UAO_MEASURE.get(label.lower())
        if measure is None:
            continue
        for index, (year, series) in columns.items():
            value = to_number(row[index])
            if value is None:
                continue
            if measure == "offer_rate" and value <= 1.5:
                value *= 100
            records.append(
                {
                    "year": year,
                    "series": series,
                    "state_abbreviation": state,
                    "dimension": "State",
                    "dimension_value": state_name,
                    "application_source": "Combined",
                    "measure": measure,
                    "value": value,
                }
            )
    return records


def clean_application_offer(
    appendices_2025: str | Path, appendices_2021: str | Path
) -> pd.DataFrame:
    """Undergraduate applications appendices -> application_offer.

    Table A1 carries the headline state series from 2010. Acceptances stopped
    being published after the 2021 round, so they are read from the 2021
    appendices and only from its unsuffixed columns: that vintage uses the "a"
    and "b" suffixes for a different pair of revisions than the current one, and
    mixing the two conventions would mislabel the series.
    """
    records = _uao_band_table(_sheet_rows(appendices_2025, "Table A1"), 2, 3)

    for row in _uao_band_table(_sheet_rows(appendices_2021, "Table A1"), 2, 3):
        if row["measure"] == "acceptances" and row["series"] == "Original":
            records.append(row)

    long = pd.DataFrame(records)
    keys = [
        "year",
        "series",
        "state_abbreviation",
        "dimension",
        "dimension_value",
        "application_source",
    ]
    wide = _pivot_keeping_nulls(long, keys, "measure", "value")
    for column in UAO_MEASURES:
        if column not in wide.columns:
            wide[column] = pd.NA
    wide["year"] = wide["year"].astype("Int64")
    for column in ("applicants", "offers", "acceptances"):
        wide[column] = (
            pd.to_numeric(wide[column], errors="coerce")
            .round()
            .astype("Int64")
        )
    wide["offer_rate"] = pd.to_numeric(
        wide["offer_rate"], errors="coerce"
    ).astype("Float64")
    return (
        wide[[*keys, *UAO_MEASURES]].sort_values(keys).reset_index(drop=True)
    )


def clean_equity_reference_value(path: str | Path) -> pd.DataFrame:
    """Section 16.14 -> equity_reference_value.

    The denominator behind the participation, retention and success *ratios* in
    ``student_equity_performance``: each group's share of the reference
    population. It does not fit that table's grid, because the source publishes
    two values for a census year — one on the previous census basis and one on
    the new one ("2016 (2011 Census)" beside "2016 (2016 Census)") — and picking
    between them would be a guess. The census basis is kept as its own column
    instead, so the table stays lossless and the join is explicit.

    Layout: a state band row spanning sixteen columns each, a year header row in
    which census years carry their basis in brackets, and one row per equity
    group.
    """
    rows = _sheet_rows(path, "16.14")
    states = _forward_fill(rows[3])
    columns: dict[int, tuple[int, str | None, str]] = {}
    for index, value in enumerate(rows[4]):
        if value is None:
            continue
        text = re.sub(r"\s+", " ", str(value)).strip()
        match = re.match(
            r"^((?:19|20)\d{2})(?:\s*\((\d{4}\s*Census)\))?$", text
        )
        state = states[index]
        if not match or state is None:
            continue
        census = (
            re.sub(r"\s+", " ", match.group(2)).strip()
            if match.group(2)
            else None
        )
        abbreviation = STATE_ABBREVIATION.get(state)
        if abbreviation is None:
            continue
        columns[index] = (int(match.group(1)), census, abbreviation)

    records = []
    for row in rows[5:]:
        if row[0] is None:
            continue
        label = str(row[0]).strip()
        if NON_DATA_ROW.match(label) or label.lower().startswith(
            "methodology"
        ):
            continue
        equity, _classification, _basis = parse_equity_label(label)
        for index, (year, census, abbreviation) in columns.items():
            value = to_number(row[index])
            if value is None:
                continue
            records.append(
                {
                    "year": year,
                    "state_abbreviation": abbreviation,
                    "equity_group": equity,
                    "census_basis": census,
                    "reference_value": value,
                }
            )

    frame = pd.DataFrame(records)
    keys = ["year", "state_abbreviation", "equity_group", "census_basis"]
    frame["year"] = frame["year"].astype("Int64")
    frame["reference_value"] = pd.to_numeric(
        frame["reference_value"], errors="coerce"
    ).astype("Float64")
    return (
        frame.drop_duplicates(subset=keys)
        .sort_values(keys)
        .reset_index(drop=True)
    )


# ---------------------------------------------------------------------------
# Source discovery and download
#
# Nothing about the download URLs is stable: the department assigns each file
# an opaque ``/download/<nid>/<slug>/<fid>/document/xlsx`` path that changes
# every release. What *is* stable is the resource slug, which carries the year.
# So discovery walks landing page -> resource slug -> resource page -> href.
# ---------------------------------------------------------------------------

RESOURCE_HREF = re.compile(
    r"/higher-education-statistics/resources/([a-z0-9-]+)"
)
DOWNLOAD_HREF = re.compile(r"/download/\d+/[a-z0-9-]+/\d+/document/[a-z]+")
STAFF_YEAR_PAGE = re.compile(
    r"selected-higher-education-statistics-(\d{4})-staff-data"
)


def build_session() -> object:
    """A session that retries: the site is slow and intermittently stalls.

    Read timeouts, not refusals, are the observed failure, so a retry on a
    fresh connection is the right response. Connect and read timeouts are set
    separately because a stalled read needs a long leash while a dead host
    should fail fast.
    """
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_text(url: str, session: object | None = None) -> str:
    """GET a page as text, with the browser headers the site demands."""
    from pipelines.datasets.au_doe_higher_education.constants import constants

    getter = session if session is not None else build_session()
    response = getter.get(  # type: ignore[union-attr]
        url, headers=constants.HEADERS.value, timeout=(30, 300)
    )
    response.raise_for_status()
    return response.text


def resource_slugs(html: str) -> set[str]:
    """Every ``/resources/<slug>`` linked from a page."""
    return set(RESOURCE_HREF.findall(html))


def newest_slug(slugs: set[str], pattern: str) -> tuple[str, int] | None:
    """The slug matching ``pattern`` with the highest year, and that year."""
    matches = []
    for slug in slugs:
        found = re.match(pattern, slug)
        if found:
            matches.append((slug, int(found.group(1))))
    if not matches:
        return None
    return max(matches, key=lambda pair: pair[1])


def resolve_download_url(slug: str, session: object | None = None) -> str:
    """Resource slug -> absolute download URL for its document."""
    from pipelines.datasets.au_doe_higher_education.constants import constants

    base = constants.BASE_URL.value
    html = fetch_text(
        f"{base}/higher-education-statistics/resources/{slug}", session
    )
    href = DOWNLOAD_HREF.search(html)
    if not href:
        raise ValueError(f"no download link on resource page: {slug}")
    return f"{base}{href.group(0)}"


def discover_sources(session: object | None = None) -> dict[str, dict]:
    """Locate the newest release of every document the build needs.

    Returns ``{local_name: {"slug", "year", "url"}}``. Staff resources live on
    a per-year sub-page rather than the landing page, so that page is resolved
    first.
    """
    from pipelines.datasets.au_doe_higher_education.constants import constants

    base = constants.BASE_URL.value
    slugs: set[str] = set()
    for page in (
        constants.STUDENT_PAGE.value,
        constants.STAFF_PAGE.value,
        constants.UAO_PAGE.value,
    ):
        slugs |= resource_slugs(fetch_text(f"{base}{page}", session))

    staff_html = fetch_text(f"{base}{constants.STAFF_PAGE.value}", session)
    staff_years = [int(year) for year in STAFF_YEAR_PAGE.findall(staff_html)]
    if staff_years:
        slugs |= resource_slugs(
            fetch_text(
                f"{base}{constants.STAFF_PAGE.value}"
                f"/selected-higher-education-statistics-{max(staff_years)}-staff-data",
                session,
            )
        )

    found: dict[str, dict] = {}
    for name, pattern in constants.RESOURCES.value.items():
        newest = newest_slug(slugs, pattern)
        if newest is None:
            continue
        slug, year = newest
        found[name] = {"slug": slug, "year": year}
    return found


def source_max_year(sources: dict[str, dict]) -> int:
    """The newest reference year across the discovered documents."""
    return max(entry["year"] for entry in sources.values())


def download_sources(
    input_dir: str | Path, sources: dict[str, dict] | None = None
) -> dict[str, Path]:
    """Download each discovered document to ``<input_dir>/<name>.xlsx``.

    The local names are stable and year-free so the build does not have to
    know which release it is reading.
    """
    from pipelines.datasets.au_doe_higher_education.constants import constants

    target = Path(input_dir)
    target.mkdir(parents=True, exist_ok=True)
    session = build_session()
    if sources is None:
        sources = discover_sources(session)

    written: dict[str, Path] = {}
    for name, entry in sources.items():
        url = entry.get("url") or resolve_download_url(entry["slug"], session)
        path = target / f"{name}.xlsx"
        with session.get(  # type: ignore[union-attr]
            url,
            headers=constants.HEADERS.value,
            stream=True,
            timeout=(30, 600),
        ) as response:
            response.raise_for_status()
            # decode_content matters: without it a Brotli/gzip response is
            # written to disk still encoded.
            response.raw.decode_content = True
            with path.open("wb") as handle:
                shutil.copyfileobj(response.raw, handle)
        written[name] = path
        print(
            f"downloaded {name:20} {entry['slug']:55} {path.stat().st_size:>12,} B"
        )
    return written


# ---------------------------------------------------------------------------
# The build, shared by the one-shot bootstrap and the recurring flow
# ---------------------------------------------------------------------------

DIMENSIONS = ["year", "institution_id", "state_abbreviation"]

CUBES: dict[str, tuple[str, dict[str, str], list[str]]] = {
    "student_enrolment": (
        "enrol",
        {"enrolments": "INT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "commencing",
            "course_level_broad",
            "course_level_detailed",
            "gender",
            "attendance_mode",
            "attendance_type",
            "special_course",
            "field_of_education_primary",
            "field_of_education_secondary",
        ],
    ),
    "student_load": (
        "load",
        {"student_load_eftsl": "FLOAT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "commencing",
            "course_level_broad",
            "course_level_detailed",
            "discipline",
            "gender",
            "liability_status",
        ],
    ),
    "award_course_completion": (
        "compl",
        {"completions": "INT64"},
        [
            *DIMENSIONS,
            "citizenship",
            "course_level_broad",
            "course_level_detailed",
            "gender",
            "attendance_mode",
            "attendance_type",
            "special_course",
            "field_of_education_primary",
            "field_of_education_secondary",
        ],
    ),
    "staff": (
        "staff",
        {"staff_headcount": "INT64", "staff_fte": "FLOAT64"},
        [
            *DIMENSIONS,
            "gender",
            "duties_classification",
            "function",
            "organisational_unit",
            "work_contract",
        ],
    ),
}


def write_partitioned(
    frame: pd.DataFrame,
    output_dir: str | Path,
    table: str,
    partition: str = "year",
) -> int:
    """Write hive-partitioned Parquet with every column a string.

    Staging is all-STRING by house convention and the dbt model ``safe_cast``s
    each column to its architecture type. Casting through arrow rather than
    ``astype(str)`` matters twice over: ``astype(str)`` renders null as the
    literal "nan", which ``safe_cast`` will not turn back into NULL, and it
    would render an Int64 year as "2024.0".
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    target = Path(output_dir) / table
    frame = frame.copy()
    for column in frame.columns:
        if column == partition:
            continue
        values = frame[column]
        if (
            pd.api.types.is_float_dtype(values)
            or str(values.dtype) == "Float64"
        ):
            frame[column] = values.map(
                lambda value: None if pd.isna(value) else repr(float(value))
            )
        elif str(values.dtype) in ("Int64", "int64"):
            frame[column] = values.map(
                lambda value: None if pd.isna(value) else str(int(value))
            )
        else:
            objects = values.astype("object")
            objects[values.isna()] = None
            frame[column] = objects

    schema = pa.schema(
        [
            (name, pa.int64() if name == partition else pa.string())
            for name in frame.columns
        ]
    )
    pq.write_to_dataset(
        pa.Table.from_pandas(frame, schema=schema, preserve_index=False),
        root_path=str(target),
        partition_cols=[partition],
        compression="snappy",
        existing_data_behavior="delete_matching",
    )
    return len(frame)


def build_all(input_dir: str | Path) -> dict[str, pd.DataFrame]:
    """Build every table from the workbooks in ``input_dir``.

    Cube tables stack whatever vintages are present: the bootstrap holds
    several (``enrol_v2020.xlsx`` ... ``enrol_v2024.xlsx``) and reaches back to
    2016, while a scheduled run downloads only the current release and
    therefore rebuilds only that release's window.
    """
    source = Path(input_dir)
    built: dict[str, pd.DataFrame] = {}

    for table, (prefix, measures, dimensions) in CUBES.items():
        paths = sorted(source.glob(f"{prefix}_v*.xlsx")) or sorted(
            source.glob(f"{prefix}.xlsx")
        )
        if not paths:
            raise FileNotFoundError(f"no workbook for {table} in {source}")
        built[table] = stack_vintages(paths, measures, dimensions)

    built["student_equity_group"] = clean_equity_group(
        source / "sec11_equity.xlsx"
    )
    built["student_equity_performance"] = clean_equity_performance(
        source / "sec16_equityperf.xlsx"
    )
    built["equity_reference_value"] = clean_equity_reference_value(
        source / "sec16_equityperf.xlsx"
    )
    built["student_attrition_retention_success"] = clean_attrition(
        source / "sec15_attrition.xlsx"
    )
    built["student_completion_rate"] = clean_completion_rate(
        source / "sec17_complrate.xlsx"
    )
    # Order is load-bearing: the current appendices supply the headline
    # series, the 2021 file only the acceptances the source stopped
    # publishing after that round. Passing them the other way round mislabels
    # the revised series.
    current = source / "uao_current.xlsx"
    legacy = source / "uao_2021.xlsx"
    if not current.exists():
        vintages = sorted(source.glob("uao_*appendices.xlsx"))
        current, legacy = vintages[-1], vintages[0]
    built["application_offer"] = clean_application_offer(current, legacy)
    return built


def observed_institutions(built: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Every institution appearing in any built table, with its state."""
    observed = pd.concat(
        [
            frame[["institution_id", "state_abbreviation"]]
            for frame in built.values()
            if "institution_id" in frame.columns
        ]
    ).dropna(subset=["institution_id"])
    observed = observed.drop_duplicates("institution_id")
    observed["institution_name"] = (
        observed["institution_id"].str.replace("_", " ").str.title()
    )
    return observed


def merge_institution_directory(
    existing: pd.DataFrame,
    observed: pd.DataFrame,
    codes: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Extend the published directory with institutions seen for the first time.

    A scheduled run rebuilds only the current release's window, so the
    institutions it observes are a subset of those the directory already holds.
    Replacing the directory with that subset would orphan every foreign key in
    the older partitions, so existing rows always survive and only genuinely
    new institutions are appended.
    """
    known = set(existing["id_higher_education_institution"])
    fresh = observed[~observed["institution_id"].isin(known)]
    if fresh.empty:
        return existing.copy()

    codes = codes or {}
    added = pd.DataFrame(
        {
            "id_higher_education_institution": fresh["institution_id"],
            "name": fresh["institution_name"],
            "state_abbreviation": fresh["state_abbreviation"],
            "provider_category": None,
            "is_aggregate": "no",
            "provider_code": fresh["institution_id"].map(codes),
        }
    )
    for column in existing.columns:
        if column not in added.columns:
            added[column] = None
    return pd.concat([existing, added[existing.columns]], ignore_index=True)


def refreshed_partitions(
    built: dict[str, pd.DataFrame], partition_override: dict[str, str]
) -> dict[str, list[str]]:
    """The partition values each rebuilt table covers.

    Only these are replaced in staging. Everything older stays, which is what
    keeps 2016-2019 alive once the department delists those vintages.
    """
    covered: dict[str, list[str]] = {}
    for table, frame in built.items():
        column = partition_override.get(table, "year")
        if column not in frame.columns:
            continue
        values = frame[column].dropna().unique()
        covered[table] = sorted(str(int(value)) for value in values)
    return covered
