"""Constants for the au_doe_higher_education recurring pipeline."""

from enum import Enum


class constants(Enum):
    """Source locations, table list and partitioning for the annual refresh."""

    DATASET_ID = "au_doe_higher_education"
    DIRECTORY_DATASET_ID = "br_bd_diretorios_au"
    DIRECTORY_TABLE_ID = "higher_education_institution"

    BASE_URL = "https://www.education.gov.au"

    #: The three collection landing pages. Student and undergraduate resources
    #: are linked from the landing page itself; staff data is one level deeper,
    #: under a per-year sub-page (``selected-...-2025-staff-data``).
    STUDENT_PAGE = "/higher-education-statistics/student-data"
    STAFF_PAGE = "/higher-education-statistics/staff-data"
    UAO_PAGE = (
        "/higher-education-statistics/"
        "undergraduate-applications-offers-and-acceptances-publications"
    )

    #: The site serves 403 to an unadorned client.
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
        ),
        "Accept": (
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        ),
    }

    #: Local filename -> regex matching that document's resource slug. The year
    #: is group 1 and the newest match wins, so a renamed release is picked up
    #: without editing this table. The undergraduate appendices are matched
    #: loosely because the department switched from a single year
    #: (``...-2021-appendix``) to a range (``...-20222024-appendices``).
    RESOURCES = {
        "enrol": r"^perturbed-student-enrolments-pivot-table-(\d{4})$",
        "load": r"^perturbed-student-load-pivot-table-(\d{4})$",
        "compl": r"^perturbed-award-course-completions-pivot-table-(\d{4})$",
        "staff": r"^(\d{4})-staff-pivot-table$",
        "sec11_equity": r"^(\d{4})-section-11-equity-groups$",
        "sec15_attrition": r"^(\d{4})-section-15-attrition-success-and-retention$",
        "sec16_equityperf": r"^(\d{4})-section-16-equity-performance-data$",
        "sec17_complrate": r"^(\d{4})-section-17-completion-rates$",
        # The department moved from a single year ("...-2021-appendix") to a
        # range ("...-20222024-appendices"), so the year is the last four
        # digits before the suffix. The 2021 file is fetched as well and is
        # not interchangeable: acceptances were discontinued after that
        # round and are read only from it.
        "uao_current": r"^undergraduate-applications.*?(\d{4})-appendi[cx]",
        "uao_2021": r"^undergraduate-applications-offers-and-acceptances-(2021)-appendix$",
    }

    #: Tables built by the flow, in dependency order. The directory is
    #: materialised first because every other model's relationship test
    #: resolves against it.
    TABLES = [
        "student_enrolment",
        "student_load",
        "award_course_completion",
        "staff",
        "student_equity_group",
        "student_equity_performance",
        "equity_reference_value",
        "student_attrition_retention_success",
        "student_completion_rate",
        "application_offer",
    ]

    #: Tables whose partition column is not ``year``.
    PARTITION_OVERRIDE = {"student_completion_rate": "cohort_start_year"}

    #: The tables whose coverage the source publishes as a rolling window. A
    #: release carries only the last five to seven years, so a refresh must
    #: replace exactly the partitions it rebuilds and leave older ones alone.
    ROLLING_WINDOW_TABLES = [
        "student_enrolment",
        "student_load",
        "award_course_completion",
        "staff",
    ]
